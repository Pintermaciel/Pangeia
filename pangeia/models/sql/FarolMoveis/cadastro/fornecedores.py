import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from models.Cadastro.fornecedores import Fornecedores
from models.connection import DatabaseConnection, DatabaseLoad
from sqlalchemy import MetaData, Table, select
from sqlalchemy.exc import SQLAlchemyError

logging.basicConfig(
    level=logging.DEBUG, 
    format='%(asctime)s - %(levelname)s - %(message)s'
    )
logger = logging.getLogger(__name__)


class FornecedoresQuery:
    def __init__(self):
        self.db = DatabaseConnection()
        self.metadata = MetaData()
        self.clientes_fornecedores = Table(
            'CLIENTES_FORNECEDORES', self.metadata, autoload_with=self.db.engine
        )
        self.cidades = Table(
            'CIDADES', self.metadata, autoload_with=self.db.engine
        )
        self.db_load = DatabaseLoad()

    def fetch_and_transform_data(self):
        session = self.db.get_session()

        try:
            # Consulta da tabela CLIENTES_FORNECEDORES com filtro CLI_FOR_AMBOS
            query_clientes_fornecedores = select(
                self.clientes_fornecedores.c.COD_CLI_FOR.label('id_fornecedor'),
                self.clientes_fornecedores.c.RAZ_CLI_FOR.label('razao_social'),
                self.clientes_fornecedores.c.END_CLI_FOR.label('endereco'),
                self.clientes_fornecedores.c.SIG_ESTADO.label('estado'),
                self.clientes_fornecedores.c.COD_CIDADE.label('cod_cidade'),
                self.clientes_fornecedores.c.TEL_CLI_FOR.label('telefone')
            ).where(self.clientes_fornecedores.c.CLI_FOR_AMBOS.in_(["F", "A"]))

            start_time = time.time()
            result_clientes_fornecedores = session.execute(query_clientes_fornecedores).fetchall()
            elapsed_time = time.time() - start_time
            logger.info(
                f"Consulta completada em {elapsed_time:.2f} segundos, "
                f"Total de registros encontrados: {len(result_clientes_fornecedores)}"
            )

            # Obter os códigos das cidades dos resultados da consulta anterior
            cod_cidades = {row.cod_cidade for row in result_clientes_fornecedores}
            logger.info(f"Códigos de cidades obtidos para consulta subsequente: {cod_cidades}")

            # Consulta da tabela CIDADES
            query_cidades = select(
                self.cidades.c.COD_CIDADE,
                self.cidades.c.NOM_CIDADE
            ).where(self.cidades.c.COD_CIDADE.in_(cod_cidades))

            result_cidades = session.execute(query_cidades).fetchall()

            # Criar um dicionário para mapear cod_cidade para nom_cidade
            cidade_map = {row.COD_CIDADE: row.NOM_CIDADE for row in result_cidades}
            logger.info(f"Mapa de cidades criado com sucesso: {cidade_map}")

            # Criar a lista de registros de fornecedores
            fornecedores_list = [
                Fornecedores(
                    id_contrato=1,
                    id_fornecedor=row.id_fornecedor,
                    fk_contrato_fornecedor=f"1-{row.id_fornecedor}",
                    id_emp_cli=1,
                    razao_social=row.razao_social,
                    endereco=row.endereco,
                    estado=row.estado,
                    cidade=cidade_map.get(row.cod_cidade, "Cidade Não Encontrada"),
                    telefone=row.telefone
                )
                for row in result_clientes_fornecedores
            ]
            logger.info("Lista de fornecedores criada com sucesso")
            return fornecedores_list

        except Exception as e:
            logger.error(f"Erro ao executar consulta: {e}")
            raise
        finally:
            logger.info("Fechando a sessão de consulta")
            session.close()

    def process_batch(self, batch, batch_index):
        session = self.db_load.get_session()
        logger.info(
            f'Iniciando processamento do lote {batch_index} com '
            f'{len(batch)} itens'
        )

        try:
            start_time = time.time()

            existing_ids = [fornecedor.id_fornecedor for fornecedor in batch]
            existing_records = {
                v.id_fornecedor: v
                for v in session.query(Fornecedores)
                .filter(Fornecedores.id_fornecedor.in_(existing_ids))
                .all()
            }

            logger.info(
                f'Registros existentes carregados para o lote {batch_index}'
            )

            to_update = []
            to_add = []
            for fornecedor in batch:
                existing_fornecedor = existing_records.get(fornecedor.id_fornecedor)
                if existing_fornecedor:
                    if self.is_fornecedor_different(existing_fornecedor, fornecedor):
                        self.update_existing_fornecedor(existing_fornecedor, fornecedor)
                        to_update.append(fornecedor)
                else:
                    to_add.append(fornecedor)

            if to_update:
                valid_updates = [
                    {
                        'id_fornecedor': fornecedor.id_fornecedor,
                        'razao_social': fornecedor.razao_social,
                        'endereco': fornecedor.endereco,
                        'estado': fornecedor.estado,
                        'cidade': fornecedor.cidade,
                        'telefone': fornecedor.telefone,
                        'fk_contrato_fornecedor': fornecedor.fk_contrato_fornecedor,
                    }
                    for fornecedor in to_update if fornecedor.id_fornecedor is not None
                ]

                if valid_updates:
                    session.bulk_update_mappings(Fornecedores, valid_updates)
                    logger.info(
                        f'Atualizados {len(valid_updates)} itens no lote {batch_index}'
                    )
            if to_add:
                logger.info(f"Total de itens a serem adicionados no lote {batch_index}: {len(to_add)}")
                session.bulk_save_objects(to_add)
                logger.info(
                    f'Adicionados {len(to_add)} novos itens no lote '
                    f'{batch_index}'
                )
            session.commit()
            logger.info(f"Commit realizado para o lote {batch_index}")

            total_time = time.time() - start_time
            logger.info(
                f'Tempo total para processar o lote {batch_index}: '
                f'{total_time:.2f} segundos'
            )
        except SQLAlchemyError as e:
            logger.error(f'Erro ao persistir dados no lote {batch_index}: {e}')
            session.rollback()
        finally:
            session.close()
            logger.info(
                f'Sessão de banco de dados fechada após processar o lote '
                f'{batch_index}'
            )

    @staticmethod
    def is_fornecedor_different(existing_fornecedor, fornecedor):
        return (
            existing_fornecedor.razao_social != fornecedor.razao_social
            or existing_fornecedor.endereco != fornecedor.endereco
            or existing_fornecedor.estado != fornecedor.estado
            or existing_fornecedor.cidade != fornecedor.cidade
            or existing_fornecedor.telefone != fornecedor.telefone
        )

    @staticmethod
    def update_existing_fornecedor(existing_fornecedor, fornecedor):
        existing_fornecedor.razao_social = fornecedor.razao_social
        existing_fornecedor.endereco = fornecedor.endereco
        existing_fornecedor.estado = fornecedor.estado
        existing_fornecedor.cidade = fornecedor.cidade
        existing_fornecedor.telefone = fornecedor.telefone

    def persist_data(self):
        try:
            fornecedores_list = self.fetch_and_transform_data()
            logger.info(
                f'Total de itens recuperados para processamento: '
                f'{len(fornecedores_list)}'
            )
            batch_size = 500

            with ThreadPoolExecutor(max_workers=15) as executor:
                futures = [
                    executor.submit(
                        self.process_batch,
                        fornecedores_list[i: i + batch_size],
                        i // batch_size + 1,
                    )
                    for i in range(0, len(fornecedores_list), batch_size)
                ]

                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f'Erro no processamento do lote: {e}')
        except Exception as e:
            logger.error(f'Erro não tratado em persist_data: {e}')