import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from models.Cadastro.clientes import Clientes
from models.connection import DatabaseConnection, DatabaseLoad
from sqlalchemy import MetaData, Table, select
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)


class ClientesQuery:
    def __init__(self):
        self.db = DatabaseConnection()
        self.metadata = MetaData()
        self.clientes_fornecedores = Table('CLIENTES_FORNECEDORES', self.metadata, autoload_with=self.db.engine)
        self.cidades = Table('CIDADES', self.metadata, autoload_with=self.db.engine)
        self.db_load = DatabaseLoad()

    def fetch_and_transform_data(self):
        logger.info("Iniciando a sessão para consulta de clientes e fornecedores")
        session = self.db.get_session()

        try:
            # Consulta da tabela CLIENTES_FORNECEDORES com filtro CLI_FOR_AMBOS
            logger.info("Executando consulta para clientes e fornecedores com status 'C' e 'A'")
            query_clientes_fornecedores = select(
                self.clientes_fornecedores.c.COD_CLI_FOR.label('id_cliente'),
                self.clientes_fornecedores.c.RAZ_CLI_FOR.label('razao_social'),
                self.clientes_fornecedores.c.END_CLI_FOR.label('endereco'),
                self.clientes_fornecedores.c.SIG_ESTADO.label('estado'),
                self.clientes_fornecedores.c.COD_CIDADE.label('cod_cidade'),
                self.clientes_fornecedores.c.TEL_CLI_FOR.label('telefone')
            ).where(self.clientes_fornecedores.c.CLI_FOR_AMBOS.in_(["C", "A"]))

            start_time = time.time()
            result_clientes_fornecedores = session.execute(query_clientes_fornecedores).fetchall()
            elapsed_time = time.time() - start_time
            logger.info(f"Consulta completada em {elapsed_time} segundos, Total de registros encontrados: {len(result_clientes_fornecedores)}")

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

            # Criar a lista de registros de clientes
            clientes_list = []
            for row in result_clientes_fornecedores:
                nom_cidade = cidade_map.get(row.cod_cidade, "Cidade Não Encontrada")
                cliente = Clientes(
                    id_contrato=1,
                    id_cliente=row.id_cliente,
                    id_emp_cli=1,
                    razao_social=row.razao_social,
                    endereco=row.endereco,
                    estado=row.estado,
                    cidade=nom_cidade,
                    telefone=row.telefone,
                    id_cliente_completo=f"1-1-{row.id_cliente}"
                )
                clientes_list.append(cliente)
                logger.debug(f"Cliente adicionado à lista: {cliente}")

            return clientes_list

        except Exception as e:
            logger.error(f"Erro ao executar consulta: {e}, Query: {query_clientes_fornecedores}")
            raise
        finally:
            logger.info("Fechando a sessão de consulta")
            session.close()

    def process_batch(self, batch, batch_index):
        session_load = self.db_load.get_session()
        logger.info(
            f'Iniciando processamento do lote {batch_index} com '
            f'{len(batch)} itens'
        )

        try:
            start_time = time.time()

            existing_ids = [cliente.id_cliente for cliente in batch]
            existing_records = {
                v.id_cliente: v
                for v in session_load.query(Clientes)
                .filter(Clientes.id_cliente.in_(existing_ids))
                .all()
            }

            logger.info(
                f'Registros existentes carregados para o lote {batch_index}'
            )

            to_update = []
            to_add = []
            for cliente in batch:
                existing_cliente = existing_records.get(cliente.id_cliente)
                if existing_cliente:
                    if self.is_cliente_different(existing_cliente, cliente):
                        self.update_existing_cliente(existing_cliente, cliente)
                        to_update.append(existing_cliente)
                        logger.debug(f'Atualizado: {cliente}')
                else:
                    to_add.append(cliente)
                    logger.debug(f'Adicionado: {cliente}')

            if to_update:
                session_load.bulk_update_mappings(
                    Clientes,
                    [
                        {
                            'id_cliente': cliente.id_cliente,
                            'id_contrato': cliente.id_contrato,
                            'id_emp_cli': cliente.id_emp_cli,
                            'razao_social': cliente.razao_social,
                            'endereco': cliente.endereco,
                            'estado': cliente.estado,
                            'cidade': cliente.cidade,
                            'telefone': cliente.telefone,
                            'id_cliente_completo': cliente.id_cliente_completo
                        }
                        for cliente in to_update
                    ],
                )
                logger.info(
                    f'Atualizados {len(to_update)} itens no lote '
                    f'{batch_index}'
                )
            if to_add:
                session_load.bulk_save_objects(to_add)
                logger.info(
                    f'Adicionados {len(to_add)} novos itens no lote '
                    f'{batch_index}'
                )
            session_load.commit()

            total_time = time.time() - start_time
            logger.info(
                f'Tempo total para processar o lote {batch_index}: '
                f'{total_time:.2f} segundos'
            )
        except SQLAlchemyError as e:
            logger.error(f'Erro ao persistir dados no lote {batch_index}: {e}')
            session_load.rollback()
        finally:
            session_load.close()
            logger.info(
                f'Sessão de banco de dados fechada após processar o lote '
                f'{batch_index}'
            )

    @staticmethod
    def is_cliente_different(existing_cliente, cliente):
        return (
            existing_cliente.razao_social != cliente.razao_social
            or existing_cliente.endereco != cliente.endereco
            or existing_cliente.estado != cliente.estado
            or existing_cliente.cidade != cliente.cidade
            or existing_cliente.telefone != cliente.telefone
        )

    @staticmethod
    def update_existing_cliente(existing_cliente, cliente):
        existing_cliente.razao_social = cliente.razao_social
        existing_cliente.endereco = cliente.endereco
        existing_cliente.estado = cliente.estado
        existing_cliente.cidade = cliente.cidade
        existing_cliente.telefone = cliente.telefone

    def persist_data(self):
        try:
            clientes_list = self.fetch_and_transform_data()
            logger.info(
                f'Total de itens recuperados para processamento: '
                f'{len(clientes_list)}'
            )
            batch_size = 500

            with ThreadPoolExecutor(max_workers=15) as executor:
                futures = [
                    executor.submit(
                        self.process_batch,
                        clientes_list[i : i + batch_size],
                        i // batch_size + 1,
                    )
                    for i in range(0, len(clientes_list), batch_size)
                ]

                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f'Erro no processamento do lote: {e}')
        except Exception as e:
            logger.error(f'Erro não tratado em persist_data: {e}')
