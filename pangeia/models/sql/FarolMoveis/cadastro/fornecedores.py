import logging
import time

from models.Cadastro.fornecedores import Fornecedores
from models.connection import DatabaseConnection, DatabaseLoad
from sqlalchemy import MetaData, Table, select

logger = logging.getLogger(__name__)


class FornecedoresQuery:
    def __init__(self):
        self.db = DatabaseConnection()
        self.metadata = MetaData()
        self.clientes_fornecedores = Table('CLIENTES_FORNECEDORES', self.metadata, autoload_with=self.db.engine)
        self.cidades = Table('CIDADES', self.metadata, autoload_with=self.db.engine)
        self.db_load = DatabaseLoad()

    def fetch_and_transform_data(self):
        session = self.db.get_session()
        logger.info("Iniciando a sessão para consulta de fornecedores")

        try:
            # Consulta da tabela CLIENTES_FORNECEDORES com filtro CLI_FOR_AMBOS
            logger.info("Executando consulta para fornecedores com status 'F' e 'A'")
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

            # Criar a lista de registros de fornecedores
            fornecedores_list = []
            for row in result_clientes_fornecedores:
                nom_cidade = cidade_map.get(row.cod_cidade, "Cidade Não Encontrada")
                fornecedor = Fornecedores(
                    id_contrato=1,
                    id_fornecedor=row.id_fornecedor,
                    id_emp_cli=1,
                    razao_social=row.razao_social,
                    endereco=row.endereco,
                    estado=row.estado,
                    cidade=nom_cidade,
                    telefone=row.telefone
                )
                fornecedores_list.append(fornecedor)
                logger.debug(f"Fornecedor adicionado à lista: {fornecedor}")

            return fornecedores_list

        except Exception as e:
            logger.error(f"Erro ao executar consulta: {e}, Query: {query_clientes_fornecedores}")
            raise
        finally:
            logger.info("Fechando a sessão de consulta")
            session.close()

    def persist_data(self):
        session_load = self.db_load.get_session()
        logger.info("Iniciando a sessão para persistência de dados dos fornecedores")

        try:
            fornecedores_list = self.fetch_and_transform_data()

            for fornecedor in fornecedores_list:
                existing_fornecedor = session_load.query(Fornecedores).filter_by(id_fornecedor=fornecedor.id_fornecedor).first()
                if existing_fornecedor:
                    logger.info(f"Fornecedor existente encontrado com ID: {fornecedor.id_fornecedor}, verificando necessidade de atualização")
                    if (existing_fornecedor.razao_social != fornecedor.razao_social or
                        existing_fornecedor.endereco != fornecedor.endereco or
                        existing_fornecedor.estado != fornecedor.estado or
                        existing_fornecedor.cidade != fornecedor.cidade or
                        existing_fornecedor.telefone != fornecedor.telefone):
                        logger.info(f"Atualizando dados do fornecedor ID: {fornecedor.id_fornecedor}")
                        existing_fornecedor.razao_social = fornecedor.razao_social
                        existing_fornecedor.endereco = fornecedor.endereco
                        existing_fornecedor.estado = fornecedor.estado
                        existing_fornecedor.cidade = fornecedor.cidade
                        existing_fornecedor.telefone = fornecedor.telefone
                        session_load.add(existing_fornecedor)
                else:
                    logger.info(f"Adicionando novo fornecedor com ID: {fornecedor.id_fornecedor}")
                    session_load.add(fornecedor)

            session_load.commit()
            logger.info("Dados de fornecedores persistidos com sucesso!")

        except Exception as e:
            logger.error(f"Erro ao persistir dados: {e}")
            session_load.rollback()
        finally:
            logger.info("Fechando a sessão de persistência")
            session_load.close()
