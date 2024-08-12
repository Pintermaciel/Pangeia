import logging
import time

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
                    telefone=row.telefone
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

    def persist_data(self):
        try:
            clientes_list = self.fetch_and_transform_data()
            session_load = self.db_load.get_session()
            logger.info("Iniciando a sessão para persistência de dados")

            for cliente in clientes_list:
                existing_cliente = session_load.query(Clientes).filter_by(id_cliente=cliente.id_cliente).first()

                if existing_cliente:
                    logger.info(f"Cliente existente encontrado com ID: {cliente.id_cliente}, verificando necessidade de atualização")
                    if (existing_cliente.razao_social != cliente.razao_social or
                        existing_cliente.endereco != cliente.endereco or
                        existing_cliente.estado != cliente.estado or
                        existing_cliente.cidade != cliente.cidade or
                        existing_cliente.telefone != cliente.telefone):

                        logger.info(f"Atualizando dados do cliente ID: {cliente.id_cliente}")
                        existing_cliente.razao_social = cliente.razao_social
                        existing_cliente.endereco = cliente.endereco
                        existing_cliente.estado = cliente.estado
                        existing_cliente.cidade = cliente.cidade
                        existing_cliente.telefone = cliente.telefone
                        session_load.add(existing_cliente)
                else:
                    logger.info(f"Adicionando novo cliente com ID: {cliente.id_cliente}")
                    session_load.add(cliente)

            session_load.commit()
            logger.info("Dados persistidos com sucesso!")

        except SQLAlchemyError as e:
            logger.error(f"Erro ao persistir dados: {e}")
            session_load.rollback()
        finally:
            logger.info("Fechando a sessão de persistência")
            session_load.close()
