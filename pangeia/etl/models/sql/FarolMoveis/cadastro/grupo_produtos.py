import logging
import time

from models.Cadastro.grupo_produtos import Grupo_Produtos
from models.connection import DatabaseConnection, DatabaseLoad
from sqlalchemy import Integer, MetaData, Table, cast, select

logger = logging.getLogger(__name__)


class GrupoProdutosQuery:
    def __init__(self):
        self.db = DatabaseConnection()
        self.metadata = MetaData()
        self.grupos_produtos_servicos = Table('GRUPOS_PRODUTOS_SERVICOS', self.metadata, autoload_with=self.db.engine)
        self.db_load = DatabaseLoad()

    def fetch_and_transform_data(self):
        session = self.db.get_session()
        logger.info("Iniciando a sessão para consulta de grupos de produtos")

        try:
            # Consulta da tabela GRUPOS_PRODUTOS_SERVICOS
            logger.info("Executando consulta para grupos de produtos e serviços")
            query_grupos_produtos_servicos = select(
                self.grupos_produtos_servicos.c.COD_GRUPO.label('id_grupo_prod'),
                self.grupos_produtos_servicos.c.COD_EMPRESA.label('id_emp_cli'),
                self.grupos_produtos_servicos.c.DES_GRUPO.label('descricao'),
            )

            start_time = time.time()
            result_produtos_servicos = session.execute(query_grupos_produtos_servicos).fetchAll()
            elapsed_time = time.time() - start_time
            logger.info(f"Consulta completada em {elapsed_time} segundos, Total de registros encontrados: {len(result_produtos_servicos)}")

            # Criar a lista de registros de produtos
            grupo_produtos_list = []
            for row in result_produtos_servicos:
                produto = Grupo_Produtos(
                    id_contrato=1,
                    id_grupo_prod=row.id_grupo_prod,
                    id_emp_cli=row.id_emp_cli,
                    descricao=row.descricao,
                )
                grupo_produtos_list.append(produto)
                logger.debug(f"Grupo de produto adicionado à lista: {produto}")

            return grupo_produtos_list

        except Exception as e:
            logger.error(f"Erro ao executar consulta: {e}, Query: {query_grupos_produtos_servicos}")
            raise
        finally:
            logger.info("Fechando a sessão de consulta")
            session.close()

    def persist_data(self):
        session_load = self.db_load.get_session()
        logger.info("Iniciando a sessão para persistência de dados dos grupos de produtos")

        try:
            grupo_produtos_list = self.fetch_and_transform_data()

            for grupo_produto in grupo_produtos_list:
                existing_grupo_produto = session_load.query(Grupo_Produtos).filter(
                    cast(Grupo_Produtos.id_grupo_prod, Integer) == int(grupo_produto.id_grupo_prod)  # Convertendo para Integer se necessário
                ).first()
                if existing_grupo_produto:
                    logger.info(f"Grupo de produtos existente encontrado com ID: {grupo_produto.id_grupo_prod}, verificando necessidade de atualização")
                    if existing_grupo_produto.descricao != grupo_produto.descricao:
                        logger.info(f"Atualizando dados do grupo de produtos ID: {grupo_produto.id_grupo_prod}")
                        existing_grupo_produto.descricao = grupo_produto.descricao
                        session_load.add(existing_grupo_produto)
                else:
                    logger.info(f"Adicionando novo grupo de produtos com ID: {grupo_produto.id_grupo_prod}")
                    session_load.add(grupo_produto)

            session_load.commit()
            logger.info("Dados de grupos de produtos persistidos com sucesso!")

        except Exception as e:
            logger.error(f"Erro ao persistir dados: {e}")
            session_load.rollback()
        finally:
            logger.info("Fechando a sessão de persistência")
            session_load.close()
