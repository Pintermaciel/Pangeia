import logging
import time

from models.Cadastro.produtos import Produtos
from models.connection import DatabaseConnection, DatabaseLoad
from sqlalchemy import MetaData, Table, and_, select
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)


class ProdutosQuery:
    def __init__(self):
        self.db = DatabaseConnection()
        self.db_load = DatabaseLoad()
        self.metadata = MetaData()
        self.produtos_servicos = Table('PRODUTOS_SERVICOS', self.metadata, autoload_with=self.db.engine)
        self.derivacoes = Table('DERIVACOES', self.metadata, autoload_with=self.db.engine)

    def fetch_and_transform_data(self):
        session = self.db.get_session()
        logger.info("Iniciando a sessão para consulta de produtos e derivacoes")

        try:
            # Consulta da tabela PRODUTOS_SERVICOS
            logger.info("Executando consulta para produtos")
            query_produtos_servicos = select(
                self.produtos_servicos.c.COD_PRODUTO.label('id_produto'),
                self.produtos_servicos.c.COD_EMPRESA.label('id_emp_cli'),
                self.produtos_servicos.c.DES_PRODUTO.label('descricao'),
                self.produtos_servicos.c.COD_GRUPO.label('id_grupo_prod'),
            )

            start_time = time.time()
            result_produtos_servicos = session.execute(query_produtos_servicos).fetchall()
            elapsed_time = time.time() - start_time
            logger.info(f"Consulta de produtos completada em {elapsed_time:.2f} segundos, {len(result_produtos_servicos)} registros encontrados")

            # Obter os códigos dos produtos e id_emp_cli
            cod_produto_emp_cli = {(row.id_produto, row.id_emp_cli) for row in result_produtos_servicos}

            produto_map = {}
            for chunk in self.chunks(list(cod_produto_emp_cli), 1000):
                query_derivacoes = select(
                    self.derivacoes.c.COD_PRODUTO,
                    self.derivacoes.c.COD_EMPRESA,
                    self.derivacoes.c.VLR_PRECO_VENDA_ULT_SAI,
                    self.derivacoes.c.VLR_PRECO_CUSTO_MEDIO,
                ).where(
                    and_(
                        self.derivacoes.c.COD_PRODUTO.in_([cp[0] for cp in chunk]),
                        self.derivacoes.c.COD_EMPRESA.in_([cp[1] for cp in chunk])
                    )
                )

                result_derivacoes = session.execute(query_derivacoes).fetchall()
                produto_map.update({(row.COD_PRODUTO, row.COD_EMPRESA): (row.VLR_PRECO_VENDA_ULT_SAI, row.VLR_PRECO_CUSTO_MEDIO) for row in result_derivacoes})

            produtos_list = [Produtos(
                id_contrato=1,
                id_produto=row.id_produto,
                id_emp_cli=row.id_emp_cli,
                descricao=row.descricao,
                id_grupo_prod=row.id_grupo_prod,
                preco=produto_map.get((row.id_produto, row.id_emp_cli), (None, None))[0],
                custo=produto_map.get((row.id_produto, row.id_emp_cli), (None, None))[1]
            ) for row in result_produtos_servicos]

            logger.info(f"Total de {len(produtos_list)} produtos transformados para persistência")
            return produtos_list

        except Exception as e:
            logger.error(f"Erro ao executar consulta de produtos: {e}")
            raise
        finally:
            logger.info("Fechando a sessão de consulta")
            session.close()

    def chunks(self, lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def persist_data(self):
        try:
            produtos_list = self.fetch_and_transform_data()
            session_load = self.db_load.get_session()

            existing_produtos = {p.id_produto: p for p in session_load.query(Produtos).filter(Produtos.id_produto.in_([p.id_produto for p in produtos_list])).all()}
            to_update, to_add = [], []

            for produto in produtos_list:
                existing_produto = existing_produtos.get(produto.id_produto)

                if existing_produto and (existing_produto.descricao != produto.descricao or
                                        existing_produto.id_grupo_prod != produto.id_grupo_prod or
                                        existing_produto.preco != produto.preco or
                                        existing_produto.custo != produto.custo):
                    existing_produto.descricao = produto.descricao
                    existing_produto.id_grupo_prod = produto.id_grupo_prod
                    existing_produto.preco = produto.preco
                    existing_produto.custo = produto.custo
                    to_update.append(existing_produto)
                    logger.info(f"Preparado para atualizar produto {produto.id_produto}")

                elif not existing_produto:
                    to_add.append(produto)
                    logger.info(f"Preparado para adicionar novo produto {produto.id_produto}")

            if to_update:
                session_load.bulk_save_objects(to_update)
                logger.info(f"Atualizados {len(to_update)} produtos")
            if to_add:
                session_load.bulk_save_objects(to_add)
                logger.info(f"Adicionados {len(to_add)} novos produtos")
            session_load.commit()
        except SQLAlchemyError as e:
            logger.error(f"Erro ao persistir dados de produtos: {e}")
            session_load.rollback()
        finally:
            logger.info("Fechando a sessão de persistência")
            session_load.close()
