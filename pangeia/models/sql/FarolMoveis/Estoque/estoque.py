import logging

from models.connection import DatabaseConnection
from models.Estoque.estoques import Estoque
from sqlalchemy import MetaData, Table, select

logger = logging.getLogger(__name__)


class EstoqueQuery:
    def __init__(self):
        self.db = DatabaseConnection()
        self.metadata = MetaData()
        self.estoque = Table('ESTOQUES', self.metadata, autoload_with=self.db.engine)

    def fetch_and_transform_data(self):
        session = self.db.get_session()

        try:
            # Consulta da tabela ESTOQUES
            query_estoques = select(
                self.estoque.c.COD_DEPOSITO.label('id_deposito'),
                self.estoque.c.COD_PRODUTO.label('id_produto'),
                self.estoque.c.QTD_ATUAL.label('qtd_estoque'),
            )

            result_estoques = session.execute(query_estoques).fetchall()

            # Criar a lista de registros de vendas
            estoque = []
            for row in result_estoques:
                estoque = Estoque(
                    id_contrato=1,
                    id_emp_cli=1,
                    id_deposito=row.id_deposito,
                    id_produto=row.id_produto,
                    qtd_estoque=row.qtd_estoque,
                )

                estoque.append(estoque)

            return estoque

        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise
        finally:
            logger.info("Closing the session")
            session.close()
