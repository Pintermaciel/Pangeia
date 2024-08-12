import logging

from models.connection import DatabaseConnection
from models.Financeiro.plano_conta import PlanoConta
from sqlalchemy import MetaData, Table, select

logger = logging.getLogger(__name__)


class PlanoContaQuery:
    def __init__(self):
        self.db = DatabaseConnection()
        self.metadata = MetaData()
        self.un_plano_contas = Table('UN_PLANO_CONTAS', self.metadata, autoload_with=self.db.engine)

    def fetch_and_transform_data(self):
        session = self.db.get_session()

        try:
            # Consulta da tabela UN_PLANO_CONTAS
            query_un_plano_contas = select(
                self.un_plano_contas.c.COD_CONTA_GERENCIAL.label('id_conta'),
                self.un_plano_contas.c.COD_CLASS_FISCAL.label('mascara'),
                self.un_plano_contas.c.DES_CONTA_GERENCIAL.label('descricao_conta'),
            )

            result_un_plano_contas = session.execute(query_un_plano_contas).fetchall()

            # Criar a lista de registros de vendas
            plano_contas = []
            for row in result_un_plano_contas:
                plano_conta = PlanoConta(
                    id_contrato=1,
                    id_conta=row.id_conta,
                    mascara=row.mascara,
                    descricao_conta=row.descricao_conta,
                )
                plano_contas.append(plano_conta)

            return plano_contas

        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise
        finally:
            logger.info("Closing the session")
            session.close()
