import logging
import unittest

import pandas as pd
from models.connection import DatabaseConnection
from models.sql.FarolMoveis.Financeiro.plano_conta import PlanoContaQuery
from sqlalchemy import Column, Integer, MetaData, String, Table

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestPlanoContaQuery(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        logger.info("Configuração do banco de dados das vendas")
        # Configuração do banco de dados do vendas
        cls.db = DatabaseConnection()
        cls.engine = cls.db.engine
        cls.Session = cls.db.Session

        # Apenas para garantir que as tabelas existem, sem dados de teste inseridos
        metadata = MetaData()

        cls.vendas = Table('UN_PLANO_CONTAS', metadata,
            Column('COD_CONTA_GERENCIAL', Integer, primary_key=True),
            Column('DES_CONTA_GERENCIAL', String, nullable=False),
            Column('COD_CLASS_FISCAL', String, nullable=False)
        )

    def setUp(self):
        logger.info("Iniciando uma nova sessão do banco de dados")
        self.session = self.Session()
        self.service = PlanoContaQuery()

    def tearDown(self):
        logger.info("Fechando a sessão do banco de dados")
        self.session.close()

    def test_fetch_and_transform_data(self):
        logger.info("Executando fetch_and_transform_data")
        plano_conta_list = self.service.fetch_and_transform_data()
        logger.info(f"Number of records fetched: {len(plano_conta_list)}")

        self.assertGreater(len(plano_conta_list), 0)

        # Converter para DataFrame
        df = pd.DataFrame([plano_conta.to_dict() for plano_conta in plano_conta_list])
        df.to_csv('plano_conta.csv', index=False)
        logger.info("Dados salvos em plano_conta.csv")

        # Verificar se os dados foram salvos corretamente
        saved_df = pd.read_csv('plano_conta.csv')
        self.assertEqual(len(df), len(saved_df))
        logger.info("Verificação de integridade dos dados salva em CSV concluída com sucesso")


if __name__ == "__main__":
    unittest.main()
