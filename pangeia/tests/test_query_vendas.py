import logging
import unittest

import pandas as pd
from models.connection import DatabaseConnection
from models.sql.FarolMoveis.comercial.vendas import VendasQuery
from sqlalchemy import Column, Date, Integer, MetaData, Numeric, Table

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestVendasQuery(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        logger.info("Configuração do banco de dados das vendas")
        # Configuração do banco de dados do vendas
        cls.db = DatabaseConnection()
        cls.engine = cls.db.engine
        cls.Session = cls.db.Session

        # Apenas para garantir que as tabelas existem, sem dados de teste inseridos
        metadata = MetaData()

        cls.vendas = Table('NOTAS_FISCAIS_VENDA', metadata,
            Column('NUM_NF_VENDA', Integer, primary_key=True),
            Column('COD_EMPRESA', Integer, nullable=False),
            Column('DAT_EMISSAO', Date, nullable=False),
            Column('VLR_BRUTO', Numeric, nullable=False),
            Column('VLR_LIQUIDO', Numeric, nullable=False),
        )

    def setUp(self):
        logger.info("Iniciando uma nova sessão do banco de dados")
        self.session = self.Session()
        self.service = VendasQuery()

    def tearDown(self):
        logger.info("Fechando a sessão do banco de dados")
        self.session.close()

    def test_fetch_and_transform_data(self):
        logger.info("Executando fetch_and_transform_data")
        clientes_list = self.service.fetch_and_transform_data()
        logger.info(f"Number of records fetched: {len(clientes_list)}")

        self.assertGreater(len(clientes_list), 0)

        # Converter para DataFrame
        df = pd.DataFrame([cliente.to_dict() for cliente in clientes_list])
        df.to_csv('vendas.csv', index=False)
        logger.info("Dados salvos em vendas.csv")

        # Verificar se os dados foram salvos corretamente
        saved_df = pd.read_csv('vendas.csv')
        self.assertEqual(len(df), len(saved_df))
        logger.info("Verificação de integridade dos dados salva em CSV concluída com sucesso")


if __name__ == "__main__":
    unittest.main()
