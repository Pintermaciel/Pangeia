import logging
import unittest

import pandas as pd
from models.connection import DatabaseConnection
from models.sql.FarolMoveis.cadastro.clientes import ClientesQuery
from sqlalchemy import Column, Integer, MetaData, String, Table

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestClientesQuery(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        logger.info("Configuração do banco de dados do cliente")
        # Configuração do banco de dados do cliente
        cls.db = DatabaseConnection()
        cls.engine = cls.db.engine
        cls.Session = cls.db.Session

        # Apenas para garantir que as tabelas existem, sem dados de teste inseridos
        metadata = MetaData()

        cls.clientes_fornecedores = Table('CLIENTES_FORNECEDORES', metadata,
            Column('COD_CLI_FOR', String(50), primary_key=True),
            Column('COD_EMPRESA', Integer, nullable=False),
            Column('RAZ_CLI_FOR', String(100), nullable=False),
            Column('END_CLI_FOR', String(100), nullable=False),
            Column('SIG_ESTADO', String(2), nullable=False),
            Column('COD_CIDADE', Integer, nullable=False),
            Column('TEL_CLI_FOR', String(14), nullable=True),
            Column('CLI_FOR_AMBOS', String(1), nullable=False)
        )

        cls.cidades = Table('CIDADES', metadata,
            Column('COD_CIDADE', Integer, primary_key=True),
            Column('NOM_CIDADE', String(50), nullable=False)
        )

        metadata.create_all(cls.engine)

    def setUp(self):
        logger.info("Iniciando uma nova sessão do banco de dados")
        self.session = self.Session()
        self.service = ClientesQuery()

    def tearDown(self):
        logger.info("Fechando a sessão do banco de dados")
        self.session.close()

    def test_fetch_and_transform_data(self):
        logger.info("Executando fetch_and_transform_data")
        clientes_list = self.service.fetch_and_transform_data()
        logger.info(f"Number of records fetched: {len(clientes_list)}")

        # Verificar que há registros
        self.assertGreater(len(clientes_list), 0)

        # Converter para DataFrame
        df = pd.DataFrame([cliente.to_dict() for cliente in clientes_list])
        df.to_csv('clientes.csv', index=False)
        logger.info("Dados salvos em clientes.csv")

        # Verificar se os dados foram salvos corretamente
        saved_df = pd.read_csv('clientes.csv')
        self.assertEqual(len(df), len(saved_df))
        logger.info("Verificação de integridade dos dados salva em CSV concluída com sucesso")


if __name__ == "__main__":
    unittest.main()
