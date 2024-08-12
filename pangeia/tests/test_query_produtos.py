import logging
import unittest

import pandas as pd
from models.connection import DatabaseConnection
from models.sql.FarolMoveis.cadastro.produtos import ProdutosQuery
from sqlalchemy import Column, Integer, MetaData, String, Table

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestProdutosQuery(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        logger.info("Configuração do banco de dados dos produtos")
        # Configuração do banco de dados do produtos
        cls.db = DatabaseConnection()
        cls.engine = cls.db.engine
        cls.Session = cls.db.Session

        # Apenas para garantir que as tabelas existem, sem dados de teste inseridos
        metadata = MetaData()

        cls.produtos = Table('PRODUTOS_SERVICOS', metadata,
            Column('COD_PRODUTO', Integer, primary_key=True),
            Column('COD_EMPRESA', Integer, nullable=False),
            Column('DES_PRODUTO', String(100), nullable=False),
            Column('COD_GRUPO', Integer, nullable=False)
        )

        cls.cidades = Table('DERIVACOES', metadata,
            Column('COD_PRODUTO', Integer, primary_key=True),
            Column('VLR_PRECO_VENDA_ULT_SAI', String(50), nullable=False),
            Column('VLR_PRECO_CUSTO_MEDIO', String(50), nullable=False)
        )

        metadata.create_all(cls.engine)

    def setUp(self):
        logger.info("Iniciando uma nova sessão do banco de dados")
        self.session = self.Session()
        self.service = ProdutosQuery()

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
        df.to_csv('produtos.csv', index=False)
        logger.info("Dados salvos em produtos.csv")

        # Verificar se os dados foram salvos corretamente
        saved_df = pd.read_csv('produtos.csv')
        self.assertEqual(len(df), len(saved_df))
        logger.info("Verificação de integridade dos dados salva em CSV concluída com sucesso")


if __name__ == "__main__":
    unittest.main()
