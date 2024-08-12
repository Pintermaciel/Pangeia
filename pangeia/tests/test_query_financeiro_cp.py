import logging
import unittest

import pandas as pd
from models.connection import DatabaseConnection
from models.sql.FarolMoveis.Financeiro.contaspagar import ContasPagarQuery
from sqlalchemy import (
    Column,
    Date,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
)

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestQueries(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        logger.info("Configuração do banco de dados do cliente")
        # Configuração do banco de dados do cliente
        cls.db = DatabaseConnection()
        cls.engine = cls.db.engine
        cls.Session = cls.db.Session

        # Apenas para garantir que as tabelas existem, sem dados de teste inseridos
        metadata = MetaData()

        cls.titulos_pagar = Table('TITULOS_PAGAR', metadata,
            Column('COD_CONTRATO', Integer, primary_key=True),
            Column('COD_EMPRESA', Integer, nullable=False),
            Column('NUM_INTERNO', String, nullable=False),
            Column('COD_CLI_FOR', Integer, nullable=False),
            Column('DAT_EMISSAO', Date, nullable=False),
            Column('DAT_ULTIMO_PAGAMENTO', Date, nullable=True),
            Column('SIT_TITULO', String(50), nullable=True),
            Column('VCT_ORIGINAL', Date, nullable=False),
            Column('VLR_ORIGINAL', Numeric(precision=10, scale=2), nullable=False),
            Column('TOT_VALOR_PAGO', Numeric(precision=10, scale=2), nullable=True)
        )

        cls.un_titulos_pagar = Table('UN_TITULOS_PAGAR', metadata,
            Column('NUM_INTERNO', String, primary_key=True),
            Column('COD_EMPRESA', Integer, primary_key=True),
            Column('ID_DESPESA', Integer, nullable=False)
        )

        cls.un_plano_contas = Table('UN_PLANO_CONTAS', metadata,
            Column('COD_CONTA_GERENCIAL', Integer, primary_key=True),
            Column('ID_DESPESA', Integer, nullable=False)
        )

        metadata.create_all(cls.engine)

    def setUp(self):
        logger.info("Iniciando uma nova sessão do banco de dados")
        self.session = self.Session()
        self.service = ContasPagarQuery()

    def tearDown(self):
        logger.info("Fechando a sessão do banco de dados")
        self.session.close()

    def test_fetch_and_transform_data(self):
        logger.info("Executando fetch_and_transform_data")
        contas_pagar_list = self.service.fetch_and_transform_data()
        logger.info(f"Number of records fetched: {len(contas_pagar_list)}")

        # Verificar que há registros
        self.assertGreater(len(contas_pagar_list), 0)

        # Converter para DataFrame
        df = pd.DataFrame([cp.to_dict() for cp in contas_pagar_list])
        df.to_csv('teste.csv')
        # Calcular o total de valor_pagto_cp
        total_valor_pagto_cp = df['valor_pagto_cp'].sum()

        logger.info(f"Total valor_pagto_cp: {total_valor_pagto_cp}")
        print(f"Number of rows: {len(df)}")
        print(f"Total valor_pagto_cp: {total_valor_pagto_cp}")


if __name__ == "__main__":
    unittest.main()
