import logging

import pandas as pd
from models.connection import DatabaseConnection
from models.sql.FarolMoveis.comercial.vendas import VendasQuery
from sqlalchemy.orm import sessionmaker

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class VendasQueryProduction:
    def __init__(self):
        logger.info("Configuração do banco de dados das vendas")
        self.db = DatabaseConnection()
        self.engine = self.db.engine
        self.Session = sessionmaker(bind=self.engine)

        """# Garantir que as tabelas existem
        metadata = MetaData()
        self.vendas = Table('NOTAS_FISCAIS_VENDA', metadata,
                            Column('NUM_NF_VENDA', Integer, primary_key=True),
                            Column('COD_EMPRESA', Integer, nullable=False),
                            Column('DAT_EMISSAO', Date, nullable=False),
                            Column('VLR_BRUTO', Numeric, nullable=False),
                            Column('VLR_LIQUIDO', Numeric, nullable=False))
        metadata.create_all(self.engine)"""

    def fetch_and_transform_data(self):
        logger.info("Iniciando uma nova sessão do banco de dados")
        session = self.Session()
        service = VendasQuery()

        try:
            logger.info("Executando fetch_and_transform_data")
            clientes_list = service.fetch_and_transform_data()
            logger.info(f"Number of records fetched: {len(clientes_list)}")

            if len(clientes_list) > 0:
                # Converter para DataFrame
                df = pd.DataFrame([cliente.to_dict() for cliente in clientes_list])
                logger.info(f"Colunas do DataFrame: {df.columns.tolist()}")
                """df.to_csv('vendas.csv', index=False)
                logger.info("Dados salvos em vendas.csv")"""

                # Verificar se os dados foram salvos corretamente
                saved_df = pd.read_csv('vendas.csv')
                if len(df) == len(saved_df):
                    logger.info("Verificação de integridade dos dados salva em CSV concluída com sucesso")
                else:
                    logger.error("Discrepância entre o DataFrame original e o CSV salvo")

                # Agrupar os dados por dia
                self.agrupar_por_dia(df)
            else:
                logger.warning("Nenhum registro foi encontrado na consulta")

        except Exception as e:
            logger.error(f"Erro durante o fetch_and_transform_data: {e}")
        finally:
            session.close()
            logger.info("Sessão do banco de dados fechada")

    def agrupar_por_dia(self, df):
        logger.info("Agrupando dados por dia")
        try:
            df_grouped = df.groupby('data_emissao').agg({
                'valor_bruto': 'sum',
                'valor_liquido': 'sum'
            }).reset_index()
            """df_grouped.to_csv('vendas_agrupadas_por_dia.csv', index=False)
            logger.info("Dados agrupados salvos em vendas_agrupadas_por_dia.csv")"""
            return df_grouped
        except KeyError as e:
            logger.error(f"Erro durante o agrupamento dos dados: {e}")
