import logging

import pandas as pd
from cfg import Config
from langchain_community.embeddings.openai import OpenAIEmbeddings
from langchain_community.vectorstores import PGVector
from sqlalchemy import (
    Column,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
)
from sqlalchemy.exc import SQLAlchemyError

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LLMUtils:
    def __init__(self, api_key):
        self.api_key = api_key
        self.embeddings = OpenAIEmbeddings(api_key=self.api_key)

    def get_embedding(self, text):
        return self.embeddings.embed(text)


class DBUtils:
    def __init__(self):
        # Carregar configurações
        self.config = Config()

        # Configurar OpenAI via LangChain
        self.llm_utils = LLMUtils(api_key=self.config.OPENAI_API_KEY)

        # Configurar conexão com o banco de dados
        self.engine = create_engine(self.config.DATABASE_URL)
        self.metadata = MetaData()

        # Configurar PGVector
        self.vector_store = PGVector(
            collection_name='vector_store',
            connection_string=self.config.DATABASE_URL,
            embedding=self.llm_utils.embeddings,
            use_jsonb=True
        )

    def create_table_if_not_exists(self, table_name, df):
        if not self.engine.dialect.has_table(self.engine, table_name):
            logger.info(f"Criando a tabela {table_name}")
            columns = [Column('id', Integer, primary_key=True, autoincrement=True)]
            for col in df.columns:
                if df[col].dtype == 'object':
                    columns.append(Column(col, String))
                elif df[col].dtype in ['int64', 'int32']:
                    columns.append(Column(col, Integer))
                elif df[col].dtype in ['float64', 'float32']:
                    columns.append(Column(col, Float))
                else:
                    columns.append(Column(col, String))
            columns.append(Column('embedding', String))  # Coluna para armazenar os embeddings
            table = Table(table_name, self.metadata, *columns)
            self.metadata.create_all(self.engine)
            return table
        else:
            logger.info(f"Tabela {table_name} já existe")
            return Table(table_name, self.metadata, autoload_with=self.engine)

    def insert_data_from_csv(self, csv_path):
        try:
            df = pd.read_csv(csv_path)
            table_name = csv_path.split('.')[0]
            table = self.create_table_if_not_exists(table_name, df)

            # Converter dados em documentos e calcular embeddings
            docs = []
            for index, row in df.iterrows():
                text = " ".join(row.astype(str).values)
                embedding = self.llm_utils.get_embedding(text)
                docs.append({
                    'text': text,
                    'embedding': embedding
                })

            # Adicionar documentos ao PGVector
            self.vector_store.add_documents(docs)
            logger.info(f"Dados do CSV {csv_path} inseridos na tabela {table_name} com sucesso")

        except SQLAlchemyError as e:
            logger.error(f"Erro ao interagir com o banco de dados: {e}")
        except Exception as e:
            logger.error(f"Erro geral: {e}")


# Exemplo de uso
if __name__ == "__main__":
    db_utils = DBUtils()
    db_utils.insert_data_from_csv('vendas_agrupadas_por_dia.csv')
