# ai_integration/database.py
from langchain_community.utilities import SQLDatabase
from sqlalchemy import text

from pangeia.cfg import Config

# Configuração do banco de dados
db_url = Config()
db = SQLDatabase.from_uri(db_url.VECTOR_DATABASE_URL)


def get_schema():
    """Retorna o esquema das tabelas do banco de dados."""
    return db.get_table_info()


def run_query(query: str):
    """Execute a query no banco de dados e retorne os dados."""
    print(query)
    try:
        # Converte a string SQL em um objeto text do SQLAlchemy
        sql_query = text(query)

        with db._engine.connect() as conn:
            result = conn.execute(sql_query)
            fetched_result = result.fetchall()
        return fetched_result
    except Exception as e:
        return f"Error executing query: {str(e)}"
