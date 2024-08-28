# ai_integration/database.py
from pangeia.cfg import Config
from langchain_community.utilities import SQLDatabase

# Configuração do banco de dados
db_url = Config()
db = SQLDatabase.from_uri(db_url.VECTOR_DATABASE_URL)

def get_schema():
    """Retorna o esquema das tabelas do banco de dados."""
    return db.get_table_info()

def run_query(query: str):
    """Execute a query in the database and return the data."""
    print(query)
    try:
        result = db.run(query, fetch="all", include_columns=True)
        return result
    except Exception as e:
        return f"Error executing query: {str(e)}"
