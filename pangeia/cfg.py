import os
from urllib.parse import quote_plus

from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo .env
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
else:
    raise FileNotFoundError(
        f"Arquivo .env não encontrado no caminho: {dotenv_path}"
    )


class Config:
    # Configurações do banco de dados do cliente
    DATABASE_SERVER = os.getenv('DATABASE_SERVER')
    DATABASE_DATABASE = os.getenv('DATABASE_DATABASE')
    DATABASE_USERNAME = os.getenv('DATABASE_USERNAME')
    DATABASE_PASSWORD = os.getenv('DATABASE_PASSWORD')
    DATABASE_TYPE = os.getenv('DATABASE_TYPE')

    # Configurações do banco de dados vetorizado
    VECTOR_DATABASE_SERVER = os.getenv('VECTOR_DATABASE_SERVER')
    VECTOR_DATABASE_PORT = os.getenv('VECTOR_DATABASE_PORT')
    VECTOR_DATABASE_USERNAME = os.getenv('VECTOR_DATABASE_USERNAME')
    VECTOR_DATABASE_PASSWORD = os.getenv('VECTOR_DATABASE_PASSWORD')
    VECTOR_DATABASE_NAME = os.getenv('VECTOR_DATABASE_NAME')

    # Configurações da OpenAI
    OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

    # Configurações do Groq
    GROQ_API_KEY = os.getenv('GROQ_API_KEY')

    @property
    def DATABASE_URL(self):
        password_encoded = quote_plus(self.DATABASE_PASSWORD)

        # Construa a URL de acordo com o tipo de banco de dados
        if self.DATABASE_TYPE == 'sqlserver':
            return f"mssql+pytds://{self.DATABASE_USERNAME}:{password_encoded}@{self.DATABASE_SERVER}/{self.DATABASE_DATABASE}"
        elif self.DATABASE_TYPE == 'postgresql':
            return f"postgresql://{self.DATABASE_USERNAME}:{password_encoded}@{self.DATABASE_SERVER}/{self.DATABASE_DATABASE}"
        elif self.DATABASE_TYPE == 'mysql':
            return f"mysql+pymysql://{self.DATABASE_USERNAME}:{password_encoded}@{self.DATABASE_SERVER}/{self.DATABASE_DATABASE}"
        else:
            raise ValueError("Tipo de banco de dados não suportado.")

    @property
    def VECTOR_DATABASE_URL(self):
        # Construir a URL para o banco de dados vetorizado
        password_encoded = quote_plus(self.VECTOR_DATABASE_PASSWORD)
        return f"postgresql://{self.VECTOR_DATABASE_USERNAME}:{password_encoded}@{self.VECTOR_DATABASE_SERVER}:{self.VECTOR_DATABASE_PORT}/{self.VECTOR_DATABASE_NAME}"
