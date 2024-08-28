from cfg import Config
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Definir a base para os modelos
Base = declarative_base()


class DatabaseConnection:
    def __init__(self):
        self.config = Config()
        self.engine = create_engine(self.config.DATABASE_URL)
        self.Session = sessionmaker(bind=self.engine)

    def get_session(self):
        return self.Session()

    def create_tables(self):
        Base.metadata.create_all(self.engine)
        print("Tabelas criadas com sucesso!")


class DatabaseLoad:
    def __init__(self):
        self.config = Config()
        self.engine = create_engine(self.config.VECTOR_DATABASE_URL)
        self.Session = sessionmaker(bind=self.engine)

    def get_session(self):
        return self.Session()

    def create_tables(self):
        Base.metadata.create_all(self.engine)
        print("Tabelas criadas com sucesso!")
