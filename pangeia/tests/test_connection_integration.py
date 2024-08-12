import logging
import unittest

from models.connection import DatabaseConnection
from sqlalchemy import text


class TestDatabaseConnectionIntegration(unittest.TestCase):

    def setUp(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        self.db_connection = DatabaseConnection()

    def test_connection(self):
        session = None
        try:
            session = self.db_connection.get_session()
            result = session.execute(text("SELECT 1"))
            self.assertIsNotNone(result.fetchone(), "O resultado da consulta é None")
            self.logger.info("Conexão com o banco de dados estabelecida com sucesso!")
        except Exception as e:
            self.logger.error(f"Erro ao tentar conectar ao banco de dados: {e}", exc_info=True)
            self.fail(f"Erro ao tentar conectar ao banco de dados: {e}")
        finally:
            if session:
                session.close()


if __name__ == '__main__':
    unittest.main()
