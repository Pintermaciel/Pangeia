import logging
import unittest
from unittest.mock import MagicMock, patch

from models.connection import DatabaseConnection

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestDatabaseConnection(unittest.TestCase):

    @patch('models.connection.create_engine')
    @patch('models.connection.sessionmaker')
    def test_connection(self, mock_sessionmaker, mock_create_engine):
        # Configurar mocks
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_session = MagicMock()
        mock_sessionmaker.return_value = mock_session

        logger.info("Iniciando o teste de conexão com o banco de dados.")

        # Criar a instância de DatabaseConnection
        db_connection = DatabaseConnection()
        logger.info("Instância de DatabaseConnection criada.")

        # Testar a criação do engine
        mock_create_engine.assert_called_with(db_connection.config.DATABASE_URL)
        logger.info(f"Engine criado com a URL: {db_connection.config.DATABASE_URL}")

        # Testar a criação da sessão
        session = db_connection.get_session()
        self.assertEqual(session, mock_session())
        logger.info("Sessão criada e verificada.")

        # Verificar se a sessão foi chamada corretamente
        mock_sessionmaker.assert_called_with(bind=mock_engine)
        logger.info("sessionmaker chamado com o engine mock.")


if __name__ == '__main__':
    unittest.main()
