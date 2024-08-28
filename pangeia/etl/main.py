import logging
import sys

from models.connection import DatabaseLoad
from models.sql.FarolMoveis.Financeiro.contaspagar import ContasPagarQuery

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
    )
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    db = DatabaseLoad()
    db.create_tables()

    # Crie uma lista de objetos de consulta na ordem em que deseja executá-los
    queries = [
        # ClientesQuery(),
        # ContasReceberQuery(),
        # FornecedoresQuery(),
        ContasPagarQuery(),
        # PlanoContaQuery(),
        # ProdutosQuery(),
        # GrupoProdutosQuery(),
        # ItensVendaQuery(),
        # VendasQuery(),
        # VendedoresQuery(),
        # EstoqueQuery(),
    ]

    for query in queries:
        try:
            logger.info(f"Iniciando {query.__class__.__name__}")
            query.persist_data()
            logger.info(f"Finalizado {query.__class__.__name__}")
        except Exception as e:
            logger.error(f"Erro ao executar {query.__class__.__name__}: {e}")
