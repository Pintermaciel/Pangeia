import logging

from models.connection import DatabaseLoad
from models.sql.FarolMoveis.cadastro.clientes import ClientesQuery
from models.sql.FarolMoveis.cadastro.fornecedores import FornecedoresQuery
from models.sql.FarolMoveis.cadastro.grupo_produtos import GrupoProdutosQuery
from models.sql.FarolMoveis.cadastro.itens_venda import ItensVendaQuery
from models.sql.FarolMoveis.cadastro.produtos import ProdutosQuery
from models.sql.FarolMoveis.cadastro.vendedores import VendedoresQuery
from models.sql.FarolMoveis.comercial.vendas import VendasQuery
from models.sql.FarolMoveis.Financeiro.contasreceber import ContasReceberQuery
from models.sql.FarolMoveis.Financeiro.contaspagar import ContasPagarQuery
import sys

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
        #ClientesQuery(),
        #ContasReceberQuery(),
        # Adicione outras consultas conforme necessário
        # FornecedoresQuery(),
        ContasPagarQuery(),
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
