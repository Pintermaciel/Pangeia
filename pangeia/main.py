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

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    db = DatabaseLoad()
    db.create_tables()

    # Crie uma lista de objetos de consulta na ordem em que deseja executá-los
    queries = [
        ClientesQuery(),
        ContasReceberQuery(),
        # Adicione outras consultas conforme necessário
        # ProdutosQuery(),
        # FornecedoresQuery(),
        # GrupoProdutosQuery(),
        # ItensVendaQuery(),
        # VendasQuery(),
        # VendedoresQuery(),
        # EstoqueQuery(),
    ]

    # Itere sobre a lista e execute o método persist_data para cada consulta
    for query in queries:
        try:
            query.persist_data()
        except Exception as e:
            logger.error(f"Erro ao executar {query.__class__.__name__}: {e}")