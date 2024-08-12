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

    # produtos_query = ProdutosQuery()
    # produtos_query.persist_data()
    # clientes_query = ClientesQuery()
    # clientes_query.persist_data()
    # fornecedores_query = FornecedoresQuery()
    # fornecedores_query.persist_data()
    # grupo_produtos_query = GrupoProdutosQuery()
    # grupo_produtos_query.persist_data()
    # itens_venda_query = ItensVendaQuery()
    # itens_venda_query.persist_data()
    # vendas_query = VendasQuery()
    # vendas_query.persist_data()
    # vendedores_query = VendedoresQuery()
    # vendedores_query.persist_data()
    # estoque_query = EstoqueQuery
    # estoque_query.persist.data()
    contas_receber_query = ContasReceberQuery()
    contas_receber_query.persist_data()
