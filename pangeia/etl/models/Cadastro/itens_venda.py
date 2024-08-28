from sqlalchemy import Column, Integer, Numeric, PrimaryKeyConstraint

from models.connection import Base


class Itens_Venda(Base):
    __tablename__ = 'Itens_Venda'

    id_contrato = Column('ID_CONTRATO', Integer, nullable=False)
    id_venda = Column('ID_VENDA', Integer, nullable=False)
    id_emp_cli = Column('ID_EMP_CLI', Integer, nullable=False)
    id_produto = Column('ID_PRODUTO', Integer, nullable=False)
    quantidade_venda = Column('QUANTIDADE_VENDA', Integer, nullable=False)
    valor_custo_total = Column('VALOR_CUSTO_TOTAL', Numeric, nullable=True)
    valor_liquido = Column('VALOR_LIQUIDO', Numeric, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint('ID_CONTRATO', 'ID_VENDA', 'ID_PRODUTO'),
    )

    def to_dict(self):
        return {
            "id_contrato": self.id_contrato,
            "id_venda": self.id_venda,
            "id_emp_cli": self.id_emp_cli,
            "id_produto": self.id_produto,
            "quantidade_venda": self.quantidade_venda,
            "valor_custo_total": self.valor_custo_total,
            "valor_liquido": self.valor_liquido
        }

    def __repr__(self):
        return str(self.to_dict())
