from models.connection import Base
from sqlalchemy import Column, Integer, Numeric


class Estoque(Base):
    __tablename__ = 'Estoques'

    id_contrato = Column('ID_CONTRATO', Integer, nullable=False)
    id_emp_cli = Column('ID_EMP_CLI', Integer, nullable=False)
    id_deposito = Column('COD_DEPOSITO', Integer, primary_key=True, nullable=False)
    id_produto = Column('COD_PRODUTO', Integer, nullable=False)
    qtd_estoque = Column('QTD_ATUAL', Numeric(precision=10, scale=2), nullable=False)

    def to_dict(self):
        return {
            "id_contrato": self.id_contrato,
            "id_emp_cli": self.id_emp_cli,
            "id_deposito": self.id_deposito,
            "id_produto": self.id_produto,
            "qtd_estoque": self.qtd_estoque,
        }

    def __repr__(self):
        return str(self.to_dict())
