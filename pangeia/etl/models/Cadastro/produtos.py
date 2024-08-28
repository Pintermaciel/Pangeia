from sqlalchemy import Column, Integer, String

from models.connection import Base


class Produtos(Base):
    __tablename__ = 'Produtos'

    id_contrato = Column('ID_CONTRATO', Integer, nullable=False)
    id_produto = Column('ID_PRODUTO', Integer, primary_key=True, nullable=False)
    id_emp_cli = Column('ID_EMP_CLI', Integer, nullable=False)
    descricao = Column('DESCRICAO', String, nullable=False)
    id_grupo_prod = Column('ID_GRUPO_PROD', Integer, nullable=False)
    preco = Column('PRECO', String, nullable=True)
    custo = Column('CUSTO', String, nullable=True)

    def to_dict(self):
        return {
            "id_contrato": self.id_contrato,
            "id_produto": self.id_produto,
            "id_emp_cli": self.id_emp_cli,
            "descricao": self.descricao,
            "id_grupo_prod": self.id_grupo_prod,
            "preco": self.preco,
            "custo": self.custo
        }

    def __repr__(self):
        return str(self.to_dict())
