from sqlalchemy import Column, Integer, String

from models.connection import Base


class Vendedores(Base):
    __tablename__ = 'Vendedores'

    id_contrato = Column('ID_CONTRATO', Integer, nullable=False)
    id_vendedor = Column('ID_VENDEDOR', Integer, primary_key=True, nullable=False)
    id_emp_cli = Column('ID_EMP_CLI', Integer, nullable=False)
    nome_vend = Column('NOME_VEND', String(100), nullable=False)

    def to_dict(self):
        return {
            "id_contrato": self.id_contrato,
            "id_vendedor": self.id_vendedor,
            "id_emp_cli": self.id_emp_cli,
            "nome_vend": self.nome_vend
        }

    def __repr__(self):
        return str(self.to_dict())
