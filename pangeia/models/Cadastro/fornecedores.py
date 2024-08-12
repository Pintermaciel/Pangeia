from models.connection import Base
from sqlalchemy import Column, Integer, String


class Fornecedores(Base):
    __tablename__ = 'Fornecedores'

    id_contrato = Column('ID_CONTRATO', Integer, nullable=False)
    id_fornecedor = Column('ID_FORNECEDOR', Integer, primary_key=True, nullable=False)
    id_emp_cli = Column('ID_EMP_CLI', Integer, nullable=False)
    razao_social = Column('RAZAO_SOCIAL', String(100), nullable=False)
    endereco = Column('ENDERECO', String(100), nullable=True)
    estado = Column('ESTADO', String(2), nullable=False)
    cidade = Column('CIDADE', String(50), nullable=False)
    telefone = Column('TELEFONE', String, nullable=True)

    def to_dict(self):
        return {
            "id_contrato": self.id_contrato,
            "id_fornecedor": self.id_fornecedor,
            "id_emp_cli": self.id_emp_cli,
            "razao_social": self.razao_social,
            "endereco": self.endereco,
            "estado": self.estado,
            "cidade": self.cidade,
            "telefone": self.telefone
        }

    def __repr__(self):
        return str(self.to_dict())
