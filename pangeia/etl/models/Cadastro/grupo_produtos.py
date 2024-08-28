from sqlalchemy import Column, Integer, String

from models.connection import Base


class Grupo_Produtos(Base):
    __tablename__ = 'grupo_produtos'

    id_contrato = Column('ID_CONTRATO', Integer, nullable=False)
    id_grupo_prod = Column('ID_GRUPO_PROD', Integer, primary_key=True, nullable=False)
    id_emp_cli = Column('ID_EMP_CLI', Integer, nullable=False)
    descricao = Column('DESCRICAO', String(150), nullable=False)

    def to_dict(self):
        return {
            "id_contrato": self.id_contrato,
            "id_grupo_prod": self.id_grupo_prod,
            "id_emp_cli": self.id_emp_cli,
            "descricao": self.descricao,
        }

    def __repr__(self):
        return str(self.to_dict())
