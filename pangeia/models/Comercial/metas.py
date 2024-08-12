from models.connection import Base
from sqlalchemy import Column, Date, Integer, Numeric, String


class Metas(Base):
    __tablename__ = 'Metas'

    id_contrato = Column('ID_CONTRATO', Integer, nullable=False)
    id_vendedor = Column('ID_VENDEDOR', String(50), primary_key=True, nullable=True)
    id_emp_cli = Column('ID_EMP_CLI', Integer, nullable=False)
    data_meta = Column('DAT_META', Date, nullable=False)
    valor_meta = Column('VALOR_META', Numeric, nullable=False)

    def to_dict(self):
        return {
            "id_contrato": self.id_contrato,
            "id_vendedor": self.id_vendedor,
            "id_emp_cli": self.id_emp_cli,
            "data_meta": self.data_meta,
            "valor_meta": self.valor_meta,

        }

    def __repr__(self):
        return str(self.to_dict())
