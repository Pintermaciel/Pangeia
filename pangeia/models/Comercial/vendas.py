from models.connection import Base
from sqlalchemy import Column, Date, Index, Integer, Numeric, String


class Vendas(Base):
    __tablename__ = 'Vendas'

    id_contrato = Column('ID_CONTRATO', Integer, nullable=False)
    id_venda = Column('ID_VENDA', Integer, primary_key=True, nullable=False)
    id_emp_cli = Column('ID_EMP_CLI', Integer, nullable=False)
    tipo_venda = Column('TIPO_VENDA', String(150), nullable=False)
    data_emissao = Column('DAT_EMISSAO', Date, nullable=False)
    valor_bruto = Column('VALOR_BRUTO', Numeric, nullable=False)
    valor_liquido = Column('VALOR_LIQUIDO', Numeric, nullable=True)

        # Definindo índices
    __table_args__ = (
        Index('idx_id_venda', 'ID_VENDA'),  # Índice para id_venda (já é uma primary key, então o índice é redundante aqui)
        Index('idx_id_contrato', 'ID_CONTRATO'),
        Index('idx_id_emp_cli', 'ID_EMP_CLI'),
        Index('idx_data_emissao', 'DAT_EMISSAO')
    )

    def to_dict(self):
        return {
            "id_contrato": self.id_contrato,
            "id_venda": self.id_venda,
            "id_emp_cli": self.id_emp_cli,
            "tipo_venda": self.tipo_venda,
            "data_emissao": self.data_emissao,
            "valor_bruto": self.valor_bruto,
            "valor_liquido": self.valor_liquido
        }

    def __repr__(self):
        return str(self.to_dict())
