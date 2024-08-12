from models.connection import Base
from sqlalchemy import Date, ForeignKey, Integer, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column

class ContasReceber(Base):
    __tablename__ = 'CONTAS_RECEBER'

    id_contrato: Mapped[int] = mapped_column(
        'ID_CONTRATO',
        Integer,
        nullable=False,
    )
    id_contas_receber: Mapped[str] = mapped_column(
        'ID_CONTAS_RECEBER', String, primary_key=True, nullable=False
    )
    id_emp_cli: Mapped[int] = mapped_column(
        'ID_EMP_CLI',
        Integer,
        nullable=False,
    )
    id_cliente: Mapped[int] = mapped_column(
        'ID_CLIENTE',
        Integer,
        #ForeignKey('Clientes.ID_CLIENTE'),
        nullable=False,
    )
    id_cliente_completo: Mapped[str] = mapped_column(
        'ID_CLIENTE_COMPLETO',
        String,
        #ForeignKey('Clientes.ID_CLIENTE_COMPLETO'),
        nullable=False,
    )
    data_emissao: Mapped[Date] = mapped_column(
        'DATA_EMISSAO', Date, nullable=False
    )
    data_vencimento: Mapped[Date] = mapped_column(
        'DATA_VENCIMENTO', Date, nullable=False
    )
    data_pagamento: Mapped[Date] = mapped_column(
        'DATA_PAGAMENTO', Date, nullable=True
    )
    situacao: Mapped[str] = mapped_column(
        'SITUACAO', String(50), nullable=True
    )
    valor_docto: Mapped[Numeric] = mapped_column(
        'VALOR_DOCTO', Numeric(precision=10, scale=2), nullable=False
    )
    valor_pagto: Mapped[Numeric] = mapped_column(
        'VALOR_PAGTO', Numeric(precision=10, scale=2), nullable=True
    )
    forma_pagamento: Mapped[int] = mapped_column(
        'FORMA_PAGAMENTO', Integer, nullable=False
    )

    def to_dict(self):
        return {
            'id_contrato': self.id_contrato,
            'id_contas_receber': self.id_contas_receber,
            'id_emp_cli': self.id_emp_cli,
            'id_cliente': self.id_cliente,
            'id_cliente_completo': self.id_cliente_completo,
            'data_emissao': self.data_emissao,
            'data_vencimento': self.data_vencimento,
            'data_pagamento': self.data_pagamento,
            'situacao': self.situacao,
            'valor_docto': self.valor_docto,
            'valor_pagto': self.valor_pagto,
            'forma_pagamento': self.forma_pagamento,
        }

    def __repr__(self):
        return str(self.to_dict())