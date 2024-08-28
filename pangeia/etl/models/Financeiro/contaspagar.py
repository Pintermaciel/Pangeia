from sqlalchemy import Date, ForeignKey, Integer, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from models.connection import Base


class ContasPagar(Base):
    __tablename__ = 'contas_pagar'

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    fk_contrato_fornecedor: Mapped[str] = mapped_column(
        'FK_CONTRATO_FORNECEDOR',
        String,
        ForeignKey('fornecedores.FK_CONTRATO_FORNECEDOR'),
        nullable=False,
    )

    fornecedor = relationship('Fornecedores', back_populates='contas_pagar')

    fk_contrato_despesa: Mapped[str] = mapped_column(
        'FK_CONTRATO_DESPESA',
        String,
        ForeignKey('plano_conta.FK_CONTRATO_DESPESA'),
        nullable=False,
    )

    plano_conta = relationship(
        'PlanoConta',
        back_populates='contas_pagar',
        primaryjoin=(
            "foreign(ContasPagar.fk_contrato_despesa) == "
            "PlanoConta.fk_contrato_despesa"
        )
    )

    id_contrato: Mapped[int] = mapped_column(
        'ID_CONTRATO', Integer, nullable=False
    )
    id_contas_pagar: Mapped[str] = mapped_column(
        'ID_CONTAS_PAGAR', String, nullable=False
    )
    id_emp_cli: Mapped[int] = mapped_column(
        'ID_EMP_CLI', Integer, nullable=False
    )
    id_fornecedor: Mapped[int] = mapped_column(
        'ID_FORNECEDOR', Integer, nullable=False
    )
    id_despesa: Mapped[int] = mapped_column(
        'ID_DESPESA', Integer, nullable=False
    )
    data_emissao_cp: Mapped[Date] = mapped_column(
        'DATA_EMISSAO_CP', Date, nullable=False
    )
    data_pagamento_cp: Mapped[Date] = mapped_column(
        'DATA_PAGAMENTO_CP', Date, nullable=True
    )
    situacao_cp: Mapped[str] = mapped_column(
        'SITUACAO_CP', String(50), nullable=True
    )
    valor_docto_cp: Mapped[Numeric] = mapped_column(
        'VALOR_DOCTO_CP', Numeric(precision=10, scale=2), nullable=False
    )
    valor_pagto_cp: Mapped[Numeric] = mapped_column(
        'VALOR_PAGTO_CP', Numeric(precision=10, scale=2), nullable=True
    )
    data_vencimento_cp: Mapped[Date] = mapped_column(
        'DATA_VENCIMENTO_CP', Date, nullable=False
    )
    class_fiscal: Mapped[str] = mapped_column(
        'CLASS_FISCAL', String, nullable=True
    )
    valor_total_despesa: Mapped[Numeric] = mapped_column(
        'VALOR_TOTAL_DESPESA', Numeric, nullable=True
    )
    valor_total_custo: Mapped[Numeric] = mapped_column(
        'VALOR_TOTAL_CUSTO', Numeric, nullable=True
    )

    def to_dict(self):
        return {
            'id': self.id,
            'fk_contrato_fornecedor': self.fk_contrato_fornecedor,
            'id_contrato': self.id_contrato,
            'id_contas_pagar': self.id_contas_pagar,
            'id_emp_cli': self.id_emp_cli,
            'id_fornecedor': self.id_fornecedor,
            'id_despesa': self.id_despesa,
            'data_emissao_cp': self.data_emissao_cp,
            'data_pagamento_cp': self.data_pagamento_cp,
            'situacao_cp': self.situacao_cp,
            'valor_docto_cp': self.valor_docto_cp,
            'valor_pagto_cp': self.valor_pagto_cp,
            'data_vencimento_cp': self.data_vencimento_cp,
            'class_fiscal': self.class_fiscal,
            'valor_total_despesa': self.valor_total_despesa,
            'valor_total_custo': self.valor_total_custo
        }

    def __repr__(self):
        return str(self.to_dict())
