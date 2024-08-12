from models.connection import Base
from sqlalchemy import Date, ForeignKey, Integer, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column


class ContasPagar(Base):
    __tablename__ = 'contas_pagar'

    id_contrato_fornecedor: Mapped[int] = mapped_column('ID_CONTRATO_FORNECEDOR', Integer, ForeignKey('contratos_fornecedor.id_contrato_fornecedor'), nullable=False)
    id_contrato: Mapped[int] = mapped_column('ID_CONTRATO', Integer, ForeignKey('contratos.id_contrato'), nullable=False)
    id_contas_pagar: Mapped[str] = mapped_column('ID_CONTAS_PAGAR', String, primary_key=True, nullable=False)
    id_emp_cli: Mapped[int] = mapped_column('ID_EMP_CLI', Integer, ForeignKey('empresas_clientes.id_emp_cli'), nullable=False)
    id_fornecedor: Mapped[int] = mapped_column('ID_FORNECEDOR', Integer, ForeignKey('fornecedores.id_fornecedor'), nullable=False)
    id_despesa: Mapped[int] = mapped_column('ID_DESPESA', Integer, ForeignKey('despesas.id_despesa'), nullable=False)
    data_emissao_cp: Mapped[Date] = mapped_column('DATA_EMISSAO_CP', Date, nullable=False)
    data_pagamento_cp: Mapped[Date] = mapped_column('DATA_PAGAMENTO_CP', Date, nullable=False)
    situacao_cp: Mapped[str] = mapped_column('SITUACAO_CP', String(50), nullable=True)
    valor_docto_cp: Mapped[Numeric] = mapped_column('VALOR_DOCTO_CP', Numeric(precision=10, scale=2), nullable=False)
    valor_pagto_cp: Mapped[Numeric] = mapped_column('VALOR_PAGTO_CP', Numeric(precision=10, scale=2), nullable=True)
    data_vencimento_cp: Mapped[Date] = mapped_column('DATA_VENCIMENTO_CP', Date, nullable=False)
    id_conta: Mapped[int] = mapped_column('ID_CONTA', Integer, ForeignKey('contas.id_conta'), nullable=False)

    def to_dict(self):
        return {
            "id_contrato": self.id_contrato,
            "id_contas_pagar": self.id_contas_pagar,
            "id_emp_cli": self.id_emp_cli,
            "id_fornecedor": self.id_fornecedor,
            "id_despesa": self.id_despesa,
            "data_emissao_cp": self.data_emissao_cp,
            "data_pagamento_cp": self.data_pagamento_cp,
            "situacao_cp": self.situacao_cp,
            "valor_docto_cp": self.valor_docto_cp,
            "valor_pagto_cp": self.valor_pagto_cp,
            "data_vencimento_cp": self.data_vencimento_cp,
            "id_conta": self.id_conta
        }

    def __repr__(self):
        return str(self.to_dict())
