from sqlalchemy import Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from models.connection import Base


class PlanoConta(Base):
    __tablename__ = 'plano_conta'
    __table_args__ = (UniqueConstraint('ID_CONTA', 'FK_CONTRATO_DESPESA'),)

    id_contrato: Mapped[int] = mapped_column(
        'ID_CONTRATO',
        Integer,
        nullable=False
        )
    id_conta: Mapped[int] = mapped_column(
        'ID_CONTA',
        Integer,
        primary_key=True,
        nullable=False)
    mascara: Mapped[str] = mapped_column(
        'MASCARA',
        String,
        nullable=False
        )
    descricao_conta: Mapped[str] = mapped_column(
        'DESCRICAO_CONTA',
        String,
        nullable=False
        )

    fk_contrato_despesa: Mapped[str] = mapped_column(
        'FK_CONTRATO_DESPESA',
        String,
        nullable=False,
        unique=True
    )

    contas_pagar = relationship(
        'ContasPagar',
        back_populates='plano_conta',
        primaryjoin=(
            "PlanoConta.fk_contrato_despesa == "
            "foreign(ContasPagar.fk_contrato_despesa)"
        )
    )

    def to_dict(self):
        return {
            "id_contrato": self.id_contrato,
            "id_conta": self.id_conta,
            "mascara": self.mascara,
            "descricao_conta": self.descricao_conta,
        }

    def __repr__(self):
        return str(self.to_dict())
