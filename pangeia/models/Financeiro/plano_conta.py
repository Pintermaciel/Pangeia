from models.connection import Base
from sqlalchemy.orm import Mapped, mapped_column


class PlanoConta(Base):
    __tablename__ = 'Plano_Conta'

    id_contrato: Mapped[int] = mapped_column('ID_CONTRATO', nullable=False)
    id_conta: Mapped[int] = mapped_column('ID_CONTA', primary_key=True, nullable=False)
    mascara: Mapped[str] = mapped_column('MASCARA', nullable=False)
    descricao_conta: Mapped[str] = mapped_column('DESCRICAO_CONTA', nullable=False)

    def to_dict(self):
        return {
            "id_contrato": self.id_contrato,
            "id_conta": self.id_conta,
            "mascara": self.mascara,
            "descricao_conta": self.descricao_conta,
        }

    def __repr__(self):
        return str(self.to_dict())
