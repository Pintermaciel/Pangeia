from sqlalchemy import Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from models.connection import Base


class Fornecedores(Base):
    __tablename__ = 'fornecedores'

    fk_contrato_fornecedor: Mapped[str] = mapped_column(
        'FK_CONTRATO_FORNECEDOR',
        String,
        primary_key=True,
        nullable=False,
    )

    contas_pagar = relationship(
        'ContasPagar',
        back_populates='fornecedor',
        primaryjoin=(
            "Fornecedores.fk_contrato_fornecedor == "
            "foreign(ContasPagar.fk_contrato_fornecedor)"
        )
    )

    id_contrato: Mapped[int] = mapped_column(
        'ID_CONTRATO', Integer, nullable=False
    )
    id_fornecedor: Mapped[int] = mapped_column(
        'ID_FORNECEDOR', Integer, nullable=False
    )
    id_emp_cli: Mapped[int] = mapped_column(
        'ID_EMP_CLI', Integer, nullable=False
    )
    razao_social: Mapped[str] = mapped_column(
        'RAZAO_SOCIAL', String(100), nullable=False
    )
    endereco: Mapped[str] = mapped_column(
        'ENDERECO', String(100), nullable=True
    )
    estado: Mapped[str] = mapped_column(
        'ESTADO', String(2), nullable=False
    )
    cidade: Mapped[str] = mapped_column(
        'CIDADE', String(50), nullable=False
    )
    telefone: Mapped[str] = mapped_column(
        'TELEFONE', String, nullable=True
    )

    def to_dict(self):
        return {
            "fk_contrato_fornecedor": self.fk_contrato_fornecedor,
            "id_contrato": self.id_contrato,
            "id_fornecedor": self.id_fornecedor,
            "id_emp_cli": self.id_emp_cli,
            "razao_social": self.razao_social,
            "endereco": self.endereco,
            "estado": self.estado,
            "cidade": self.cidade,
            "telefone": self.telefone,
        }

    def __repr__(self):
        return str(self.to_dict())
