from models.connection import Base
from sqlalchemy import Computed, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship


class Clientes(Base):
    __tablename__ = 'Clientes'

    id_contrato: Mapped[int] = mapped_column(
        'ID_CONTRATO', Integer, nullable=False
    )
    
    id_cliente: Mapped[int] = mapped_column(
        'ID_CLIENTE', Integer, nullable=False
    )
    
    id_emp_cli: Mapped[int] = mapped_column(
        'ID_EMP_CLI', Integer, nullable=False
    )
    
    razao_social: Mapped[str] = mapped_column(
        'RAZAO_SOCIAL', String(100), nullable=False
    )
    
    endereco: Mapped[str | None] = mapped_column(
        'ENDERECO', String, nullable=True
    )
    
    estado: Mapped[str] = mapped_column(
        'ESTADO', 
        String(2), 
        nullable=False
        )
    cidade: Mapped[str] = mapped_column(
        'CIDADE', 
        String(50), 
        nullable=False
        )
    
    telefone: Mapped[str | None] = mapped_column(
        'TELEFONE', 
        String, 
        nullable=True
    )

    id_cliente_completo: Mapped[str] = mapped_column(
        'ID_CLIENTE_COMPLETO',
        String,
        primary_key=True
    )

    contas_receber = relationship(
        'ContasReceber', 
        back_populates='cliente'
    )

    def to_dict(self) -> dict:
        return {
            'id_contrato': self.id_contrato,
            'id_cliente': self.id_cliente,
            'id_emp_cli': self.id_emp_cli,
            'razao_social': self.razao_social,
            'endereco': self.endereco,
            'estado': self.estado,
            'cidade': self.cidade,
            'telefone': self.telefone,
            'id_cliente_completo': self.id_cliente_completo,
        }

    def __repr__(self) -> str:
        return str(self.to_dict())
