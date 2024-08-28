import logging

from models.Cadastro.vendedores import Vendedores
from models.connection import DatabaseConnection, DatabaseLoad
from sqlalchemy import Integer, MetaData, Table, cast, select

logger = logging.getLogger(__name__)


class VendedoresQuery:
    def __init__(self):
        self.db = DatabaseConnection()
        self.metadata = MetaData()
        self.vendedores = Table('REPRESENTANTES', self.metadata, autoload_with=self.db.engine)
        self.db_load = DatabaseLoad()

    def fetch_and_transform_data(self):
        session = self.db.get_session()
        logger.info("Iniciando a sessão para consulta de vendedores")

        try:
            # Consulta da tabela REPRESENTANTES
            logger.info("Executando consulta para a tabela REPRESENTANTES")
            query_vendedores = select(
                cast(self.vendedores.c.COD_REPRESENTANTE, Integer).label('id_vendedor'),
                self.vendedores.c.RAZ_REPRESENTANTE.label('nome_vend')
            )

            result_vendedores = session.execute(query_vendedores).fetchall()
            logger.info(f"Consulta completada. Total de vendedores encontrados: {len(result_vendedores)}")

            # Criar a lista de registros de vendedores
            vendedores_list = []
            for row in result_vendedores:
                vendedor = Vendedores(
                    id_contrato=1,
                    id_vendedor=int(row.id_vendedor),
                    id_emp_cli=1,
                    nome_vend=row.nome_vend,
                )
                vendedores_list.append(vendedor)
                logger.debug(f"Vendedor transformado para persistência: {vendedor}")

            return vendedores_list

        except Exception as e:
            logger.error(f"Erro ao executar consulta: {e}")
            raise
        finally:
            logger.info("Fechando a sessão de consulta")
            session.close()

    def persist_data(self):
        try:
            vendedores_list = self.fetch_and_transform_data()
            session_load = self.db_load.get_session()
            logger.info("Iniciando a sessão para persistência de dados dos vendedores")

            updated = 0
            added = 0

            for vendedor in vendedores_list:
                # Busca o vendedor existente pelo id
                existing_vendedor = session_load.query(Vendedores).filter_by(id_vendedor=vendedor.id_vendedor).first()

                if existing_vendedor:
                    # Verifica se os dados são diferentes e atualiza
                    if existing_vendedor.nome_vend != vendedor.nome_vend:
                        existing_vendedor.nome_vend = vendedor.nome_vend
                        session_load.add(existing_vendedor)
                        updated += 1
                        logger.debug(f"Atualizado vendedor ID: {vendedor.id_vendedor}")
                else:
                    # Adiciona novo vendedor se não existir
                    session_load.add(vendedor)
                    added += 1
                    logger.debug(f"Adicionado novo vendedor ID: {vendedor.id_vendedor}")

            session_load.commit()
            logger.info(f"Dados persistidos com sucesso. {updated} vendedores atualizados, {added} vendedores adicionados.")
        except Exception as e:
            session_load.rollback()
            logger.error(f"Erro ao persistir dados: {e}")
        finally:
            logger.info("Fechando a sessão de persistência")
            session_load.close()
