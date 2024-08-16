import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from models.connection import DatabaseConnection, DatabaseLoad
from models.Financeiro.plano_conta import PlanoConta
from sqlalchemy import MetaData, Table, select
from sqlalchemy.exc import SQLAlchemyError

logging.basicConfig(
    level=logging.DEBUG, 
    format='%(asctime)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)


class PlanoContaQuery:
    def __init__(self):
        self.db = DatabaseConnection()
        self.metadata = MetaData()
        self.un_plano_contas = Table(
            'UN_PLANO_CONTAS', self.metadata, autoload_with=self.db.engine
        )
        self.db_load = DatabaseLoad()

    def fetch_and_transform_data(self):
        session = self.db.get_session()

        try:
            # Consulta da tabela UN_PLANO_CONTAS
            query_un_plano_contas = select(
                self.un_plano_contas.c.COD_CONTA_GERENCIAL.label('id_conta'),
                self.un_plano_contas.c.COD_CLASS_FISCAL.label('mascara'),
                self.un_plano_contas.c.DES_CONTA_GERENCIAL.label('descricao_conta'),
            )

            result_un_plano_contas = session.execute(query_un_plano_contas).fetchall()

            plano_contas = []
            for row in result_un_plano_contas:
                plano_conta = PlanoConta(
                    id_contrato=1,
                    id_conta=row.id_conta,
                    mascara=row.mascara,
                    descricao_conta=row.descricao_conta,
                    fk_contrato_despesa=f"1-{row.id_conta}"
                )
                plano_contas.append(plano_conta)

            return plano_contas

        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise
        finally:
            logger.info("Closing the session")
            session.close()

    def process_batch(self, batch, batch_index):
        session = self.db_load.get_session()
        logger.info(
            f'Iniciando processamento do lote {batch_index} com '
            f'{len(batch)} itens'
        )

        try:
            start_time = time.time()

            existing_ids = [conta.id_conta for conta in batch]
            existing_records = {
                v.id_conta: v
                for v in session.query(PlanoConta)
                .filter(PlanoConta.id_conta.in_(existing_ids))
                .all()
            }

            logger.info(
                f'Registros existentes carregados para o lote {batch_index}'
            )

            to_update = []
            to_add = []
            for conta in batch:
                existing_conta = existing_records.get(conta.id_conta)
                if existing_conta:
                    if self.is_conta_different(existing_conta, conta):
                        self.update_existing_conta(existing_conta, conta)
                        to_update.append(conta)
                else:
                    to_add.append(conta)

            if to_update:
                valid_updates = [
                    {
                        'id_conta': conta.id_conta,
                        'id_contrato': conta.id_contrato,
                        'mascara': conta.mascara,
                        'descricao_conta': conta.descricao_conta,
                        'fk_contrato_despesa': conta.fk_contrato_despesa
                    }
                    for conta in to_update if conta.id_conta is not None
                ]

                if valid_updates:
                    session.bulk_update_mappings(PlanoConta, valid_updates)
                    logger.info(
                        f'Atualizados {len(valid_updates)} itens no lote {batch_index}'
                    )
            if to_add:
                session.bulk_save_objects(to_add)
                logger.info(
                    f'Adicionados {len(to_add)} novos itens no lote '
                    f'{batch_index}'
                )
            session.commit()

            total_time = time.time() - start_time
            logger.info(
                f'Tempo total para processar o lote {batch_index}: '
                f'{total_time:.2f} segundos'
            )
        except SQLAlchemyError as e:
            logger.error(f'Erro ao persistir dados no lote {batch_index}: {e}')
            session.rollback()
        finally:
            session.close()
            logger.info(
                f'Sessão de banco de dados fechada após processar o lote '
                f'{batch_index}'
            )

    @staticmethod
    def is_conta_different(existing_conta, conta):
        return (
            existing_conta.id_contrato != conta.id_contrato
            or existing_conta.mascara != conta.mascara
            or existing_conta.descricao_conta != conta.descricao_conta
        )

    @staticmethod
    def update_existing_conta(existing_conta, conta):
        existing_conta.id_contrato = conta.id_contrato
        existing_conta.mascara = conta.mascara
        existing_conta.descricao_conta = conta.descricao_conta

    def persist_data(self):
        try:
            plano_contas_list = self.fetch_and_transform_data()
            logger.info(
                f'Total de itens recuperados para processamento: '
                f'{len(plano_contas_list)}'
            )
            batch_size = 500

            with ThreadPoolExecutor(max_workers=15) as executor:
                futures = [
                    executor.submit(
                        self.process_batch,
                        plano_contas_list[i : i + batch_size],
                        i // batch_size + 1,
                    )
                    for i in range(0, len(plano_contas_list), batch_size)
                ]

                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f'Erro no processamento do lote: {e}')
        except Exception as e:
            logger.error(f'Erro não tratado em persist_data: {e}')
