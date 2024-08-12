import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from models.Comercial.vendas import Vendas
from models.connection import DatabaseConnection, DatabaseLoad
from sqlalchemy import MetaData, Table, and_, select
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)


class VendasQuery:
    def __init__(self):
        self.db = DatabaseConnection()
        self.db_load = DatabaseLoad()
        self.metadata = MetaData()
        self.notas_fiscais_vendas = Table('NOTAS_FISCAIS_VENDA', self.metadata, autoload_with=self.db.engine)

    def fetch_and_transform_data(self):
        session = self.db.get_session()

        try:
            # Consulta da tabela NOTAS_FISCAIS_VENDA
            query_notas_fiscais_vendas = select(
                self.notas_fiscais_vendas.c.NUM_NF_VENDA.label('id_venda'),
                self.notas_fiscais_vendas.c.COD_EMPRESA.label('id_emp_cli'),
                self.notas_fiscais_vendas.c.DAT_EMISSAO.label('data_emissao'),
                self.notas_fiscais_vendas.c.VLR_BRUTO.label('valor_bruto'),
                self.notas_fiscais_vendas.c.VLR_LIQUIDO.label('valor_liquido')
            ).where(
                and_(
                    self.notas_fiscais_vendas.c.SIT_NF == 'PR',
                    self.notas_fiscais_vendas.c.TIP_NF_VENDA == 'N',
                    self.notas_fiscais_vendas.c.COD_OPERACAO.in_(['5101.1', '5102.1', '6101.1', '6102.1', '6108.1']),
                    self.notas_fiscais_vendas.c.COD_EMPRESA == 1
                )
            )

            result_notas_fiscais_vendas = session.execute(query_notas_fiscais_vendas).fetchall()

            # Criar a lista de registros de vendas
            vendas_list = []
            for row in result_notas_fiscais_vendas:
                venda = Vendas(
                    id_contrato=1,
                    id_venda=row.id_venda,
                    id_emp_cli=row.id_emp_cli,
                    tipo_venda='NF',
                    data_emissao=row.data_emissao,
                    valor_bruto=row.valor_bruto,
                    valor_liquido=row.valor_liquido,
                )
                vendas_list.append(venda)

            return vendas_list

        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise
        finally:
            logger.info("Closing the session")
            session.close()

    def process_batch(self, batch, batch_index):
        session_load = self.db_load.get_session()
        try:
            start_time = time.time()

            # Carregar IDs e registros existentes
            existing_ids = [v.id_venda for v in batch]
            existing_vendas = {v.id_venda: v for v in session_load.query(Vendas).filter(Vendas.id_venda.in_(existing_ids)).all()}
            to_update, to_add = [], []
            logger.info(f"Checking for updates or additions in batch {batch_index}...")

            end_loading_time = time.time()
            logger.info(f"Time taken to load existing records: {end_loading_time - start_time:.2f} seconds")

            # Classificação de vendas para atualização e adição
            start_classification_time = time.time()
            vendas_diferentes = []
            for venda in batch:
                existing_venda = existing_vendas.get(venda.id_venda)
                if existing_venda:
                    if (
                        existing_venda.data_emissao != venda.data_emissao or
                        existing_venda.valor_bruto != venda.valor_bruto or
                        existing_venda.valor_liquido != venda.valor_liquido
                    ):
                        vendas_diferentes.append((existing_venda, venda))
                else:
                    to_add.append(venda)
            end_classification_time = time.time()
            logger.info(f"Time taken to classify records: {end_classification_time - start_classification_time:.2f} seconds")

            # Processamento das atualizações
            start_update_time = time.time()
            for existing_venda, venda in vendas_diferentes:
                existing_venda.data_emissao = venda.data_emissao
                existing_venda.valor_bruto = venda.valor_bruto
                existing_venda.valor_liquido = venda.valor_liquido
                to_update.append(existing_venda)
            end_update_time = time.time()
            logger.info(f"Time taken to process updates: {end_update_time - start_update_time:.2f} seconds")

            # Persistência dos dados
            start_persistence_time = time.time()
            if to_update:
                session_load.bulk_save_objects(to_update)
                logger.info(f"Updated {len(to_update)} sales in batch {batch_index}")
            if to_add:
                session_load.bulk_save_objects(to_add)
                logger.info(f"Added {len(to_add)} new sales in batch {batch_index}")
            session_load.commit()
            end_persistence_time = time.time()
            logger.info(f"Time taken to persist data: {end_persistence_time - start_persistence_time:.2f} seconds")

            total_time = end_persistence_time - start_time
            logger.info(f"Total time to process batch {batch_index}: {total_time:.2f} seconds")
        except SQLAlchemyError as e:
            logger.error(f"Error persisting data: {e}")
            session_load.rollback()
        finally:
            session_load.close()
            logger.info(f"Database session closed after processing batch {batch_index}")

    def persist_data(self):
        try:
            vendas_list = self.fetch_and_transform_data()
            logger.info(f"Total sales retrieved for processing: {len(vendas_list)}")
            batch_size = 500  # Ajuste o tamanho do lote conforme necessário

            with ThreadPoolExecutor(max_workers=15) as executor:  # Ajuste o número de workers conforme necessário
                futures = []
                for i in range(0, len(vendas_list), batch_size):
                    batch = vendas_list[i:i + batch_size]
                    futures.append(executor.submit(self.process_batch, batch, i // batch_size + 1))

                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Error in batch processing: {e}")
        except Exception as e:
            logger.error(f"Unhandled error in persist_data: {e}")
