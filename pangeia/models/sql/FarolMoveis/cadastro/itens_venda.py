import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from models.Cadastro.itens_venda import Itens_Venda
from models.connection import DatabaseConnection, DatabaseLoad
from sqlalchemy import MetaData, Table, and_, select
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)


class ItensVendaQuery:
    def __init__(self):
        self.db = DatabaseConnection()
        self.db_load = DatabaseLoad()
        self.metadata = MetaData()
        self.itens_venda = Table('ITENS_NF_VENDA', self.metadata, autoload_with=self.db.engine)

    def fetch_and_transform_data(self):
        session = self.db.get_session()
        logger.info("Iniciando a sessão para consulta de itens de venda")

        try:
            # Consulta da tabela ITENS_NF_VENDA
            logger.info("Executando consulta para itens de venda")
            query_itens_venda = select(
                self.itens_venda.c.NUM_NF_VENDA.label('id_venda'),
                self.itens_venda.c.COD_EMPRESA.label('id_emp_cli'),
                self.itens_venda.c.COD_PRODUTO.label('id_produto'),
                self.itens_venda.c.QTD_FATURADA.label('quantidade_venda'),
                self.itens_venda.c.VLR_FINAL_CUSTO_ULT_ENT.label('valor_custo_total'),
                self.itens_venda.c.VLR_LIQUIDO.label('valor_liquido'),
            )

            start_time = time.time()
            result_itens_venda = session.execute(query_itens_venda).fetchall()
            elapsed_time = time.time() - start_time
            logger.info(f"Consulta completada em {elapsed_time:.2f} segundos, Total de registros encontrados: {len(result_itens_venda)}")

            # Criar a lista de registros de itens de venda
            itens_venda_list = []
            for row in result_itens_venda:
                item = Itens_Venda(
                    id_contrato=1,
                    id_venda=row.id_venda,
                    id_emp_cli=row.id_emp_cli,
                    id_produto=row.id_produto,
                    quantidade_venda=row.quantidade_venda,
                    valor_custo_total=row.valor_custo_total,
                    valor_liquido=row.valor_liquido,
                )
                itens_venda_list.append(item)
                logger.debug(f"Item de venda adicionado à lista: {item}")

            return itens_venda_list

        except Exception as e:
            logger.error(f"Erro ao executar consulta: {e}")
            raise
        finally:
            logger.info("Fechando a sessão de consulta")
            session.close()

    def process_batch(self, batch, batch_index):
        session_load = self.db_load.get_session()
        logger.info(f"Iniciando processamento do lote {batch_index} com {len(batch)} itens")

        try:
            start_time = time.time()

            # Carregar IDs e registros existentes
            existing_ids = [(v.id_venda, v.id_produto) for v in batch]
            existing_vendas = {
                (v.id_venda, v.id_produto): v
                for v in session_load.query(Itens_Venda).filter(
                    and_(
                        Itens_Venda.id_venda.in_([id_venda for id_venda, _ in existing_ids]),
                        Itens_Venda.id_produto.in_([id_produto for _, id_produto in existing_ids])
                    )
                ).all()
            }

            logger.info(f"Registros existentes carregados para o lote {batch_index}")

            # Classificação de vendas para atualização e adição
            to_update, to_add = [], []
            for venda in batch:
                existing_venda = existing_vendas.get((venda.id_venda, venda.id_produto))
                if existing_venda:
                    if (
                        existing_venda.quantidade_venda != venda.quantidade_venda or
                        existing_venda.valor_custo_total != venda.valor_custo_total or
                        existing_venda.valor_liquido != venda.valor_liquido
                    ):
                        existing_venda.quantidade_venda = venda.quantidade_venda
                        existing_venda.valor_custo_total = venda.valor_custo_total
                        existing_venda.valor_liquido = venda.valor_liquido
                        to_update.append(existing_venda)
                        logger.debug(f"Atualizado: {existing_venda}")
                else:
                    to_add.append(venda)
                    logger.debug(f"Adicionado: {venda}")

            # Persistência dos dados
            if to_update:
                session_load.bulk_save_objects(to_update)
                logger.info(f"Atualizados {len(to_update)} itens no lote {batch_index}")
            if to_add:
                session_load.bulk_save_objects(to_add)
                logger.info(f"Adicionados {len(to_add)} novos itens no lote {batch_index}")
            session_load.commit()

            total_time = time.time() - start_time
            logger.info(f"Tempo total para processar o lote {batch_index}: {total_time:.2f} segundos")
        except SQLAlchemyError as e:
            logger.error(f"Erro ao persistir dados no lote {batch_index}: {e}")
            session_load.rollback()
        finally:
            session_load.close()
            logger.info(f"Sessão de banco de dados fechada após processar o lote {batch_index}")

    def persist_data(self):
        try:
            itens_venda_list = self.fetch_and_transform_data()
            logger.info(f"Total de itens recuperados para processamento: {len(itens_venda_list)}")
            batch_size = 500  # Ajuste o tamanho do lote conforme necessário

            with ThreadPoolExecutor(max_workers=15) as executor:  # Ajuste o número de workers conforme necessário
                futures = []
                for i in range(0, len(itens_venda_list), batch_size):
                    batch = itens_venda_list[i:i + batch_size]
                    futures.append(executor.submit(self.process_batch, batch, i // batch_size + 1))

                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Erro no processamento do lote: {e}")
        except Exception as e:
            logger.error(f"Erro não tratado em persist_data: {e}")
