import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from models.connection import DatabaseConnection, DatabaseLoad
from models.Financeiro.contasreceber import ContasReceber
from sqlalchemy import MetaData, select
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)


class ContasReceberQuery:
    def __init__(self):
        self.db = DatabaseConnection()
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.db.engine)
        self.titulos_receber = self.metadata.tables.get('TITULOS_RECEBER')

        self.db_load = DatabaseLoad()
        self.metadata_load = MetaData()
        self.metadata_load.reflect(bind=self.db_load.engine)
        self.contas_receber_table = self.metadata_load.tables.get(
            'contasreceber'
        )

    def fetch_and_transform_data(self):
        session = self.db.get_session()
        try:
            query_titulos_receber = select(
                self.titulos_receber.c.NUM_TITULO.label('ID_CONTAS_RECEBER'),
                self.titulos_receber.c.COD_EMPRESA.label('ID_EMP_CLI'),
                self.titulos_receber.c.COD_CLI_FOR.label('ID_CLIENTE'),
                self.titulos_receber.c.DAT_EMISSAO.label('DATA_EMISSAO'),
                self.titulos_receber.c.VCT_ORIGINAL.label('DATA_VENCIMENTO'),
                self.titulos_receber.c.DAT_ULTIMO_RECEBIMENTO.label(
                    'DATA_PAGAMENTO'
                ),
                self.titulos_receber.c.SIT_TITULO.label('SITUACAO'),
                self.titulos_receber.c.VLR_ORIGINAL.label('VALOR_DOCTO'),
                self.titulos_receber.c.TOT_VALOR_RECEBIDO.label('VALOR_PAGTO'),
                self.titulos_receber.c.COD_TIPO_TITULO.label(
                    'FORMA_PAGAMENTO'
                ),
            )

            result_titulos_receber = session.execute(
                query_titulos_receber
            ).fetchall()

            contas_receber_list = [
                ContasReceber(
                    id_contas_receber=row[0],  # ID_CONTAS_RECEBER
                    id_contrato=1,
                    id_emp_cli=row[1],  # ID_EMP_CLI
                    id_cliente=row[2],  # ID_CLIENTE
                    data_emissao=row[3],  # DATA_EMISSAO
                    data_vencimento=row[4],  # DATA_VENCIMENTO
                    data_pagamento=row[5],  # DATA_PAGAMENTO
                    situacao=row[6],  # SITUACAO
                    valor_docto=row[7],  # VALOR_DOCTO
                    valor_pagto=row[8],  # VALOR_PAGTO
                    forma_pagamento=row[9],  # FORMA_PAGAMENTO
                    id_cliente_completo=(
                        f'{row[0]}-{row[1]}-{row[2]}'
                    ),
                )
                for row in result_titulos_receber
            ]

            return contas_receber_list
        except Exception as e:
            logger.error(f'Error executing query: {e}')
            raise
        finally:
            logger.info('Closing the session')
            session.close()

    def process_batch(self, batch, batch_index):
        session_load = self.db_load.get_session()
        logger.info(
            f'Iniciando processamento do lote {batch_index} com '
            f'{len(batch)} itens'
        )

        try:
            start_time = time.time()

            for conta in batch:
                conta.id_cliente_completo = (
                    f'{conta.id_contrato}-'
                    f'{conta.id_emp_cli}-'
                    f'{conta.id_cliente}'
                )

            existing_ids = [conta.id_cliente_completo for conta in batch]
            existing_records = {
                v.id_cliente_completo: v
                for v in session_load.query(ContasReceber)
                .filter(ContasReceber.id_cliente_completo.in_(existing_ids))
                .all()
            }

            logger.info(
                f'Registros existentes carregados para o lote {batch_index}'
            )

            to_update = []
            to_add = []
            for conta in batch:
                existing_conta = existing_records.get(
                    conta.id_cliente_completo
                )
                if existing_conta:
                    if self.is_conta_different(existing_conta, conta):
                        self.update_existing_conta(existing_conta, conta)
                        to_update.append(conta)
                        logger.debug(f'Atualizado: {conta}')
                else:
                    to_add.append(conta)
                    logger.debug(f'Adicionado: {conta}')

            if to_update:
                valid_updates = [
                    {
                        'pk_contasreceber': conta.pk_contasreceber,
                        'id_contas_receber': conta.id_contas_receber,
                        'id_contrato': conta.id_contrato,
                        'id_emp_cli': conta.id_emp_cli,
                        'id_cliente': conta.id_cliente,
                        'data_emissao': conta.data_emissao,
                        'data_vencimento': conta.data_vencimento,
                        'data_pagamento': conta.data_pagamento,
                        'situacao': conta.situacao,
                        'valor_docto': conta.valor_docto,
                        'valor_pagto': conta.valor_pagto,
                        'forma_pagamento': conta.forma_pagamento,
                        'id_cliente_completo': conta.id_cliente_completo,
                    }
                    for conta in to_update if conta.pk_contasreceber is not None
                ]

                if valid_updates:
                    session_load.bulk_update_mappings(ContasReceber, valid_updates)
                    logger.info(
                        f'Atualizados {len(valid_updates)} itens no lote {batch_index}'
                    )
            if to_add:
                session_load.bulk_save_objects(to_add)
                logger.info(
                    f'Adicionados {len(to_add)} novos itens no lote '
                    f'{batch_index}'
                )
            session_load.commit()

            total_time = time.time() - start_time
            logger.info(
                f'Tempo total para processar o lote {batch_index}: '
                f'{total_time:.2f} segundos'
            )
        except SQLAlchemyError as e:
            logger.error(f'Erro ao persistir dados no lote {batch_index}: {e}')
            session_load.rollback()
        finally:
            session_load.close()
            logger.info(
                f'Sessão de banco de dados fechada após processar o lote '
                f'{batch_index}'
            )

    @staticmethod
    def is_conta_different(existing_conta, conta):
        return (
            existing_conta.id_contrato != conta.id_contrato
            or existing_conta.id_emp_cli != conta.id_emp_cli
            or existing_conta.id_cliente != conta.id_cliente
            or existing_conta.data_emissao != conta.data_emissao
            or existing_conta.data_vencimento != conta.data_vencimento
            or existing_conta.data_pagamento != conta.data_pagamento
            or existing_conta.situacao != conta.situacao
            or existing_conta.valor_docto != conta.valor_docto
            or existing_conta.valor_pagto != conta.valor_pagto
            or existing_conta.forma_pagamento != conta.forma_pagamento
            or existing_conta.id_cliente_completo != conta.id_cliente_completo
        )

    @staticmethod
    def update_existing_conta(existing_conta, conta):
        existing_conta.id_contrato = conta.id_contrato
        existing_conta.id_emp_cli = conta.id_emp_cli
        existing_conta.id_cliente = conta.id_cliente
        existing_conta.data_emissao = conta.data_emissao
        existing_conta.data_vencimento = conta.data_vencimento
        existing_conta.data_pagamento = conta.data_pagamento
        existing_conta.situacao = conta.situacao
        existing_conta.valor_docto = conta.valor_docto
        existing_conta.valor_pagto = conta.valor_pagto
        existing_conta.forma_pagamento = conta.forma_pagamento
        existing_conta.id_cliente_completo = conta.id_cliente_completo

    def persist_data(self):
        try:
            contas_receber_list = self.fetch_and_transform_data()
            logger.info(
                f'Total de itens recuperados para processamento: '
                f'{len(contas_receber_list)}'
            )
            batch_size = 500

            with ThreadPoolExecutor(max_workers=15) as executor:
                futures = [
                    executor.submit(
                        self.process_batch,
                        contas_receber_list[i : i + batch_size],
                        i // batch_size + 1,
                    )
                    for i in range(0, len(contas_receber_list), batch_size)
                ]

                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f'Erro no processamento do lote: {e}')
        except Exception as e:
            logger.error(f'Erro não tratado em persist_data: {e}')
