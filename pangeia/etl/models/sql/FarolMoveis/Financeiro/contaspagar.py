import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from models.connection import DatabaseConnection, DatabaseLoad
from models.Financeiro.contaspagar import ContasPagar
from sqlalchemy import MetaData, Table, select
from sqlalchemy.exc import SQLAlchemyError

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    )
logger = logging.getLogger(__name__)


class ContasPagarQuery:
    def __init__(self):
        self.db = DatabaseConnection()
        self.metadata = MetaData()
        self.titulos_pagar = Table(
            'TITULOS_PAGAR', self.metadata, autoload_with=self.db.engine
        )
        self.un_titulos_pagar = Table(
            'UN_TITULOS_PAGAR', self.metadata, autoload_with=self.db.engine
        )
        self.un_plano_contas = Table(
            'UN_PLANO_CONTAS', self.metadata, autoload_with=self.db.engine
        )

        self.db_load = DatabaseLoad()

    def fetch_and_transform_data(self):
        session = self.db.get_session()

        try:
            # Primeira consulta: TITULOS_PAGAR
            query_titulos_pagar = select(
                self.titulos_pagar.c.NUM_INTERNO.label('ID_CONTAS_PAGAR'),
                self.titulos_pagar.c.COD_EMPRESA.label('ID_EMP_CLI'),
                self.titulos_pagar.c.COD_CLI_FOR.label('ID_FORNECEDOR'),
                self.titulos_pagar.c.DAT_EMISSAO.label('DATA_EMISSAO_CP'),
                self.titulos_pagar.c.DAT_ULTIMO_PAGAMENTO.label(
                    'DATA_PAGAMENTO_CP'
                ),
                self.titulos_pagar.c.SIT_TITULO.label('SITUACAO_CP'),
                self.titulos_pagar.c.VCT_ORIGINAL.label('DATA_VENCIMENTO_CP'),
                self.titulos_pagar.c.VLR_ORIGINAL.label('VALOR_DOCTO_CP'),
                self.titulos_pagar.c.TOT_VALOR_PAGO.label('VALOR_PAGTO_CP'),
            )

            result_titulos_pagar = session.execute(query_titulos_pagar).fetchall()

            # Segunda consulta: UN_TITULOS_PAGAR
            query_un_titulos_pagar = select(
                self.un_titulos_pagar.c.NUM_INTERNO,
                self.un_titulos_pagar.c.COD_EMPRESA,
                self.un_titulos_pagar.c.COD_CONTA_GERENCIAL
            )

            result_un_titulos_pagar = session.execute(query_un_titulos_pagar).fetchall()

            # Terceira consulta: UN_PLANO_CONTAS
            query_un_plano_contas = select(
                self.un_plano_contas.c.COD_CONTA_GERENCIAL,
                self.un_plano_contas.c.COD_CLASS_FISCAL
            )

            result_un_plano_contas = session.execute(query_un_plano_contas).fetchall()

            # Combinar os resultados
            un_titulos_map = {
                (row.NUM_INTERNO, row.COD_EMPRESA): row.COD_CONTA_GERENCIAL
                for row in result_un_titulos_pagar
            }
            logger.info(f"un_titulos_map: {un_titulos_map}")

            plano_contas_map = {
                row.COD_CONTA_GERENCIAL: row.COD_CONTA_GERENCIAL
                for row in result_un_plano_contas
            }
            logger.info(f"plano_contas_map: {plano_contas_map}")

            mapa_fiscal = {
                row.COD_CONTA_GERENCIAL: row.COD_CLASS_FISCAL
                for row in result_un_plano_contas
            }
            logger.info(f"plano_contas_map: {plano_contas_map}")

            contas_pagar_list = []
            for row in result_titulos_pagar:
                chave = (row.ID_CONTAS_PAGAR, row.ID_EMP_CLI)
                cod_conta_gerencial = un_titulos_map.get(chave)
                id_mapa_fiscal = mapa_fiscal.get(cod_conta_gerencial)

                if cod_conta_gerencial is None:
                    logger.warning(f"Chave {chave} não encontrada em un_titulos_map")
                    logger.debug(
                        f"Dados do TITULOS_PAGAR: ID_CONTAS_PAGAR={row.ID_CONTAS_PAGAR}, "
                        f"ID_EMP_CLI={row.ID_EMP_CLI}"
                    )
                else:
                    id_despesa = plano_contas_map.get(cod_conta_gerencial)
                    if id_despesa is None:
                        logger.warning(
                            f"cod_conta_gerencial {cod_conta_gerencial} não encontrado em plano_contas_map"
                        )

                    fk_contrato_fornecedor = f"1-{row.ID_FORNECEDOR}"
                    fk_contrato_despesa = f"1-{id_despesa}"

                    contas_pagar = ContasPagar(
                        id_contas_pagar=row.ID_CONTAS_PAGAR,
                        id_contrato=1,
                        fk_contrato_fornecedor=fk_contrato_fornecedor,
                        fk_contrato_despesa=fk_contrato_despesa,
                        id_emp_cli=row.ID_EMP_CLI,
                        id_fornecedor=row.ID_FORNECEDOR,
                        id_despesa=id_despesa,
                        data_emissao_cp=row.DATA_EMISSAO_CP,
                        data_pagamento_cp=row.DATA_PAGAMENTO_CP,
                        situacao_cp=row.SITUACAO_CP,
                        valor_docto_cp=row.VALOR_DOCTO_CP,
                        valor_pagto_cp=row.VALOR_PAGTO_CP,
                        data_vencimento_cp=row.DATA_VENCIMENTO_CP,
                        class_fiscal=id_mapa_fiscal
                    )
                    contas_pagar_list.append(contas_pagar)

            return contas_pagar_list
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise
        finally:
            logger.info("Closing the session")
            session.close()

    def update_column_based_on_regex(
        self,
        contas_pagar_list,
        column_to_update,
        column_to_check,
        regex_pattern,
        update_value_column=None
    ):
        pattern = re.compile(regex_pattern)

        updated_count = 0
        for conta in contas_pagar_list:
            column_value = getattr(conta, column_to_check.key)
            if pattern.match(column_value):
                update_value = getattr(conta, update_value_column.key) if update_value_column else None
                setattr(conta, column_to_update.key, update_value)
                updated_count += 1

        logger.info(
            "Atualizados %d registros na coluna %s.",
            updated_count,
            column_to_update.key
        )

    def process_batch(self, batch, batch_index):
        session = self.db_load.get_session()
        logger.info(
            f'Iniciando processamento do lote {batch_index} com '
            f'{len(batch)} itens'
        )

        try:
            start_time = time.time()

            existing_ids = [conta.fk_contrato_fornecedor for conta in batch]
            existing_records = {
                v.fk_contrato_fornecedor: v
                for v in session.query(ContasPagar)
                .filter(ContasPagar.fk_contrato_fornecedor.in_(existing_ids))
                .all()
            }

            logger.info(
                f'Registros existentes carregados para o lote {batch_index}'
            )

            to_update = []
            to_add = []
            for conta in batch:
                existing_conta = existing_records.get(conta.fk_contrato_fornecedor)
                if existing_conta:
                    if self.is_conta_different(existing_conta, conta):
                        self.update_existing_conta(existing_conta, conta)
                        to_update.append(conta)
                else:
                    to_add.append(conta)

            if to_update:
                valid_updates = [
                    {
                        'id': conta.id,
                        'id_contas_pagar': conta.id_contas_pagar,
                        'id_contrato': conta.id_contrato,
                        'fk_contrato_fornecedor': conta.fk_contrato_fornecedor,
                        'id_emp_cli': conta.id_emp_cli,
                        'id_fornecedor': conta.id_fornecedor,
                        'id_despesa': conta.id_despesa,
                        'data_emissao_cp': conta.data_emissao_cp,
                        'data_pagamento_cp': conta.data_pagamento_cp,
                        'situacao_cp': conta.situacao_cp,
                        'valor_docto_cp': conta.valor_docto_cp,
                        'valor_pagto_cp': conta.valor_pagto_cp,
                        'data_vencimento_cp': conta.data_vencimento_cp
                    }
                    for conta in to_update if conta.id is not None
                ]

                if valid_updates:
                    session.bulk_update_mappings(ContasPagar, valid_updates)
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
            or existing_conta.id_emp_cli != conta.id_emp_cli
            or existing_conta.id_fornecedor != conta.id_fornecedor
            or existing_conta.id_despesa != conta.id_despesa
            or existing_conta.data_emissao_cp != conta.data_emissao_cp
            or existing_conta.data_pagamento_cp != conta.data_pagamento_cp
            or existing_conta.situacao_cp != conta.situacao_cp
            or existing_conta.valor_docto_cp != conta.valor_docto_cp
            or existing_conta.valor_pagto_cp != conta.valor_pagto_cp
            or existing_conta.data_vencimento_cp != conta.data_vencimento_cp
            or existing_conta.fk_contrato_fornecedor != conta.fk_contrato_fornecedor
        )

    @staticmethod
    def update_existing_conta(existing_conta, conta):
        existing_conta.id_contrato = conta.id_contrato
        existing_conta.id_emp_cli = conta.id_emp_cli
        existing_conta.id_fornecedor = conta.id_fornecedor
        existing_conta.id_despesa = conta.id_despesa
        existing_conta.data_emissao_cp = conta.data_emissao_cp
        existing_conta.data_pagamento_cp = conta.data_pagamento_cp
        existing_conta.situacao_cp = conta.situacao_cp
        existing_conta.valor_docto_cp = conta.valor_docto_cp
        existing_conta.valor_pagto_cp = conta.valor_pagto_cp
        existing_conta.data_vencimento_cp = conta.data_vencimento_cp
        existing_conta.fk_contrato_fornecedor = conta.fk_contrato_fornecedor

    def persist_data(self):
        try:
            contas_pagar_list = self.fetch_and_transform_data()
            logger.info(
                f'Total de itens recuperados para processamento: '
                f'{len(contas_pagar_list)}'
            )

            self.update_column_based_on_regex(
                contas_pagar_list=contas_pagar_list,
                column_to_update=ContasPagar.valor_total_despesa,
                column_to_check=ContasPagar.class_fiscal,
                regex_pattern=r'6.*',
                update_value_column=ContasPagar.valor_pagto_cp
            )

            self.update_column_based_on_regex(
                contas_pagar_list=contas_pagar_list,
                column_to_update=ContasPagar.valor_total_custo,
                column_to_check=ContasPagar.class_fiscal,
                regex_pattern=r'4.*',
                update_value_column=ContasPagar.valor_pagto_cp
            )

            batch_size = 500

            with ThreadPoolExecutor(max_workers=15) as executor:
                futures = [
                    executor.submit(
                        self.process_batch,
                        contas_pagar_list[i : i + batch_size],
                        i // batch_size + 1,
                    )
                    for i in range(0, len(contas_pagar_list), batch_size)
                ]

                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f'Erro no processamento do lote: {e}')
        except Exception as e:
            logger.error(f'Erro não tratado em persist_data: {e}')
