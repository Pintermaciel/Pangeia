import logging

from models.connection import DatabaseConnection
from models.Financeiro.contaspagar import ContasPagar
from sqlalchemy import MetaData, Table, select

logger = logging.getLogger(__name__)


class ContasPagarQuery:
    def __init__(self):
        self.db = DatabaseConnection()
        self.metadata = MetaData()
        self.titulos_pagar = Table('TITULOS_PAGAR', self.metadata, autoload_with=self.db.engine)
        self.un_titulos_pagar = Table('UN_TITULOS_PAGAR', self.metadata, autoload_with=self.db.engine)
        self.un_plano_contas = Table('UN_PLANO_CONTAS', self.metadata, autoload_with=self.db.engine)

    def fetch_and_transform_data(self):
        session = self.db.get_session()

        try:
            # Primeira consulta: TITULOS_PAGAR
            query_titulos_pagar = select(
                self.titulos_pagar.c.NUM_INTERNO.label('ID_CONTAS_PAGAR'),
                self.titulos_pagar.c.COD_EMPRESA.label('ID_EMP_CLI'),
                self.titulos_pagar.c.COD_CLI_FOR.label('ID_FORNECEDOR'),
                self.titulos_pagar.c.DAT_EMISSAO.label('DATA_EMISSAO_CP'),
                self.titulos_pagar.c.DAT_ULTIMO_PAGAMENTO.label('DATA_PAGAMENTO_CP'),
                self.titulos_pagar.c.SIT_TITULO.label('SITUACAO_CP'),
                self.titulos_pagar.c.VCT_ORIGINAL.label('DATA_VENCIMENTO_CP'),
                self.titulos_pagar.c.VLR_ORIGINAL.label('VALOR_DOCTO_CP'),
                self.titulos_pagar.c.TOT_VALOR_PAGO.label('VALOR_PAGTO_CP')
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

            contas_pagar_list = []
            for row in result_titulos_pagar:
                chave = (row.ID_CONTAS_PAGAR, row.ID_EMP_CLI)
                cod_conta_gerencial = un_titulos_map.get(chave)

                if cod_conta_gerencial is None:
                    logger.warning(f"Chave {chave} não encontrada em un_titulos_map")
                    # Adicionando log para verificar dados do TITULOS_PAGAR
                    logger.debug(f"Dados do TITULOS_PAGAR: ID_CONTAS_PAGAR={row.ID_CONTAS_PAGAR}, ID_EMP_CLI={row.ID_EMP_CLI}")
                else:
                    id_despesa = plano_contas_map.get(cod_conta_gerencial)
                    if id_despesa is None:
                        logger.warning(f"cod_conta_gerencial {cod_conta_gerencial} não encontrado em plano_contas_map")

                    contas_pagar = ContasPagar(
                        id_contas_pagar=row.ID_CONTAS_PAGAR,
                        id_contrato=1,  # Defina isso de acordo com a lógica de mapeamento
                        id_emp_cli=row.ID_EMP_CLI,
                        id_fornecedor=row.ID_FORNECEDOR,
                        id_despesa=id_despesa,
                        data_emissao_cp=row.DATA_EMISSAO_CP,
                        data_pagamento_cp=row.DATA_PAGAMENTO_CP,
                        situacao_cp=row.SITUACAO_CP,
                        valor_docto_cp=row.VALOR_DOCTO_CP,
                        valor_pagto_cp=row.VALOR_PAGTO_CP,
                        data_vencimento_cp=row.DATA_VENCIMENTO_CP
                    )
                    contas_pagar_list.append(contas_pagar)

            return contas_pagar_list
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise
        finally:
            logger.info("Closing the session")
            session.close()
