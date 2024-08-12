import json

from hook.aws.aws import S3Manager
from hook.hinova.hinova_requester_hook import (
    APIRequesterHook as api_requester_hook,
)
from operators import date_operations
from operators import parquet_transformer as parquet
from requests.exceptions import HTTPError


class DataFetcherHinova:
    def __init__(self, token, api_requester_hook=api_requester_hook):
        self.api_requester = api_requester_hook(api_token=token, connection="beneficiar_hinov")
        self.parquet_transformer = parquet.ParquetTransformer()
        self.s3_manager = S3Manager(bucket_name='beneficiar')
        self.token = token

    # def request_data_aws(self, endpoint, params=None, method='GET'):
    #     return self.api_requester.request(endpoint=endpoint, params=params, method=method)

    def transform_to_parquet(self, data, filename, column=None, subcolumn=None, id_field=None):
        self.parquet_transformer.transform_to_parquet(data, f'/tmp/{filename}.parquet', column=column, subcolumn=subcolumn, id_field=id_field)

    def upload_to_s3(self, filename):
        self.s3_manager.upload_file(f'/tmp/{filename}.parquet', f'dados/{filename}/{filename}.parquet')

    def fetch_and_upload_data(self, endpoints, filename, params_or_dates=None, column=None, id_field=None, subcolumn=None):
        resultados = []

        if params_or_dates:
            if filename == 'boletos':
                for situacao in params_or_dates:
                    quantidade_por_pagina = 0
                    erro_no_loop = False
                    while not erro_no_loop:
                        params = {
                            'quantidade_por_pagina': '4000',
                            'inicio_paginacao': f"{quantidade_por_pagina}",
                            'codigo_situacao': situacao
                        }
                        date_range_json = json.dumps(params)
                        print(date_range_json)
                        try:
                            boletos = self.api_requester.request(
                                endpoint=endpoints,
                                method='POST',
                                params=date_range_json,
                            )
                            if boletos and 'error' not in boletos:
                                resultados.append(boletos)
                                quantidade_por_pagina += 1
                            else:
                                print(f'Erro na requisição para a situação {situacao}: {boletos}')
                                erro_no_loop = True
                        except HTTPError as e:
                            print(f"Erro de HTTP durante a requisição: {e}")
                            erro_no_loop = True

            elif isinstance(params_or_dates, tuple):
                if len(params_or_dates) == 2 and isinstance(params_or_dates[1], int):
                    num_months, name = params_or_dates
                    parameters = None
                elif len(params_or_dates) == 2:
                    num_months, parameters = params_or_dates
                    name = 1
                elif len(params_or_dates) == 3 and isinstance(params_or_dates[2], int):
                    num_months, parameters, name = params_or_dates
                else:
                    num_months, parameters = params_or_dates
                    name = None
                dates = date_operations.DateOperations().get_date_ranges(num_months, parameters, name)
                for date in dates:
                    data = self.api_requester.request(endpoint=endpoints, params=date, method='POST')
                    resultados.append(data)
                    print(f'{filename}: {date}')
            elif isinstance(params_or_dates, list):
                for param in params_or_dates:
                    data = self.api_requester.request(endpoint=endpoints, params=param, method='POST')
                    resultados.append(data)
                    print(f'{filename}: {param}')
        else:
            for endpoint in endpoints:
                data = self.api_requester.request(endpoint=endpoint, method='GET')
                resultados.append(data)
                print(f'{filename}: {endpoint}')
        self.transform_to_parquet(resultados, filename, column=column, subcolumn=subcolumn, id_field=id_field)
        self.upload_to_s3(filename)
