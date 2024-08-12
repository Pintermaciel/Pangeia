import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


class ParquetTransformer:
    def _extract_data(self, data, column=None, subcolumn=None, id_field=None):
        extracted_data = []

        if column and not subcolumn and isinstance(data, dict):
            column_data = data.get(column)
            if isinstance(column_data, list):
                extracted_data.extend(column_data)

        if column and not subcolumn and isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    column_data = item.get(column)
                    extracted_data.append(column_data)

        if column and subcolumn and isinstance(data, dict):
            column_data = data.get(column)
            processed_ids = set()
            if isinstance(column_data, list):
                for item in column_data:
                    if isinstance(item, dict):
                        parcelas = item.get(subcolumn)
                        if isinstance(parcelas, list):
                            for parcela in parcelas:
                                parcela_id = parcela.get('codigo_parcela')
                                if parcela_id not in processed_ids:
                                    temp_dict = parcela.copy()
                                    temp_dict[id_field] = item.get(id_field)
                                    extracted_data.append(temp_dict)
                                    processed_ids.add(parcela_id)

        if not column:
            if isinstance(data, list):
                extracted_data.extend(data)
            elif isinstance(data, dict):
                extracted_data.append(data)
        return extracted_data

    def _write_parquet(self, data, output_path, concat=True):
        if concat:
            data.to_parquet(output_path)
        else:
            table = pa.Table.from_pandas(data)
            pq.write_table(table, output_path)

    def _convert_data_types(self, data):
        for column_name in data.columns:
            if data[column_name].dtype == 'object':
                data[column_name] = data[column_name].astype(str)
            elif data[column_name].dtype == 'datetime64[ns]':
                data[column_name] = data[column_name].astype('datetime64[ns]')
            elif data[column_name].dtype == 'Int64':
                data[column_name] = data[column_name].astype('Int64')
        return data

    def transform_to_parquet(self, json_data_list, output_path, column=None, subcolumn=None, id_field=None, concat=True):
        all_lines = []

        for data in json_data_list:
            extracted_data = self._extract_data(data, column, subcolumn, id_field)
            all_lines.extend(extracted_data)
        all_lines = [np.nan if x is None else x for x in all_lines]
        all_lines = [{} if isinstance(x, float) else x for x in all_lines]
        all_lines_df = pd.DataFrame(all_lines)
        all_lines_df = self._convert_data_types(all_lines_df)
        print(all_lines_df)

        self._write_parquet(all_lines_df, output_path, concat)
        print(f'Dados transformados e salvos em {output_path}')
