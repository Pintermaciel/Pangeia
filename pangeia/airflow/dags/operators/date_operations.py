import datetime
import json


class DateOperations:
    def __init__(self):
        self.today = datetime.datetime.today()

    def _get_first_and_last_day(self):
        first_day = self.today.replace(day=1)
        if first_day.month == self.today.month:
            last_day = self.today
        else:
            last_day = (first_day + datetime.timedelta(days=32)).replace(day=1) - datetime.timedelta(days=1)
        return first_day, last_day

    def _generate_date_range_json(self, start_key, end_key, hinova_parametros=None):
        first_day, last_day = self._get_first_and_last_day()
        date_range = {
            start_key: first_day.strftime('%d/%m/%Y'),
            end_key: last_day.strftime('%d/%m/%Y')
        }
        if hinova_parametros:
            date_range['campos'] = hinova_parametros
        return json.dumps(date_range) if hinova_parametros else json.dumps({start_key: first_day.strftime('%d/%m/%Y'), end_key: last_day.strftime('%d/%m/%Y')})

    def get_date_ranges(self, meses, hinova_parametros=None, name=1):
        date_ranges = []
        start_key = ""
        end_key = ""

        if name == 1:
            start_key = 'data_inicial'
            end_key = 'data_final'
        elif name == 2:
            start_key = 'data_vencimento_inicial'
            end_key = 'data_vencimento_final'
        elif name in [3, 4]:
            start_key = 'data_cadastro'
            end_key = 'data_cadastro_final'

        for _ in range(meses):
            date_range_json = self._generate_date_range_json(start_key, end_key, hinova_parametros)
            date_ranges.append(date_range_json)
            self.today = self.today.replace(day=1) - datetime.timedelta(days=1)

        return date_ranges
