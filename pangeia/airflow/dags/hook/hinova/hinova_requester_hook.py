import requests
from airflow.hooks.base import BaseHook


class APIRequesterHook(BaseHook):
    def __init__(self, api_token, connection):
        conn = self.get_connection(f'{connection}')
        self.api_url = conn.host
        self.api_token = api_token

    def request(self, endpoint, params=None, method='GET', headers=None):
        if headers is None: headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.api_token}'
        }

        url = f'{self.api_url}/{endpoint}'

        if method == 'GET':
            response = requests.get(url, headers=headers, data=params)
        elif method == 'POST' and endpoint == 'usuario/autenticar':
            response = requests.post(url, headers=headers, json=params)
            print(response)
        elif method == 'POST':
            response = requests.post(url, headers=headers, data=params)
            print(response)
        else:
            raise ValueError("Unsupported HTTP method")

        response.raise_for_status()
        return response.json()
