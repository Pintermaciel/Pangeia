import re

from airflow.hooks.base import BaseHook
from hook.hinova.hinova_requester_hook import (
    APIRequesterHook as api_requester_hook,
)


class APIAuthenticatorHook(BaseHook):
    def __init__(self, api_requester_hook=api_requester_hook, headers=None):
        self.api_requester_hook = api_requester_hook
        self.headers = headers or {}
        self.token = None

    def authenticate(self, endpoint, connection):
        conn = self.get_connection(f'{connection}')
        usuario = conn.login
        senha = conn.password
        dados_envio = {
            "usuario": usuario,
            "senha": senha
        }
        extra_cleaned = conn.extra.strip('{}').replace('"', "")
        match = re.search(r'Authorization: Bearer (.+)', extra_cleaned)
        if match:
            token = match.group(1)
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {token}'
        }
        api_requester_hook_instance = api_requester_hook(headers, connection)
        self.token = api_requester_hook_instance.request(
            endpoint=endpoint,
            method='POST',
            params=dados_envio,
            headers=headers
        )
        return self.token['token_usuario']
