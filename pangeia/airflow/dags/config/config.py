import json
import os

from dotenv import load_dotenv

load_dotenv()

hinova_api_url = os.getenv('HINOVA_API_URL')
hinova_token = os.getenv('HINOVA_TOKEN_API')


''' AUTENTICAÇÃO '''

# Endpoint
hinova_autenticacao = os.getenv('HINOVA_ENDPOINT_AUTENTICACAO')

# Parametros
usuario = str(os.getenv('HINOVA_USUARIO_BENEFICIAR_USUARIO'))
senha = str(os.getenv('HINOVA_USUARIO_BENEFICIAR_SENHA'))

# Criar um dicionário com os dados
dados_envio = {
    "usuario": usuario,
    "senha": senha
}
# Converter o dicionário para uma string JSON
hinova_usuario_beneficiar = json.dumps(dados_envio)

''' AUTENTICAÇÃO GOOGLE'''
token_google = os.getenv('CREDENCIAL_TOKEN_GOOGLE')
scopes_google = ['https://www.googleapis.com/auth/spreadsheets.readonly']

''' ASSOCIADOS '''

# Endpoint
hinova_associados = os.getenv('HINOVA_ENDPOINT_ASSOCIADOS')

# Parametros
hinova_associados_parametros = [
    os.getenv('HINOVA_COD_ASSOCIADO_ATIVO'),
    os.getenv('HINOVA_COD_ASSOCIADO_INATIVO'),
    os.getenv('HINOVA_COD_ASSOCIADO_PENDENTE'),
    os.getenv('HINOVA_COD_ASSOCIADO_INADIMPLENTE'),
    os.getenv('HINOVA_COD_ASSOCIADO_NEGADO'),
    os.getenv('HINOVA_COD_ASSOCIADO_AGUARDANDO_PRIMEIRO_PAGAMENTO')
]

''' VEICULOS'''

# Endpoint
hinova_veiculos = os.getenv('HINOVA_ENDPOINT_VEICULOS')

# Parametros
hinova_veiculos_parametros = [
    os.getenv('HINOVA_COD_VEICULOS_ATIVO'),
    os.getenv('HINOVA_COD_VEICULOS_INATIVO'),
    os.getenv('HINOVA_COD_VEICULOS_PENDENTE'),
    os.getenv('HINOVA_COD_VEICULOS_INADIMPLENTE'),
    os.getenv('HINOVA_COD_VEICULOS_NEGADO'),
    os.getenv('HINOVA_COD_VEICULOS_AGUARDANDO_PRIMEIRO_PAGAMENTO')
]

''' VOLUNTARIOS'''

# Endpoint
hinova_voluntarios = [
    os.getenv('HINOVA_ENDPOINT_VOLUNTARIOS_ATIVO'),
    os.getenv('HINOVA_ENDPOINT_VOLUNTARIOS_INATIVO')
]

''' ALTERACAO VEICULOS '''

# Endpoint
hinova_veiculos_alterados = os.getenv('HINOVA_ENDPOINT_VEICULOS_ALTERADOS')

# Parametros
hinova_veiculos_alterados_parametros = [
    os.getenv('HINOVA_VEICULOS_ALTERADOS_CAMPOS'),
]

'''MGF-Listar Lançamentos'''
hinova_mgf_listar_lancamentos = os.getenv('HINOVA_ENDPOINT_MGF_LISTAR_LANCAMENTO')

''' EVENTOS'''
# Endpoint
hinova_eventos_listar = os.getenv('HINOVA_ENDPOINT_EVENTOS')
hinova_eventos_alterados = os.getenv('HINOVA_ENDPOINT_EVENTOS_ALTERADOS')

# Parametros
hinova_eventos_alterados_parametros = [
    os.getenv('HINOVA_EVENTOS_ALTERADOS_CAMPOS'),
]

hinova_boletos_parametros = [os.getenv('HINOVA_BOLETOS_BAIXADO'),
    os.getenv('HINOVA_BOLETOS_ABERTO'),
    os.getenv('HINOVA_BOLETOS_CANCELADO'),
    os.getenv('HINOVA_BOLETOS_BAIXADO_CPENDENCIA'),
    os.getenv('HINOVA_BOLETOS_EXCLUIDO')
]

hinova_boletos = os.getenv('HINOVA_ENDPOINT_BOLETOS')


''' AWS '''

# Configurações AWS
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')

# Configurações S3
S3_BUCKET = os.getenv('S3_BUCKET')

CRAWLER = os.getenv('CRAWLER')
