from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.glue_crawler import GlueCrawlerHook
from config import config as cfg
from hook.hinova.hinova_authenticator_hook import APIAuthenticatorHook
from operators.hinova_transformer import DataFetcherHinova

default_args = {
    'owner': 'Matheus',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 17),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': True
}

dag = DAG(
    'Beneficiar-Hinova',
    default_args=default_args,
    description='Pipeline de processamento de dados da Hinova',
    schedule="@hourly",
)


def Auth(**context):
    hook = APIAuthenticatorHook()
    token = hook.authenticate("usuario/autenticar", "beneficiar_hinov")
    context['ti'].xcom_push(key="token_beneficiar", value=token)


def Associados(**context):
    token = context['ti'].xcom_pull(task_ids="Auth", key="token_beneficiar")
    data_fetcherHinova = DataFetcherHinova(token=token)
    data_fetcherHinova.fetch_and_upload_data(
        cfg.hinova_associados,
        'associados',
        cfg.hinova_associados_parametros,
        'associados')


def Veiculos(**context):
    token = context['ti'].xcom_pull(task_ids="Auth", key="token_beneficiar")
    data_fetcherHinova = DataFetcherHinova(token=token)
    data_fetcherHinova.fetch_and_upload_data(
        cfg.hinova_veiculos,
        'veiculos',
        cfg.hinova_veiculos_parametros,
        'veiculos')


def Voluntarios(**context):
    token = context['ti'].xcom_pull(task_ids="Auth", key="token_beneficiar")
    data_fetcherHinova = DataFetcherHinova(token=token)
    data_fetcherHinova.fetch_and_upload_data(
        cfg.hinova_voluntarios,
        'voluntarios')


def Alteracao_Veiculos(**context):
    token = context['ti'].xcom_pull(task_ids="Auth", key="token_beneficiar")
    data_fetcherHinova = DataFetcherHinova(token=token)
    data_fetcherHinova.fetch_and_upload_data(
        cfg.hinova_veiculos_alterados,
        'alteracao_veiculos',
        (24, cfg.hinova_veiculos_alterados_parametros))


def Mgf_Eventos(**context):
    token = context['ti'].xcom_pull(task_ids="Auth", key="token_beneficiar")
    data_fetcherHinova = DataFetcherHinova(token=token)
    data_fetcherHinova.fetch_and_upload_data(
        cfg.hinova_mgf_listar_lancamentos,
        'mgf_eventos_parcelas',
        (24, 2),
        'retorno',
        'codigo_lancamento',
        'parcelas')


def Eventos(**context):
    token = context['ti'].xcom_pull(task_ids="Auth", key="token_beneficiar")
    data_fetcherHinova = DataFetcherHinova(token=token)
    data_fetcherHinova.fetch_and_upload_data(
        cfg.hinova_eventos_listar,
        'eventos',
        (24, 3))


def Associados_Eventos(**context):
    token = context['ti'].xcom_pull(task_ids="Auth", key="token_beneficiar")
    data_fetcherHinova = DataFetcherHinova(token=token)
    data_fetcherHinova.fetch_and_upload_data(
        cfg.hinova_eventos_listar,
        'associados_eventos',
        (24, 3),
        'associado',
        'codigo_evento')


def Veiculos_Eventos(**context):
    token = context['ti'].xcom_pull(task_ids="Auth", key="token_beneficiar")
    data_fetcherHinova = DataFetcherHinova(token=token)
    data_fetcherHinova.fetch_and_upload_data(
        cfg.hinova_eventos_listar,
        'veiculos_eventos',
        (24, 3),
        'veiculo',
        'codigo_evento')


def Voluntarios_Eventos(**context):
    token = context['ti'].xcom_pull(task_ids="Auth", key="token_beneficiar")
    data_fetcherHinova = DataFetcherHinova(token=token)
    data_fetcherHinova.fetch_and_upload_data(
        cfg.hinova_eventos_listar,
        'voluntarios_eventos',
        (24, cfg.hinova_eventos_alterados_parametros, 3),
        'voluntario',
        'codigo_evento')


def Eventos_Alterados(**context):
    token = context['ti'].xcom_pull(task_ids="Auth", key="token_beneficiar")
    data_fetcherHinova = DataFetcherHinova(token=token)
    data_fetcherHinova.fetch_and_upload_data(
        cfg.hinova_eventos_alterados,
        'eventos_alterados',
        (24, cfg.hinova_eventos_alterados_parametros),
        'resultado')


def Boletos(**context):
    token = context['ti'].xcom_pull(task_ids="Auth", key="token_beneficiar")
    data_fetcherHinova = DataFetcherHinova(token=token)
    data_fetcherHinova.fetch_and_upload_data(
        cfg.hinova_boletos,
        'boletos',
        cfg.hinova_boletos_parametros)


glue_crawler_hook = GlueCrawlerHook(
    aws_conn_id="aws_default",
)


def Glue_Crawler():
    glue_crawler_hook.start_crawler(crawler_name=cfg.CRAWLER)
    print(f"O crawler '{cfg.CRAWLER}' foi ativado com sucesso.")


Glue_Crawler = PythonOperator(
    task_id='Glue-Crawler',
    python_callable=Glue_Crawler,
    dag=dag,
)

Auth = PythonOperator(
    task_id='Auth',
    python_callable=Auth,
    dag=dag,
    provide_context=True,
)

Associados = PythonOperator(
    task_id='Associados',
    python_callable=Associados,
    dag=dag,
    provide_context=True
)

Veiculos = PythonOperator(
    task_id="Veiculos",
    python_callable=Veiculos,
    dag=dag,
    provide_context=True
)

Voluntarios = PythonOperator(
    task_id='Voluntarios',
    python_callable=Voluntarios,
    dag=dag,
    provide_context=True
)

Alteracao_Veiculos = PythonOperator(
    task_id='Alteracao-Veiculos',
    python_callable=Alteracao_Veiculos,
    dag=dag,
    provide_context=True
)

Mgf_Eventos = PythonOperator(
    task_id='Mgf-Eventos',
    python_callable=Mgf_Eventos,
    dag=dag,
    provide_context=True
)

Eventos = PythonOperator(
    task_id='Eventos',
    python_callable=Eventos,
    dag=dag,
    provide_context=True
)

Associados_Eventos = PythonOperator(
    task_id='Associados-Eventos',
    python_callable=Associados_Eventos,
    dag=dag,
    provide_context=True
)

Veiculos_Eventos = PythonOperator(
    task_id='Veiculos-Eventos',
    python_callable=Veiculos_Eventos,
    dag=dag,
    provide_context=True
)

Voluntarios_Eventos = PythonOperator(
    task_id='Voluntarios-Eventos',
    python_callable=Voluntarios_Eventos,
    dag=dag,
    provide_context=True
)

Eventos_Alterados = PythonOperator(
    task_id='Eventos-Alterados',
    python_callable=Eventos_Alterados,
    dag=dag,
    provide_context=True
)

Boletos = PythonOperator(
    task_id='Boletos',
    python_callable=Boletos,
    dag=dag,
    provide_context=True
)

Auth >> [Associados, Veiculos, Voluntarios, Alteracao_Veiculos, Mgf_Eventos, Eventos, Associados_Eventos, Veiculos_Eventos, Voluntarios_Eventos, Eventos_Alterados, Boletos] >> Glue_Crawler
