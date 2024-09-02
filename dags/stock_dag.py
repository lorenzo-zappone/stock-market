from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from include.stock_request import nasdaq_data
from include.setup_analysis import dave_landry_analysis

default_args={
    'owner': 'lorenzo',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    schedule_interval='@daily',  # Executa diariamente
    start_date=datetime(2024, 1, 1),  # Data de início da DAG
    catchup=False,  # Não realizar catchup de execuções passadas
    default_args=default_args,
    description='DAG para ingestão de dados da Alpha Vantage',
    tags=['nasdaq']
)
def alpha_vantage_data_ingestion_dag():
    # Define a tarefa usando PythonOperator
    fetch_data_task = PythonOperator(
        task_id='nasdaq_data',
        python_callable=nasdaq_data,
    )

    analysis_task = PythonOperator(
        task_id='dave_landry_setup',
        python_callable=dave_landry_analysis
    )

    # Define a ordem das tarefas (neste caso, só temos uma)
    fetch_data_task >> analysis_task

# Instancia a DAG
alpha_vantage_data_ingestion = alpha_vantage_data_ingestion_dag()
