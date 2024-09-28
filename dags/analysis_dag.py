from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from include.setup_analysis import dave_landry_analysis

default_args={
    'owner': 'lorenzo',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    schedule_interval=None,  # Executa diariamente
    start_date=datetime(2024, 1, 1),  # Data de início da DAG
    catchup=False,  # Não realizar catchup de execuções passadas
    default_args=default_args,
    description='DAG para análise de dados da Alpha Vantage',
    tags=['nasdaq']
)


def analysis_dag():
    
    # Define a tarefa usando PythonOperator
    analysis_task = PythonOperator(
        task_id='dave_landry_setup',
        python_callable=dave_landry_analysis,
    )
    
    # Define a ordem das tarefas 
    analysis_task 

# Instancia a DAG
analysis_dag = analysis_dag()
