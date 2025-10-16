from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models.dag import DAG
import pendulum
from datetime import datetime

# Установка параметров DAG
default_args = {
    'owner': 'Daria',
    'start_date': datetime(2025, 10, 16),
}

with DAG(
    dag_id='test_db_connection_dag',
    default_args=default_args, 
    start_date=pendulum.datetime(2025, 10, 16, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=['test'],
) as dag:
    test_task = PostgresOperator(
        task_id='test_postgres_connection',
        postgres_conn_id='banker_bd',  
        sql="SELECT 1;",
    )
