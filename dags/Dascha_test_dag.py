# Импорт библиотек
from airflow.decorators import task, dag
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
from datetime import datetime 
import logging

# Установка параметров DAG
default_args = {
    'owner': 'Daria',
    'start_date': datetime(2025, 10, 14),
}

# Константы
directory = '/home/dariya/test'
postgres_id = 'banker_bd'

# вспомогательная функция, используемая в задачах
def save_query_csv_task(sql_name:str, sql: str, file_path: str):
    """
    Функция выполняет SQL-запрос и записывает результат в csv файл.
    """
    # создание экземпляра хука - подключение к БД
    hook = PostgresHook(postgres_conn_id=postgres_id)
    try:
        #проверка / создания директории
        os.makedirs(directory, exist_ok=True)
        logging.info(f"Выполняется экспорт по запросу: {sql_name}")
        hook.copy_expert(sql, filename=file_path)
        logging.info(f"Данные успешно сохранены в {file_path}")
    except Exception as e:
        logging.error(f"Ошибка при экспорте данных: {e}")
        raise

@dag(dag_id='postgres_dag_test_2', default_args=default_args, schedule_interval="@once", catchup=False, tags=['my_dags'])
def postgres_dag_flow():

    @task()    
    def total_revenue_task():
        """
        Функция содержит SQL-запрос для формирования таблицы общей выручки с полетов по дням.
        В результате вызывает функцию сохранения данных запроса в csv файл.
        """
        sql = """
            COPY (
                SELECT *
                    FROM customers
            ) TO STDOUT WITH CSV HEADER
            """
        sql_name = 'customers_test_3'
        save_query_csv_task(sql_name, sql, f"{directory}/{sql_name}.csv")
    
    total_revenue = total_revenue_task()

    total_revenue 

dag = postgres_dag_flow()