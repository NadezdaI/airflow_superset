from airflow import DAG
from airflow.operators.python import PythonOperator
import os
from datetime import datetime

# Базовая директория для сохранения данных
DATA_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', 'data')
)

# Установка параметров DAG
default_args = {
    'owner': 'Dmitriy',
    'start_date': datetime(2025, 10, 15),
}

# Определение функции, которая создает папку test_dir
def create_directory():
    os.makedirs(DATA_DIR, exist_ok=True)

def create_file():
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(os.path.join(DATA_DIR, 'test.txt'), 'a') as f:
        f.write('Ok!')

# Создание DAG
with DAG(dag_id='create_dir', default_args=default_args, schedule_interval=None, catchup=False) as dag:

    # Создание задачи с помощью PythonOperator
    create_directory_task = PythonOperator( # <- оператор в данном случае – функция python
        task_id='create_test_dir_task',     # <- id задачи
        python_callable=create_directory    # <- название функции создания папки
    )
    # Создание задачи с помощью PythonOperator
    create_test_file_task = PythonOperator(
        task_id='create_test_file_task',
        python_callable=create_file
    )

# Установка порядка выполнения задач
create_directory_task >> create_test_file_task