from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timezone
import pandas as pd
import io
import requests
import logging

URL = "https://9c579ca6-fee2-41d7-9396-601da1103a3b.selstorage.ru/credit_clients.csv"

default_args = {
    'owner': 'Dmitriy',
}


@dag(
    dag_id="load_new_clients_hourly",
    start_date=datetime(2025, 10, 15, tzinfo=timezone.utc),
    schedule_interval="@hourly",
    catchup=False,
    tags=["clients", "postgres", "etl"],
    default_args=default_args,
)
def load_new_clients_dag():

    @task
    def load_csv_from_url() -> list:
        response = requests.get(URL)
        response.raise_for_status()
        # Определяем разделитель автоматически или явно через ","
        df = pd.read_csv(io.StringIO(response.text), sep=',')
        # Приводим имена колонок к нижнему регистру
        df.columns = [c.strip().lower() for c in df.columns]
        print("📄 Загружены колонки:", df.columns.tolist())
        return df.to_dict(orient="records")

    @task
    def get_existing_clients() -> list:
        hook = PostgresHook(postgres_conn_id="banker_bd")
        sql = "SELECT customerid FROM customers"
        df = hook.get_pandas_df(sql)
        return df["customerid"].astype(str).tolist()


    @task
    def filter_new_clients(new_clients: list, existing_clients: list) -> list:
        """
        Сравнивает новые данные с существующими.
        Возвращает только новых клиентов.
        """
        df_new = pd.DataFrame(new_clients)
        # Приводим customerid к строке
        df_new["customerid"] = df_new["customerid"].astype(str)
        # Фильтруем только тех, кого нет в существующих
        df_filtered = df_new[~df_new["customerid"].isin(existing_clients)]
        print(f"✨ Найдено новых клиентов: {len(df_filtered)}")
        return df_filtered.to_dict(orient="records")

    @task
    def insert_new_clients(clients: list) -> int:
        """
        Вставляет новые записи клиентов в таблицу PostgreSQL `customers`.
        Возвращает количество вставленных строк.
        """
        if not clients:
            logging.info("Нет новых клиентов для вставки.")
            return 0

        
        df = pd.DataFrame(clients)
        target_fields = df.columns.tolist()
        rows = df.where(pd.notnull(df), None).values.tolist()

        hook = PostgresHook(postgres_conn_id="banker_bd")
        hook.insert_rows(table="customers", rows=rows, target_fields=target_fields)
        logging.info("Успешно вставлено %d записей в customers", len(rows))
        return len(rows)

    new_clients = load_csv_from_url()
    existing_clients = get_existing_clients()
    filtered_clients = filter_new_clients(new_clients, existing_clients)
    insert_new_clients(filtered_clients)

dag = load_new_clients_dag()

