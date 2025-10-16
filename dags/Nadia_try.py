from __future__ import annotations
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook

import csv
import io
import logging
import requests
from datetime import timedelta

POSTGRES_CONN_ID = "banker_bd"
CSV_URL = "https://9c579ca6-fee2-41d7-9396-601da1103a3b.selstorage.ru/credit_clients.csv"

default_args = {
    'owner': 'Nadia',
}

@dag(
    dag_id="s3_parsing",
    schedule=timedelta(hours=1),
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["S3", "customers", "hourly"],
)
def s3_customers_ingestion():
    @task
    def load_new_customers():
        # 1. Скачиваем CSV с S3
        resp = requests.get(CSV_URL, timeout=60)
        resp.raise_for_status()

        reader = csv.DictReader(io.StringIO(resp.text), delimiter=";")
        rows = list(reader)
        logging.info(f"Загружено {len(rows)} строк из S3")
        return rows

    @task
    def update_postgres(rows: list[dict]):
        # 2. Подключаемся к PostgreSQL
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        inserted = 0
        for row in rows:
            try:
                cursor.execute(
                    """
                    INSERT INTO customers (
                        date, customerid, surname, creditscore, geography, gender,
                        age, tenure, balance, numofproducts, hascrcard, isactivemember,
                        estimatedsalary, exited
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (customerid) DO UPDATE
                    SET
                        date = EXCLUDED.date,
                        surname = EXCLUDED.surname,
                        creditscore = EXCLUDED.creditscore,
                        geography = EXCLUDED.geography,
                        gender = EXCLUDED.gender,
                        age = EXCLUDED.age,
                        tenure = EXCLUDED.tenure,
                        balance = EXCLUDED.balance,
                        numofproducts = EXCLUDED.numofproducts,
                        hascrcard = EXCLUDED.hascrcard,
                        isactivemember = EXCLUDED.isactivemember,
                        estimatedsalary = EXCLUDED.estimatedsalary,
                        exited = EXCLUDED.exited;
                    """,
                    (
                        row.get("Date"),
                        int(row.get("CustomerId")),
                        row.get("Surname"),
                        int(row.get("CreditScore")),
                        row.get("Geography"),
                        row.get("Gender"),
                        int(row.get("Age")),
                        int(row.get("Tenure")),
                        round(float(row.get("Balance")), 2),
                        int(row.get("NumOfProducts")),
                        int(row.get("HasCrCard")),
                        int(row.get("IsActiveMember")),
                        round(float(row.get("EstimatedSalary")), 2),
                        int(row.get("Exited")),
                    ),
                )
                inserted += 1
            except Exception as e:
                logging.warning(f"Ошибка при вставке строки {row.get('CustomerId')}: {e}")

        conn.commit()
        cursor.close()
        conn.close()
        logging.info(f"Успешно обновлено {inserted} строк в Postgres")

    rows = load_new_customers()
    update_postgres(rows)

s3_customers_ingestion()