## Платформа для анализа и визуализации данных пользователей банковского сервиса

### Цель
Создать инфраструктуру для обработки и визуализации данных о клиентах банка, включая автоматизацию процессов загрузки новых данных и обновления дашборда аналитиков.

### Технологии
PosgreSQL - в качестве базы данных
Airflow - инструмента для автоматизации процессов загрузки новых данных и обновления дашборда
Superset - для построения дашборда

![Схема](scheme.png)

### Задачи
# Инструкция по запуску платформы с нуля

## 1. Подготовка виртуальной машины
1. Арендуйте виртуальную машину на Selectel с достаточными ресурсами для нескольких Docker-контейнеров.
2. Подключитесь к виртуальной машине по SSH:
ssh your_user@your_vm_ip
3. Обновите систему и установите необходимые пакеты:
sudo apt update && sudo apt upgrade -y
sudo apt install -y python3 python3-venv python3-pip docker.io docker-compose git
4. Убедитесь, что Docker работает:
sudo systemctl start docker
sudo systemctl enable docker
docker --version
docker-compose --version

## 2. Установка и настройка PostgreSQL
1. Установите PostgreSQL:
sudo apt install -y postgresql postgresql-contrib
2. Создайте базу данных и пользователя:
sudo -i -u postgres
psql
CREATE DATABASE bankusers;
CREATE USER bankuser WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE bankusers TO bankuser;
\q
exit

## 3. Настройка Airflow
1. Создайте виртуальное окружение:
python3 -m venv venv
source venv/bin/activate
2. Установите Airflow и провайдер PostgreSQL:
pip install "apache-airflow[postgres]==2.7.3" psycopg2-binary
3. Инициализируйте базу Airflow:
airflow db init
4. Создайте соединение к PostgreSQL в Airflow:
airflow connections add banker_bd \
    --conn-type postgres \
    --conn-host localhost \
    --conn-login bankuser \
    --conn-password your_password \
    --conn-schema bankusers \
    --conn-port 5432
5. Запустите Airflow веб-сервер и шедулер:
airflow webserver --port 8080
airflow scheduler

## 4. Настройка DAG для подгрузки данных из S3
1. Создайте папку `dags` и добавьте DAG (Python-файл) с логикой загрузки CSV из S3 в PostgreSQL.
2. Убедитесь, что DAG корректно подключен к `banker_bd` и расписание установлено на каждый час.
3. Включите DAG через веб-интерфейс Airflow.

## 5. Установка Superset
1. Установите Superset в виртуальное окружение:
pip install apache-superset
2. Инициализируйте Superset:
superset db upgrade
superset fab create-admin
superset init
3. Запустите Superset:
superset run -p 8088 --with-threads --reload --debugger
4. Добавьте подключение к базе `bankusers` в Superset.
5. Создайте дашборды и настройте автоматическое обновление данных каждые 60 минут.

