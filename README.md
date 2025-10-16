## Платформа для анализа и визуализации данных пользователей банковского сервиса

### Цель
Создать инфраструктуру для обработки и визуализации данных о клиентах банка, включая автоматизацию процессов загрузки новых данных и обновления дашборда аналитиков.

### Технологии
```
PosgreSQL - в качестве базы данных
Airflow   - инструмент для автоматизации процессов загрузки новых данных и обновления дашборда
Superset  - для построения дашборда
```
![Схема](scheme.png)

### Задачи
# Инструкция по запуску платформы с нуля

## 1. Подготовка виртуальной машины
1. Арендуйте виртуальную машину на Selectel с достаточными ресурсами для нескольких Docker-контейнеров.
2. Подключитесь к виртуальной машине по SSH:
```bash
ssh your_user@your_vm_ip
```
3. Обновите систему:
```bash
sudo apt update && sudo apt upgrade -y
```
4. Создайте виртуальное окружение и установите необходимые пакеты:
```bash
python3 -m venv venv
source venv/bin/activate
```
4. Убедитесь, что Docker работает:
```bash
sudo systemctl start docker
sudo systemctl enable docker
docker --version
docker-compose --version
```

## 2. Установка и настройка PostgreSQL
1. Создайте хранилище (volume) для контейнера с БД:
```bash
docker volume create postgres_1_vol
```
2. Создайте базу данных и пользователя:
```bash
docker run -d \
    --name postgres_1 \
    -e POSTGRES_USER=postgres \
    -e POSTGRES_PASSWORD='123' \
    -e POSTGRES_DB=test_app \
    -v postgres_1_vol:/var/lib/postgresql \
    postgres:18
```
3. В локальном терминале прокидываем соединение:
```bash
ssh -L  8080:localhost:8080 <user_name>@<ip-address>
```
4. В браузере переходим по адресу: http://localhost:8080/login/

## 3. Настройка Airflow

1. Установите Airflow и провайдер PostgreSQL:
```bash
pip install "apache-airflow[postgres]==2.10.5" psycopg2-binary
```
2. Инициализируйте базу Airflow:
```bash
airflow db init
```
3. Создайте соединение к PostgreSQL в Airflow:
```bash
airflow connections add banker_bd \
    --conn-type postgres \
    --conn-host localhost \
    --conn-login bankuser \
    --conn-password your_password \
    --conn-schema bankusers \
    --conn-port 5432
```
4. Запустите Airflow веб-сервер и шедулер:
```bash
airflow webserver --port 8080
airflow scheduler
```
## 4. Настройка DAG для подгрузки данных из S3
1. Создайте папку `dags` и добавьте DAG (Python-файл) с логикой загрузки CSV из S3 в PostgreSQL.
2. Убедитесь, что DAG корректно подключен к `banker_bd` и расписание установлено на каждый час.
3. Включите DAG через веб-интерфейс Airflow.

## 5. Установка Superset
1. Запуск контейнера Superset:
```bash
docker run -d \
    -p 8080:8088 \
    -e "SUPERSET_SECRET_KEY=$(openssl rand -base64 42)" \
    --name superset \
    apache/superset:latest-dev
```
3. Инициализируйте Superset:
```bash
docker exec -it superset superset fab create-admin \
            --username admin \
            --firstname Superset \
            --lastname Admin \
            --email admin@superset.com \
            --password admin

docker exec -it superset superset db upgrade
```
4. Запустите Superset:
```bash
docker exec -it superset superset init
```
6. Добавьте подключение к базе `bankusers` в Superset.
7. Создайте дашборды и настройте автоматическое обновление данных каждые 60 минут.

