import logging
import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import time
from datetime import timedelta
from sqlalchemy import create_engine, text

OWNER = "tyopa"
DAG_ID = "add_dim_users_data_v2"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 6, 10, tz="Europe/Moscow"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

def sync_dim_users() -> None:
    """
    Синхронизирует таблицу ods.dim_users с актуальными данными из raw.raw_users.

    Использует staging таблицу в PostgreSQL для эффективного обновления данных.
    Вся логика выполняется на стороне базы данных с помощью SQL-запросов.

    :return: dict: Статистика выполнения операции
    """
    # Подключение к базе данных
    db_url = "postgresql://postgres:postgres@postgres_dwh:5432/postgres"
    engine = create_engine(db_url)

    start_time = time.time()
    with engine.begin() as conn:
        # Шаг 1: Создание временной таблицы с актуальными данными
        logger.info("Создание временной таблицы с актуальными данными")
        conn.execute(text("""
            DROP TABLE IF EXISTS stg.tmp_users;

            CREATE TABLE stg.tmp_users AS
            WITH latest_users AS (
                SELECT DISTINCT ON (id)
                    id,
                    created_at,
                    updated_at,
                    first_name,
                    last_name,
                    middle_name,
                    birthday::date,
                    email,
                    ts_db
                FROM raw.raw_users
                ORDER BY id, ts_db DESC  -- Используем ts_db для определения актуальности
            )
            SELECT * FROM latest_users;

            -- Добавляем индекс для ускорения операций
            CREATE INDEX idx_tmp_users_id ON stg.tmp_users (id);
        """))

        # Шаг 2: Получаем количество записей для статистики
        result = conn.execute(text("SELECT COUNT(*) FROM stg.tmp_users"))
        records_count = result.scalar()

        # Шаг 3: Выполняем MERGE (UPSERT) операцию для обновления ods.dim_users
        logger.info("Выполнение синхронизации данных")
        conn.execute(text("""
            INSERT INTO ods.dim_users (
                id, created_at, updated_at, first_name, last_name,
                middle_name, birthday, email
            )
            SELECT
                id, created_at, updated_at, first_name, last_name,
                middle_name, birthday, email
            FROM stg.tmp_users
            ON CONFLICT (id) DO UPDATE SET
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at,
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                middle_name = EXCLUDED.middle_name,
                birthday = EXCLUDED.birthday,
                email = EXCLUDED.email
            ;
        """))

        # Шаг 4: Удаляем временную таблиц
        logger.info("Очистка временных данных")
        conn.execute(text("DROP TABLE IF EXISTS stg.tmp_users"))

        execution_time = time.time() - start_time

        logger.info(f"Синхронизация завершена: обработано {records_count} записей за {execution_time:.2f} секунд")

def add_dim_users_data():
    logger.info("Выполняется синхронизация...")
    sync_dim_users()
    logger.info("Ожидание минуты до следующего обновления...")

with DAG(
        dag_id=DAG_ID,
        schedule=timedelta(minutes=1),
        default_args=args,
        tags=["s3", "ods", "pg", "json"],
        max_active_tasks=1,
        max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    add_dim_users_data = PythonOperator(
        task_id="add_dim_users_data",
        python_callable=add_dim_users_data,
    )

    end = EmptyOperator(task_id="end")

    start >> add_dim_users_data >> end