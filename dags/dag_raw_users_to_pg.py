import logging
import pendulum
from airflow import DAG
import psycopg2
from psycopg2 import sql
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import datetime
from datetime import timedelta
import random
import uuid
import pandas as pd
from faker import Faker
from sqlalchemy import create_engine, text

OWNER = "tyopa"
DAG_ID = "add_new_or_update_user"

PG_HOST = "postgres_dwh"
PG_PORT = 5432
PG_DATABASE = "postgres"
PG_USER = "postgres"
PG_PASSWORD = "postgres"
fake = Faker(locale="ru_RU")

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

def add_new_user() -> None:
    """
    Создает нового пользователя и добавляет в базу данных.

    Генерирует случайные данные пользователя при помощи библиотеки Faker
    и добавляет запись в таблицу raw.raw_users в PostgreSQL.

    :return: None
    """
    list_of_dict = []
    first_date = fake.date_time_ad(
        start_datetime=datetime.date(year=2024, month=1, day=1),
        end_datetime=datetime.date(year=2025, month=1, day=1),
    )
    dict_ = {
        "id": uuid.uuid4(),
        "created_at": first_date,
        "updated_at": first_date,
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "middle_name": fake.middle_name(),
        "birthday": fake.date_time_ad(
            start_datetime=datetime.date(year=1980, month=1, day=1),
            end_datetime=datetime.date(year=2005, month=1, day=1),
        ),
        "email": fake.email(),
        "ts_db": datetime.datetime.now(tz=datetime.UTC),
    }

    list_of_dict.append(dict_)

    df = pd.DataFrame(list_of_dict)

    df["birthday"] = df["birthday"].dt.strftime("%Y-%m-%d")

    df.to_sql(
        schema="raw",
        name="raw_users",
        con="postgresql://postgres:postgres@postgres_dwh:5432/postgres",
        if_exists="append",
        index=False,
    )

    logging.info(f'Создан новый пользователь с id {dict_["id"]}')


def update_info_about_current_user() -> None:
    """
    Обновляет информацию о случайном пользователе, реализуя CDC-подобную логику.

    Выбирает случайного пользователя из базы данных, модифицирует одно или
    несколько полей и добавляет новую запись с тем же ID, но обновленными данными.
    Поле updated_at устанавливается в текущую дату и время. Для обновления значений
    полей используется библиотека Faker.

    :return: None
    """
    # Подключение к базе данных
    db_url = "postgresql://postgres:postgres@postgres_dwh:5432/postgres"
    engine = create_engine(db_url)

    try:
        # Получение случайного пользователя из базы
        with engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM raw.raw_users ORDER BY random() LIMIT 1"))
            user = result.mappings().one()

        # Определение полей для обновления
        fields_to_update = ["first_name", "last_name", "middle_name", "birthday", "email"]

        # Случайно выбираем количество полей для обновления (от 1 до 5)
        num_fields_to_update = random.randint(a=1, b=len(fields_to_update))  # noqa: S311
        selected_fields = random.sample(fields_to_update, num_fields_to_update)

        # Создаем новую запись на основе старой с обновленными данными
        new_user_data = dict(user)

        # Обновляем выбранные поля с использованием Faker
        changes = {}
        for field in selected_fields:
            if field == "first_name":
                new_user_data[field] = fake.first_name()
                changes[field] = f"{user[field]} -> {new_user_data[field]}"
            elif field == "last_name":
                new_user_data[field] = fake.last_name()
                changes[field] = f"{user[field]} -> {new_user_data[field]}"
            elif field == "middle_name":
                new_user_data[field] = fake.middle_name()
                changes[field] = f"{user[field]} -> {new_user_data[field]}"
            elif field == "birthday":
                new_birthday = fake.date_time_ad(
                    start_datetime=datetime.date(year=1980, month=1, day=1),
                    end_datetime=datetime.date(year=2005, month=1, day=1),
                ).strftime("%Y-%m-%d")
                new_user_data[field] = new_birthday
                changes[field] = f"{user[field]} -> {new_user_data[field]}"
            elif field == "email":
                new_user_data[field] = fake.email()
                changes[field] = f"{user[field]} -> {new_user_data[field]}"

        # Обновляем поле updated_at
        new_user_data["updated_at"] = datetime.datetime.now(tz=datetime.UTC)

        # Обновляем поле ts_db
        new_user_data["ts_db"] = datetime.datetime.now(tz=datetime.UTC)

        # Создаем DataFrame с новыми данными пользователя
        new_df = pd.DataFrame([new_user_data])

        # Добавляем запись в базу данных
        new_df.to_sql(
            schema="raw",
            name="raw_users",
            con=engine,
            if_exists="append",
            index=False,
        )

        # Выводим информацию об обновлении
        logging.info(f"Обновлен пользователь с ID {user['id']}")
        for field, change in changes.items():
            logging.info(f"  {field}: {change}")

    except Exception as e:  # noqa: BLE001
        logging.info(f"Ошибка при обновлении пользователя: {e}")

def add_new_or_update_user():
    count = 1
    while count <= 100:
        v = random.randint(a=1, b=100)  # noqa: S311
        if v % 2 == 0:
            add_new_user()
        else:
            update_info_about_current_user()
        count += 1

with DAG(
        dag_id=DAG_ID,
        schedule=timedelta(minutes=1),
        default_args=args,
        tags=["s3", "ods", "pg", "json"],
        max_active_tasks=1,
        max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    add_new_or_update_user = PythonOperator(
        task_id="add_new_or_update_user",
        python_callable=add_new_or_update_user,
    )

    end = EmptyOperator(task_id="end")

    start >> add_new_or_update_user >> end



