import logging

from sqlalchemy import create_engine, text

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

db_url = "postgresql://postgres:postgres@localhost:5432/postgres"
engine = create_engine(db_url)

with engine.begin() as conn:

    logger.info("Создание схем raw, stg, ods, dds")
    conn.execute(text("""
        CREATE SCHEMA raw;
        CREATE SCHEMA stg;
        CREATE SCHEMA ods;
        CREATE SCHEMA dds;
    """))
    logger.info("Схемы raw, stg, ods, dds созданы")

    logger.info("Создание таблицы raw.raw_users")
    conn.execute(text("""
        DROP TABLE IF EXISTS raw.raw_users;
        CREATE TABLE raw.raw_users (
            id uuid NULL,
            created_at timestamp NULL,
            updated_at timestamp NULL,
            first_name text NULL,
            last_name text NULL,
            middle_name text NULL,
            birthday date NULL,
            email text NULL,
            ts_db timestamp NULL
        );
        """))
    logger.info("Таблица raw.raw_users создана")

    logger.info("Создание таблицы ods.dim_users")
    conn.execute(text("""
        DROP TABLE IF EXISTS ods.dim_users;
        CREATE TABLE ods.dim_users (
            id uuid PRIMARY KEY,
            created_at timestamp NULL,
            updated_at timestamp NULL,
            first_name text NULL,
            last_name text NULL,
            middle_name text NULL,
            birthday date NULL,
            email text NULL
        );
        """))
    logger.info("Таблица ods.dim_users создана")

    logger.info("Создание таблицы dds.dim_scd2_users")
    conn.execute(text("""
        DROP TABLE IF EXISTS dds.dim_scd2_users;
        CREATE TABLE dds.dim_scd2_users (
            id uuid,
            created_at timestamp NULL,
            updated_at timestamp NULL,
            first_name text NULL,
            last_name text NULL,
            middle_name text NULL,
            birthday date NULL,
            email text NULL,
            actual_from timestamp NULL,
            actual_to timestamp NULL,
            ts_db timestamp NULL
        );
        """))
    logger.info("Таблица dds.dim_scd2_users создана")


