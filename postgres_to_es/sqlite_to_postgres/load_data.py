import sqlite3

from psycopg2.extensions import connection as _connection

from sqlite_load import SQLiteExtractor
from config import dsl, tables_names
from postgres_save import PostgresSaver
from serializes import sql_data_to_schema
from tools import conn_context_postgres, conn_context_sqlite


def load_from_sqlite(connection: sqlite3.Connection,
                     pg_conn: _connection,
                     tables_names: dict,
                     size_rows: int):
    """Основной метод загрузки данных из SQLite в Postgres"""
    # Создание класоов для загрузи и выгрузки данных
    sqlite_extractor = SQLiteExtractor(connection, size_rows)
    postgres_saver = PostgresSaver(pg_conn, size_rows)
    # Загрузки и выгрузка данных в соответсвии с таблицами
    for table_name, schema in tables_names.items():
        # Получение генератора из данных одной таблицы по n штук
        sql_data = sqlite_extractor.load_table(table_name, schema)
        # Сериализация данных. создание генератора
        generator_ser_data = sql_data_to_schema(sql_data, schema)
        # Вставка данных в Postgres на основе генератора сериализованных данных
        postgres_saver.save_all_data(generator_ser_data, table_name)


if __name__ == '__main__':
    size_rows: int = 100
    with conn_context_sqlite('db.sqlite') as sqlite_conn, conn_context_postgres(dsl) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn, tables_names, size_rows)
