import os
import sqlite3

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

load_dotenv(dotenv_path='../.env')

dsl = {'dbname': os.environ.get('DB_NAME'),
       'user': os.environ.get('DB_USER'),
       'password': os.environ.get('DB_PASSWORD'),
       'host': os.environ.get('DB_HOST'),
       'port': os.environ.get('DB_PORT')}

path_dbsqlite = '../db.sqlite'

connect_sqlite = sqlite3.connect(path_dbsqlite)
connect_sqlite.row_factory = sqlite3.Row
connect_postgres = psycopg2.connect(**dsl, cursor_factory=RealDictCursor)

cursor_sqlite = connect_sqlite.cursor()
cursor_postgres = connect_postgres.cursor()


def change_description(data: list[dict]) -> list[dict]:
    for i in data:
        if i['description'] is None:
            i['description'] = ''
    return data


def get_data_base(query: str, table_name: str) -> bool:
    cursor_sqlite.execute(
        '{query}'.format(query=query))
    result_sqlite = cursor_sqlite.fetchall()
    result_sqlite = [dict(_) for _ in result_sqlite]
    if table_name == 'film_work' or table_name == 'genre':
        change_description(result_sqlite)
    cursor_postgres.execute(
        '{query}'.format(query=query))
    cursor_postgres.fetchall()
    result_postgres = [dict(_) for _ in result_sqlite]

    for i in result_sqlite:
        result_postgres.remove(i)

    return result_postgres == []


def get_count_base(table_name: str):
    count_rows_sqlite = cursor_sqlite.execute(
        'select count(*) from {table_name}'.format(table_name=table_name)).fetchone()
    cursor_postgres.execute('select count(*) from {table_name}'.format(table_name=table_name))
    count_rows_postgres = cursor_postgres.fetchone()
    count_rows_sqlite = dict(count_rows_sqlite)['count(*)']
    count_rows_postgres = dict(count_rows_postgres)['count']
    return count_rows_sqlite == count_rows_postgres


def test_count_film_work():
    table_name = 'film_work'
    assert get_count_base(table_name)


def test_count_person():
    table_name = 'person'
    assert get_count_base(table_name)


def test_count_genre():
    table_name = 'genre'
    assert get_count_base(table_name)


def test_count_genre_film_work():
    table_name = 'genre_film_work'
    assert get_count_base(table_name)


def test_count_person_film_work():
    table_name = 'person_film_work'
    assert get_count_base(table_name)


def test_data_base_film_work():
    query = 'select id, title, description, creation_date, rating, type from film_work'
    table_name = 'film_work'
    assert get_data_base(query, table_name)


def test_person_data():
    query = 'select id, full_name from person'
    table_name = 'person'
    assert get_data_base(query, table_name)


def test_data_base_genre():
    query = 'select id, name, description from genre'
    table_name = 'genre'
    assert get_data_base(query, table_name)


def test_genre_film_work_data():
    query = 'select id, genre_id, film_work_id from genre_film_work'
    table_name = 'genre_film_work'
    assert get_data_base(query, table_name)


def test_person_film_work_data():
    query = 'select id, person_id, film_work_id, role from person_film_work'
    table_name = 'person_film_work'
    assert get_data_base(query, table_name)
