import sqlite3
from contextlib import contextmanager
from dataclasses import astuple
from dataclasses import fields
from typing import Union

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from schemas import (FilmWork,
                                        Genre,
                                        GenreFilmWork,
                                        Person,
                                        PersonFilmWork)


@contextmanager
def conn_context_sqlite(db_path: str) -> sqlite3.Connection:
    """контекстный менеджер"""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def conn_context_postgres(dsl: dict) -> _connection:
    """контекстный менеджер"""
    pg_connection = psycopg2.connect(**dsl, cursor_factory=DictCursor)
    try:
        yield pg_connection
    finally:
        pg_connection.close()


def change_create_update(field: str) -> str:
    if field == 'created_at':
        field = 'created'
    if field == 'updated_at':
        field = 'modified'
    return field


def get_change_fields_txt(schema: Union[FilmWork,
                                        Genre,
                                        GenreFilmWork,
                                        Person,
                                        PersonFilmWork]) -> str:
    """Возращает строку измененных полей пол Postgres"""
    old_fields = get_get_fields_on_schema_list(schema)
    new_fields_list = list(map(change_create_update, old_fields))
    return ', '.join(new_fields_list)


def get_get_fields_on_schema_list(schema: Union[FilmWork,
                                                Genre,
                                                GenreFilmWork,
                                                Person,
                                                PersonFilmWork]) -> list:
    """Возращает список из полей дата класса"""
    return [field.name for field in fields(schema)]


def get_fields_on_schema_txt(schema: Union[FilmWork,
                                           Genre,
                                           GenreFilmWork,
                                           Person,
                                           PersonFilmWork]) -> str:
    """Возращает строку из полей дата класса"""
    return ', '.join(get_get_fields_on_schema_list(schema))


def get_values_on_schema_test(batch: dict[Union[FilmWork,
                                                Genre,
                                                GenreFilmWork,
                                                Person,
                                                PersonFilmWork]]) -> list[tuple]:
    """Возращает значение из дата класса в виде списка кортежей"""
    return [astuple(row) for row in batch]
