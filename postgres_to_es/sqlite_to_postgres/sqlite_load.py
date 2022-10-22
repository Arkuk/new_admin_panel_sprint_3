import sqlite3
from typing import Union, Generator

from schemas import (FilmWork,
                                        Genre,
                                        GenreFilmWork,
                                        Person,
                                        PersonFilmWork)
from tools import get_fields_on_schema_txt


class SQLiteExtractor:
    """
        Парсинг данных из sqlite
    """

    def __init__(self, connection: sqlite3.Connection, size_rows: int):
        self.connection = connection
        self.size_rows = size_rows

    def load_table(self,
                   table_name: str,
                   schema: Union[FilmWork,
                                 Genre,
                                 GenreFilmWork,
                                 Person,
                                 PersonFilmWork]) -> Generator:
        """Получение данных из таблицы"""
        # Получение полей для  выгрузки из таблицы
        name_fields = get_fields_on_schema_txt(schema)
        # Получение курсора
        cursor = self.connection.cursor()
        # Запрос
        cursor.execute('SELECT {name_fields} FROM {table_name};'
                       .format(name_fields=name_fields,
                               table_name=table_name))
        # Получение первых n записей
        while sql_data := cursor.fetchmany(self.size_rows):
            yield sql_data
