from schemas import (FilmWork,
                                        Genre,
                                        GenreFilmWork,
                                        Person,
                                        PersonFilmWork)
from typing import Union, Generator


def sql_data_to_schema(sql_data_generator: Generator,
                       schema: Union[FilmWork,
                                     Genre,
                                     GenreFilmWork,
                                     Person,
                                     PersonFilmWork]):
    """Сериализация данных из sqlite3.Row в одну из схем и создание генератоора"""
    for dict_rows in sql_data_generator:
        dict_schemas = [schema(**(dict(row)))for row in dict_rows]
        yield dict_schemas
