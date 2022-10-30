import time

import psycopg2
from psycopg2.extras import DictCursor

from store_state import State
from tools import (backoff,
                   get_logger)

logger = get_logger(__name__)


class PgExtractor:
    def __init__(self, curs: DictCursor,
                 state: State,
                 batch_size: int,
                 table_names: list[str]):
        self.curs = curs
        self.state = state
        self.date_from = self.state.get_state('date_from')
        self.batch_size = batch_size
        self.table_names = table_names

    @backoff()
    def get_data(self, query: str, curs: DictCursor) -> list:
        try:
            curs.execute(query)
            while data := curs.fetchmany(self.batch_size):
                yield data
        except psycopg2.Error as e:
            logger.error(e)

    def get_ids(self, table: str) -> list:
        """
        Получаем ids фильмов|персон|жанров, которые были обновлены
        :return: list[id]
        """
        try:
            query = f"""SELECT id
                        FROM content.{table}
                        WHERE modified > '{self.date_from}'
                        ORDER BY modified"""

            data = self.get_data(query, self.curs)
            ids = []
            for batch in data:
                for item in batch:
                    ids.append(item[0])
            return ids
        except psycopg2.OperationalError as e:
            logger.error(f'Errors in getting ids {table}')
            raise e

    def get_ids_m2m(self, table: str, field: str, list_ids: list[str]) -> list:
        """
        Получаем ids фильмов, в которые был добавлен|удален персона|жанр
        :return: list[id]
        """
        try:
            if len(list_ids) == 1:
                list_ids = f"('{list_ids[0]}')"
            else:
                list_ids = tuple(list_ids)
            query = f"""SELECT fw.id
                        FROM content.film_work fw
                        LEFT JOIN content.{table} zfw ON zfw.film_work_id = fw.id
                        WHERE zfw.{field} IN {list_ids}
                        ORDER BY fw.modified"""

            data = self.get_data(query, self.curs)
            ids = []
            for batch in data:
                for item in batch:
                    ids.append(item[0])
            return ids

        except psycopg2.OperationalError as e:
            logger.error(f'Errors in getting ids {table}')
            raise e

    def get_extract_ids(self) -> dict:
        """
        Получаем ids которые были модернизированны
        :return: кортеж id
        """
        film_work_ids: list[str] = []
        persons_ids: list[str] = []
        genre_ids: list[str] = []
        person_film_work_ids: list[str] = []
        genre_film_work_ids: list[str] = []
        for table in self.table_names:
            match table:
                case 'film_work':
                    film_work_ids = self.get_ids(table)
                    time.sleep(2)
                case 'person':
                    persons_ids = self.get_ids(table)
                    time.sleep(2)
                case 'genre':
                    genre_ids = self.get_ids(table)
                    time.sleep(2)
                case 'person_film_work':
                    if film_work_ids and persons_ids:
                        field = 'person_id'
                        person_film_work_ids = self.get_ids_m2m(
                            table, field, persons_ids)
                case 'genre_film_work':
                    if film_work_ids and genre_ids:
                        field = 'genre_id'
                        genre_film_work_ids = self.get_ids_m2m(
                            table, field, genre_ids)

        films_ids = film_work_ids + person_film_work_ids + genre_film_work_ids
        films_ids = set(films_ids)
        films_ids = list(films_ids)
        extract_ids = {
            'films_ids': films_ids,
            'persons_ids': persons_ids,
            'genre_ids': genre_ids
        }

        return extract_ids

    def check_ids(self, ids: list) -> tuple | None | str:
        """Проверка на наличие ids и форматирование"""
        if not ids:
            return None
        if len(ids) == 1:
            return f"('{ids[0]}')"
        return tuple(ids)

    def get_query_persons(self, ids: tuple) -> str:
        """Получние запроса для персон"""

        query = f"""SELECT p.id,
                           p.full_name,
                           array_agg(DISTINCT pfw.role) as role,
                           json_agg(DISTINCT pfw.film_work_id) as film_ids
                    FROM content.person as p
                    LEFT JOIN content.person_film_work pfw ON pfw.person_id = p.id
                    WHERE p.id IN {ids}
                    GROUP BY p.id
                    ORDER BY p.modified"""
        return query

    def get_query_genre(self, ids: tuple) -> str:
        """Получние запроса для жанров"""
        query = f"""SELECT id,
                           name
                    FROM content.genre
                    WHERE id IN {ids}
                    ORDER BY modified"""
        return query

    def get_query_films(self, ids: tuple) -> str:
        """Получние запроса для фильмов"""
        query = f"""SELECT
                                   fw.id,
                                   fw.title,
                                   fw.description,
                                   fw.rating,
                                   fw.created,
                                   fw.modified,
                                   COALESCE (
                                       json_agg(
                                           DISTINCT jsonb_build_object(
                                               'role', pfw.role,
                                               'id', p.id,
                                               'name', p.full_name
                                           )
                                       ) FILTER (WHERE p.id is not null),
                                       '[]'
                                   ) as persons,
                                   COALESCE (
                                       json_agg(
                                           DISTINCT jsonb_build_object(
                                               'id', g.id,
                                               'name', g.name
                                           )
                                       ) FILTER (WHERE g.id is not null),
                                       '[]'
                                   ) as genre
                                FROM content.film_work fw
                                LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                                LEFT JOIN content.person p ON p.id = pfw.person_id
                                LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                                LEFT JOIN content.genre g ON g.id = gfw.genre_id
                                WHERE fw.id IN {ids}
                                GROUP BY fw.id
                                ORDER BY fw.modified
                                """
        return query

    def get_extract_film_work(self, ids: list) -> list | None:
        """Получение всех обновленных фильмов"""
        try:
            ids = self.check_ids(ids)
            if not ids:
                return None
            query = self.get_query_films(ids)
            data = self.get_data(query, self.curs)
            result = []
            for batch in data:
                for item in batch:
                    result.append(item)
            logger.info('Dates movie for upload get')
            return result
        except psycopg2.OperationalError as e:
            logger.error(f'Errors in getting for final dates movie')
            raise e

    def get_extract(self, extract_ids: dict, table_name) -> list | None:
        """Получение всех обновленных фильмов"""
        try:
            query: str = ''
            ids = None
            match table_name:
                case 'film_work':
                    ids = self.check_ids(extract_ids['films_ids'])
                    query = self.get_query_films(ids)
                case 'genre':
                    ids = self.check_ids(extract_ids['genre_ids'])
                    query = self.get_query_genre(ids)
                case 'person':
                    ids = self.check_ids(extract_ids['persons_ids'])
                    query = self.get_query_persons(ids)
            if not ids:
                return None
            data = self.get_data(query, self.curs)
            result = []
            for batch in data:
                for item in batch:
                    result.append(item)
            logger.info('Dates movie for upload get')
            return result
        except psycopg2.OperationalError as e:
            logger.error(f'Errors in getting for final dates movie')
            raise e
