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

    def get_extract_ids(self) -> tuple[str]:
        """
        Получаем ids которые были модернизированны
        :return: кортеж id
        """
        film_work_ids: list[str] = []
        person_ids: list[str] = []
        genre_ids: list[str] = []
        person_film_work_ids: list[str] = []
        genre_film_work_ids: list[str] = []
        for table in self.table_names:
            match table:
                case 'film_work':
                    film_work_ids = self.get_ids(table)
                    time.sleep(2)
                case 'person':
                    person_ids = self.get_ids(table)
                    time.sleep(2)
                case 'genre':
                    genre_ids = self.get_ids(table)
                    time.sleep(2)
                case 'person_film_work':
                    if film_work_ids and person_ids:
                        field = 'person_id'
                        person_film_work_ids = self.get_ids_m2m(
                            table, field, person_ids)
                case 'genre_film_work':
                    if film_work_ids and genre_ids:
                        field = 'genre_id'
                        genre_film_work_ids = self.get_ids_m2m(
                            table, field, genre_ids)

        extract_ids = film_work_ids + person_film_work_ids + genre_film_work_ids
        extract_ids = set(extract_ids)
        extract_ids = tuple(extract_ids)
        return extract_ids

    def get_extract_film_work(self):
        try:
            ids = self.get_extract_ids()
            if not ids:
                return []
            if len(ids) == 1:
                ids = f"('{ids[0]}')"
            else:
                ids = tuple(ids)
            ids = tuple(ids)
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
                           array_agg(DISTINCT g.name) as genres
                        FROM content.film_work fw
                        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                        LEFT JOIN content.person p ON p.id = pfw.person_id
                        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                        LEFT JOIN content.genre g ON g.id = gfw.genre_id
                        WHERE fw.id IN {ids}
                        GROUP BY fw.id
                        ORDER BY fw.modified
                        """
            data = self.get_data(query, self.curs)
            result = []
            for batch in data:
                for item in batch:
                    result.append(item)
            logger.info('Dates for upload get')
            return result
        except psycopg2.OperationalError as e:
            logger.error(f'Errors in getting for final dates')
            raise e
