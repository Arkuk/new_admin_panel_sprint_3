import time
from contextlib import closing
from datetime import datetime

import psycopg2
from psycopg2.extras import DictCursor

from config import (General,
                    SettingsPG)
from extract import PgExtractor
from first_start import FirstStartEtl
from load import EsLoader
from serializer import Serializer
from store_state import (get_state_obj,
                         state_init)
from tools import get_logger

logger = get_logger(__name__)


def etl_process() -> None:
    """Функция для запуска логики etl"""
    # Получение данных для соединения с PG и ES
    dsl = SettingsPG().dict()
    # Инит хранилища
    state_init()
    # Проверка существования индекса или создание в ES
    check_index = FirstStartEtl().check_or_create_index('movies')
    logger.info('Get ES object')
    # Получение настроек
    settings = General()
    batch_size = settings.batch_size
    table_names = settings.tables_names_pg
    if check_index:
        # Запуск мониторинга изменений
        while True:
            try:
                with closing(psycopg2.connect(**dsl, cursor_factory=DictCursor)) as pg_conn, pg_conn.cursor() as curs:
                    # получение новой даты обновления
                    new_date_from = datetime.utcnow().isoformat()
                    # получение объект хранилища
                    state = get_state_obj()
                    # Получени данных из PG
                    pg_extractor = PgExtractor(curs,
                                               state,
                                               batch_size,
                                               table_names)
                    film_works = pg_extractor.get_extract_film_work()
                    if not film_works:
                        logger.info('Dates is not update')
                        continue
                    # Сериализация данных
                    serializer = Serializer(film_works)
                    # Получение сериализованных фильмов PG модели
                    film_works_pg = serializer.ser_pg_data()
                    # Получение сериализованных фильмов ES модели
                    film_works_es = serializer.pg_to_es_models(film_works_pg)
                    logger.info('Dates serialized')
                    # Загрузка данных в ES
                    es_loader = EsLoader()
                    result = es_loader.load_data(film_works_es)
                    logger.info(f'In ES load {result[0]} row')

                    state.set_state('date_from', new_date_from)

            except psycopg2.Error as e:
                logger.error('Cannot get rows PG')
                logger.error(e)
            time.sleep(1)


if __name__ == '__main__':
    etl_process()
