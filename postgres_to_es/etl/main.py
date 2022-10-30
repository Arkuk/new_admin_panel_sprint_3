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
    print(dsl)
    # Инит хранилища
    state_init()
    # Проверка существования индекса или создание в ES
    check_index = FirstStartEtl().check_or_create_index('movies')
    check_index = FirstStartEtl().check_or_create_index('persons')
    check_index = FirstStartEtl().check_or_create_index('genre')
    logger.info('Get ES object')
    # Получение настроек
    settings = General()
    batch_size = settings.batch_size
    table_names = settings.tables_names_pg
    table_names_for_es = table_names[:-2]
    if check_index:
        # Запуск мониторинга изменений
        while True:
            try:
                state_flag = False
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
                    # Получение ids(persons, genre, film)
                    extract_data = pg_extractor.get_extract_ids()
                    # Получение обновление в каждой таблице
                    pg_data: list = []
                    for table_name in table_names_for_es:
                        pg_data = pg_extractor.get_extract(extract_data, table_name)
                        if not pg_data:
                            logger.info(f'Dates is not update {table_name}')
                        else:
                            # Сериализация данных
                            serializer = Serializer()
                            # Получение сериализованных фильмов PG модели
                            pg_data_models = serializer.ser_pg_data(pg_data, table_name)
                            # Получение сериализованных фильмов ES модели
                            es_data_models = serializer.pg_to_es_models(pg_data_models, table_name)
                            logger.info(f'Dates {table_name} serialized')
                            # Загрузка данных в ES
                            es_loader = EsLoader()
                            result = es_loader.load_data(es_data_models, table_name)
                            logger.info(f'In ES load {result[0]} row {table_name}')
                            state_flag = True
                    if state_flag:
                        state.set_state('date_from', new_date_from)
            except psycopg2.Error as e:
                logger.error('Cannot get rows PG')
                logger.error(e)
            time.sleep(1)


if __name__ == '__main__':
    etl_process()
