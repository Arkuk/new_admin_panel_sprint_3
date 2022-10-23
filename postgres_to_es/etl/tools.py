import logging
import time
from contextlib import contextmanager
from functools import wraps

import psycopg2
from elasticsearch import Elasticsearch
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from config import SettingsEs


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


logger = get_logger(__name__)


def backoff(
        start_sleep_time: float = 0.1,
        factor: int = 2,
        border_sleep_time: int = 10):
    """
    Функция для повторного выполнения функции через некоторое время,
    если возникла ошибка. Использует наивный экспоненциальный рост
    времени повтора (factor) до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :return: результат выполнения функции
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            sleep_time = start_sleep_time
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logger.error(e)
                    sleep_time = sleep_time * factor
                    if sleep_time > border_sleep_time:
                        sleep_time = border_sleep_time
                    time.sleep(sleep_time)

        return inner

    return func_wrapper


@backoff()
@contextmanager
def conn_context_postgres(dsl: dict) -> _connection:
    """Контекстный менеджер для postgres"""
    pg_connection = psycopg2.connect(**dsl, cursor_factory=DictCursor)
    logger.info('Получено соединение с PG')
    try:
        yield pg_connection
    except Exception as e:
        logger.error('It cannot get connection with PG')
        logger.error(e)
    finally:
        pg_connection.close()


@backoff()
def get_es() -> Elasticsearch:
    """Получение обьект соединения с ES"""
    try:
        settings_es = SettingsEs()
        es = Elasticsearch(settings_es.host, verify_certs=False)
        return es
    except ConnectionError as e:
        logger.error(e)


@backoff()
def get_generator_data_pg(connection: _connection,
                          query: str,
                          batch_size: int) -> list:
    """Генератор PG"""

    with connection.cursor() as cursor:
        cursor.execute(query)
        while data := cursor.fetchmany(batch_size):
            yield data
