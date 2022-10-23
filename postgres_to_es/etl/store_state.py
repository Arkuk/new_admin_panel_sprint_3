import abc
import json
import os
from typing import Any, Optional

from config import General
from tools import backoff, get_logger

logger = get_logger(__name__)


class BaseStorage():
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path

    def retrieve_state(self) -> dict:
        try:
            with open(self.file_path) as file:
                return json.load(file)
        except (FileNotFoundError, json.JSONDecodeError):
            with open(self.file_path, 'w') as f:
                json.dump({}, f)
            return self.retrieve_state()

    def save_state(self, state: dict) -> None:
        data = self.retrieve_state()
        data.update(state)
        with open(self.file_path, 'w') as file:
            json.dump(data, file)


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно не перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    В целом ничего не мешает поменять это поведение на работу с БД или распределённым хранилищем.
    """

    def __init__(self, storage: BaseStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        data = self.storage.retrieve_state()
        data[key] = value
        self.storage.save_state(data)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        data = self.storage.retrieve_state()
        return data.get(key, None)


@backoff()
def get_state_obj() -> State:
    """Получение объекта State"""
    general_settings = General()
    storage = JsonFileStorage(general_settings.path_json_storage)
    state = State(storage)
    return state


@backoff()
def state_init() -> None:
    """
    Проверяет существоавние Storage, существующих данных в нем. Иначе создает стандартное
    """
    general_settings = General()
    state = get_state_obj()
    if not os.path.isfile(general_settings.path_json_storage):
        state.set_state('date_from', general_settings.time_state)

    else:
        logger.info('Local storage is created')
        if state.get_state('date_from') is not None:
            logger.info('In the local storage is date_from')
            return
        state.set_state('date_from', general_settings.time_state)
