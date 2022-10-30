import json

from tools import (backoff,
                   get_es,
                   get_logger)

logger = get_logger(__name__)


class FirstStartEtl:

    def __init__(self):
        self.es = get_es()

    def _get_existence_index(self, index_name: str):
        """
        Проверка на наличие индекса
        :param index_name: название индекса
        """
        return self.es.indices.exists(index=index_name)

    @backoff()
    def check_or_create_index(self, index_name: str = None) -> bool:
        """
        Создание индекса если его нет
        :param index_name: название индекса
        :return: bool
        """
        if self._get_existence_index(index_name):
            logger.info(f'Index {index_name} have already created')
            return True
        else:
            try:
                with open(f'elastic_shema_{index_name}.json', 'r') as f:
                    data = json.loads(f.read())
                    self.es.indices.create(index=index_name, body=data)
                    logger.info(f'Index {index_name} have already created')
                    self.es.close()
                    return True
            except Exception as e:
                self.es.close()
                logger.info(f'Index {index_name} do not have and do not create')
                logger.error(e)
                return False
