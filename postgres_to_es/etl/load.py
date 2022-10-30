from elasticsearch import helpers

from shemas import (ElasticFilmWork,
                    Genre,
                    PersonPgES)
from tools import (backoff,
                   get_es,
                   get_logger)

logger = get_logger(__name__)


class EsLoader:
    def __init__(self):
        self.es = get_es()
        logger.info('Object load ES created')

    @backoff()
    def load_data(self, es_data: list[ElasticFilmWork | Genre | PersonPgES], table_name) -> tuple[int, list]:
        query: list = []
        match table_name:
            case 'film_work':
                query = [{'_index': 'movies', '_id': data.id, '_source': data.json()}
                         for data in es_data]
            case 'genre':
                query = [{'_index': 'genre', '_id': data.id, '_source': data.json()}
                         for data in es_data]
            case 'person':
                query = [{'_index': 'persons', '_id': data.id, '_source': data.json()}
                         for data in es_data]
        rows_count, errors = helpers.bulk(self.es, query)
        if errors:
            logger.info('Error load in ES')
            logger.error(errors)
            raise Exception('Error load in ES')
        return rows_count, errors
