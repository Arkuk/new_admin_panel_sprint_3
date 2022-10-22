from shemas import *
from tools import get_logger

logger = get_logger(__name__)


class Serializer:
    """
    Класс для сериализации данных
    """

    def __init__(self, list_data: list):
        self.list_data = list_data

    def ser_pg_data(self) -> list[PgFilmWork]:
        """
        Преобразует сырые данные из баззы данных в список типовых моделей
        :return: list[PgFilmWork]
        """
        try:
            result = [PgFilmWork.parse_obj(item) for item in self.list_data]
            return result
        except Exception as e:
            logger.info('Данные невозможно сериализовать')
            raise e

    def pg_to_es_models(
            self,
            list_pg_filmworks: list[PgFilmWork]) -> list[ElasticFilmWork]:
        """
        Преобразует list[PgFilmWork] -> list[ElasticFilmWork]
        :return: list[ElasticFilmWork]
        """
        try:
            result = []
            for item in list_pg_filmworks:
                actors_names = []
                writers_names = []
                director = []
                actors = []
                writers = []
                for person in item.persons:
                    match person.role:
                        case 'director':
                            director.append(person.name)
                        case 'actor':
                            actors.append(
                                PersonBase(
                                    id=person.id,
                                    name=person.name,
                                )
                            )
                            actors_names.append(person.name)
                        case 'writer':
                            writers_names.append(person.name)
                elastic_item = ElasticFilmWork(
                    id=item.id,
                    imdb_rating=item.rating,
                    genre=item.genres,
                    title=item.title,
                    description=item.description,
                    director=director,
                    actors_names=actors_names,
                    writers_names=writers_names,
                    actors=actors,
                    writers=writers
                )
                result.append(elastic_item)
            return result
        except Exception as e:
            logger.info('Данные невозможно сериализовать')
            raise e
