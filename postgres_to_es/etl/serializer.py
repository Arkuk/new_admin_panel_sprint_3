from shemas import (PgFilmWork,
                    Genre,
                    ElasticFilmWork,
                    PersonBase,
                    PersonPgES)
from tools import get_logger

logger = get_logger(__name__)


class Serializer:
    """
    Класс для сериализации данных
    """

    def ser_pg_data(self, pg_data, table_name) -> list[PgFilmWork | Genre]:
        """
        Преобразует сырые данные из баззы данных в список типовых моделей
        """
        try:
            result: list = []
            match table_name:
                case 'film_work':
                    result = [PgFilmWork.parse_obj(item) for item in pg_data]
                case 'person':
                    result = [PersonPgES.parse_obj(item) for item in pg_data]
                case 'genre':
                    result = [Genre.parse_obj(item) for item in pg_data]

            return result
        except Exception as e:
            logger.info(f'Dates {table_name} impossible serialized')
            raise e

    def pg_to_es_models_film_work(self, list_pg_models: list[PgFilmWork]) -> list[ElasticFilmWork]:
        """Сериализация фильмов для еластика """
        result: list = []
        for item in list_pg_models:
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
                genre=item.genre,
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

    def pg_to_es_models_persons(self, list_pg_models: list[PersonPgES]) -> list[PersonPgES]:
        """Сериализация фильмов для еластика """
        result: list = []
        for person in list_pg_models:
            person.role = list(set(person.role))
            result.append(person)
        return result

    def pg_to_es_models(
            self,
            list_pg_models: list[PgFilmWork | Genre | PersonPgES], table_name) -> \
            list[ElasticFilmWork | Genre | PersonPgES]:
        """
        Преобразует list[PgFilmWork] -> list[ElasticFilmWork]
        :return: list[ElasticFilmWork]
        """
        try:
            result: list = []
            match table_name:
                case 'film_work':
                    result = self.pg_to_es_models_film_work(list_pg_models)
                case 'genre':
                    result = list_pg_models
                case 'person':
                    result = self.pg_to_es_models_persons(list_pg_models)
            return result
        except Exception as e:
            logger.info('Dates impossible serialized')
            raise e
