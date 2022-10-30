import logging
from datetime import datetime

from pydantic import BaseSettings, Field

logging.basicConfig(level=logging.DEBUG)


class General(BaseSettings):
    batch_size: int = 500
    path_json_storage: str = 'state.json'
    time_state: str = datetime.min.isoformat()
    tables_names_pg: list = ['film_work',
                             'person',
                             'genre',
                             'person_film_work',
                             'genre_film_work']


class SettingsPG(BaseSettings):
    dbname: str = Field("movies_database", env="DB_NAME")
    user: str = Field("app", env="DB_USER")
    password: str = Field("123qwe", env="DB_PASSWORD")
    host: str = Field("127.0.0.1", env="DB_HOST")
    port: int = Field("5432", env="DB_PORT")




class SettingsEs(BaseSettings):
    host: str = Field('http://127.0.0.1:9200', env='ES_HOST')

