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
    host: str = Field(...)
    port: str = Field(...)
    dbname: str = Field(...)
    user: str = Field(...)
    password: str = Field(...)

    class Config:
        env_prefix = 'DB_'
        env_nested_delimiter = '_'


class SettingsEs(BaseSettings):
    host: str = Field('http://elastic:9200', env='ES_HOST')

    class Config:
        env_prefix = 'ES_'
        env_nested_delimiter = '_'
