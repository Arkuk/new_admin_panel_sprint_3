import logging
from datetime import datetime

from pydantic import BaseSettings, Field

logging.basicConfig(level=logging.DEBUG)


class General(BaseSettings):
    batch_size: int = 500
    path_json_storage: str = 'state.json'
    time_state: str = datetime(2000, 6, 6, 6, 6, 6, 66666).isoformat()
    tables_names_pg: list = ['film_work',
                             'person',
                             'genre',
                             'person_film_work',
                             'genre_film_work']


class SettingsPG(BaseSettings):
    host: str = Field(..., env='DB_HOST')
    port: str = Field(..., env='DB_PORT')
    dbname: str = Field(..., env='DB_NAME')
    user: str = Field(..., env='DB_USER')
    password: str = Field(..., env='DB_PASSWORD')


class SettingsEs(BaseSettings):
    host: str = Field('http://elastic:9200', env='ES_HOST')
