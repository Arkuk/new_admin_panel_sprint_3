import os
from typing import Final

from dotenv import load_dotenv

from schemas import (FilmWork, Genre,
                                        GenreFilmWork,
                                        Person,
                                        PersonFilmWork)

load_dotenv(dotenv_path='.env')

dsl = {'dbname': os.environ.get('DB_NAME'),
       'user': os.environ.get('DB_USER'),
       'password': os.environ.get('DB_PASSWORD'),
       'host': os.environ.get('DB_HOST'),
       'port': os.environ.get('DB_PORT')}

tables_names = {'film_work': FilmWork,
                'genre': Genre,
                'genre_film_work': GenreFilmWork,
                'person': Person,
                'person_film_work': PersonFilmWork}

SIZE_ROWS: Final = 100
