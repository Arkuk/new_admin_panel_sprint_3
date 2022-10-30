from datetime import datetime

from pydantic import BaseModel


class PersonBase(BaseModel):
    id: str
    name: str


class Person(PersonBase):
    role: str


class Genre(BaseModel):
    id: str
    name: str


class PersonPgES(BaseModel):
    id: str
    full_name: str
    role: list[str | None]
    film_ids: list[str | None]


class PgFilmWork(BaseModel):
    id: str
    title: str
    description: str | None
    rating: float | None
    type: str | None
    created: datetime
    modified: datetime
    persons: list[Person]
    genre: list[Genre]


class ElasticFilmWork(BaseModel):
    id: str
    imdb_rating: float | None
    genre: list[Genre]
    title: str
    description: str | None
    director: list[str]
    actors_names: list[str]
    writers_names: list[str]
    actors: list[PersonBase]
    writers: list[PersonBase]
