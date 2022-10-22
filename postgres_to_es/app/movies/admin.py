from django.contrib import admin
from django.utils.translation import gettext_lazy as _

from .models import Filmwork, Genre, GenreFilmwork, Person, PersonFilmwork


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    search_fields = ('name', 'description',)
    list_display = ('name', 'description',)


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    search_fields = ('full_name',)
    list_display = ('full_name',)


class GenreFilmworkInline(admin.TabularInline):
    model = GenreFilmwork
    extra = 0
    verbose_name = _('Genre')
    verbose_name_plural = _('Genres')


class PersonFilmworkInline(admin.TabularInline):
    model = PersonFilmwork
    extra = 0
    autocomplete_fields = ('person',)
    verbose_name = _('Actor')
    verbose_name_plural = _('Actors')


@admin.register(Filmwork)
class FilmworkAdmin(admin.ModelAdmin):
    inlines = (GenreFilmworkInline, PersonFilmworkInline,)
    list_display = (
        'title',
        'type',
        'creation_date',
        'rating',
        'created',
        'modified')
    list_filter = ('type',)
    search_fields = ('title', 'description', 'id')
