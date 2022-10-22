#!/bin/sh

while ! nc -z $DB_HOST $DB_PORT; do
      sleep 0.1
done
python manage.py migrate
python manage.py collectstatic --clear --noinput
gunicorn --bind 0.0.0.0:8000 config.wsgi:application