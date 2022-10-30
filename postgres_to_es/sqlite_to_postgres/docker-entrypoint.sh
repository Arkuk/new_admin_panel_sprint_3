#!/bin/sh

while ! nc -z $DB_HOST $DB_PORT; do
      sleep 0.1
done
sleep 10
python load_data.py