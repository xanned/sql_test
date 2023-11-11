#!/bin/sh

if [ "$DATABASE" = "postgres" ]
then
    while ! nc -z $POSTGRES_HOST $POSTGRES_PORT; do
      echo "Waiting for db to be ready..."
      sleep 0.6
    done
fi
python main.py