#!/bin/sh

echo "Starting Superset service: $1"

if [ "$1" = "app-gunicorn" ]; then
    gunicorn \
        --bind 0.0.0.0:8088 \
        --workers 2 \
        --timeout 120 \
        "superset.app:create_app()"
elif [ "$1" = "worker" ]; then
    celery --app=superset.tasks.celery_app:app worker
elif [ "$1" = "beat" ]; then
    celery --app=superset.tasks.celery_app:app beat
fi