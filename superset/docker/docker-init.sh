#!/bin/sh

echo "🚀 Initializing Superset..."

superset db upgrade

superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@example.com \
    --password admin || true

superset init

echo "✅ Superset initialized!"