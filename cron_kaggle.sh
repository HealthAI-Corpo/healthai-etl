#!/bin/bash
cd "$(dirname "$0")"
docker compose run --rm api uv run python src/data_pipeline/downloader/api_client.py >> logs/cron_kaggle.log 2>&1