@echo off
:: 1. Se déplacer dynamiquement dans le dossier du script
cd /d "%~dp0"

:: 2. Lancer l'orchestrateur complet (main.py) 
:: On utilise 'exec' car le service healthai-api doit rester 'Up'
docker exec -t -w /app healthai_api env PYTHONPATH=/app uv run python src/main.py >> logs\cron_kaggle.log 2>&1
pause