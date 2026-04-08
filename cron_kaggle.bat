@echo off
:: 1. Se déplacer dans le dossier du projet
cd /d "C:\Users\jorda\Documents\Cours EPSI\MSPR\BLOC1\healthai-etl"

:: 2. Lancer le script via uv (utilise le chemin trouvé à l'étape 1)
"c:\Users\jorda\.local\bin\uv.exe" run python src/data_pipeline/downloader/api_client.py >> logs\cron_kaggle.log 2>&1