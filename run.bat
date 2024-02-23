@echo off

REM Activate the virtual environment
call D:\Data-Engineer\Data-Pipelines\csv-to-postgres\.venv\Scripts\Activate.ps1

REM Install dependencies
call pip install -r D:\Data-Engineer\Data-Pipelines\csv-to-postgres\requirements.txt

REM Run the Python script
python D:\Data-Engineer\Data-Pipelines\csv-to-postgres\run.py %*