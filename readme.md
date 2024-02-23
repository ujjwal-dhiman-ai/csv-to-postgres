# CSV to PostgreSQL Uploader

This script allows you to upload a CSV file to a PostgreSQL database, either creating a new table or appending/replacing data in an existing table.

## Prerequisites

Before running the script, ensure you have the following:

- Python 3.x installed
- Required Python packages (install them using `pip install -r requirements.txt`)
- PostgreSQL database with the required credentials
- Environment variable `DB_STRING` set with the PostgreSQL connection string
- Before usage, add the project dir path to `Path` environment variable.

## Usage

Go to the folder where your csv file is located, and open terminal in that folder. Run the below command by replacing `csv_file` with actuall csv file name.

```bash
run <csv_file> <table_name> [-schema <schema_name>] [-mode <append/replace>]
