import os, sys
import pandas as pd
from time import time
import argparse
from database_manager import DatabaseManager

def parse_args():
    parser = argparse.ArgumentParser(description='Upload CSV to PostgreSQL database.')
    parser.add_argument('csv_file', type=str, help='Path to the CSV file')
    parser.add_argument('table_name', type=str, help='Name of the table in the database')
    parser.add_argument('-schema', type=str, default='public', help='Database schema (default: public)')
    parser.add_argument('-mode', type=str, choices=['append', 'replace'], default='append', help='Insertion mode (default: append)')
    return parser.parse_args()


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.append(script_dir)
    
    args = parse_args()
    
    csv_file_path = args.csv_file
    table = args.table_name
    schema = args.schema
    mode = args.mode
    database_url = os.environ.get('DB_STRING')

    try:
        db_manager = DatabaseManager(database_url)
        db_manager.connect()
        db_manager.connection.set_session(autocommit=True)

        s = time()
        df = pd.read_csv(csv_file_path)
        print(f"Data read in {round((time() - s),2)} seconds")
        
        s = time()
        if not db_manager.table_exists(table, schema):
            db_manager.create_table(df, schema, table)
            print(f"Table created in {round((time() - s),2)} seconds")
        else:
            print("Table already exists.")
        
        s = time()
        db_manager.push_df_to_database(df, schema, table, mode)
        print(f"Data pushed to table in {round((time() - s),2)} seconds in {mode} mode.")

        # version_query = "SELECT version();"
        # version = db_manager.execute_query(version_query)
        # print("PostgreSQL database version:", version)
        # print(psutil.Process().memory_info().peak_wset)

    except Exception as e:
        print("Error: Unable to connect to the database.")
        print(e)

    finally:
        if db_manager:
            db_manager.disconnect()

if __name__ == "__main__":
    main()
