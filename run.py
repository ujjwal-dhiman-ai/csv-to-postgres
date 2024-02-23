import psycopg2
from psycopg2 import sql
import csv
import io
import os
import pandas as pd
import psutil
from time import time


class DatabaseManager:
    def __init__(self, database_url):
        self.database_url = database_url
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = psycopg2.connect(self.database_url)
            self.cursor = self.connection.cursor()
            print("Connection established.")
        except psycopg2.Error as e:
            print("Error: ", e)

    def disconnect(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()
            print("Connection closed.")
            
    def table_exists(self, table_name, schema='public'):
        try:
            # Check if the table exists in the specified schema
            query = """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = %s
                    AND table_name = %s
                );
            """
            self.cursor.execute(query, (schema, table_name))
            return self.cursor.fetchone()[0]
        except psycopg2.Error as e:
            print(f"Error checking if table exists: {e}")
            return False

    def create_table(self, df, schema='public', table_name='your_table_name'):
        type_mapping = {
            'int64': 'INTEGER',
            'float64': 'FLOAT',
            'object': 'TEXT',
            'bool': 'BOOLEAN',
        }

        columns_data_types = {
            column: type_mapping.get(str(df.dtypes[column]), 'TEXT')
            for column in df.columns
        }

        table_creation_query = sql.SQL('''
            CREATE TABLE IF NOT EXISTS {}.{} (
                {}
            );
        ''').format(
            sql.Identifier(schema),
            sql.Identifier(table_name),
            sql.SQL(', ').join(
                sql.SQL('{} {}').format(
                    sql.Identifier(column),
                    sql.SQL(data_type)
                )
                for column, data_type in columns_data_types.items()
            )
        )

        try:
            self.cursor.execute(table_creation_query)
            print(f"Table '{schema}.{table_name}' created successfully.")
        except Exception as e:
            print(f"Error creating table '{schema}.{table_name}': {e}")

    def upload_df_to_database(self, df, schema='public', table_name='your_table_name', mode='append'):
            buffer = io.StringIO()
            df.to_csv(buffer, index=False, header=False, sep='\t')
            buffer.seek(0)

            if mode == 'append':
                copy_query = """
                    COPY {}.{} FROM STDIN WITH CSV DELIMITER E'\\t' NULL AS '';
                """.format(schema, table_name)
            elif mode == 'replace':
                copy_query = """
                    TRUNCATE TABLE {}.{};  -- This will delete all rows in the table
                    COPY {}.{} FROM STDIN WITH CSV DELIMITER E'\\t' NULL AS '';
                """.format(schema, table_name, schema, table_name)
            else:
                raise ValueError("Unsupported insertion mode. Use 'append' or 'replace'.")

            self.cursor.copy_expert(sql=copy_query, file=buffer)

    def execute_query(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()


def main():
    csv_file_path = r"D:\Data Engineer\data\archive1\Bank_Stock_Price_10Y.csv"
    table = 'bank_stock_data'
    schema = "public"
    mode = "append"
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
        # db_manager.connection.commit()
        
        s = time()
        db_manager.upload_df_to_database(df, schema, table, mode)
        # db_manager.connection.commit()
        print(f"Table uploaded in {round((time() - s),2)} seconds in {mode} mode.")

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
