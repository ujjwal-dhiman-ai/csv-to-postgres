import psycopg2
from psycopg2 import sql
import csv
import io
import os
import pandas as pd


class DatabaseManager:
    def __init__(self, database_url):
        self.database_url = database_url
        self.connection = None
        self.cursor = None

    def connect(self):
        self.connection = psycopg2.connect(self.database_url)
        self.cursor = self.connection.cursor()

    def disconnect(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()
            print("Connection closed.")

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

    def upload_df_to_database(self, df, schema='public', table_name='your_table_name'):
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False, sep='\t')
        buffer.seek(0)

        copy_query = """
            COPY {}.{} FROM STDIN WITH CSV DELIMITER E'\\t' NULL AS '';
        """.format(schema, table_name)

        self.cursor.copy_expert(sql=copy_query, file=buffer)

    def execute_query(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()


def main():
    csv_file_path = r"D:\Data Engineer\Data Pipelines\data\archive\PS_20174392719_1491204439457_log.csv"
    table_name = 'financial_data'
    schema = "public"
    database_url = os.environ.get('DB_STRING')

    try:
        db_manager = DatabaseManager(database_url)
        db_manager.connect()

        df = pd.read_csv(csv_file_path)

        db_manager.create_table(df, schema, table_name)
        db_manager.connection.commit()

        db_manager.upload_df_to_database(df, schema, table_name)
        db_manager.connection.commit()

        version_query = "SELECT version();"
        version = db_manager.execute_query(version_query)
        print("PostgreSQL database version:", version)

    except Exception as e:
        print("Error: Unable to connect to the database.")
        print(e)

    finally:
        if db_manager:
            db_manager.disconnect()


if __name__ == "__main__":
    main()
