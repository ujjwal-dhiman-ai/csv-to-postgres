import psycopg2
from psycopg2 import sql
import csv, io, os
import pandas as pd

def create_table(cursor, df, schema='public', table_name='your_table_name'):
    # Map Pandas data types to equivalent PostgreSQL data types
    type_mapping = {
        'int64': 'INTEGER',
        'float64': 'FLOAT',
        'object': 'TEXT',
        'bool': 'BOOLEAN',
        # Add more mappings as needed
    }

    # Get the DataFrame columns and their data types
    columns_data_types = {
        column: type_mapping.get(str(df.dtypes[column]), 'TEXT')  # Use 'TEXT' as a default type
        for column in df.columns
    }

    # Define the table creation query dynamically
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
        cursor.execute(table_creation_query)
        print(f"Table '{schema}.{table_name}' created successfully.")
    except Exception as e:
        print(f"Error creating table '{schema}.{table_name}': {e}")


def upload_df_to_database(connection, df, schema='public', table_name='your_table_name'):
    # Create a temporary buffer to store the DataFrame data in CSV format
    buffer = io.StringIO()
    # Use tab as a separator for better performance
    df.to_csv(buffer, index=False, header=False, sep='\t')

    # Move the buffer position to the beginning
    buffer.seek(0)

    # Create the COPY command dynamically
    copy_query = """
        COPY {}.{} FROM STDIN WITH CSV DELIMITER E'\\t' NULL AS '';
    """.format(schema, table_name)

    # Open a connection and create a cursor
    with connection, connection.cursor() as cursor:
        # Execute the COPY command using the buffer as the data source
        cursor.copy_expert(sql=copy_query, file=buffer)


def main():

    # CSV file path
    csv_file_path = r"D:\Data Engineer\Data Pipelines\data\archive\PS_20174392719_1491204439457_log.csv"
    table_name = 'fraud_detection_data'
    schema = "public"
    
    database_url = os.environ.get('DB_STRING')

    try:
        # Connect to the PostgreSQL database
        connection = psycopg2.connect(database_url)

        # Create a cursor object to execute SQL queries
        cursor = connection.cursor()
        
        df = pd.read_csv(csv_file_path)
        
        # Create the table (if not exists) based on DataFrame headers
        create_table(cursor, df, schema, table_name)
        connection.commit()
        
        # Upload DataFrame to the database
        upload_df_to_database(connection, df, schema, table_name)
        connection.commit()

        # Example query
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print("PostgreSQL database version:", version)

    except Exception as e:
        print("Error: Unable to connect to the database.")
        print(e)

    finally:
        # Close the cursor and connection
        if connection:
            cursor.close()
            connection.close()
            print("Connection closed.")

if __name__ == "__main__":
    main()
