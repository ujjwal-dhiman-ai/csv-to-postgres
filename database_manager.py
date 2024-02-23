import psycopg2
from psycopg2 import sql
import io


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

        # Adding a surrogate key
        columns_data_types = {
            'id': 'SERIAL PRIMARY KEY',  # Auto-incrementing integer
            **{
                column: type_mapping.get(str(df.dtypes[column]), 'TEXT')
                for column in df.columns
            }
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

    def fetch_last_id(self, schema, table_name):
        last_id_query = """
            SELECT MAX(id) FROM {}.{}; 
        """.format(schema, table_name)
        self.cursor.execute(last_id_query)
        last_id = self.cursor.fetchone()[0]

        # If the table is empty, set last_id to 0
        last_id = last_id if last_id is not None else 0

        return last_id

    def update_id_column(self, df, last_id):
        df['id'] = range(last_id + 1, last_id + 1 + len(df))
        return df

    def push_df_to_database(self, df, schema='public', table_name='your_table_name', mode='append'):
        # Retrieve the last 'id' value from the existing table in the database if mode == 'append':
        if mode == 'append':
            last_id = self.fetch_last_id(schema, table_name)
            df = self.update_id_column(df, last_id)
        else:
            df = self.update_id_column(df, 0)

        # Reorder columns to have 'id' in the first position
        df = df[['id'] + [col for col in df.columns if col != 'id']]

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
            raise ValueError(
                "Unsupported insertion mode. Use 'append' or 'replace'.")

        self.cursor.copy_expert(sql=copy_query, file=buffer)

    def execute_query(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()
