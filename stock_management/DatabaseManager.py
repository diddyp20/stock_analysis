import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_DATABASE = os.getenv('DB_DATABASE')
DB_HOST = os.getenv('DB_HOST')


class DatabaseManager:
    @staticmethod
    def select_data(query):
        db_connection_str = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_DATABASE}'
        db_connection = create_engine(db_connection_str)
        print(f'Connected to the database {DB_DATABASE}')
        try:
            print('reading from the database')
            symbols_df = pd.read_sql(query, con=db_connection)
            db_connection.dispose()
        except Exception as e:
            print('Issue while reading the database', e)
        return symbols_df

    # function to insert data into the database

    @staticmethod
    def insert_data(query, data_tuple):
        db_connection_str = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_DATABASE}'
        db_connection = create_engine(db_connection_str)
        try:
            insert_id = db_connection.execute(query, data_tuple)
            print("Rows Added  = ", insert_id.rowcount)
        except Exception as e:
            print("Database error ", e)

    @staticmethod
    def update_table(query, table, where_column, set_column1, set_value):
        db_connection_str = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_DATABASE}'
        db_connection = create_engine(db_connection_str)
        for symbol in query:
            # stmt = table.update().where(table.c.where_column == symbol[0]).values(set_column1=set_value)
            stmt = f"UPDATE {table} SET {set_column1} = {set_value} where {where_column} = {symbol[0]}"
            try:
                print(f"executing query {stmt}")
                db_connection.execute(stmt)
                print(f"{table} successfully updated...")
            except Exception as e:
                print(f"Error updating table {table}, Reason: ", e)
                next
