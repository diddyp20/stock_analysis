import pandas as pd
import numpy as np
import mysql.connector

file_location = r'C:\Users\user\Documents\BndNetworks\Projects\Stocks\resources\nasdaq_screener.csv'


def load_db(user, password, database):
    stock_df = pd.read_csv(file_location)
    cnx = mysql.connector.connect(user=f'{user}', password=f'{password}', host='localhost', database=f'{database}',
                                  auth_plugin='mysql_native_password')
    mycur = cnx.cursor()
    # converting dataframe into tuples
    stock_df = stock_df.astype(object).where(pd.notnull(stock_df), None)
    # removing unneeded columns
    stock_df = stock_df[stock_df.columns[~stock_df.columns.isin(['Last Sale', 'Net Change', '% Change', 'Volume'])]]
    stock_tuples = [tuple(x) for x in stock_df.to_numpy()]

    # Write the insert query
    query = 'INSERT INTO STOCKS (symbol, name, Market_Cap, Country, IPO_Year, Sector, Industry)\
                                VALUES (%s, %s, %s, %s,%s, %s, %s)'

    # Inserting
    try:
        mycur.executemany(query, stock_tuples)
        cnx.commit()
        print('Successfully inserted stocks into the database...')
        cnx.close()
    except Exception as e:
        print('Error while inserting data into the database', e)
        cnx.close()


if __name__ == '__main__':
    load_db('admin', 'root', 'stock_analysis')
