import pandas as pd
from pandas_datareader import data as pdr
import mysql.connector
import fix_yahoo_finance as yf
import datetime
from sqlalchemy import create_engine
import numpy as np

'''
Using SQLAchemy to get Stocks symbols from the database
Convert symbols to a list or array
pass the array to the yahoo api to get value from 2022-01-01 to date
transform the dataframe and keep only columns interested
convert dataframe to tuple
load tuple into table
'''
user = 'admin'
password = 'root'
database = 'stock_analysis'
start = datetime.datetime(2022, 5, 31)
end = datetime.datetime(2022, 6, 4)


def get_symbols_from_db():
    db_connection_str = f'mysql+pymysql://{user}:{password}@localhost/{database}'
    db_connection = create_engine(db_connection_str)
    try:
        print('pulling data')
        symbols_df = pd.read_sql(
            "Select distinct symbol from stocks\
                        where id < 7",
            con=db_connection)
        db_connection.dispose()
    except Exception as e:
        print('Issue while reading the database', e)
    return symbols_df


# get daily transaction from yahoo api
def get_daily_transactions(stock_list):
    npList = np.array(stock_list)
    my_list = [npList[x][0] for x in range(len(npList))]
    # print(my_list)
    allData = pd.DataFrame()
    finalTickers = []
    for ticker in my_list:
        try:
            allData = pd.merge(allData, pd.DataFrame(pdr.get_data_yahoo(ticker,
                                                                        start=start, end=end)['Adj Close']),
                               right_index=True, left_index=True, suffixes=('', '_delme'), how='outer')
            finalTickers.append(ticker)
            print(f"Successfully reading data for {ticker}")
        except:
            print(f"Error reading data for {ticker}")
            next

    # call yahoo api
    allData.columns = finalTickers
    allData = allData.dropna(axis='columns')
    return allData.reset_index()


def load_increment_data(data_tuple):
    cnx = mysql.connector.connect(user=f'{user}', password=f'{password}', host='localhost', database=f'{database}',
                                  auth_plugin='mysql_native_password')
    mycur = cnx.cursor()

    # Write the insert query
    query = 'INSERT INTO STOCK_DAILY (closing_dt, Close, symbol)\
            VALUES (%s, %s, %s)'

    # Inserting
    try:
        print('Inserting '+ str(data_tuple)+ 'into the database', end='\n')
        mycur.executemany(query, data_tuple)
        cnx.commit()
        print('Successfully inserted stocks into the database...')
        cnx.close()
    except Exception as e:
        print('Error while inserting data into the database', e)
        cnx.close()


if __name__ == '__main__':
    symbol_df = get_symbols_from_db()
    stock_df = get_daily_transactions(symbol_df)
    date_df = stock_df['Date']
    for tickers in symbol_df.to_numpy():
        t = pd.DataFrame(date_df)
        t['DateTypeCol'] = np.datetime_as_string(t['Date'], unit='D')
        t.drop('Date', inplace=True, axis=1)
        if tickers[0] in stock_df.columns:
            print(f"cant found column {tickers[0]}")
            tdf = pd.DataFrame(stock_df[tickers[0]])
            tdf = tdf.round(2)
            data_df = pd.concat([t, tdf], axis=1)
            data_df['Ticker'] = str(tickers[0])
            tup_data = [tuple(x) for x in data_df.to_numpy()]
            # Load the data into mysql
            if len(tup_data) > 0:
                load_increment_data(tup_data)
