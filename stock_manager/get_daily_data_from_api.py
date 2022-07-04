"""
1. Get tickers from database
2. Pull data from api
3. load pulled data into database
"""
import stock_management.DatabaseManager as DbMgr
import numpy as np
import pandas as pd
import datetime
from pandas_datareader import data as pdr

start = datetime.datetime(2022, 5, 31)
end = datetime.datetime(2022, 6, 4)


def get_daily_data_from_api():
    query = "select distinct symbol from stocks"
    dm = DbMgr.DatabaseManager()
    tickers = dm.select_data(query)
    if len(tickers) > 0:
        # call api to get data
        tickers_tuple = np.array(tickers)
        all_data = pd.DataFrame()
        final_tickers = []
        for x in tickers_tuple:
            # call api for individual stocks
            try:
                all_data = pd.merge(all_data, pd.DataFrame(pdr.get_data_yahoo(x[0],
                                                                              start=start, end=end)['Adj Close']),
                                    right_index=True, left_index=True, suffixes=('', '_delme'), how='outer')
                final_tickers.append(x[0])
                print(f"Successfully reading data for {x[0]}")
            except:
                print(f"Error reading data for {x[0]}")
                next
        all_data.columns = final_tickers
        all_data = all_data.dropna(axis='columns')
        return all_data.reset_index()


def insert_data_into_db(stocks_df):
    db_mgr = DbMgr.DatabaseManager()
    query = 'INSERT INTO STOCK_DAILY (closing_dt, Close, symbol)\
            VALUES (%s, %s, %s)'
    date_df = stocks_df['Date']
    tickers = stocks_df.columns.to_numpy()[1::]
    # print(tickers)
    for ticker in tickers:
        time_df = pd.DataFrame(date_df)
        time_df['DateTypeCol'] = np.datetime_as_string(time_df['Date'], unit='D')
        time_df.drop('Date', inplace=True, axis=1)
        # rounding the dataframe to two digit
        ticker_df = pd.DataFrame(stocks_df[ticker]).round(2)
        data_df = pd.concat([time_df, ticker_df], axis=1)
        data_df['Ticker'] = str(ticker)
        tup_data = [tuple(x) for x in data_df.to_numpy()]
        db_mgr.insert_data(query, tup_data)

    pass


if __name__ == '__main__':
    my_stocks = get_daily_data_from_api()
    insert_data_into_db(my_stocks)
