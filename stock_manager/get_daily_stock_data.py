import pandas as pd
from pandas_datareader import data as pdr
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
start = datetime.datetime(2022, 5, 31)
end = datetime.datetime(2022, 6, 4)

def get_symbols_from_db():
    db_connection_str = 'mysql+pymysql://admin:root@localhost/stock_analysis'
    db_connection = create_engine(db_connection_str)
    try:
        symbols_df = pd.read_sql("Select symbol from stocks where symbol in ('MO', 'PG', 'LMT')", con=db_connection)
        db_connection.dispose()
    except Exception as e:
        print('Issue while reading the database', e)
    return symbols_df


# get daily transaction from yahoo api
def get_daily_transactions(stock_list):
    npList = np.array(stock_list)
    my_list = [npList[x][0] for x in range(len(npList))]
    #call yahoo api
    daily_stocks_df = pdr.get_data_yahoo(my_list, start=start, end=end)

    print(daily_stocks_df.count())
    #write into a csv file to inspect
    daily_stocks_df.to_csv(r'C:\Users\user\Documents\BndNetworks\Projects\Stocks\resources\dailyStock.csv')


if __name__ == '__main__':
    symbol_df = get_symbols_from_db()
    get_daily_transactions(symbol_df)

'''
stocks = ["MO", "AAPL"]
start = datetime.datetime(2022, 5, 31)
end = datetime.datetime(2022, 6, 4)

f = pdr.get_data_yahoo(stocks, start=start, end=end)

print(f.head(2))

# yf.download(stocks,  start=start, end=end)
'''
