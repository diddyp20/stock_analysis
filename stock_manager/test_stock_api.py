import pandas as pd
from pandas_datareader import data as pdr
import fix_yahoo_finance as yf
import datetime
from sqlalchemy import create_engine
import numpy as np


def get_stocks_data(tickers, start_date, end_date):
    data = pdr.get_data_yahoo(tickers,
                              start=start_date, end=end_date)['Adj Close']
    data = data.reset_index()
    my_df = pd.DataFrame(data)
    print(my_df)


if __name__ == "__main__":
    start = datetime.datetime(2022, 5, 31)
    end = datetime.datetime(2022, 6, 4)
    get_stocks_data('MO', start, end)
