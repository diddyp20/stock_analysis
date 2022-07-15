import stock_manager.get_daily_data_from_api as dd
import pandas as pd


def test_update_db():
    stocks_df = pd.DataFrame([['MO'], ['LMT'], ['PG']], columns=['symbol'])
    print(stocks_df)
    db_mgr = dd.update_stock_database(stocks_df)
    assert db_mgr
