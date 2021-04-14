from pykrx import stock
import pandas as pd
from datetime import datetime

sd = '20210325'
ed = datetime.today().strftime("%Y%m%d")

tickers = stock.get_etf_ticker_list('20210325')
df = pd.DataFrame()

for ticker in tickers[1:10]:
    ticker_df = stock.get_etf_ohlcv_by_date(sd, ed, ticker)
    ticker_df.reset_index(inplace = True)
    ticker_df.columns = ['Date', 'NAV', 'Open', 'High', 'Low', 'Close', 'Volume', 'VolumeAmount', 'BaseIndex']
    ticker_df ['Ticker'] = ticker
    df = df.append(ticker_df, ignore_index=True)
print(df)