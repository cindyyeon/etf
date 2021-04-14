import FinanceDataReader as fdr
from db_connect import *

start_time = timeit.default_timer() #시작 시간 체크

pd.set_option("display.precision", 10)
int_col = ['close', 'open', 'high', 'low', 'volume']
#################################################################################################
# 종목 정보 입력합니다.
#
# S&P 500, 뉴욕증권거래소, 나스닥, 아멕스 종목정보 업데이트
# 종목정보: 티커, 거래소, 종목명, 섹터, 산업
#################################################################################################

def ticker_info_update(table_name='ticker_info'):

    df_nyse = fdr.StockListing('NYSE')
    df_amex = fdr.StockListing('AMEX')
    df_nasdaq = fdr.StockListing('NASDAQ')


    df_nyse['exchange'] = 'NYSE'
    df_amex['exchange'] = 'AMEX'
    df_nasdaq['exchange'] = 'NASDAQ'

    ticker_info = pd.concat([df_nyse, df_nasdaq, df_amex], axis=0)
    ticker_info.rename(columns={'Symbol': 'ticker', 'Name':'name', 'Industry':'industry'}, inplace=True)
    ticker_info = ticker_info[['ticker', 'exchange', 'name']]
    ticker_info = ticker_info.astype('string')
    engine = Engine()
    conn = engine.connect('etf')
    # db에 입력
    freq = int(np.ceil(len(ticker_info) / 1000))
    for i in range(freq):
        ticker_info[i*1000:(i+1)*1000].to_sql(name = table_name, con = conn, if_exists='append', index=False)
    print('종목 정보가 입력되었습니다.')

#################################################################################################
# 가격 정보 입력합니다. 입력 시작일자부터 종료 일자까지 입력합니다. 종료일은 자동으로 최신까지로 합니다.
#
# S&P 500, 뉴욕증권거래소, 나스닥, 아멕스 종목정보 업데이트
# 가격정보: 티커, 종가, 일간수익률, 시가, 고가, 저가, 거래량 등
#################################################################################################

def ticker_price_update(exchange = 'NYSE'):
    if exchange == 'NYSE':
        df_info = fdr.StockListing('NYSE')
        table_name = 'nyse_price'
    elif exchange == 'AMEX':
        df_info = fdr.StockListing('AMEX')
        table_name = 'amex_price'
    else:
        df_info = fdr.StockListing('NASDAQ')
        table_name = 'nasdaq_price'

    df_info.rename(columns={'Symbol': 'ticker', 'Name': 'name', 'Industry': 'industry'},
                   inplace=True)
    # ticker_price = pd.DataFrame()
    for ticker in df_info.ticker:
        engine = Engine()
        conn = engine.connect('etf')
        sql = "select last_date from ticker_price_data_info where ticker='%s' limit 1"
        start = conn.execute(sql % (ticker)).fetchall()
        if start == []:
            start = None
        try:
            df = fdr.DataReader(ticker, start=start)
        except:
            continue
        if len(df) == 0:
            continue
        else:
            if 'Volume' in df.columns:
                df.columns = ['close', 'open', 'high', 'low', 'volume', 'change']
            else:
                df.columns = ['close', 'open', 'high', 'low', 'change']
                df['volume'] = None
            df['td'] = df.index
            df['ticker'] = ticker
            df['rtn'] = (df['close'] - df['close'].shift(1)) / df['close'].shift(1)
            df.reset_index(inplace=True)
            df = df[['td', 'ticker', 'close', 'rtn', 'open', 'high', 'low', 'volume']]
            df['clo5'] = df['close'].rolling(window=5).mean()
            df['clo10'] = df['close'].rolling(window=10).mean()
            df['clo20'] = df['close'].rolling(window=20).mean()
            df['clo60'] = df['close'].rolling(window=60).mean()
            df['clo120'] = df['close'].rolling(window=120).mean()
            df['clo5_diff_rate'] = (df['close'] - df['clo5']) / df['clo5']
            df['clo10_diff_rate'] = (df['close'] - df['clo10']) / df['clo10']
            df['clo20_diff_rate'] = (df['close'] - df['clo20']) / df['clo20']
            df['clo60_diff_rate'] = (df['close'] - df['clo60']) / df['clo60']
            df['clo120_diff_rate'] = (df['close'] - df['clo120']) / df['clo120']
            df.replace([np.inf, -np.inf], np.nan, inplace=True)
            df.to_sql(name=table_name, con=conn, if_exists='append', index=False)

            if start == None:
                update_info= [(exchange, ticker, max(df['td']))]
                update_info = pd.DataFrame(update_info, columns=['exchange', 'ticker', 'last_date'])
                update_info.to_sql(name='ticker_price_data_info', con=conn, if_exists='append', index=False)
            else:
                sql = "UPDATE ticker_price_data_info SET last_date='%s' where ticker='%s' and exchange='%s'"
                conn.execute(sql % (max(df['td']), ticker, exchange))
            print(ticker, ' 가격 정보가 입력되었습니다.')
            time.sleep(1)
        # ticker_price = pd.concat([ticker_price, df], axis=0)

    # db 저장을 위해 dataframe 데이터 타입을 일부 변경한다.

    # ticker_price.reset_index(inplace=True)
    # ticker_price.drop(['index'], axis=1, inplace=True)
    # ticker_price[int_col] = ticker_price[int_col].astype(dtype='int')
    # engine = Engine()
    # conn = engine.connect('etf')
    #
    # freq = int(np.ceil(len(ticker_price) / 1000))
    # for i in range(freq):
    #     ticker_price[i * 1000:(i + 1) * 1000].to_sql(name=table_name, con=conn, if_exists='append', index=False)

    # print('가격 정보가 입력되었습니다.')


ticker_price_update(exchange = 'NYSE')