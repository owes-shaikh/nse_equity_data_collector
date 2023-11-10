import pandas as pd

from config import *
from py5paisa import FivePaisaClient
from model import read_symbol_list
import datetime

def get_client(cred, email, passwd, dob):
    client = FivePaisaClient(email=email, passwd=passwd, dob=dob, cred=cred)
    client.login()
    return client

def get_candle(client, scr):
    '''This Function Gets the data from NSE for stock'''
    t1 = datetime.datetime.today().strftime('%Y-%m-%d')
    t2 = (datetime.datetime.today() - datetime.timedelta(days=180)).strftime('%Y-%m-%d')
    try:
        df1 = client.historical_data("N", "C", scr, '1d', t2, t1)
        # print(df1)
        return df1
    except:
        return None

def get_scrip_code(symbol):
    symbol_df = read_symbol_list()
    df = symbol_df.query(f'Symbol=="{symbol}"').reset_index()
    if ~df.empty:
        scrip_code = df.Scripcode.iloc[0]
        return scrip_code

def get_data(symbol,client):
    scrip_code = get_scrip_code(symbol)
    if scrip_code is not None:
        # client = get_client(cred,email,passwd,dob)
        df = get_candle(client,scrip_code)
        return df

def saveDataInDatabase(symbol,client):
    from sqlalchemy import create_engine
    engine = create_engine('sqlite:///dataset/nifty')
    df = get_data(symbol,client)
    if df is not None:
        df.to_sql(symbol,con=engine,index=False)
        print(f'Data Saved Successfully for {symbol}')
    else:
        print(f'Error in saving Data for {symbol}')

def save_all_data():
    from sqlalchemy import create_engine
    engine = create_engine('sqlite:///dataset/nifty')
    df = pd.read_sql('SELECT * FROM symbols',con=engine)
    client = get_client(cred,email,passwd,dob)
    for symbol in df.Symbol:
        try:
            saveDataInDatabase(symbol,client)
        except BaseException  as e:
            print(e.args)


