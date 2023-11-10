import datetime
import pandas as pd
from py5paisa import FivePaisaClient
from sqlalchemy import create_engine

def create_symbol_table():
    l1 = [{'Scripcode': 7, 'Root': 'AARTIIND'}, {'Scripcode': 13, 'Root': 'ABB'}, {'Scripcode': 22, 'Root': 'ACC'},
      {'Scripcode': 25, 'Root': 'ADANIENT'}, {'Scripcode': 157, 'Root': 'APOLLOHOSP'},
      {'Scripcode': 163, 'Root': 'APOLLOTYRE'}, {'Scripcode': 212, 'Root': 'ASHOKLEY'},
      {'Scripcode': 220, 'Root': 'IEX'}, {'Scripcode': 236, 'Root': 'ASIANPAINT'}, {'Scripcode': 263, 'Root': 'ATUL'},
      {'Scripcode': 275, 'Root': 'AUROPHARMA'}, {'Scripcode': 317, 'Root': 'BAJFINANCE'},
      {'Scripcode': 335, 'Root': 'BALKRISIND'}, {'Scripcode': 341, 'Root': 'BALRAMCHIN'},
      {'Scripcode': 371, 'Root': 'BATAINDIA'}, {'Scripcode': 383, 'Root': 'BEL'},
      {'Scripcode': 404, 'Root': 'BERGEPAINT'}, {'Scripcode': 422, 'Root': 'BHARATFORG'},
      {'Scripcode': 438, 'Root': 'BHEL'}, {'Scripcode': 467, 'Root': 'HDFCLIFE'},
      {'Scripcode': 498, 'Root': 'SHRIRAMFIN'}, {'Scripcode': 526, 'Root': 'BPCL'},
      {'Scripcode': 547, 'Root': 'BRITANNIA'}, {'Scripcode': 583, 'Root': 'CANFINHOME'},
      {'Scripcode': 637, 'Root': 'CHAMBLFERT'}, {'Scripcode': 676, 'Root': 'EXIDEIND'},
      {'Scripcode': 685, 'Root': 'CHOLAFIN'}, {'Scripcode': 694, 'Root': 'CIPLA'},
      {'Scripcode': 739, 'Root': 'COROMANDEL'}, {'Scripcode': 772, 'Root': 'DABUR'},
      {'Scripcode': 881, 'Root': 'DRREDDY'}, {'Scripcode': 910, 'Root': 'EICHERMOT'},
      {'Scripcode': 958, 'Root': 'ESCORTS'}, {'Scripcode': 1023, 'Root': 'FEDERALBNK'},
      {'Scripcode': 1174, 'Root': 'GNFC'}, {'Scripcode': 1232, 'Root': 'GRASIM'},
      {'Scripcode': 1270, 'Root': 'AMBUJACEM'}, {'Scripcode': 1333, 'Root': 'HDFCBANK'},
      {'Scripcode': 1348, 'Root': 'HEROMOTOCO'}, {'Scripcode': 1363, 'Root': 'HINDALCO'},
      {'Scripcode': 1394, 'Root': 'HINDUNILVR'}, {'Scripcode': 1406, 'Root': 'HINDPETRO'},
      {'Scripcode': 1512, 'Root': 'INDHOTEL'}, {'Scripcode': 1515, 'Root': 'INDIACEM'},
      {'Scripcode': 1594, 'Root': 'INFY'}, {'Scripcode': 1624, 'Root': 'IOC'}, {'Scripcode': 1633, 'Root': 'IPCALAB'},
      {'Scripcode': 1660, 'Root': 'ITC'}, {'Scripcode': 1901, 'Root': 'CUMMINSIND'},
      {'Scripcode': 1922, 'Root': 'KOTAKBANK'}, {'Scripcode': 1964, 'Root': 'TRENT'},
      {'Scripcode': 1997, 'Root': 'LICHSGFIN'}, {'Scripcode': 2031, 'Root': 'M&M'},
      {'Scripcode': 2043, 'Root': 'RAMCOCEM'}, {'Scripcode': 2142, 'Root': 'MFSL'},
      {'Scripcode': 2181, 'Root': 'BOSCHLTD'}, {'Scripcode': 2263, 'Root': 'BANDHANBNK'},
      {'Scripcode': 2277, 'Root': 'MRF'}, {'Scripcode': 2303, 'Root': 'HAL'}, {'Scripcode': 2412, 'Root': 'PEL'},
      {'Scripcode': 2475, 'Root': 'ONGC'}, {'Scripcode': 2664, 'Root': 'PIDILITIND'},
      {'Scripcode': 2885, 'Root': 'RELIANCE'}, {'Scripcode': 2931, 'Root': 'RECLTD'},
      {'Scripcode': 2963, 'Root': 'SAIL'}, {'Scripcode': 3045, 'Root': 'SBIN'}, {'Scripcode': 3063, 'Root': 'VEDL'},
      {'Scripcode': 3103, 'Root': 'SHREECEM'}, {'Scripcode': 3150, 'Root': 'SIEMENS'},
      {'Scripcode': 3273, 'Root': 'SRF'}, {'Scripcode': 3351, 'Root': 'SUNPHARMA'},
      {'Scripcode': 3405, 'Root': 'TATACHEM'}, {'Scripcode': 3426, 'Root': 'TATAPOWER'},
      {'Scripcode': 3432, 'Root': 'TATACONSUM'}, {'Scripcode': 3456, 'Root': 'TATAMOTORS'},
      {'Scripcode': 3499, 'Root': 'TATASTEEL'}, {'Scripcode': 3506, 'Root': 'TITAN'},
      {'Scripcode': 3518, 'Root': 'TORNTPHARM'}, {'Scripcode': 3718, 'Root': 'VOLTAS'},
      {'Scripcode': 3721, 'Root': 'TATACOMM'}, {'Scripcode': 3787, 'Root': 'WIPRO'},
      {'Scripcode': 3812, 'Root': 'ZEEL'},
      {'Scripcode': 4067, 'Root': 'MARICO'}, {'Scripcode': 4204, 'Root': 'MOTHERSON'},
      {'Scripcode': 4244, 'Root': 'HDFCAMC'}, {'Scripcode': 4503, 'Root': 'MPHASIS'},
      {'Scripcode': 4668, 'Root': 'BANKBARODA'}, {'Scripcode': 4717, 'Root': 'GAIL'},
      {'Scripcode': 4749, 'Root': 'CONCOR'}, {'Scripcode': 4963, 'Root': 'ICICIBANK'},
      {'Scripcode': 5258, 'Root': 'INDUSINDBK'}, {'Scripcode': 5701, 'Root': 'CUB'},
      {'Scripcode': 5815, 'Root': 'IBULHSGFIN'}, {'Scripcode': 5900, 'Root': 'AXISBANK'},
      {'Scripcode': 5926, 'Root': 'INTELLECT'}, {'Scripcode': 6364, 'Root': 'NATIONALUM'},
      {'Scripcode': 6733, 'Root': 'JINDALSTEL'}, {'Scripcode': 6994, 'Root': 'BSOFT'},
      {'Scripcode': 7229, 'Root': 'HCLTECH'}, {'Scripcode': 7377, 'Root': 'NTPC'},
      {'Scripcode': 7406, 'Root': 'GLENMARK'}, {'Scripcode': 7929, 'Root': 'ZYDUSLIFE'},
      {'Scripcode': 8075, 'Root': 'DALBHARAT'}, {'Scripcode': 8479, 'Root': 'TVSMOTOR'},
      {'Scripcode': 9581, 'Root': 'METROPOLIS'}, {'Scripcode': 9590, 'Root': 'POLYCAB'},
      {'Scripcode': 9819, 'Root': 'HAVELLS'}, {'Scripcode': 10099, 'Root': 'GODREJCP'},
      {'Scripcode': 10243, 'Root': 'SYNGENE'}, {'Scripcode': 10440, 'Root': 'LUPIN'},
      {'Scripcode': 10447, 'Root': 'MCDOWELL-N'}, {'Scripcode': 10599, 'Root': 'GUJGASLTD'},
      {'Scripcode': 10604, 'Root': 'BHARTIARTL'}, {'Scripcode': 10666, 'Root': 'PNB'},
      {'Scripcode': 10726, 'Root': 'INDIAMART'}, {'Scripcode': 10738, 'Root': 'OFSS'},
      {'Scripcode': 10794, 'Root': 'CANBK'}, {'Scripcode': 10940, 'Root': 'DIVISLAB'},
      {'Scripcode': 10999, 'Root': 'MARUTI'}, {'Scripcode': 11184, 'Root': 'IDFCFIRSTB'},
      {'Scripcode': 11195, 'Root': 'INDIGO'}, {'Scripcode': 11262, 'Root': 'IGL'}, {'Scripcode': 11287, 'Root': 'UPL'},
      {'Scripcode': 11351, 'Root': 'PETRONET'}, {'Scripcode': 11373, 'Root': 'BIOCON'},
      {'Scripcode': 11483, 'Root': 'LT'}, {'Scripcode': 11532, 'Root': 'ULTRACEMCO'},
      {'Scripcode': 11536, 'Root': 'TCS'}, {'Scripcode': 11543, 'Root': 'COFORGE'},
      {'Scripcode': 11654, 'Root': 'LALPATHLAB'}, {'Scripcode': 11703, 'Root': 'ALKEM'},
      {'Scripcode': 11723, 'Root': 'JSWSTEEL'}, {'Scripcode': 11872, 'Root': 'GRANULES'},
      {'Scripcode': 11957, 'Root': 'IDFC'}, {'Scripcode': 13147, 'Root': 'PVRINOX'},
      {'Scripcode': 13270, 'Root': 'JKCEMENT'}, {'Scripcode': 13285, 'Root': 'M&MFIN'},
      {'Scripcode': 13404, 'Root': 'SUNTV'}, {'Scripcode': 13528, 'Root': 'GMRINFRA'},
      {'Scripcode': 13538, 'Root': 'TECHM'}, {'Scripcode': 13611, 'Root': 'IRCTC'},
      {'Scripcode': 13751, 'Root': 'NAUKRI'}, {'Scripcode': 14299, 'Root': 'PFC'}, {'Scripcode': 14366, 'Root': 'IDEA'},
      {'Scripcode': 14413, 'Root': 'PAGEIND'}, {'Scripcode': 14418, 'Root': 'ASTRAL'},
      {'Scripcode': 14672, 'Root': 'NAVINFLUOR'}, {'Scripcode': 14732, 'Root': 'DLF'},
      {'Scripcode': 14977, 'Root': 'POWERGRID'}, {'Scripcode': 15044, 'Root': 'DELTACORP'},
      {'Scripcode': 15083, 'Root': 'ADANIPORTS'}, {'Scripcode': 15141, 'Root': 'COLPAL'},
      {'Scripcode': 15332, 'Root': 'NMDC'}, {'Scripcode': 16669, 'Root': 'BAJAJ-AUTO'},
      {'Scripcode': 16675, 'Root': 'BAJAJFINSV'}, {'Scripcode': 16713, 'Root': 'UBL'},
      {'Scripcode': 17094, 'Root': 'CROMPTON'}, {'Scripcode': 17534, 'Root': 'MGL'},
      {'Scripcode': 17818, 'Root': 'LTIM'}, {'Scripcode': 17875, 'Root': 'GODREJPROP'},
      {'Scripcode': 17903, 'Root': 'ABBOTINDIA'}, {'Scripcode': 17939, 'Root': 'HINDCOPPER'},
      {'Scripcode': 17963, 'Root': 'NESTLEIND'}, {'Scripcode': 17971, 'Root': 'SBICARD'},
      {'Scripcode': 18096, 'Root': 'JUBLFOOD'}, {'Scripcode': 18365, 'Root': 'PERSISTENT'},
      {'Scripcode': 18391, 'Root': 'RBLBANK'}, {'Scripcode': 18564, 'Root': 'LTTS'},
      {'Scripcode': 18652, 'Root': 'ICICIPRULI'}, {'Scripcode': 19061, 'Root': 'MANAPPURAM'},
      {'Scripcode': 19234, 'Root': 'LAURUSLABS'}, {'Scripcode': 19943, 'Root': 'DEEPAKNTR'},
      {'Scripcode': 20242, 'Root': 'OBEROIRLTY'}, {'Scripcode': 20374, 'Root': 'COALINDIA'},
      {'Scripcode': 21238, 'Root': 'AUBANK'}, {'Scripcode': 21614, 'Root': 'ABCAPITAL'},
      {'Scripcode': 21690, 'Root': 'DIXON'}, {'Scripcode': 21770, 'Root': 'ICICIGI'},
      {'Scripcode': 21808, 'Root': 'SBILIFE'}, {'Scripcode': 23650, 'Root': 'MUTHOOTFIN'},
      {'Scripcode': 24184, 'Root': 'PIIND'}, {'Scripcode': 24948, 'Root': 'L&TFH'},
          {'Scripcode': 29135, 'Root': 'INDUSTOWER'}, {'Scripcode': 31181, 'Root': 'MCX'}]

    df = pd.DataFrame(l1)
    df = df.rename(columns={'Root':'Symbol'})


    engine = create_engine('sqlite:///dataset/nifty')

    df.to_sql('symbols',con=engine,index=False)


def read_symbol_list():
    engine = create_engine('sqlite:///dataset/nifty')
    df = pd.read_sql('SELECT * FROM symbols', con=engine)
    return df

def read_data(symbol):
    engine = create_engine('sqlite:///dataset/nifty')
    df = pd.read_sql(f'SELECT * FROM "{symbol}"', con=engine)
    print(df.head())
    return df



