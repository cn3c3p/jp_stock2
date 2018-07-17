from os.path import join, relpath
import os, sys
from glob import glob
import pandas as pd
from sqlalchemy import create_engine
import os, sys
from datetime import datetime,timedelta
from time import sleep
from tqdm import tqdm

yobi = ['月','火','水','木','金','土','日']

columns = ['st_code','name','market','industry','trading_closed','stock_price','dbr_y','dbr_per','dbr_price','start','high','low',\
        'vwap','vol','vol_rate','value','capi','low_lim','upp_lim','h_price_d','h_price','h_price_kairi','l_price_d','l_price',\
        'l_price_kairi']

f_list = sorted(glob('/Users/shin/Downloads/tmp/japan-all-stock-prices-2*.csv'))

os.system('nkf -w --overwrite /Users/shin/Downloads/tmp/japan-all-stock-prices-2*.csv')

for f in tqdm(f_list):
    d2_prices = pd.read_csv(f, skiprows = 1, na_values = ('-',''), names = columns )
    file_make = datetime.fromtimestamp(os.stat(f).st_mtime)
    date0 = datetime.strptime(d2_prices.trading_closed[0], "%Y/%m/%d %H:%M")
    date = date0.strftime('%Y-%m-%d')
    date_ymd = int(date0.strftime('%Y%m%d'))
    year = date0.year
    month = date0.month
    day = date0.day
    week_no = date0.isocalendar()[1]
    dotwek = date0.weekday()
    youbi = '{}曜日'.format(yobi[date0.weekday()])
    n = len(d2_prices)
    d2_prices['date'] = [date] * n
    d2_prices['date_ymd'] = [date_ymd] * n
    d2_prices['year'] = [year] * n
    d2_prices['month'] = [month] * n
    d2_prices['day'] = [day] * n
    d2_prices['week_no'] = [week_no] * n
    d2_prices['dotwek'] = [dotwek] * n
    d2_prices['youbi'] = [youbi] * n
    d2_prices['trading_closed'] = pd.to_datetime(d2_prices['trading_closed'])
    d2_prices['date'] = pd.to_datetime(d2_prices['date'])
    d2_prices['h_price_d'] = pd.to_datetime(d2_prices['h_price_d'])
    d2_prices['l_price_d'] = pd.to_datetime(d2_prices['l_price_d'])
    engine = create_engine('postgresql://shinya@localhost:5432/saber')
    d2_prices.to_sql('daily_stock_price2', engine, if_exists = 'append', index = False)
