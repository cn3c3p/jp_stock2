from os.path import join, relpath
from glob import glob
import pandas as pd
from sqlalchemy import create_engine
import os, sys
from datetime import datetime,timedelta

# 取得した日々の株価ファイル２
f = '/Users/shin/Downloads/japan-all-stock-prices-2.csv'

# カラム名
# SC-名称-市場-業種-日時-株価-前日比-前日比（％）-前日終値-始値-高値-安値-VWAP-出来高-出来高率-売買代金（千円）-時価総額（百万円）\
# -値幅下限-値幅上限-高値日付-年初来高値-年初来高値乖離率-安値日付-年初来安値-年初来安値乖離率

columns = ['st_code','name','market','industry','trading_closed','stock_price','dbr_y','dbr_per','dbr_price','start','high','low',\
        'vwap','vol','vol_rate','value','capi','low_lim','upp_lim','h_price_d','h_price','h_price_kairi','l_price_d','l_price',\
        'l_price_kairi']

# UTF-8 に変換する。
os.system('nkf \
        -w \
        --overwrite \
        /Users/shin/Downloads/japan-all-stock-prices-2.csv')

# 株価csvファイルを d_prices に読み込む。

d2_prices = pd.read_csv(f, skiprows = 1, na_values = ('-',''), names = columns )

# 日付等の準備。
# 読み込んだファイルの0行目、日経平均の締まった時間を date0 し当日の日付け関連データに反映させる。
file_make = datetime.fromtimestamp(os.stat(f).st_mtime)
yobi = ['月','火','水','木','金','土','日']
date0 = datetime.strptime(d2_prices.trading_closed[0], "%Y/%m/%d %H:%M")
date = date0.strftime('%Y-%m-%d')
date_ymd = int(date0.strftime('%Y%m%d'))
year = date0.year
month = date0.month
day = date0.day
week_no = date0.isocalendar()[1]
dotwek = date0.weekday()
youbi = '{}曜日'.format(yobi[date0.weekday()])

# d_prices に日付関連カラムを追加する。
n = len(d2_prices)
d2_prices['date'] = [date] * n
d2_prices['date_ymd'] = [date_ymd] * n
d2_prices['year'] = [year] * n
d2_prices['month'] = [month] * n
d2_prices['day'] = [day] * n
d2_prices['week_no'] = [week_no] * n
d2_prices['dotwek'] = [dotwek] * n
d2_prices['youbi'] = [youbi] * n

# 日付をpamdas の datetime に変換する。
d2_prices['trading_closed'] = pd.to_datetime(d2_prices['trading_closed'])
d2_prices['date'] = pd.to_datetime(d2_prices['date'])
d2_prices['h_price_d'] = pd.to_datetime(d2_prices['h_price_d'])
d2_prices['l_price_d'] = pd.to_datetime(d2_prices['l_price_d'])


# PostgreSQL saber に接続する。
engine = create_engine('postgresql://shinya@localhost:5432/saber')

# daily_stock_price に データを追加する。
d2_prices.to_sql(\
        'daily_stock_price2', \
        engine, \
        if_exists = 'append', \
        index = False)

