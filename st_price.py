from os.path import join, relpath
from glob import glob
import pandas as pd
from sqlalchemy import create_engine
import os, sys
from datetime import datetime,timedelta

# 取得した日々の株価ファイル
f = '/Users/shin/Downloads/japan-all-stock-prices.csv'

# ---- 後日、消す ----
# 曜日
# yobi = ['月','火','水','木','金','土','日']
# 日付関連の準備。
# file_make = datetime.fromtimestamp(os.stat(f).st_mtime)
# file_make_time = file_make.strftime('%Y-%m-%d %H:%M:%S')
# date = file_make.strftime('%Y-%m-%d')
# date_ymd = int(file_make.strftime('%Y%m%d'))
# year = file_make.year
# month = file_make.month
# day = file_make.day
# week_no = file_make.isocalendar()[1]
# dotwek = file_make.weekday()
# youbi = '{}曜日'.format(yobi[file_make.weekday()])
# --------------------

# カラム名
# SC-名称-市場-業種-日時-株価-前日比-前日比（％）-前日終値-始値-高値-安値-出来高-売買代金（千円）-時価総額（百万円）-値幅下限-値幅上限
columns = ['st_code','name','market','industry','trading_closed','stock_price','dbr_y','dbr_per','ld_cl_pr','start',\
        'high','low','vol','value','capi','low_lim','upp_lim']

# UTF-8 に変換する。
os.system('nkf \
        -w \
        --overwrite \
        /Users/shin/Downloads/japan-all-stock-prices.csv')

# 株価csvファイルを d_prices に読み込む。

d_prices = pd.read_csv(f, skiprows = 1, na_values = ('-',''), names = columns )

# 日付等の準備。
# 読み込んだファイルの0行目、日経平均の締まった時間を date0 し当日の日付け関連データに反映させる。
file_make = datetime.fromtimestamp(os.stat(f).st_mtime)
yobi = ['月','火','水','木','金','土','日']
date0 = datetime.strptime(d_prices.trading_closed[0], "%Y/%m/%d %H:%M")
date = date0.strftime('%Y-%m-%d')
date_ymd = int(date0.strftime('%Y%m%d'))
year = date0.year
month = date0.month
day = date0.day
week_no = date0.isocalendar()[1]
dotwek = date0.weekday()
youbi = '{}曜日'.format(yobi[date0.weekday()])

# d_prices に日付関連カラムを追加する。
n = len(d_prices)
d_prices['date'] = [date] * n
d_prices['date_ymd'] = [date_ymd] * n
d_prices['year'] = [year] * n
d_prices['month'] = [month] * n
d_prices['day'] = [day] * n
d_prices['week_no'] = [week_no] * n
d_prices['dotwek'] = [dotwek] * n
d_prices['youbi'] = [youbi] * n

# 日付をpamdas の datetime に変換する。
d_prices['trading_closed'] = pd.to_datetime(d_prices['trading_closed'])
d_prices['date'] = pd.to_datetime(d_prices['date'])

# PostgreSQL saber に接続する。
engine = create_engine('postgresql://shinya@localhost:5432/saber')

# daily_stock_price に データを追加する。
d_prices.to_sql(\
        'daily_stock_price', \
        engine, \
        if_exists = 'append', \
        index = False)
