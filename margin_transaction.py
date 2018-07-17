from os.path import join, relpath
from glob import glob
import pandas as pd
from sqlalchemy import create_engine
import os, sys
from datetime import datetime,timedelta

# ファイル
f = '/Users/shin/Downloads/japan-all-stock-margin-transactions.csv'

# 曜日
yobi = ['月','火','水','木','金','土','日']

# 日付関連の準備。
file_make = datetime.fromtimestamp(os.stat(f).st_mtime)
file_make_time = file_make.strftime('%Y-%m-%d %H:%M:%S')
date = file_make.strftime('%Y-%m-%d')
date_ymd = int(file_make.strftime('%Y%m%d'))
year = file_make.year
month = file_make.month
day = file_make.day
week_no = file_make.isocalendar()[1]
dotwek = file_make.weekday()
youbi = '{}曜日'.format(yobi[file_make.weekday()])

# カラム名
# SC-信用買残高-信用買残高 前週比-信用売残高-信用売残高 前週比-貸借倍率
columns = ['st_code','kai_zan','kaizan_zsh','uri_zan','urizan_zsh','zsh_taisyaku']

# UTF-8 に変換する。
os.system('nkf \
        -w \
        --overwrite \
        /Users/shin/Downloads/japan-all-stock-margin-transactions.csv')

# 株価csvファイルを d_prices に読み込む。

w_margin = pd.read_csv(f, skiprows = 1, na_values = ('-',''), names = columns )

# 日付等を追加する。
n = len(w_margin)
w_margin['date'] = [date] * n
w_margin['date_ymd'] = [date_ymd] * n
w_margin['year'] = [year] * n
w_margin['month'] = [month] * n
w_margin['day'] = [day] * n
w_margin['week_no'] = [week_no] * n
w_margin['dotwek'] = [dotwek] * n
w_margin['youbi'] = [youbi] * n

# 日付をpamdas の datetime に変換する。
w_margin['date'] = pd.to_datetime(w_margin['date'])

# PostgreSQL saber に接続する。
engine = create_engine('postgresql://shinya@localhost:5432/saber')

w_margin.to_sql(\
        'w_margin', \
        engine, \
        if_exists = 'append', \
        index = False)

