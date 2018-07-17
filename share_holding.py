# module 読み込み。
import pandas as pd
from sqlalchemy import create_engine
from os.path import join, relpath
from glob import glob
import os, sys
from datetime import datetime,timedelta
f = '/Users/shin/Downloads/shareholding-ratio.csv'

# 曜日
yobi = ['月','火','水','木','金','土','日']

# csvファイルをダウンロードした日時をデータフレームに加える為の準備。
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

# ダウンロードしたファイルをUTF-8に変換。
os.system('nkf \
        -w \
        --overwrite\
        /Users/shin/Downloads/shareholding-ratio.csv')

# カラム名を格納。
# SC-浮動株数比率-少数特定者持株数比率-投資信託持株数比率-外国人持株数比率
columns = ['st_code','hudou_hi','syousuu','tousisinntaku','gaikokujinn']

# ファイルを sh_df に読み込む。
shareholding = pd.read_csv('/Users/shin/Downloads/shareholding-ratio.csv', \
        skiprows = 1, \
        na_values=('-',''), \
        names = columns)

# 「%」を除去
shareholding.hudou_hi = shareholding.hudou_hi.str.split('%',expand=True)
shareholding.syousuu = shareholding.syousuu.str.split('%',expand=True)
shareholding.tousisinntaku = shareholding.tousisinntaku.str.split('%',expand=True)
shareholding.gaikokujinn = shareholding.gaikokujinn.str.split('%',expand=True)

# objectのままなので欠損値は0で穴埋め、数字部分を float に変換。
shareholding.hudou_hi = shareholding.hudou_hi.astype(float)
shareholding.syousuu = shareholding.syousuu.astype(float)
shareholding.tousisinntaku = shareholding.tousisinntaku.astype(float)
shareholding.gaikokujinn = shareholding.gaikokujinn.astype(float)

# いつのデータなのかわかる様に日付カラムを追加する。
n = len(shareholding)
shareholding['date'] = [date] * n
shareholding['date_ymd'] = [date_ymd] * n
shareholding['year'] = [year] * n
shareholding['month'] = [month] * n
shareholding['day'] = [day] * n
shareholding['week_no'] = [week_no] * n
shareholding['dotwek'] = [dotwek] * n
shareholding['youbi'] = [youbi] * n

shareholding['date'] = pd.to_datetime(shareholding['date'])

#PostgreSQLに接続する。
engine = create_engine('postgresql://shinya@localhost:5432/saber')

#既存の shareholding に append する。
shareholding.to_sql('shareholding', engine, if_exists = 'append', index=False)
