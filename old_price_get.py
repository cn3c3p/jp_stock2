import os
import sys
from glob import glob
from datetime import datetime, timedelta
import pandas as pd

path = '/Users/shin/Downloads/tmp'
csv_files = glob.glob(path+'/*.csv')

# 曜日
yobi = ['月','火','水','木','金','土','日']
columns = ['st_code','name','market','industry','trading_closed','stock_price','dbr_y','dbr_per','ld_cl_pr','start',\
            'high','low','vol','value','capi','low_lim','upp_lim']

for f in csv_files:
    df = pd.read_csv(f, skiprows = 1, na_values = ('-',''), names = columns)
    file_make = datetime.fromtimestamp(os.stat(f).st_mtime)
    date0 = datetime.strptime(df.trading_closed[0], "%Y/%m/%d %H:%M")
    date = date0.strftime('%Y-%m-%d')
    date_ymd = int(date0.strftime('%Y%m%d'))
    year = date0.year
    month = date0.month
    day = date0.day
    week_no = date0.isocalendar()[1]
    dotwek = date0.weekday()
    youbi = '{}曜日'.format(yobi[date0.weekday()])
    n = len(df)
    df['date'] = [date] * n
    df['date_ymd'] = [date_ymd] * n
    df['year'] = [year] * n
    df['month'] = [month] * n
    df['day'] = [day] * n
    df['week_no'] = [week_no] * n
    df['dotwek'] = [dotwek] * n
    df['youbi'] = [youbi] * n
    df['trading_closed'] = pd.to_datetime(df['trading_closed'])
    df['date'] = pd.to_datetime(df['date'])
    s = s.append(df)
