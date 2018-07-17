# coding: utf-8
# 必要そうなmoduleを乗せとく。
from os.path import join, relpath
import os
import sys
from glob import glob
from datetime import datetime, timedelta
import pandas as pd
from pandas import DataFrame,Series
import pandas_datareader as pdr
import pandas.io.sql as psql
import psycopg2 as pg
import subprocess
from sqlalchemy import create_engine
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.dates import date2num
from pivottablejs import pivot_ui
import seaborn as sns
from time import sleep
from tqdm import tqdm
import cufflinks as cf
cf.set_config_file(offline=True)

sys.path.append('/Users/shin/stubby')
pd.set_option('display.width',280)
# pd.get_option('display.width') で表示幅の確認ができる。

# 日付系の定義。
now = datetime.now()
today = now.strftime('%Y-%m-%d')
one_week = (now - timedelta(weeks=1)).strftime('%Y-%m-%d')
four_w_ago = (now - timedelta(weeks=4)).strftime('%Y-%m-%d')
week_no = now.isocalendar()[1]
range_of_week_no = week_no - 13

# PostgreSQLに接続。
conn = pg.connect('postgresql://shinya@localhost:5432/saber')

# Pandas に読み込むTable。

d2_price = 'select \
        st_code, date, stock_price, vwap, start, high, low, dbr_y, dbr_per, vol, vol_rate, value, capi, h_price, l_price, date_ymd, week_no, dotwek, name \
        from \
        daily_stock_price2 \
        where week_no >= {}' .format(range_of_week_no)

shareholding = 'select \
        st_code, hudou_hi, syousuu, tousisinntaku,gaikokujinn, date, year, month, week_no \
        from \
        shareholding \
        where week_no >= {}' .format(range_of_week_no)

w_margin = 'select \
        st_code, kai_zan, kaizan_zsh, uri_zan,urizan_zsh, zsh_taisyaku, date, year, month, week_no \
        from \
        w_margin \
        where week_no >= {}' .format(range_of_week_no)

returns = 'select \
        * \
        from \
        returns'

return_index = 'select \
        st_code, date, stock_price, vwap, start, high, low, dbr_y, returns, ret_index, vol, vol_rate, value, capi, date_ymd, week_no, dotwek, name \
        from \
        return_index'

# PostgreSQL の各Table をそれぞれ読み込む。
d2_price = psql.read_sql(d2_price, conn)
shareholding = psql.read_sql(shareholding, conn)
w_margin = psql.read_sql(w_margin, conn)
returns = psql.read_sql(returns, conn)
return_index = psql.read_sql(return_index, conn)

# PostgreSQL との接続を切る。
conn.close()

# 各データフレームを証券コード、日付でソートする。
d2_price = d2_price.sort_values(by = ['st_code', 'date'])
shareholding = shareholding.sort_values(by = ['st_code', 'date'])
w_margin = w_margin.sort_values(by = ['st_code', 'date'])

# stocks_name 銘柄コードと会社名
stocks_name = d2_price.drop_duplicates('st_code')[['st_code','name']]
st_c_list = d2_price.drop_duplicates('st_code')[['st_code']]
stocks_code_list = st_c_list.st_code.values.tolist()

'''
d2_price.index = d2_price['date_ymd']
gb = d2_price.groupby('st_code')
n = len(stocks_name)
for i in tqdm(range(n)):
    _name, _d_price = list(gb)[i]
    _d_price['returns'] = pd.Series(_d_price.stock_price).pct_change()
    _d_price['ret_index'] = (1 + _d_price.returns).cumprod()
    returns = returns.append(_d_price)
engine = create_engine('postgresql://shinya@localhost:5432/saber')
returns.to_sql('return_index', engine, if_exists = 'replace', index = False)

# んで、ジョイントする。
# df = d_price.join((w_margin, on = 'st_code', rsuffix = '_ma', how = 'inner')

# Joinした状態ではindex出来ないので join後に
# pr = pr.set_index(['st_code', 'date'])
# df = df.reset_index
# stocks_name['st_code'] = stocks_name['st_code'].astype('str')
# d_price[d_price['stock_price'] == d_price.groupby(['st_code'])['stock_price'].transform(max)]
# d_price[(d_price['dbr_per'] > 15) & (d_price['date'] == '2018-04-13')]
# for index in stocks_code_list.st_code:
#    print(d_price[(d_price['st_code']==index) & (d_price['date'] == '2018-04-13')])
# stocks_code_list.assign(x=stocks_code_list/3)
AOA
# for i in stocks_code_list.st_code:
#      print(dd_price[(dd_price['st_code'] == i) & (dd_price['date'] >= '2018-03-30')])
'''
