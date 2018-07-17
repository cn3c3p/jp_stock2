import os
import sys
from datetime import datetime, timedelta
import pandas as pd
import shelve
from fabric.colors import green, blue, red, magenta, yellow, cyan
import subprocess

# csv files
price_file = '/Users/shin/Downloads/japan-all-stock-prices.csv'
price_file2 = '/Users/shin/Downloads/japan-all-stock-prices-2.csv'
margin_file = '/Users/shin/Downloads/japan-all-stock-margin-transactions.csv'
shareholding_file = '/Users/shin/Downloads/shareholding-ratio.csv'

# Creation date
# price
creation_price_file = datetime.fromtimestamp(os.stat(price_file).st_mtime)
d_price_d = creation_price_file.strftime('%Y-%m-%d %H:%M:%S')
pd.to_datetime(d_price_d)
# price2
creation_price2_file = datetime.fromtimestamp(os.stat(price_file2).st_mtime)
d_price2_d = creation_price2_file.strftime('%Y-%m-%d %H:%M:%S')
pd.to_datetime(d_price2_d)
# margin
creation_margin_file = datetime.fromtimestamp(os.stat(margin_file).st_mtime)
w_margin_w = creation_margin_file.strftime('%Y-%m-%d %H:%M:%S')
pd.to_datetime(w_margin_w)
# shareholding
creation_shareholding_file = datetime.fromtimestamp(os.stat(shareholding_file).st_mtime)
m_shareholding_m = creation_shareholding_file.strftime('%Y-%m-%d %H:%M:%S')
pd.to_datetime(m_shareholding_m)

# Store updated date and time of file
Previous_date_file = '/Users/shin/stubby/last_update'
last_update = shelve.open(Previous_date_file)
last_update['d_price_d'] = d_price_d
last_update['d_price2_d'] = d_price2_d
last_update['w_margin_w'] = w_margin_w
last_update['m_shareholding_m'] = m_shareholding_m
last_update.close()

# test
last_update = shelve.open(Previous_date_file)
d_price_d = last_update['d_price_d']
d_price2_d = last_update['d_price2_d']
w_margin_w = last_update['w_margin_w']
m_shareholding_m = last_update['m_shareholding_m']
last_update.close()

print(blue('旧ファイルの更新日は以下の通り。'))
print(green('japan-all-stock-prices.csv : {}' .format(d_price_d)))
print(green('japan-all-stock-prices-2.csv : {}' .format(d_price2_d)))
print(green('japan-all-stock-margin-transactions.csv : {}' .format(w_margin_w)))
print(green('shareholding-ratio.csv : {}' .format(m_shareholding_m)))

# New File
# Url of the file to download
d_price_url = 'https://csvex.com/kabu.plus/csv/japan-all-stock-prices/daily/japan-all-stock-prices.csv'
d_price2_url = 'https://csvex.com/kabu.plus/csv/japan-all-stock-prices-2/daily/japan-all-stock-prices-2.csv'
w_margin_url = 'https://csvex.com/kabu.plus/csv/japan-all-stock-margin-transactions/weekly/japan-all-stock-margin-transactions.csv'
m_shareholding_url = 'https://csvex.com/kabu.plus/csv/japan-all-stock-information/monthly/shareholding-ratio.csv'
urls = [d_price_url, d_price2_url, w_margin_url, m_shareholding_url]

# csvev pass word
csvex = shelve.open('/Users/shin/stubby/csvex')
csvex_id = csvex['csvex_id']
csvex_pass = csvex['csvex_pass']
csvex.close()

for url in urls:
    os.system(('wget \
        --http-user={0} \
        --http-passwd={1} \
        {2} \
        -P /Users/shin/Downloads/ \
        -NP /Users/shin/Downloads/ \
        -N') .format(csvex_id, csvex_pass, url))

new_price_file = '/Users/shin/Downloads/japan-all-stock-prices.csv'
new_price2_file = '/Users/shin/Downloads/japan-all-stock-prices-2.csv'
new_margin_file = '/Users/shin/Downloads/japan-all-stock-margin-transactions.csv'
new_shareholding_file = '/Users/shin/Downloads/shareholding-ratio.csv'

# New Creation date

# price
new_creation_price_file = datetime.fromtimestamp(os.stat(new_price_file).st_mtime)
new_price_d = new_creation_price_file.strftime('%Y-%m-%d %H:%M:%S')
pd.to_datetime(new_price_d)
# new_price_d = pd.to_datetime(((pd.to_datetime(datetime.fromtimestamp(os.stat('/Users/shin/Downloads/japan-all-stock-prices.csv').st_mtime))).strftime('%Y-%m-%d %H:%M:%S')))

# price2
new_creation_price2_file =datetime.fromtimestamp(os.stat(new_price2_file).st_mtime)
new_price2_d = new_creation_price2_file.strftime('%Y-%m-%d %H:%M:%S')
pd.to_datetime(new_price2_d)

# margin
new_creation_margin_file = datetime.fromtimestamp(os.stat(new_margin_file).st_mtime)
new_margin_w = new_creation_margin_file.strftime('%Y-%m-%d %H:%M:%S')
pd.to_datetime(new_margin_w)
# new_margin_w = pd.to_datetime(((pd.to_datetime(datetime.fromtimestamp(os.stat('/Users/shin/Downloads/japan-all-stock-margin-transactions.csv').st_mtime))).strftime('%Y-%m-%d %H:%M:%S')))

# shareholding
new_creation_shareholding_file = datetime.fromtimestamp(os.stat(new_shareholding_file).st_mtime)
new_shareholding_m = new_creation_shareholding_file.strftime('%Y-%m-%d %H:%M:%S')
pd.to_datetime(new_shareholding_m)
# new_shareholding_m = pd.to_datetime(((pd.to_datetime(datetime.fromtimestamp(os.stat('/Users/shin/Downloads/shareholding-ratio.csv').st_mtime))).strftime('%Y-%m-%d %H:%M:%S')))

# Store updated date and time of file
Previous_date_file = '/Users/shin/stubby/last_update'
last_update = shelve.open(Previous_date_file)
last_update['d_price_d'] = new_price_d
last_update['d_price2_d'] = new_price2_d
last_update['w_margin_w'] = new_margin_w
last_update['m_shareholding_m'] = new_shareholding_m
last_update.close()

# Comparison
if new_price_d == d_price_d:
    print(red('japan-all-stock-prices.csv 更新されていません。'))
else:
    print(green('japan-all-stock-prices.csv 更新されました。処理を継続します。'))
    subprocess.run(['python', '/Users/shin/kayaker/stantbat/st_price.py'], check = True)
    print(blue('>> 更新された ') + yellow('japan-all-stock-prices.csv ') + blue('は ') + green('PostgreSQL') + blue(' に格納されました。<<'))

if new_price2_d == d_price2_d:
    print(red('japan-all-stock-prices-2.csv 更新されていません。'))
else:
    print(green('japan-all-stock-prices-2.csv 更新されました。処理を継続します。'))
    subprocess.run(['python', '/Users/shin/kayaker/stantbat/st_price2.py'], check = True)
    print(blue('>> 更新された ') + yellow('japan-all-stock-prices-2.csv ') + blue('は ') + green('PostgreSQL') + blue(' に格納されました。<<'))

if new_margin_w == w_margin_w:
    print(red('japan-all-stock-margin-transactions.csv 更新されていません。'))
else:
    print(green('japan-all-stock-margin-transactions.csv 更新されました。処理を継続します。'))
    subprocess.run(['python', '/Users/shin/kayaker/stantbat/margin_transaction.py'], check = True)
    print(blue('>> 更新された ') + yellow('japan-all-stock-margin-transactions.csv ') + blue('は ') + green('PostgreSQL') + blue(' に格納されました。<<'))

if new_shareholding_m == m_shareholding_m:
    print(red('shareholding-ratio.csv 更新されていません。'))
else:
    print(green('shareholding-ratio.csv 更新されました。処理を継続します'))
    subprocess.run(['python', '/Users/shin/kayaker/stantbat/share_holding.py'], check = True)
    print(blue('>> 更新された ') + yellow('shareholding-ratio.csv') + blue(' は ') + green('PostgreSQL') + blue(' に格納されました。<<'))
