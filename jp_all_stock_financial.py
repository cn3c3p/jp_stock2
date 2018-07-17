from os.path import join, relpath
from glob import glob
import pandas as pd
from sqlalchemy import create_engine
import os
from datetime import datetime,timedelta
import shelve
csvex = shelve.open('/Users/shin/stubby/csvex')
csvex_id = csvex['csvex_id']
csvex_pass = csvex['csvex_pass']
csvex.close()

os.system((
        'wget --http-user={0} \
        --http-passwd={1} \
        "https://csvex.com/kabu.plus/csv/japan-all-stock-financial-results/monthly/japan-all-stock-financial-results.csv" \
        -P /Users/shin/Downloads/ \
        -NP /Users/shin/Downloads/ \
        -N').format(csvex_id,csvex_pass))

os.system(
        'nkf \
        -w \
        --overwrite \
        /Users/shin/Downloads/japan-all-stock-financial-results.csv')

finan_date = datetime.fromtimestamp(os.stat('/Users/shin/Downloads/japan-all-stock-financial-results.csv').st_mtime)
financial_date = finan_date.strftime('%Y-%m-%d %H:%M:%S')

columns_fi = [
        'st_code','name','kessannki','kessannhappyoubi','uriagedaka','eigyourieki','keijyourieki',
        'toukirieki','sousisann','jikosihonn','sihonnkinn','yuurisihusai','jikosihonnhiritu','roe',
        'roa','hakkouzumikabusikisuu'
        ]

fi_df = pd.read_csv(
        '/Users/shin/Downloads/japan-all-stock-financial-results.csv',
        skiprows = 1,
        na_values=('-',''),
        names = columns_fi)

n = len(fi_df)
fi_df['date']= [financial_date]*n
fi_df['date'] = pd.to_datetime(fi_df['date'])

engine = create_engine('postgresql://shinya@localhost:5432/saber')

fi_df.to_sql(\
        'jp_all_stock_financial',
        engine,
        if_exists = 'append',
        index=False
        )

