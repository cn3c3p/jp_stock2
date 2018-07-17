import os
import sys
import pandas as pd
from sqlalchemy import create_engine

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

