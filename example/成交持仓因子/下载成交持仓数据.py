import pandas as pd
import rqdatac as rq
from tqdm import tqdm
from datetime import datetime
from pandas import DataFrame

rq.init('license',
        'blJSw2o4ga7IMPOc-TQ3hZLCbGwAGE1ge6nj5Mj-gjUP-E3Q7Wi5WRy68eRWXIWxOdxB66UPU6uFE9wWe_NYzFHnZCunxcrIsSnT0ZUh5qQJ0u'
        '5rucs2WmYhqhaov0jtEFGCmLlnfcaTLOQUJz7Wb1xJ3qBvxXu9pJig4f4j-jg=Gq3WIxDUv98SbZSSegwPQUOg8Lrnz_iAjn2Qy7ZgFZGq8EgUY'
        'y6-aaYYhqvp7Ox6O4nuirhlPV6AIfTaVD-d0vD81hZ5Ts1kCTvLTRJjsQIj4-kuZC80RrpOkVYy4THZxRcG9Emt6zWODkl7sby4RJA5W4k_aOIc'
        'MmtmNfIZd_s=',
        ("rqdatad-pro.ricequant.com", 16011))

all_instruments = rq.all_instruments(type='Future')
all_instruments = all_instruments.rename(columns={'order_book_id': 'contract'})
all_instruments = all_instruments[~all_instruments['contract'].str.contains("88|888|889|99")]
all_instruments['maturity_date'] = pd.to_datetime(all_instruments['maturity_date'])
all_instruments['listed_date'] = pd.to_datetime(all_instruments['listed_date'])
all_instruments['de_listed_date'] = pd.to_datetime(all_instruments['de_listed_date'])

contract_info = all_instruments[['contract', 'listed_date', 'de_listed_date']].set_index('contract')

df1_list = []
df2_list = []
df3_list = []

error_dict = {}

for contract in tqdm(contract_info.index):
    listed_date = contract_info.loc[contract]['listed_date']
    de_listed_date = contract_info.loc[contract]['de_listed_date']
    try:
        df1 = rq.futures.get_member_rank(obj=contract, start_date=listed_date, end_date=de_listed_date, rank_by='volume')
        df2 = rq.futures.get_member_rank(obj=contract, start_date=listed_date, end_date=de_listed_date, rank_by='long')
        df3 = rq.futures.get_member_rank(obj=contract, start_date=listed_date, end_date=de_listed_date, rank_by='short')
        df1['rank_by'] = 'volume'
        df2['rank_by'] = 'long'
        df3['rank_by'] = 'short'
        df1_list.append(df1)
        df2_list.append(df2)
        df3_list.append(df3)
    except Exception as e:
        error_dict[contract] = e

# 检查error_dict中是否有非TypeError("'NoneType' object does not support item assignment")
second_error_dict = {}
for l in error_dict:
    if error_dict[l] != TypeError("'NoneType' object does not support item assignment"):
        print(error_dict[l])
        second_error_dict[l] = error_dict[l]

# 拼接数据
df_list = df1_list+df2_list+df3_list
data = pd.concat(df_list, axis=0)
data = data.reset_index()

# 先暂时保存数据，避免丢失
data.to_pickle("D:/LFProjects/NewPythonProject/data/volume_long_short1.pkl")

# 删除不必要的数据，减少内存占用
del second_error_dict
del df_list
del df1_list
del df2_list

# 郑商所没有具体合约排名，只有品种排名
# 郑商所品种列表
CZCE_symbol_list = all_instruments[all_instruments.exchange=='CZCE'].underlying_symbol.unique().tolist()
# 郑商所各品种的起始日期
CZCE_listed_date_info = all_instruments[all_instruments.exchange=='CZCE'].groupby("underlying_symbol")['listed_date'].min()
# 郑商所各品种的截止日期，若日期大于2020-12-31，则将其设置为2020-12-31
CZCE_de_listed_date_info = all_instruments[all_instruments.exchange=='CZCE'].groupby("underlying_symbol")['de_listed_date'].max()
CZCE_de_listed_date_info[CZCE_de_listed_date_info > datetime(2020, 12, 31)] = datetime(2020, 12, 31)

CZCE_df1_list = []
CZCE_df2_list = []
CZCE_df3_list = []
CZCE_error_dict = {}
for symbol in tqdm(CZCE_symbol_list):
    listed_date = CZCE_listed_date_info[symbol]
    de_listed_date = CZCE_de_listed_date_info[symbol]
    try:
        df1 = rq.futures.get_member_rank(obj=symbol, start_date=listed_date, end_date=de_listed_date,
                                         rank_by='volume')
        df2 = rq.futures.get_member_rank(obj=symbol, start_date=listed_date, end_date=de_listed_date,
                                         rank_by='long')
        df3 = rq.futures.get_member_rank(obj=symbol, start_date=listed_date, end_date=de_listed_date,
                                         rank_by='short')
        df1['rank_by'] = 'volume'
        df2['rank_by'] = 'long'
        df3['rank_by'] = 'short'
        CZCE_df1_list.append(df1)
        CZCE_df2_list.append(df2)
        CZCE_df3_list.append(df3)
    except Exception as e:
        CZCE_error_dict[symbol] = e

CZCE_df_list = CZCE_df1_list+CZCE_df2_list+CZCE_df3_list
CZCE_data = pd.concat(CZCE_df_list,axis=0)
CZCE_data.reset_index(inplace=True)

# 给每行数据添加上交易所
exchange_info = all_instruments[['contract', 'exchange']]
exchange_info = exchange_info.rename(columns={'contract': 'commodity_id'})
exchange_info.index = range(len(exchange_info))

data = pd.merge(left=data, right=exchange_info,
                on='commodity_id', how='left')

CZCE_exchange_info = all_instruments[all_instruments.exchange=='CZCE'][['underlying_symbol', 'exchange']]
CZCE_exchange_info = CZCE_exchange_info.rename(columns={'underlying_symbol': 'commodity_id'})
CZCE_exchange_info = CZCE_exchange_info.drop_duplicates()
CZCE_exchange_info.index = range(len(CZCE_exchange_info))

CZCE_data = pd.merge(left=CZCE_data, right=CZCE_exchange_info,
                     on='commodity_id', how='left')
CZCE_data.to_pickle("D:/LFProjects/NewPythonProject/data/volume_long_short2.pkl")
del CZCE_exchange_info
del CZCE_df1_list
del CZCE_df2_list
del CZCE_df3_list

total_data = pd.concat([data, CZCE_data], axis=0)

total_data.to_pickle("D:/LFProjects/NewPythonProject/data/volume_long_short.pkl")








