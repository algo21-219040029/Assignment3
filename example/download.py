# 导入库
import pandas as pd
import rqdatac as rq
from tqdm import tqdm

rq.init('license',
        'blJSw2o4ga7IMPOc-TQ3hZLCbGwAGE1ge6nj5Mj-gjUP-E3Q7Wi5WRy68eRWXIWxOdxB66UPU6uFE9wWe_NYzFHnZCunxcrIsSnT0ZUh5qQJ0u5rucs2WmYhqhaov0jtEFGCmLlnfcaTLOQUJz7Wb1xJ3qBvxXu9pJig4f4j-jg=Gq3WIxDUv98SbZSSegwPQUOg8Lrnz_iAjn2Qy7ZgFZGq8EgUYy6-aaYYhqvp7Ox6O4nuirhlPV6AIfTaVD-d0vD81hZ5Ts1kCTvLTRJjsQIj4-kuZC80RrpOkVYy4THZxRcG9Emt6zWODkl7sby4RJA5W4k_aOIcMmtmNfIZd_s=',
        ("rqdatad-pro.ricequant.com", 16011))

# 下载2010年的期货日线数据
all_instruments = rq.all_instruments(type='Future')
all_instruments = all_instruments[~all_instruments['order_book_id'].str.contains("88|888|889|99")]
all_instruments['maturity_date'] = pd.to_datetime(all_instruments['maturity_date'])
all_instruments['listed_date'] = pd.to_datetime(all_instruments['listed_date'])
all_instruments['de_listed_date'] = pd.to_datetime(all_instruments['de_listed_date'])

trade_cal = pd.to_datetime(rq.get_trading_dates(start_date='2009-01-01', end_date='2020-12-31'))
start_date = trade_cal[0]
end_date = trade_cal[-1]
symbol_list = all_instruments.underlying_symbol.unique().tolist()

data_list = []
for symbol in tqdm(symbol_list):
    contract_list = all_instruments[all_instruments.underlying_symbol == symbol]['order_book_id'].unique().tolist()
    for contract in tqdm(contract_list):
        df = rq.get_price(order_book_ids=contract,
                      start_date=start_date,
                      end_date=end_date,
                      frequency='1d',
                      expect_df=True,
                      fields=['settlement', 'prev_settlement']
                      )
        data_list.append(df)
data = pd.concat(data_list, axis=0)
data.reset_index(inplace=True)
data['underlying_symbol'] = data['order_book_id'].str[:-4]
# 新数据中品种数目
data_symbol_list = data.underlying_symbol.unique().tolist()
print(len(data_symbol_list))

# 读入原数据
original_data = pd.read_pickle("D:/LFProjects/PythonProject/data/futures_daily_20210129_1.pkl")
# 检查原数据有多少个品种
original_symbol_list = original_data.underlying_symbol.unique().tolist()
print(len(original_symbol_list))
# 新数据中特有的品种
difference_symbol_list = set(data_symbol_list).difference(set(original_symbol_list))

# 将原数据中的要改名的品种改名
data['underlying_symbol'] = data['underlying_symbol'].replace('WS', 'WH').replace('ER', 'RI').replace('ME', 'MA'). \
    replace('TC', 'ZC').replace('RO', 'OI')
data['order_book_id'] = data['order_book_id'].str.replace('WS', 'WH').str.replace('ER', 'RI').str.replace('ME', 'MA'). \
    str.replace('TC', 'ZC').str.replace('RO', 'OI')
# data['contract'] = data['contract'].str.replace('WS', 'WH').str.replace('ER', 'RI').str.replace('ME', 'MA'). \
#     str.replace('TC', 'ZC').str.replace('RO', 'OI')
# 列名修改
data = data.rename(columns={'order_book_id': 'contract', 'date': 'datetime', 'total_turnover': 'turnover',
                            'limit_up': 'upper_limit', 'limit_down': 'lower_limit'})

# 删除settlement和prev_settlement
data = data.drop(columns=['settlement', 'prev_settlement'])
data['upper_limit_tag'] = 0.0
data['lower_limit_tag'] = 0.0

data = pd.concat([data, original_data], axis=0)
set(symbol_list).difference(set(data.underlying_symbol.unique().tolist()))

# 修正all_instruments的结果
all_instruments.underlying_symbol = all_instruments.underlying_symbol.replace('WS', 'WH').replace('ER', 'RI').replace(
    'ME', 'MA'). \
    replace('TC', 'ZC').replace('RO', 'OI').replace("S", "A")
all_instruments.order_book_id = all_instruments.order_book_id.str.replace('WS', 'WH').str.replace('ER',
                                                                                                  'RI').str.replace(
    'ME', 'MA'). \
    str.replace('TC', 'ZC').str.replace('RO', 'OI')
all_instruments = all_instruments[~(all_instruments.underlying_symbol.isin(('LH', 'PK')))]
all_instruments = all_instruments.rename(columns={'order_book_id': 'contract'})
all_instruments.loc[all_instruments.underlying_symbol == 'A', 'contract'] \
    = all_instruments.loc[all_instruments.underlying_symbol == 'A', 'contract'].str.replace("S", "A")
# 保存数据
all_instruments.to_pickle("D:/LFProjects/PythonProject/data/all_instruments_20210130_1.pkl")
data.to_pickle("D:/LFProjects/PythonProject/data/futures_daily_20210130_1.pkl")

from data_manager import BasicsDataManager
basics_data_manager = BasicsDataManager()
all_instruments = basics_data_manager.get_all_instruments()
a = all_instruments[['underlying_symbol', 'symbol']]
a['symbol'] = a['symbol'].str[:-4]
a = a.drop_duplicates()
