import pandas as pd
import matplotlib.pyplot as plt
from data_manager.BasicsDataManager import BasicsDataManager
from data_manager.ContinuousContractDataManager import ContinuousContractDataManager

symbol_list = BasicsDataManager().get_symbol_list()
self = ContinuousContractDataManager()

data_list = []
contract = 'main'
for price in ['close', 'settlement']:
    for rebalance_num in [1, 3, 5]:
        df = self.get_continuous_contract_data(contract=contract, price=price, rebalance_num=rebalance_num)
        df = df[['datetime', 'underlying_symbol', 'continuous_price']]
        df['contract'] = contract
        df['price'] = price
        df['rebalance_num'] = rebalance_num
        data_list.append(df)

data = pd.concat(data_list, axis=0)

for symbol in symbol_list:
    df = data[data.underlying_symbol == symbol]
    plt.figure()
    df.groupby(['price', 'rebalance_num'])['continuous_price'].plot(legend=True)
    plt.savefig(f"C:/Users/qtg_i/Desktop/LFNotes/笔记/期货连续合约/主力连续合约/主力连续合约对比/{symbol}.png")
    plt.show()

import os
import pandas as pd
folder_path = "D:/LFProjects/NewPythonProject/data/continuous_contract_series"
full_file_dict = {file: folder_path+"/"+file for file in os.listdir(folder_path)}
for file in full_file_dict:
    df = pd.read_pickle(full_file_dict[file])
    df.columns = df.columns.str.replace('main_', '')
    df.to_pickle(full_file_dict[file])

