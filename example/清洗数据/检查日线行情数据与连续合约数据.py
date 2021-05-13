## 导入库
import pandas as pd
from data_manager.DailyDataManager import DailyDataManager
from data_manager.ContinuousContractDataManager import ContinuousContractDataManager

## 导入数据
volume_df = DailyDataManager().daily_data.groupby(['datetime', 'underlying_symbol'])['volume'].sum().unstack(level=-1)
price_df = ContinuousContractDataManager().get_field(field='continuous_price')

## 检查index和columns是否完全相等
print("index is equal: ", volume_df.index.equals(price_df.index))
print("columns is equal: ", volume_df.columns.equals(price_df.columns))

## 检查volume_df有数据的位置price_df是否一定有数据
check1_df = pd.DataFrame(data=False, index=volume_df.index, columns=volume_df.columns)
check1_df[(volume_df.notnull()) & (price_df.isnull())] = True
print(check1_df.astype(int).sum().sum())

check1_series = check1_df.sum()
check1_series[check1_series == 1]

check1_df.loc[check1_df==True, 'LR']

from datetime import datetime
price_df.loc[datetime(2020,7,15),'LR']
# price_df.loc[datetime(2020, 7,15), 'LR'] = price_df.loc[datetime(2020,7,14), 'LR']

## 检查price_df有数据的位置volume_df一定有数据
check2_df = pd.DataFrame(data=False, index=volume_df.index, columns=volume_df.columns)
check2_df[(volume_df.isnull()) & (price_df.notnull())] = True
print(check2_df.astype(int).sum().sum())

## 结论
# 2020年7月15日 品种LR没有连续合约数据
## 导入连续合约数据
# 补充收益率数据和连续合约数据
self = ContinuousContractDataManager()
continuous_data1 = self.get_continuous_contract_data(contract='main',price='close',rebalance_num=1)
continuous_data1.fillna(method='ffill', inplace=True)
continuous_data1.to_pickle("D:/LFProjects/NewPythonProject/data/continuous_contract_series/main close 1.pkl")

continuous_data2 = self.get_continuous_contract_data(contract='main',price='close',rebalance_num=3)
continuous_data2.fillna(method='ffill', inplace=True)
continuous_data2.to_pickle("D:/LFProjects/NewPythonProject/data/continuous_contract_series/main close 3.pkl")

continuous_data3 = self.get_continuous_contract_data(contract='main',price='close',rebalance_num=5)
continuous_data3.fillna(method='ffill', inplace=True)
continuous_data3.to_pickle("D:/LFProjects/NewPythonProject/data/continuous_contract_series/main close 5.pkl")

continuous_data4 = self.get_continuous_contract_data(contract='main',price='open',rebalance_num=1)
continuous_data4.fillna(method='ffill', inplace=True)
continuous_data4.to_pickle("D:/LFProjects/NewPythonProject/data/continuous_contract_series/main open 1.pkl")

continuous_data5 = self.get_continuous_contract_data(contract='main',price='open',rebalance_num=3)
continuous_data5.fillna(method='ffill', inplace=True)
continuous_data5.to_pickle("D:/LFProjects/NewPythonProject/data/continuous_contract_series/main open 3.pkl")

continuous_data6 = self.get_continuous_contract_data(contract='main',price='open',rebalance_num=5)
continuous_data6.fillna(method='ffill', inplace=True)
continuous_data6.to_pickle("D:/LFProjects/NewPythonProject/data/continuous_contract_series/main open 5.pkl")

continuous_data7 = self.get_continuous_contract_data(contract='main',price='settlement',rebalance_num=1)
continuous_data7.fillna(method='ffill', inplace=True)
continuous_data7.to_pickle("D:/LFProjects/NewPythonProject/data/continuous_contract_series/main settlement 1.pkl")

continuous_data8 = self.get_continuous_contract_data(contract='main',price='settlement',rebalance_num=3)
continuous_data8.fillna(method='ffill', inplace=True)
continuous_data8.to_pickle("D:/LFProjects/NewPythonProject/data/continuous_contract_series/main settlement 3.pkl")

continuous_data9 = self.get_continuous_contract_data(contract='main',price='settlement',rebalance_num=5)
continuous_data9.fillna(method='ffill', inplace=True)
continuous_data9.to_pickle("D:/LFProjects/NewPythonProject/data/continuous_contract_series/main settlement 5.pkl")