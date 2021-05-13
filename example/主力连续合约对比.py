"""
比对主力连续合约数据
"""
import pandas as pd
from tqdm import tqdm
import matplotlib.pyplot as plt

origin_data = pd.read_pickle("/data/废弃数据/continuous_main_contract_series.pkl")
data1 = pd.read_pickle("/data/continuous_contract_series/continuous_main_contract_series 1.pkl")
data2 = pd.read_pickle("/data/continuous_contract_series/continuous_main_contract_series 3.pkl")
data3 = pd.read_pickle("/data/continuous_contract_series/continuous_main_contract_series 5.pkl")

symbol_list = origin_data.underlying_symbol.unique().tolist()

for symbol in tqdm(symbol_list):
    series0 = origin_data[origin_data.underlying_symbol==symbol].set_index("datetime")['continuous_close']
    series1 = data1[data1.underlying_symbol==symbol].set_index("datetime")['continuous_close']
    series2 = data2[data2.underlying_symbol==symbol].set_index("datetime")['continuous_close']
    series3 = data3[data3.underlying_symbol==symbol].set_index("datetime")['continuous_close']
    plt.figure(figsize=(20, 8))
    series0.plot(figsize=(20, 8), label="series0")
    series1.plot(figsize=(20, 8), label="series1")
    series2.plot(figsize=(20, 8), label="series2")
    series3.plot(figsize=(20, 8), label="series3")
    plt.grid()
    plt.legend()
    plt.title(symbol)
    plt.savefig(f"D:/LFProjects/NewPythonProject/结果输出/主力连续合约对比/{symbol}.png")
    plt.show()

