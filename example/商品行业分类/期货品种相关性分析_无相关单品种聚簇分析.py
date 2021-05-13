"""
本py文件用于计算谷物板块（基于RQData的分类）中品种的相关性
验证谷物板块中单品种与整个板块（分删除该品种和包括该品种）的相关性
"""
##
# 导入库
import seaborn as sns
from typing import Dict
from pandas import DataFrame
import matplotlib.pyplot as plt

from data_manager.BasicsDataManager import BasicsDataManager
from data_manager.IndustryDataManager import IndustryDataManager
from data_manager.ContinuousContractDataManager import ContinuousContractDataManager

from correlation_analysis.analysis import get_industry_correlation_result, get_period_industry_correlation_result
##
# 导入代码名称列表
all_instruments: DataFrame = BasicsDataManager().get_all_instruments()[['underlying_symbol', 'symbol', 'industry_name']]
all_instruments['name'] = all_instruments['symbol'].str.extract(r"([^0-9]+)")
all_instruments = all_instruments[['underlying_symbol', 'name']].drop_duplicates()
symbol_name_map: Dict[str, str] = all_instruments.set_index('underlying_symbol')['name'].to_dict()
##
# 导入连续合约数据
price_df: DataFrame = ContinuousContractDataManager().get_field(field='continuous_price')
return_df = ContinuousContractDataManager().get_field(field='return')

##
# 导入行业数据
industry_data_manager = IndustryDataManager()
industry_data_manager.get_all_industry_symbol_maps()

# 从行业数据中导出谷物的组
arg_groups = {}
arg_groups['Huatai_industry_map'] = industry_data_manager.get_industry_symbol_map(group='original_industry', name='Huatai_industry_map')['谷物']
arg_groups['RQ_industry_map'] = industry_data_manager.get_industry_symbol_map(group='original_industry', name='RQ_industry_map')['谷物']
##
# 分析谷物(以RQ为例子)
industry_symbol_list_agr = arg_groups['RQ_industry_map']
return_df_agr = return_df[industry_symbol_list_agr]
group_return_series_agr = return_df.mean(axis=1)

col_names = ['A', 'C', 'JR', 'LR', 'PM', 'RI', 'RR', 'WH', 'WT']

# 谷物板块品种与板块等权组合的相关性
return_dict_agr1 = {}
for col in return_df_agr.columns:
    return_dict_agr1[col] = return_df_agr[col].corr(group_return_series_agr)

# 谷物板块品种与板块内其他品种等权组合的相关性
return_dict_agr2 = {}
for col in return_df_agr.columns:
    return_dict_agr2[col] = return_df_agr[col].corr(return_df.drop(columns=[col]).mean(axis=1))

# 谷物板块品种之间收益率相关性
# 单日收益率
plt.figure()
sns.heatmap(data=return_df_agr.corr().fillna(0.0).loc[col_names, col_names], annot=True, vmin=0, vmax=1)
plt.show()

# 5日平均收益率
plt.figure()
sns.heatmap(data=return_df_agr.rolling(window=5).mean().corr().fillna(0.0).loc[col_names, col_names], annot=True, vmin=0, vmax=1)
plt.show()

# 20日平均收益率
plt.figure()
sns.heatmap(data=return_df_agr.rolling(window=20).mean().corr().fillna(0.0).loc[col_names, col_names], annot=True, vmin=0, vmax=1)
plt.show()

# 60日平均收益率
plt.figure()
sns.heatmap(data=return_df_agr.rolling(window=60).mean().corr().fillna(0.0).loc[col_names, col_names], annot=True, vmin=0, vmax=1)
plt.show()

## 谷物板块品种之间波动率相关性
col_names = ['A', 'C', 'JR', 'LR', 'PM', 'RI', 'RR', 'WH', 'WT']
# 5日波动率
plt.figure()
sns.heatmap(data=return_df_agr.rolling(window=5).std().corr().fillna(0.0).loc[col_names, col_names], annot=True, vmin=0, vmax=1)
plt.show()

# 20日波动率
plt.figure()
sns.heatmap(data=return_df_agr.rolling(window=20).std().corr().fillna(0.0).loc[col_names, col_names], annot=True, vmin=0, vmax=1)
plt.show()

# 60日波动率
plt.figure()
sns.heatmap(data=return_df_agr.rolling(window=60).std().corr().fillna(0.0).loc[col_names, col_names], annot=True, vmin=0, vmax=1)
plt.show()

## 谷物板块品种与品种组合的波动率相关性

# 5日波动率+全品种
std_series_agr5 = return_df_agr.mean(axis=1).rolling(window=5).std()
std_dict_agr5_1 = {}
for col in return_df_agr.columns:
    std_dict_agr5_1[col] = return_df_agr[col].rolling(window=5).std().corr(std_series_agr5)

# 5日波动率+其他品种
std_dict_agr5_2 = {}
for col in return_df_agr.columns:
    std_dict_agr5_2[col] = return_df_agr[col].rolling(window=5).std().corr(return_df_agr.drop(columns=[col]).mean(axis=1).rolling(window=5).std())

# 20日波动率+全品种
std_series_agr20 = return_df_agr.mean(axis=1).rolling(window=20).std()
std_dict_agr20_1 = {}
for col in return_df_agr.columns:
    std_dict_agr20_1[col] = return_df_agr[col].rolling(window=20).std().corr(std_series_agr20)

# 20日波动率+其他品种
std_dict_agr20_2 = {}
for col in return_df_agr.columns:
    std_dict_agr20_2[col] = return_df_agr[col].rolling(window=20).std().corr(return_df_agr.drop(columns=[col]).mean(axis=1).rolling(window=20).std())

# 60日波动率+全品种
std_series_agr60 = return_df_agr.mean(axis=1).rolling(window=60).std()
std_dict_agr60_1 = {}
for col in return_df_agr.columns:
    std_dict_agr60_1[col] = return_df_agr[col].rolling(window=60).std().corr(std_series_agr60)

# 60日波动率+其他品种
std_dict_agr60_2 = {}
for col in return_df_agr.columns:
    std_dict_agr60_2[col] = return_df_agr[col].rolling(window=60).std().corr(return_df_agr.drop(columns=[col]).mean(axis=1).rolling(window=60).std())

##
# 所有品种收益率短周期, 中周期和长周期相关性分析
industry_data_manager = IndustryDataManager()
Nanhua_first_industry_symbol_map = industry_data_manager.get_industry_symbol_map(group='original_industry', name='Nanhua_first_industry_map')
# 提取出工业品和农业品
Nanhua_first_industry_symbol_map.pop("金属")
Nanhua_first_industry_symbol_map.pop("能源化工")

symbol_list1 = Nanhua_first_industry_symbol_map['工业品']
symbol_list2 = Nanhua_first_industry_symbol_map['农产品']

# 滚动收益率版本
corr1_1 = get_industry_correlation_result(data=return_df,
                                industry_symbol_map=Nanhua_first_industry_symbol_map,
                                output_file_path="C:/Users/qtg_i/Desktop/LFNotes/笔记/期货行业分析/期货行业多周期分析/滚动日收益率")

corr20_1 = get_industry_correlation_result(data=return_df.rolling(window=20).mean(),
                                industry_symbol_map=Nanhua_first_industry_symbol_map,
                                output_file_path="C:/Users/qtg_i/Desktop/LFNotes/笔记/期货行业分析/期货行业多周期分析/滚动20日收益率")

corr60_1 = get_industry_correlation_result(data=return_df.rolling(window=60).mean(),
                                industry_symbol_map=Nanhua_first_industry_symbol_map,
                                output_file_path="C:/Users/qtg_i/Desktop/LFNotes/笔记/期货行业分析/期货行业多周期分析/滚动60日收益率")

corr120_1 = get_industry_correlation_result(data=return_df.rolling(window=120).mean(),
                                industry_symbol_map=Nanhua_first_industry_symbol_map,
                                output_file_path="C:/Users/qtg_i/Desktop/LFNotes/笔记/期货行业分析/期货行业多周期分析/滚动120日收益率")

# period收益率版本
corr1_2 = get_period_industry_correlation_result(price_df,period=1,
                                industry_symbol_map=Nanhua_first_industry_symbol_map,
                                output_file_path="C:/Users/qtg_i/Desktop/LFNotes/笔记/期货行业分析/期货行业多周期分析/period日收益率")

corr20_2 = get_period_industry_correlation_result(price_df, period=20,
                                industry_symbol_map=Nanhua_first_industry_symbol_map,
                                output_file_path="C:/Users/qtg_i/Desktop/LFNotes/笔记/期货行业分析/期货行业多周期分析/period20日收益率")

corr60_2 = get_period_industry_correlation_result(price_df, period=60,
                                industry_symbol_map=Nanhua_first_industry_symbol_map,
                                output_file_path="C:/Users/qtg_i/Desktop/LFNotes/笔记/期货行业分析/期货行业多周期分析/peirod60日收益率")

corr120_2 = get_period_industry_correlation_result(price_df, period=120,
                                industry_symbol_map=Nanhua_first_industry_symbol_map,
                                output_file_path="C:/Users/qtg_i/Desktop/LFNotes/笔记/期货行业分析/期货行业多周期分析/period120日收益率")

##
corr = corr20_2.loc[symbol_list2, symbol_list2]
stack_corr = corr.stack()
res = stack_corr[(stack_corr >= 0.3) & (stack_corr < 1.0)]
res.sort_values(ascending=False, inplace=True)