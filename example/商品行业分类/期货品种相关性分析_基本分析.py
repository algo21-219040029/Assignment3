##
# 导入库
import os
import json
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from utils.utility import stack_dataframe_by_fields
from data_manager.BasicsDataManager import BasicsDataManager
from correlation_analysis.analysis import get_industry_correlation_result

##
# 导入代码名称列表
all_instruments = BasicsDataManager().get_all_instruments()[['underlying_symbol', 'symbol', 'industry_name']]
all_instruments['name'] = all_instruments['symbol'].str[:-4]
all_instruments = all_instruments[['underlying_symbol', 'name']].drop_duplicates()
symbol_name_map = all_instruments.set_index('underlying_symbol')['name'].to_dict()

##
# 导入日收益率数据
continuous_main_contract_series = pd.read_pickle("D:/LFProjects/NewPythonProject/"
                                                 "data/continuous_contract_series/continuous_main_contract_series 1.pkl")

data = stack_dataframe_by_fields(data=continuous_main_contract_series,
                                 index_field='datetime',
                                 column_field='underlying_symbol',
                                 data_field='return')
data.columns = [symbol + "_" + symbol_name_map[symbol] for symbol in data.columns]

##
# 输出行业相关性数据
industry_file_path = "/data/industry"
full_file_dict = {file: industry_file_path + "/" + file for file in os.listdir(industry_file_path)}
for file in full_file_dict:
    with open(full_file_dict[file], "rb") as f:
        industry_symbol_map = json.load(f)
        for industry in industry_symbol_map:
            industry_symbol_map[industry] = [symbol + "_" + symbol_name_map[symbol] for symbol in
                                             industry_symbol_map[industry]]
        corr = get_industry_correlation_result(data,
                                        industry_symbol_map,
                                        "D:/LFProjects/NewPythonProject/结果输出/行业内品种相关性/"
                                        + file.replace(".json", ""))

## 整理两大部分的品种列表
all_instruments = BasicsDataManager().get_all_instruments()[['underlying_symbol', 'symbol', 'industry_name']]
all_instruments['name'] = all_instruments['symbol'].str[:-4]
symbol_industry_df = all_instruments[['underlying_symbol', 'name', 'industry_name']].drop_duplicates()
symbol_industry_df['symbol'] = symbol_industry_df['underlying_symbol']+"_"+symbol_industry_df['name']
symbol_industry_df = symbol_industry_df[['symbol', 'industry_name']]

symbol_list1 = symbol_industry_df[symbol_industry_df['industry_name'].
    isin(['油脂','农产品','谷物','软商品'])].symbol.tolist()
symbol_list2 = symbol_industry_df[symbol_industry_df['industry_name'].
    isin(['贵金属','有色','建材','化工', '能源','焦煤钢矿','未知'])].symbol.tolist()

##
# 两个部分的品种列表
corr = data.corr()
corr1 = corr.loc[symbol_list1, symbol_list1]
corr1 = corr1[np.abs(corr1).mean(axis=0).sort_values(ascending=True).index]
corr1 = corr1.loc[np.abs(corr1).mean(axis=0).sort_values(ascending=True).index]

corr2 = corr.loc[symbol_list2, symbol_list2]
corr2 = corr2[np.abs(corr2).mean(axis=0).sort_values(ascending=True).index]
corr2 = corr2.loc[np.abs(corr2).mean(axis=0).sort_values(ascending=True).index]

corr1.fillna(0.0, inplace=True)
corr2.fillna(0.0, inplace=True)

plt.figure()
sns.clustermap(data=corr1, annot=True, figsize=(30,25), vmin=-1, vmax=1)
plt.title("油脂_农产品_谷物_软商品")
plt.savefig("D:/LFProjects/NewPythonProject/结果输出/行业内品种相关性/油脂_农产品_谷物_软商品1.png")

plt.figure()
sns.clustermap(data=corr1, annot=True, figsize=(30,25), vmin=0, vmax=1)
plt.title("油脂_农产品_谷物_软商品")
plt.savefig("D:/LFProjects/NewPythonProject/结果输出/行业内品种相关性/油脂_农产品_谷物_软商品2.png")

plt.figure()
sns.clustermap(data=corr2, annot=True, figsize=(30,25), vmin=-1, vmax=1)
plt.title("贵金属_有色_建材_化工_能源_焦煤钢矿_未知")
plt.savefig("D:/LFProjects/NewPythonProject/结果输出/行业内品种相关性/贵金属_有色_建材_化工_能源_焦煤钢矿_未知1.png")

plt.figure()
sns.clustermap(data=corr2, annot=True, figsize=(30,25), vmin=0, vmax=1)
plt.title("贵金属_有色_建材_化工_能源_焦煤钢矿_未知")
plt.savefig("D:/LFProjects/NewPythonProject/结果输出/行业内品种相关性/贵金属_有色_建材_化工_能源_焦煤钢矿_未知2.png")

##
# 所有品种相关性排序
stack_corr = corr.stack().sort_values(ascending=False)

##
# 全品种视野下的其他关联组
name_symbol_map = all_instruments.set_index('name')['underlying_symbol'].to_dict()

# 画沥青、原油、燃料油、低硫燃料油的相关系数图(沥青)
group_1 = ['沥青','原油','燃料油','低硫燃料油']
symbol_list_1 = [name_symbol_map[name]+"_"+name for name in name_symbol_map if name in group_1]
corr_1 = corr.loc[symbol_list_1, symbol_list_1]
plt.figure()
sns.clustermap(data=corr_1, figsize=(20, 15), annot=True, vmin=0, vmax=1)
plt.title("_".join(group_1))
plt.savefig(f"D:/LFProjects/NewPythonProject/结果输出/行业内品种相关性/其他组/{'_'.join(group_1)}.png")
plt.show()

#
group_2 = ['不锈钢', '镍', '20号胶', '天然橡胶']
symbol_list_2 = [name_symbol_map[name]+"_"+name for name in name_symbol_map if name in group_2]
corr_2 = corr.loc[symbol_list_2, symbol_list_2]
plt.figure()
sns.clustermap(data=corr_2, figsize=(20, 15), annot=True, vmin=0, vmax=1)
plt.title("_".join(group_2))
plt.savefig(f"D:/LFProjects/NewPythonProject/结果输出/行业内品种相关性/其他组/{'_'.join(group_2)}.png")
plt.show()

group_3 = ['不锈钢', '镍', '20号胶', '天然橡胶', '甲醇', '聚乙烯', '聚丙烯', '天然橡胶', '20号胶', '苯乙烯', '乙二醇', 'PTA', '沥青']
symbol_list_3 = [name_symbol_map[name]+"_"+name for name in name_symbol_map if name in group_3]
corr_3 = corr.loc[symbol_list_3, symbol_list_3]
plt.figure()
sns.clustermap(data=corr_3, figsize=(20, 15), annot=True, vmin=0, vmax=1)
plt.title("_".join(group_3))
plt.savefig(f"D:/LFProjects/NewPythonProject/结果输出/行业内品种相关性/其他组/{'_'.join(group_3)}.png")
plt.show()

group_4 = ['不锈钢', '镍', '20号胶', '天然橡胶', '铝', '铅', '铜', '锌', '锡', '国际铜']
symbol_list_4 = [name_symbol_map[name]+"_"+name for name in name_symbol_map if name in group_4]
corr_4 = corr.loc[symbol_list_4, symbol_list_4]
plt.figure()
sns.clustermap(data=corr_4, figsize=(20, 15), annot=True, vmin=0, vmax=1)
plt.title("_".join(group_4))
plt.savefig(f"D:/LFProjects/NewPythonProject/结果输出/行业内品种相关性/其他组/{'_'.join(group_4)}.png")
plt.show()

group_5 = ['聚乙烯', '聚氯乙烯', '聚丙烯']
symbol_list_5 = [name_symbol_map[name]+"_"+name for name in name_symbol_map if name in group_5]
corr_5 = corr.loc[symbol_list_5, symbol_list_5]
plt.figure()
sns.clustermap(data=corr_5, figsize=(20, 15), annot=True, vmin=0, vmax=1)
plt.title("_".join(group_5))
plt.savefig(f"D:/LFProjects/NewPythonProject/结果输出/行业内品种相关性/其他组/{'_'.join(group_5)}.png")
plt.show()

group_6 = ['聚乙烯', '聚氯乙烯', '聚丙烯', '甲醇', '聚乙烯', '聚丙烯', '天然橡胶', '20号胶', '苯乙烯', '乙二醇', 'PTA', '沥青']
symbol_list_6 = [name_symbol_map[name]+"_"+name for name in name_symbol_map if name in group_6]
corr_6 = corr.loc[symbol_list_6, symbol_list_6]
plt.figure()
sns.clustermap(data=corr_6, figsize=(20, 15), annot=True, vmin=0, vmax=1)
plt.title("_".join(group_6))
plt.savefig(f"D:/LFProjects/NewPythonProject/结果输出/行业内品种相关性/其他组/{'_'.join(group_6)}.png")
plt.show()

group_7 = ['铜', '聚乙烯','天然橡胶']
symbol_list_7 = [name_symbol_map[name]+"_"+name for name in name_symbol_map if name in group_7]
corr_7 = corr.loc[symbol_list_7, symbol_list_7]
plt.figure()
sns.clustermap(data=corr_7, figsize=(20, 15), annot=True, vmin=0, vmax=1)
plt.title("_".join(group_7))
plt.savefig(f"D:/LFProjects/NewPythonProject/结果输出/行业内品种相关性/其他组/{'_'.join(group_7)}.png")
plt.show()

group_8 = ['铜', '聚乙烯', '聚氯乙烯', '聚丙烯', '甲醇', '聚乙烯', '聚丙烯', '天然橡胶', '20号胶', '苯乙烯', '乙二醇', 'PTA', '沥青']
symbol_list_8 = [name_symbol_map[name]+"_"+name for name in name_symbol_map if name in group_8]
corr_8 = corr.loc[symbol_list_8, symbol_list_8]
plt.figure()
sns.clustermap(data=corr_8, figsize=(20, 15), annot=True, vmin=0, vmax=1)
plt.title("_".join(group_8))
plt.savefig(f"D:/LFProjects/NewPythonProject/结果输出/行业内品种相关性/其他组/{'_'.join(group_8)}.png")
plt.show()

group_9 = ['铜', '聚乙烯','天然橡胶', '镍', '铝', '铅', '铜', '锌', '锡', '国际铜']
symbol_list_9 = [name_symbol_map[name]+"_"+name for name in name_symbol_map if name in group_9]
corr_9 = corr.loc[symbol_list_9, symbol_list_9]
plt.figure()
sns.clustermap(data=corr_9, figsize=(20, 15), annot=True, vmin=0, vmax=1)
plt.title("_".join(group_9))
plt.savefig(f"D:/LFProjects/NewPythonProject/结果输出/行业内品种相关性/其他组/{'_'.join(group_9)}.png")
plt.show()

group_10 = ['短纤','天然橡胶','PTA']
symbol_list_10 = [name_symbol_map[name]+"_"+name for name in name_symbol_map if name in group_10]
corr_10 = corr.loc[symbol_list_10, symbol_list_10]
plt.figure()
sns.clustermap(data=corr_10, figsize=(20, 15), annot=True, vmin=0, vmax=1)
plt.title("_".join(group_10))
plt.savefig(f"D:/LFProjects/NewPythonProject/结果输出/行业内品种相关性/其他组/{'_'.join(group_10)}.png")
plt.show()

group_11 = ['PTA','天然橡胶']
symbol_list_11 = [name_symbol_map[name]+"_"+name for name in name_symbol_map if name in group_11]
corr_11 = corr.loc[symbol_list_11, symbol_list_11]
plt.figure()
sns.clustermap(data=corr_11, figsize=(20, 15), annot=True, vmin=0, vmax=1)
plt.title("_".join(group_11))
plt.savefig(f"D:/LFProjects/NewPythonProject/结果输出/行业内品种相关性/其他组/{'_'.join(group_11)}.png")
plt.show()

group_12 = ['PTA','短纤']
symbol_list_12 = [name_symbol_map[name]+"_"+name for name in name_symbol_map if name in group_12]
corr_12 = corr.loc[symbol_list_12, symbol_list_12]
plt.figure()
sns.clustermap(data=corr_12, figsize=(20, 15), annot=True, vmin=0, vmax=1)
plt.title("_".join(group_12))
plt.savefig(f"D:/LFProjects/NewPythonProject/结果输出/行业内品种相关性/其他组/{'_'.join(group_12)}.png")
plt.show()

group_13 = ['棉花','棉纱', '20号胶']
symbol_list_13 = [name_symbol_map[name]+"_"+name for name in name_symbol_map if name in group_13]
corr_13 = corr.loc[symbol_list_13, symbol_list_13]
plt.figure()
sns.clustermap(data=corr_13, figsize=(20, 15), annot=True, vmin=0, vmax=1)
plt.title("_".join(group_13))
plt.savefig(f"D:/LFProjects/NewPythonProject/结果输出/行业内品种相关性/其他组/{'_'.join(group_13)}.png")
plt.show()

group_14 = ['棉花','棉纱', '20号胶', '甲醇', '聚乙烯', '聚丙烯', '天然橡胶', '苯乙烯', '乙二醇', 'PTA', '沥青']
symbol_list_14 = [name_symbol_map[name]+"_"+name for name in name_symbol_map if name in group_14]
corr_14 = corr.loc[symbol_list_14, symbol_list_14]
plt.figure()
sns.clustermap(data=corr_14, figsize=(20, 15), annot=True, vmin=0, vmax=1)
plt.title("_".join(group_14))
plt.savefig(f"D:/LFProjects/NewPythonProject/结果输出/行业内品种相关性/其他组/{'_'.join(group_14)}.png")
plt.show()

group_15 = ['PTA', '低硫燃料油', '原油']
symbol_list_15 = [name_symbol_map[name]+"_"+name for name in name_symbol_map if name in group_15]
corr_15 = corr.loc[symbol_list_15, symbol_list_15]
plt.figure()
sns.clustermap(data=corr_15, figsize=(20, 15), annot=True, vmin=0, vmax=1)
plt.title("_".join(group_15))
plt.savefig(f"D:/LFProjects/NewPythonProject/结果输出/行业内品种相关性/其他组/{'_'.join(group_15)}.png")
plt.show()

group_16 = ['国际铜','锰硅']
symbol_list_16 = [name_symbol_map[name]+"_"+name for name in name_symbol_map if name in group_16]
corr_16 = corr.loc[symbol_list_16, symbol_list_16]
plt.figure()
sns.clustermap(data=corr_16, figsize=(20, 15), annot=True, vmin=0, vmax=1)
plt.title("_".join(group_16))
plt.savefig(f"D:/LFProjects/NewPythonProject/结果输出/行业内品种相关性/其他组/{'_'.join(group_16)}.png")
plt.show()

group_17 = ['锌','天然橡胶']
symbol_list_17 = [name_symbol_map[name]+"_"+name for name in name_symbol_map if name in group_17]
corr_17 = corr.loc[symbol_list_17, symbol_list_17]
plt.figure()
sns.clustermap(data=corr_17, figsize=(20, 15), annot=True, vmin=0, vmax=1)
plt.title("_".join(group_17))
plt.savefig(f"D:/LFProjects/NewPythonProject/结果输出/行业内品种相关性/其他组/{'_'.join(group_17)}.png")
plt.show()

group_18 = ['乙二醇','短纤','沥青']
symbol_list_18 = [name_symbol_map[name]+"_"+name for name in name_symbol_map if name in group_18]
corr_18 = corr.loc[symbol_list_18, symbol_list_18]
plt.figure()
sns.clustermap(data=corr_18, figsize=(20, 15), annot=True, vmin=0, vmax=1)
plt.title("_".join(group_18))
plt.savefig(f"D:/LFProjects/NewPythonProject/结果输出/行业内品种相关性/其他组/{'_'.join(group_18)}.png")
plt.show()

group_19 = ['强麦','国际铜']
symbol_list_19 = [name_symbol_map[name]+"_"+name for name in name_symbol_map if name in group_19]
corr_19 = corr.loc[symbol_list_19, symbol_list_19]
plt.figure()
sns.clustermap(data=corr_19, figsize=(20, 15), annot=True, vmin=0, vmax=1)
plt.title("_".join(group_19))
plt.savefig(f"D:/LFProjects/NewPythonProject/结果输出/行业内品种相关性/其他组/{'_'.join(group_19)}.png")
plt.show()

group_20 = ['强麦','国际铜','铝','铅','铜','锌','镍']
symbol_list_20 = [name_symbol_map[name]+"_"+name for name in name_symbol_map if name in group_20]
corr_20 = corr.loc[symbol_list_20, symbol_list_20]
plt.figure()
sns.clustermap(data=corr_20, figsize=(20, 15), annot=True, vmin=0, vmax=1)
plt.title("_".join(group_20))
plt.savefig(f"D:/LFProjects/NewPythonProject/结果输出/行业内品种相关性/其他组/{'_'.join(group_20)}.png")
plt.show()

group_21 = ['燃料油','苯乙烯']
symbol_list_21 = [name_symbol_map[name]+"_"+name for name in name_symbol_map if name in group_21]
corr_21 = corr.loc[symbol_list_21, symbol_list_21]
plt.figure()
sns.clustermap(data=corr_21, figsize=(20, 15), annot=True, vmin=0, vmax=1)
plt.title("_".join(group_21))
plt.savefig(f"D:/LFProjects/NewPythonProject/结果输出/行业内品种相关性/其他组/{'_'.join(group_21)}.png")
plt.show()

group_22 = ['苯乙烯','燃料油','原油','低硫燃料油']
symbol_list_22 = [name_symbol_map[name]+"_"+name for name in name_symbol_map if name in group_22]
corr_22 = corr.loc[symbol_list_22, symbol_list_22]
plt.figure()
sns.clustermap(data=corr_22, figsize=(20, 15), annot=True, vmin=0, vmax=1)
plt.title("_".join(group_22))
plt.savefig(f"D:/LFProjects/NewPythonProject/结果输出/行业内品种相关性/其他组/{'_'.join(group_22)}.png")
plt.show()

group_23 = ['动力煤','国际铜']
symbol_list_23 = [name_symbol_map[name]+"_"+name for name in name_symbol_map if name in group_23]
corr_23 = corr.loc[symbol_list_23, symbol_list_23]
plt.figure()
sns.clustermap(data=corr_23, figsize=(20, 15), annot=True, vmin=0, vmax=1)
plt.title("_".join(group_23))
plt.savefig(f"D:/LFProjects/NewPythonProject/结果输出/行业内品种相关性/其他组/{'_'.join(group_23)}.png")
plt.show()