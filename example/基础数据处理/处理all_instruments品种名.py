##
# 导入库
import pandas as pd

##
# 导入数据
df = pd.read_pickle("D:/LFProjects/NewPythonProject/data/all_instruments_20210130_1.pkl")
df1 = df[['underlying_symbol', 'contract', 'symbol']]
df1['name'] = df1['symbol'].str.extract(r'([^0-9]+)')
df1 = df1.sort_values(by=['underlying_symbol', 'contract'], ascending=True)

df2 = df1.drop_duplicates(['underlying_symbol', 'name'])
##
# 修正数据
# 修正品种A
df['symbol'] = df['symbol'].str.replace("黄大豆1号","豆一").str.replace("大豆",'豆一')

# 修正品种AP
df['symbol'] = df['symbol'].str.replace("鲜苹果", "苹果")
for i in df.index:
    if len(df.loc[i,'symbol']) == 5 and df.loc[i,'underlying_symbol'] == 'AP':
        string = df.loc[i, 'symbol']
        df.loc[i, 'symbol'] = string[:2]+'1'+string[2:]

# 修正品种B
df['symbol'] = df['symbol'].str.replace("黄豆一2号","豆二")

# 修正品种BB
df['symbol'] = df['symbol'].str.replace("细木工板","胶合板")

# 修正品种BU
df['symbol'] = df['symbol'].str.replace("石油沥青", "沥青")

# 修正品种C
df['symbol'] = df['symbol'].str.replace("黄玉米", "玉米")

# 修正品种CF
df['symbol'] = df['symbol'].str.replace("一号棉花", '棉花')

#
df['symbol'] = df['symbol'].str.replace('棉纱4月', '棉纱1804')


for i in df.index:
    if len(df.loc[i,'symbol']) == 5 and df.loc[i,'underlying_symbol'] == 'CF':
        string = df.loc[i, 'symbol']
        df.loc[i, 'symbol'] = string[:2]+'1'+string[2:]

for i in df.index:
    if len(df.loc[i,'symbol']) == 5 and df.loc[i,'underlying_symbol'] == 'CJ':
        string = df.loc[i, 'symbol']
        df.loc[i, 'symbol'] = string[:2]+'1'+string[2:]

for i in df.index:
    if len(df.loc[i,'symbol']) == 5 and df.loc[i,'underlying_symbol'] == 'CY':
        string = df.loc[i, 'symbol']
        df.loc[i, 'symbol'] = string[:2]+'1'+string[2:]

# 修正品种FB
df['symbol'] = df['symbol'].str.replace("中密度纤维板", "纤维板")

df['symbol'] = df['symbol'].str.replace("玻璃4月", "玻璃1804")
for i in df.index:
    if len(df.loc[i,'symbol']) == 5 and df.loc[i,'underlying_symbol'] == 'FG':
        string = df.loc[i, 'symbol']
        df.loc[i, 'symbol'] = string[:2]+'1'+string[2:]

# 修正品种IF
df['symbol'] = df['symbol'].str.replace("IF", "沪深")

# 修正品种J
df['symbol'] = df['symbol'].str.replace("冶金焦炭", "焦炭")

# 修正品种JR
df['symbol'] = df['symbol'].str.replace("粳稻谷", "粳稻")

for i in df.index:
    if len(df.loc[i,'symbol']) == 5 and df.loc[i,'underlying_symbol'] == 'JR':
        string = df.loc[i, 'symbol']
        df.loc[i, 'symbol'] = string[:2]+'1'+string[2:]

# 修正品种L
df['symbol'] = df['symbol'].str.replace("线型低密度聚乙烯", "聚乙烯")

for i in df.index:
    if len(df.loc[i,'symbol']) == 6 and df.loc[i,'underlying_symbol'] == 'LR':
        string = df.loc[i, 'symbol']
        df.loc[i, 'symbol'] = string[:3]+'1'+string[3:]

for i in df.index:
    if len(df.loc[i,'symbol']) == 5 and df.loc[i,'underlying_symbol'] == 'MA':
        string = df.loc[i, 'symbol']
        df.loc[i, 'symbol'] = string[:2]+'1'+string[2:]
df['symbol'] = df['symbol'].str.replace("甲醇4月", "甲醇1804")

for i in df.index:
    if len(df.loc[i,'symbol']) == 6 and df.loc[i,'underlying_symbol'] == 'OI':
        string = df.loc[i, 'symbol']
        df.loc[i, 'symbol'] = string[:3]+'1'+string[3:]

# 修正品种PM
df['symbol'] = df['symbol'].str.replace("普通小麦", "普麦")
for i in df.index:
    if len(df.loc[i,'symbol']) == 5 and df.loc[i,'underlying_symbol'] == 'PM':
        string = df.loc[i, 'symbol']
        df.loc[i, 'symbol'] = string[:2]+'1'+string[2:]

for i in df.index:
    if len(df.loc[i,'symbol']) == 6 and df.loc[i,'underlying_symbol'] == 'RI':
        string = df.loc[i, 'symbol']
        df.loc[i, 'symbol'] = string[:3]+'1'+string[3:]

for i in df.index:
    if len(df.loc[i,'symbol']) == 6 and df.loc[i,'underlying_symbol'] == 'RM':
        string = df.loc[i, 'symbol']
        df.loc[i, 'symbol'] = string[:3]+'1'+string[3:]

for i in df.index:
    if len(df.loc[i,'symbol']) == 6 and df.loc[i,'underlying_symbol'] == 'RS':
        string = df.loc[i, 'symbol']
        df.loc[i, 'symbol'] = string[:3]+'1'+string[3:]

for i in df.index:
    if len(df.loc[i,'symbol']) == 5 and df.loc[i,'underlying_symbol'] == 'SF':
        string = df.loc[i, 'symbol']
        df.loc[i, 'symbol'] = string[:2]+'1'+string[2:]
df['symbol'] = df['symbol'].str.replace("硅铁4月","硅铁1804")
df['symbol'] = df['symbol'].str.replace("锰硅4月","锰硅1804")

for i in df.index:
    if len(df.loc[i,'symbol']) == 5 and df.loc[i,'underlying_symbol'] == 'SM':
        string = df.loc[i, 'symbol']
        df.loc[i, 'symbol'] = string[:2]+'1'+string[2:]

# 修正品种SR
df['symbol'] = df['symbol'].str.replace("白砂糖", "白糖")
for i in df.index:
    if len(df.loc[i,'symbol']) == 5 and df.loc[i,'underlying_symbol'] == 'SR':
        string = df.loc[i, 'symbol']
        df.loc[i, 'symbol'] = string[:2]+'1'+string[2:]

# 修正品种T
df['symbol'] = df['symbol'].str.replace("长债", "10年期国债")

# 修正品种TA
df['symbol'] = df['symbol'].str.replace("精对苯二甲酸", "PTA")
df['symbol'] = df['symbol'].str.replace("PTA4月", "PTA1804")
for i in df.index:
    if len(df.loc[i,'symbol']) == 6 and df.loc[i,'underlying_symbol'] == 'TA':
        string = df.loc[i, 'symbol']
        df.loc[i, 'symbol'] = string[:3]+'1'+string[3:]

# 修正品种TF
for i in df.index:
    if len(df.loc[i,'symbol']) == 6 and df.loc[i,'underlying_symbol'] == 'TF':
        string = df.loc[i, 'symbol']
        df.loc[i, 'symbol'] = "5年期"+string

# 修正品种WH
df['symbol'] = df['symbol'].str.replace("优质强筋小麦","强麦")
for i in df.index:
    if len(df.loc[i,'symbol']) == 5 and df.loc[i,'underlying_symbol'] == 'WH':
        string = df.loc[i, 'symbol']
        df.loc[i, 'symbol'] = string[:2]+'1'+string[2:]

# 修正品种WT
df['symbol'] = df['symbol'].str.replace("硬冬白麦","硬麦").str.replace("硬白小麦","硬麦")

# 修正品种ZC
for i in df.index:
    if len(df.loc[i,'symbol']) == 6 and df.loc[i,'underlying_symbol'] == 'ZC':
        string = df.loc[i, 'symbol']
        df.loc[i, 'symbol'] = string[:3]+'1'+string[3:]
##
# 修正数字只有3位的情况
df_3 = df[df['symbol'].str.count("[0-9]") != 4].sort_values(by='contract')

##
# 处理IF,IH,IC
df_IF = df[df.underlying_symbol=='IF']
df_IH = df[df.underlying_symbol=='IH']
df_IC = df[df.underlying_symbol=='IC']

for i in df.index:
    if len(df.loc[i,'symbol']) == 6 and df.loc[i,'underlying_symbol'] == 'IF':
        string = df.loc[i, 'symbol']
        df.loc[i, 'symbol'] = df.loc[i, 'symbol'].replace("沪深","沪深300指数")

for i in df.index:
    if len(df.loc[i,'symbol']) == 6 and df.loc[i,'underlying_symbol'] == 'IH':
        string = df.loc[i, 'symbol']
        df.loc[i, 'symbol'] = df.loc[i, 'symbol'].replace("上证","上证50指数")

for i in df.index:
    if len(df.loc[i,'symbol']) == 6 and df.loc[i,'underlying_symbol'] == 'IC':
        string = df.loc[i, 'symbol']
        df.loc[i, 'symbol'] = df.loc[i, 'symbol'].replace("中证","中证500指数")
##
# 保存数据
df.to_pickle("D:/LFProjects/NewPythonProject/data/all_instruments_20210222_1.pkl")