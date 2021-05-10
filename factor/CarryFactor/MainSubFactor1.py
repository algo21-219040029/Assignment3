import numpy as np
import pandas as pd
from tqdm import tqdm
from pandas import DataFrame

from utils.utility import stack_dataframe_by_fields

from factor.CarryFactor.base import BaseCarryFactor

class MainSubFactor1(BaseCarryFactor):
    """
    利用主力和次主力合约计算得到的期限结构因子

    合约：主力合约和次主力合约(根据成交量)，两者中近的合约定义为近月合约，远的合约定义为远月合约. 若合约只有一个，则因子值为缺失值

    时间差：日历日之差

    计算方法: (近月合约价格-远月合约价格)/(远月合约价格*时间差)

    Attributes
    __________
    price: str
            用于代表合约价格的字段, close或settlement

    window: int
            因子平滑参数，所有因子都具有

    See Also
    ________
    factor.CarryFactor.bases.BaseFactor
    """

    def __init__(self, price: str = 'close', window: int = 1) -> None:
        """
        Constructor

        Parameters
        ----------
        price: str
                用于代表合约价格的字段，close或settlement

        window: int
                因子平滑参数，所有因子都具有
        """
        super().__init__(price=price, window=window)

    def compute_single_factor(self, symbol: str) -> DataFrame:
        """
        计算单品种的因子值

        Parameters
        ----------
        symbol: str
                品种代码

        Returns
        -------
        因子值: DataFrame
                默认的index, 三列, datetime, underlying_symbol(均为symbol), factor
        """
        # 首先读取每日主力合约，然后获取每日持仓量，筛选出当天可交易品种(当天和下一天都可以交易），且持仓量仅次于主力合约
        # 需要准备的数据：每日各合约收盘价df, 每日各合约持仓量df，每日shift前的主力合约列表

        # 预先检查
        # 获取每日行情数据，用于得到收盘价数据和成交量数据
        price = self.get_params()['price']
        price_df: DataFrame = self.get_field(symbol, price)
        volume_df: DataFrame = self.get_field(symbol, 'volume')

        # 获取每日shift前的main_contract
        main_contracts = self.get_continuous_contract_data(symbol=symbol, price=price)
        main_contracts = main_contracts['contract_before_shift'].tolist()

        # 获取该品种每个合约的到期日
        maturity_date = self.get_maturity_date(symbol)

        # 获取交易日期列表
        date_list = price_df.index.tolist()

        sub_main_contract = ''
        sub_main_contracts = []
        main_contract_prices = []
        sub_main_contract_prices = []

        # 开始遍历
        for i in range(len(volume_df)):
            # 选出可选合约
            # 1.今天和下一个交易日都有交易
            if i != len(volume_df) - 1:
                pool = price.iloc[i].dropna().index.intersection(price_df.iloc[i + 1].dropna().index)
                main_contract = main_contracts[i]
                try:
                    pool_except_main_contract = pool.drop(main_contract)
                except KeyError:
                    pool_except_main_contract = pool.copy()
                try:
                    sub_main_contract = volume_df.iloc[i][pool_except_main_contract].sort_values(ascending=False).index[0]
                except IndexError:
                    sub_main_contract = main_contract
                sub_main_contracts.append(sub_main_contract)
                main_contract_prices.append(price_df.iloc[i][main_contract])
                sub_main_contract_prices.append(price_df.iloc[i][sub_main_contract])
            elif i == len(volume_df) - 1:
                pool = price_df.iloc[i].dropna().index
                main_contract = main_contracts[i]
                try:
                    pool_except_main_contract = pool.drop(main_contract)
                except KeyError:
                    pool_except_main_contract = pool.copy()
                try:
                    sub_main_contract = volume_df.iloc[i][pool_except_main_contract].sort_values(ascending=False).index[0]
                except IndexError:
                    sub_main_contract = main_contract
                sub_main_contracts.append(sub_main_contract)
                main_contract_prices.append(price_df.iloc[i][main_contract])
                sub_main_contract_prices.append(price_df.iloc[i][sub_main_contract])

        # 生成主力合约和次主力合约信息DataFrame
        # df共五列：主力合约代码，次主力合约代码，主力合约收盘价，次主力合约收盘价，交易日期
        # 交易日期是主力合约和次主力合约收盘后被确定的那天
        main_sub_df = pd.DataFrame({'main_contract': main_contracts,
                                    'sub_main_contract': sub_main_contracts,
                                    'main_contract_price': main_contract_prices,
                                    'sub_main_contract_price': sub_main_contract_prices,
                                    'datetime': date_list})
        main_sub_df.index = range(len(main_sub_df))

        # 计算合约交割日期之间的差值
        main_sub_df = pd.merge(left=main_sub_df,
                               right=maturity_date.rename(columns={'contract': 'main_contract'}),
                               on='main_contract', how='left'). \
            rename(columns={'maturity_date': 'main_contract_maturity_date'})
        main_sub_df = pd.merge(left=main_sub_df,
                               right=maturity_date.rename(columns={'contract': 'sub_main_contract'}),
                               on='sub_main_contract', how='left'). \
            rename(columns={'maturity_date': 'sub_main_contract_maturity_date'})

        main_sub_df.index = range(len(main_sub_df))

        main_sub_df['near_contract'] = pd.Series(
            np.where(main_sub_df['main_contract'] < main_sub_df['sub_main_contract'],
                     main_sub_df['main_contract'],
                     main_sub_df['sub_main_contract']))

        main_sub_df['far_contract'] = pd.Series(
            np.where(main_sub_df['main_contract'] > main_sub_df['sub_main_contract'],
                     main_sub_df['main_contract'],
                     main_sub_df['sub_main_contract']))

        main_sub_df['near_price'] = pd.Series(np.where(main_sub_df['main_contract'] < main_sub_df['sub_main_contract'],
                                                       main_sub_df['main_contract_price'],
                                                       main_sub_df['sub_main_contract_price']))
        main_sub_df['far_price'] = pd.Series(np.where(main_sub_df['main_contract'] > main_sub_df['sub_main_contract'],
                                                      main_sub_df['main_contract_price'],
                                                      main_sub_df['sub_main_contract_price']))

        main_sub_df['near_maturity_date'] = pd.Series(
            np.where(main_sub_df['main_contract'] < main_sub_df['sub_main_contract'],
                     main_sub_df['main_contract_maturity_date'],
                     main_sub_df['sub_main_contract_maturity_date']))

        main_sub_df['far_maturity_date'] = pd.Series(
            np.where(main_sub_df['main_contract'] > main_sub_df['sub_main_contract'],
                     main_sub_df['main_contract_maturity_date'],
                     main_sub_df['sub_main_contract_maturity_date']))

        main_sub_df['date_delta'] = (main_sub_df['near_maturity_date'] -
                                     main_sub_df['far_maturity_date']).dt.days

        main_sub_df['factor'] = (main_sub_df['near_price'] - main_sub_df['far_price']) / (main_sub_df['far_price'] *
                                                                                          main_sub_df['date_delta'])

        main_sub_df = main_sub_df[['datetime', 'near_contract', 'far_contract',
                                   'near_price', 'far_price',
                                   'date_delta', 'factor']]

        main_sub_df['underlying_symbol'] = symbol
        main_sub_df = main_sub_df[['datetime', 'underlying_symbol', 'factor']]
        return main_sub_df

    def compute_factor(self) -> DataFrame:
        """
        计算因子，通过对compute_single_factor实现

        Returns
        -------
        因子值: DataFrame
                index为datetime, columns为underlying_symbol, data为factor
        """

        symbol_list = self.get_symbol_list()
        factor_list = []
        for symbol in tqdm(symbol_list):
            factor = self.compute_single_factor(symbol)
            factor_list.append(factor)
        factor = pd.concat(factor_list, axis=0)
        factor = stack_dataframe_by_fields(data=factor,
                                           index_field='datetime',
                                           column_field='underlying_symbol',
                                           data_field='factor')
        factor = factor.rolling(window=self.window, min_periods=1).mean()
        self.factor_value = factor
        return factor


