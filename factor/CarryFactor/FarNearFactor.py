import numpy as np
import pandas as pd
from tqdm import tqdm
from pandas import DataFrame

from factor.CarryFactor.base import BaseCarryFactor
from utils.utility import stack_dataframe_by_fields

class FarNearFactor(BaseCarryFactor):
    """
    利用近月合约和次近月合约计算的期限结构因子

    合约: 近月合约和次近月，若合约只有一个，则因子值为缺失值

    时间差: 日历日之差

    计算方法: (近月合约价格-次近月合约价格)/(次近月合约价格*时间差)

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

        # 获取每日行情数据，用于得到收盘价数据
        price_df: DataFrame = self.get_field(symbol, self.price)
        stack_price_df: DataFrame = price_df.stack().to_frame('price').reset_index()

        date_list = price_df.index.tolist()

        # 获取每个合约的到期日
        maturity_date = self.get_maturity_date(symbol)

        near_contracts = []
        far_contracts = []
        near_contract = ''
        far_contract = ''

        for i in range(len(price_df)):
            # 选出可选合约
            pool = price_df.iloc[i].dropna().index.sort_values(ascending=True)
            near_contract = pool[0]
            try:
                far_contract = pool[1]
            except IndexError:
                far_contract = np.nan
            near_contracts.append(near_contract)
            far_contracts.append(far_contract)

        # 生成近月合约和远月合约信息DataFrame
        # df共五列: 近月合约代码，远月合约代码，近月合约代码，交易日期
        # 交易日期是主力合约和次主力合约收盘后被确定的那天
        near_far_df = pd.DataFrame({'near_contract': near_contracts,
                                    'far_contract': far_contracts,
                                    'datetime': date_list})
        near_far_df = pd.merge(left=near_far_df,
                               right=stack_price_df.rename(columns={'contract': 'near_contract',
                                                                 'price': 'near_price'}),
                               on=['near_contract', 'datetime'],
                               how='left')
        near_far_df = pd.merge(left=near_far_df,
                               right=stack_price_df.rename(columns={'contract': 'far_contract',
                                                                 'price': 'far_price'}),
                               on=['far_contract', 'datetime'],
                               how='left')

        near_far_df = pd.merge(left=near_far_df,
                               right=maturity_date.rename(columns={'contract': 'near_contract',
                                                                   'maturity_date': 'near_maturity_date'}),
                               on='near_contract', how='left')

        near_far_df = pd.merge(left=near_far_df,
                               right=maturity_date.rename(columns={'contract': 'far_contract',
                                                                   'maturity_date': 'far_maturity_date'}),
                               on='far_contract', how='left')

        near_far_df['date_delta'] = (near_far_df['near_maturity_date'] -
                                     near_far_df['far_maturity_date']).dt.days

        near_far_df['factor'] = (near_far_df['near_price'] - near_far_df['far_price']) / (near_far_df['far_price'] *
                                                                                          near_far_df['date_delta'])

        near_far_df = near_far_df[['datetime', 'near_contract', 'far_contract',
                                   'near_price', 'far_price',
                                   'date_delta', 'factor']]

        near_far_df['underlying_symbol'] = symbol
        near_far_df = near_far_df[['datetime', 'underlying_symbol', 'factor']]
        return near_far_df

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

