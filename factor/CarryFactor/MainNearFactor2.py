import numpy as np
import pandas as pd
from tqdm import tqdm
from pandas import DataFrame

from factor.base import BaseFactor

from utils.utility import stack_dataframe_by_fields
from factor.contract_utility import (get_maturity_date,
                                     get_near_contract_except_main)

date_range = pd.date_range(start='2011-01-01', end='2021-12-31').to_frame(name='date')
date_range['year'] = date_range['date'].dt.year
date_range['month'] = date_range['date'].dt.month
date_range.index = range(len(date_range))
date_range.sort_values(by='date', inplace=True)
month_end_date = date_range.groupby(['year', 'month'], as_index=True)['date'].nth(-1)
month_start_date = date_range.groupby(['year', 'month'], as_index=True)['date'].nth(0)

class MainNearFactor2(BaseFactor):
    """
    利用主力合约和去除主力合约后最近合约计算的期限结构因子

    合约: 主力合约和去除主力合约后最近合约，两者中近的合约定义为近月合约，远的合约定义为远月合约. 若合约只有一个，则因子值为缺失值

    时间差: 两个月份差的日历天数

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

    def __init__(self, price: str = 'close', window: int = 1):
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

        # 生成主力合约列表
        # continuous_main_contract_series = self.continuous_main_contract_series\
        #     [self.continuous_main_contract_series['underlying_symbol'] == symbol]
        # main_contract_df = continuous_main_contract_series[['datetime', 'contract_before_shift']]. \
        #     rename(columns={'contract_before_shift': 'main_contract'})
        params = self.get_params()
        price = params['price']
        main_contract_df = self.get_continuous_contract_data(symbol=symbol, price=price)
        main_contract_df = main_contract_df[['datetime', 'contract_before_shift']]\
            .rename(columns={'contract_before_shift': 'main_contract'})

        # 获取除主力合约以外的近月合约
        # daily_data = self.daily_data[self.daily_data.underlying_symbol == symbol]
        # near_contract_df = get_near_contract_except_main(daily_data, main_contract_df)
        price_df: DataFrame = self.get_field(symbol=symbol, field=price)
        stack_price_df: DataFrame = price_df.stack().to_frame('price').reset_index()
        near_contract_df = get_near_contract_except_main(price_df, main_contract_df)

        # 获取每个合约的到期日
        maturity_date = self.get_maturity_date(symbol)

        # 获取合约每天的收盘价
        # close = self.daily_data[self.daily_data.underlying_symbol == symbol][['datetime', 'contract', 'close']]

        # 预先拼接工作
        # main_contract_df = pd.merge(left=main_contract_df,
        #                             right=maturity_date.rename(columns={'contract': 'main_contract',
        #                                                                 'maturity_date':
        #                                                                     'main_contract_maturity_date'}),
        #                             on='main_contract', how='left')
        #
        # main_contract_df = pd.merge(left=main_contract_df,
        #                             right=close.rename(
        #                                 columns={'contract': 'main_contract', 'close': 'main_close'}),
        #                             on=['datetime', 'main_contract'], how='left')
        #
        # near_contract_df = pd.merge(left=near_contract_df,
        #                             right=maturity_date.rename(columns={'contract': 'near_contract',
        #                                                                 'maturity_date':
        #                                                                     'near_contract_maturity_date'}),
        #                             on='near_contract', how='left')
        #
        # near_contract_df = pd.merge(left=near_contract_df,
        #                             right=close.rename(
        #                                 columns={'contract': 'near_contract', 'close': 'near_close'}),
        #                             on=['datetime', 'near_contract'], how='left')

        main_contract_df = pd.merge(left=main_contract_df,
                                    right=maturity_date.rename(columns={'contract': 'main_contract',
                                                                        'maturity_date':
                                                                            'main_contract_maturity_date'}),
                                    on='main_contract', how='left')

        main_contract_df = pd.merge(left=main_contract_df,
                                    right=stack_price_df.rename(
                                        columns={'contract': 'main_contract', 'price': 'main_price'}),
                                    on=['datetime', 'main_contract'], how='left')

        near_contract_df = pd.merge(left=near_contract_df,
                                    right=maturity_date.rename(columns={'contract': 'near_contract',
                                                                        'maturity_date':
                                                                            'near_contract_maturity_date'}),
                                    on='near_contract', how='left')

        near_contract_df = pd.merge(left=near_contract_df,
                                    right=stack_price_df.rename(
                                        columns={'contract': 'near_contract', 'price': 'near_price'}),
                                    on=['datetime', 'near_contract'], how='left')

        # factor: 因子值
        main_near_df = pd.concat([main_contract_df.set_index('datetime'), near_contract_df.set_index('datetime')],
                                 axis=1).reset_index()
        main_near_df.index = range(len(main_near_df))
        # 远月合约价格
        main_near_df['far_price'] = pd.Series(np.where(main_near_df['main_contract'] > main_near_df['near_contract'],
                                                       main_near_df['main_price'],
                                                       main_near_df['near_price']))

        main_near_df['near_price'] = pd.Series(np.where(main_near_df['main_contract'] < main_near_df['near_contract'],
                                                        main_near_df['main_price'],
                                                        main_near_df['near_price']))

        main_near_df['far_contract'] = pd.Series(np.where(main_near_df['main_contract'] > main_near_df['near_contract'],
                                                          main_near_df['main_contract'],
                                                          main_near_df['near_contract']))

        main_near_df['near_contract'] = pd.Series(
            np.where(main_near_df['main_contract'] < main_near_df['near_contract'],
                     main_near_df['main_contract'],
                     main_near_df['near_contract']))

        main_near_df['near_maturity_date'] = pd.Series(
            np.where(main_near_df['main_contract_maturity_date'] < main_near_df['near_contract_maturity_date'],
                     main_near_df['main_contract_maturity_date'],
                     main_near_df['near_contract_maturity_date']))

        main_near_df['far_maturity_date'] = pd.Series(
            np.where(main_near_df['main_contract_maturity_date'] > main_near_df['near_contract_maturity_date'],
                     main_near_df['main_contract_maturity_date'],
                     main_near_df['near_contract_maturity_date']))

        # main_near_df['date_delta'] = (main_near_df['near_maturity_date'] -
        #                               main_near_df['far_maturity_date']).dt.days

        main_near_df['far_year_month'] = pd.to_datetime('20'+main_near_df['far_contract'].str[-4:]+'01')
        main_near_df['far_year'] = main_near_df['far_year_month'].dt.year
        main_near_df['far_month'] = main_near_df['far_year_month'].dt.month
        far_multi_index = pd.MultiIndex.from_frame(main_near_df[['far_year', 'far_month']])
        main_near_df['far_month_date'] = month_end_date[far_multi_index].tolist()

        main_near_df['near_year_month'] = pd.to_datetime('20' + main_near_df['near_contract'].str[-4:] + '01')
        main_near_df['near_year'] = main_near_df['near_year_month'].dt.year
        main_near_df['near_month'] = main_near_df['near_year_month'].dt.month
        near_multi_index = pd.MultiIndex.from_frame(main_near_df[['near_year', 'near_month']])
        main_near_df['near_month_date'] = month_end_date[near_multi_index].tolist()
        main_near_df['date_delta'] = (main_near_df['near_month_date'] - main_near_df['far_month_date']).dt.days

        main_near_df['factor'] = (main_near_df['near_price'] - main_near_df['far_price']) / (main_near_df['far_price'] *
                                                                                             main_near_df['date_delta'])

        main_near_df = main_near_df[['datetime', 'near_contract', 'far_contract',
                                    'near_close', 'far_close',
                                    'date_delta', 'factor']]

        main_near_df['underlying_symbol'] = symbol
        factor = main_near_df[['datetime', 'underlying_symbol', 'factor']]
        return main_near_df

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

