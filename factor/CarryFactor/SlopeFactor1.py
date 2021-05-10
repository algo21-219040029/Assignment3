import numpy as np
import pandas as pd
from tqdm import tqdm
from typing import Dict
from pathlib import Path
import statsmodels.api as sm
from pandas import Series, DataFrame
from sklearn.linear_model import LinearRegression

from factor.base import BaseFactor
from data_manager import BasicsDataManager
from utils.utility import stack_dataframe_by_fields

class SlopeFactor1(BaseFactor):
    """
    利用斜率计算得到的期限结构因子

    合约: 一个品种所有合约

    因变量y: ln(合约价格)

    自变量x：各合约到期日与近月合约到期日的日历日差（即近月合约为0）

    计算方法: 对每个品种每个交易日所有合约的y,x求线性回归, 得到的斜率即为因子值

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

        # 获取每日收盘价数据
        price_df: DataFrame = self.get_field(symbol, self.price)
        price_df = price_df.replace(0.0, np.nan)
        new_price_df = price_df.fillna(method='ffill').copy()
        new_price_df[(pd.isnull(price_df.fillna(method='ffill'))) | (pd.isnull(price_df.fillna(method='bfill')))] = np.nan
        price_df = new_price_df

        # 对收盘价取对数
        ln_price_df = np.log(price_df)

        # 输出每日可交易合约
        daily_contract_count = price_df.count(axis=1)

        # 获取每个合约的到期日期
        maturity_date = self.get_maturity_date(symbol)

        # 对收盘价对数数据进行stack，方便与到期日进行merge
        data = pd.merge(left=ln_price_df.stack().to_frame('ln_price').reset_index(),
                        right=maturity_date,
                        on='contract',
                        how='left')

        # 生成每个合约到期日与当天最近交割合约到期日的自然日之差
        data['date_delta'] = data.groupby('datetime')['maturity_date'].transform(lambda x: (x-x.min()).dt.days)

        # 只保留datetime, ln_close和date_delta
        data = data[['datetime', 'ln_price', 'date_delta']]

        # 回归函数
        def apply_regression(df):
            y = df['ln_price'].dropna().values
            x = df['date_delta'].dropna().values
            if len(x) == 0 or len(x) == 1:
                return np.nan
            else:
                x = sm.add_constant(x.reshape(-1, 1))
                model = LinearRegression()
                model.fit(x, y)
                return model.coef_[1]

        daily_factor = data.groupby('datetime').apply(apply_regression).to_frame("factor")
        daily_factor['underlying_symbol'] = symbol
        daily_factor = daily_factor.reset_index()
        return daily_factor

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
