import pandas as pd
from pandas import DataFrame
from typing import Dict, Any

from utils.utility import Dev1, Dev2, Dev3
from factor.TradeHoldFactor.base import BaseTradeHoldFactor

class BilateralTradeHoldFactor2(BaseTradeHoldFactor):
    """
    双边成交持仓因子2: sum(持多仓量*持多仓量/品种持仓量, N)-sum(持空仓量*持空仓量/品种持仓量, N)
    """
    def __init__(self, N: int = 20, window: int = 1, R: int = 20, func: str = None) -> None:
        """
        Constructor

        Parameters
        ----------
        N: int, default 20
            选择的前N位成员的数据

        window: int, default 1
                平滑窗口

        R: int, default 20
            异常度函数回溯期

        func: str, default None
                异常度函数
        """

        super().__init__(N=N, window=window, R=R, func=func)

    def compute_factor(self) -> DataFrame:
        """
        计算因子值

        Returns
        -------
        factor_value: DataFrame
                    因子值
        """

        # 提取参数
        params: Dict[str, Any] = self.get_params()
        window: int = params['window']
        R: int = params['R']
        func: str = params['func']

        # 提取数据
        long_df: DataFrame = self.get_trade_hold_data_by_rank(rank_by='long', group_by_symbol=False).sort_values(by=['datetime', 'underlying_symbol', 'volume'], ascending=[True, True, False])
        short_df: DataFrame = self.get_trade_hold_data_by_rank(rank_by='short', group_by_symbol=False).sort_values(by=['datetime', 'underlying_symbol', 'volume'], ascending=[True, True, False])
        open_interest_df: DataFrame = self.get_groupby_field(field='open_interest', func='sum')

        # 处理数据
        long_df = pd.merge(left=long_df[['datetime', 'underlying_symbol', 'member_name','volume']], right=open_interest_df, on=['datetime', 'underlying_symbol'], how='left')
        long_df.rename(columns={'volume':'value'}, inplace=True)

        short_df = pd.merge(left=short_df[['datetime', 'underlying_symbol', 'member_name', 'volume']], right=open_interest_df, on=['datetime', 'underlying_symbol'], how='left')
        short_df.rename(columns={'volume': 'value'}, inplace=True)

        long_df['weight'] = long_df['value'] / long_df['open_interest']
        short_df['weight'] = short_df['value'] / short_df['open_interest']

        long_df['long_side'] = long_df['value'] * long_df['weight']
        short_df['short_side'] = short_df['value'] * short_df['weight']

        long_side_df: DataFrame = long_df.groupby(['datetime', 'underlying_symbol'])['long_side'].sum()
        short_side_df: DataFrame = short_df.groupby(['datetime', 'underlying_symbol'])['short_side'].sum()

        factor_value: DataFrame = (long_side_df - short_side_df).unstack(level=-1)
        factor_value = factor_value.rolling(window=window, min_periods=0).mean()

        # 叠加函数
        if func == 'Dev1':
            factor_value = Dev1(factor_value, R)
        elif func == 'Dev2':
            factor_value = Dev2(factor_value, R)
        elif func == 'Dev3':
            factor_value = Dev3(factor_value, R)
        elif not func:
            pass
        else:
            raise ValueError("func must be Dev1, Dev2, Dev3 or None!")

        self.factor_value = factor_value
        return factor_value

