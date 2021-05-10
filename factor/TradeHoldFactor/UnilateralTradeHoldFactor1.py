from pandas import DataFrame
from typing import Dict, Any
from utils.utility import Dev1, Dev2, Dev3
from factor.TradeHoldFactor.base import BaseTradeHoldFactor

class UnilateralTradeHoldFactor1(BaseTradeHoldFactor):
    """
    单边成交持仓因子1: sum(持多头量,N)/品种持仓量

    Attributes
    __________
    N: int, default 20
        选择的前N位成员的数据

    window: int, default 1
            平滑窗口

    R: int, default 20
        异常度函数回溯期

    func: str, default None
        异常度函数
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
        N: int = params['N']
        window: int = params['window']
        R: int = params['R']
        func: str = params['func']

        # 提取数据
        long_df: DataFrame = self.get_trade_hold_data_by_rank(rank_by='long', group_by_symbol=False)
        long_df = long_df.sort_values(by=['datetime', 'underlying_symbol', 'volume'], ascending=[True, True, False]).\
            set_index(['datetime', 'underlying_symbol']).groupby(['datetime', 'underlying_symbol'], as_index=True)['volume'].head(N)
        long_df = long_df.groupby(['datetime', 'underlying_symbol']).sum().unstack(level=-1)

        # open_interest_df: DataFrame = self.get_groupby_field(field='open_interest', func='sum').set_index(['datetime', 'underlying_symbol'])['open_interest'].unstack(level=-1)
        open_interest_df: DataFrame = self.daily_data_manager.get_daily_data().groupby(['datetime', 'underlying_symbol'])['open_interest'].sum().unstack(level=-1)

        common_index = long_df.index.intersection(open_interest_df.index)
        common_columns = long_df.columns.intersection(open_interest_df.columns)

        long_df = long_df.loc[common_index, common_columns]
        open_interest_df = open_interest_df.loc[common_index, common_columns]

        # 计算原始因子值
        factor_value: DataFrame = long_df / open_interest_df
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
        return long_df
