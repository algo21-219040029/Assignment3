from pandas import DataFrame
from typing import Callable, Union

from factor.base import BaseFactor
from data_manager.TradeHoldDataManager import TradeHoldDataManager

class BaseTradeHoldFactor(BaseFactor):
    """
    成交持仓因子基类
    """
    def __init__(self, **params) -> None:
        """Constructor"""
        super().__init__(**params)

        self.trade_hold_data_manager: TradeHoldDataManager = TradeHoldDataManager()

    def get_trade_hold_data_by_rank(self, rank_by: str = 'volume', group_by_symbol: bool = True) -> DataFrame:
        """
        根据排名标准获取成交持仓数据

        Parameters
        ----------
        rank_by: str, default volume
                排名标准, volume, long, short

        group_by_symbol: bool, default True
                        是否根据品种聚合

        Returns
        -------
        data: 成交持仓数据
        """
        data: DataFrame = self.trade_hold_data_manager.get_trade_hold_data_by_rank(rank_by=rank_by, group_by_symbol=group_by_symbol)
        return data

    def get_groupby_field(self, field: str, func: Union[str, Callable]) -> DataFrame:
        """
        获取根据各合约按日聚合而成以品种为单位的DataFrame

        Parameters
        ----------
        field: str
                字段

        func: Union[str, Callable]
                聚合方式

        Returns
        -------

        """
        daily_data: DataFrame = self.daily_data_manager.daily_data
        if isinstance(func, str):
            result: DataFrame = getattr(daily_data.groupby(['datetime', 'underlying_symbol'])[field], func)()
            result = result.to_frame(field).reset_index()
        elif isinstance(func, Callable):
            result: DataFrame = daily_data.groupby(['datetime', 'underlying_symbol'])[field].apply(func)
            result = result.to_frame(field).reset_index()
        else:
            raise TypeError("func must be a string or a function!")
        return result

    def compute_factor(self) -> DataFrame:
        """计算因子"""
        pass
