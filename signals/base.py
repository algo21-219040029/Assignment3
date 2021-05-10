import inspect
from pathlib import Path
from pandas import DataFrame
from abc import abstractmethod

from bases.base import BaseClass
from data_manager.DailyDataManager import DailyDataManager

class BaseSignal(BaseClass):
    """
    因子值转化为信号, 做多为1, 做空为-1, 空仓为0

    Attributes
    __________
    factor_data: DataFrame
                因子数据, index为交易时间, columns为品种代码, data为因子值

    commodity_pool: DataFrame
                    商品池, index为交易时间, columns为品种代码, data为因子值

    See Also
    ________
    bases.bases.BaseClass
    """
    def __init__(self, **params) -> None:
        """Constructor"""
        super().__init__(**params)
        self.group: str = Path(inspect.getfile(self.__class__)).parent.name
        self.name: str = self.__class__.__name__
        self.factor_data: DataFrame = None
        self.commodity_pool: DataFrame = None

        self.daily_data_manager: DailyDataManager = DailyDataManager()

    def set_factor_data(self, factor_data: DataFrame) -> None:
        """
        设置因子值

        Parameters
        ----------
        factor_data: DataFrame
                    因子值, index为交易时间, columns为品种代码, data为因子值

        Returns
        -------
        None
        """
        self.factor_data = factor_data

    def set_commodity_pool(self, commodity_pool: DataFrame) -> None:
        """
        设置商品池

        Parameters
        ----------
        commodity_poolL DataFrame
                        商品吃, index为交易时间, columns为品种代码, data为商品池

        Returns
        -------
        None
        """
        self.commodity_pool = commodity_pool

    @abstractmethod
    def transform(self, *args, **kwargs) -> DataFrame:
        """
        根据因子值和商品池生成信号
        """
        raise NotImplementedError

    def __repr__(self):
        group = self.group
        name = self.name
        title = ''
        title += f"signal(group={group}, name={name}, "
        # 添加因子参数
        for key, value in self.get_params().items():
            title += f"{key}={value}, "
        title = title[:-2]
        title += ")"
        return title

    def get_string(self) -> str:
        return self.__repr__()

    def get_groupby_field(self, field: str, method: str) -> DataFrame:
        """
        获取以品种为单位的日线行情数据

        Parameters
        ----------
        field: str,
                字段

        method: str,
            first, last, sum, min, max等
        Returns
        -------

        """
        df: DataFrame = self.daily_data_manager.daily_data.groupby(['datetime', 'underlying_symbol'])[field].agg(method).unstack(level=-1)
        return df