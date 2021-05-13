import inspect
from pathlib import Path
from pandas import DataFrame
from abc import abstractmethod

from bases.base import BaseClass

class BaseWeight(BaseClass):

    def __init__(self, **params) -> None:
        """Constructor"""
        super().__init__(**params)
        self.group: str = Path(inspect.getfile(self.__class__)).parent.name
        self.name: str = self.__class__.__name__
        self.signal_df: DataFrame = None
        self.weight_df: DataFrame = None

    def set_signal(self, signal_df: DataFrame) -> None:
        """
        设置信号DataFrame

        Parameters
        ----------
        signal_df: DataFrame
                    信号DataFrame, index为交易时间, columns为品种代码, data为信号, 做多为1, 做空为-1,空仓为0

        Returns
        -------
        None
        """
        self.signal_df = signal_df

    @abstractmethod
    def get_weight(self) -> DataFrame:
        """
        获取权重
        Returns
        -------
        权重DataFrame
        """
        raise NotImplementedError

    def __repr__(self):
        group = self.group
        name = self.name
        title = ''
        title += f"weight(group={group}, name={name}, "
        # 添加因子参数
        for key, value in self.get_params().items():
            title += f"{key}={value}, "
        title = title[:-2]
        title += ")"
        return title

    def get_string(self) -> str:
        return self.__repr__()