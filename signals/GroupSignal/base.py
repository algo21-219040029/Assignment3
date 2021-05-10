from enum import Enum
from pandas import DataFrame
from signals.base import BaseSignal

class GroupSignalType(Enum):

    AllGroupSignal = 'all'
    PositiveGroupSignal = 'positive'
    NegativeGroupSignal = 'negative'

class BaseGroupSignal(BaseSignal):
    """
    分组信号基类

    分组信号生成器主要用于分组回测，用于因子分组测试
    """
    def __init__(self, **params) -> None:
        """Constructor"""
        super().__init__(**params)

    def transform(self, *args, **kwargs) -> DataFrame:
        pass
