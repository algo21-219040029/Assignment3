from pandas import DataFrame
from signals.base import BaseSignal

class BaseTimeSeriesSignal(BaseSignal):
    """
    时间序列信号基类
    """

    def __init__(self, **params) -> None:
        """
        Constructor
        """
        super().__init__(**params)

    def transform(self, *args, **kwargs) -> DataFrame:
        pass
