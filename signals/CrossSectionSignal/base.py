from pandas import DataFrame
from signals.base import BaseSignal

class BaseCrossSectionSignal(BaseSignal):
    """
    横截面信号基类
    """
    def __init__(self, **params) -> None:
        """
        Constructor
        """
        super().__init__(**params)

    def transform(self, *args, **kwargs) -> DataFrame:
        pass