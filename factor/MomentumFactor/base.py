from pandas import DataFrame
from factor.base import BaseFactor

class BaseMomentumFactor(BaseFactor):
    """
    动量因子基类
    """

    def __init__(self, **params) -> None:
        """Constructor"""
        super().__init__(**params)

    def compute_factor(self) -> DataFrame:
        pass