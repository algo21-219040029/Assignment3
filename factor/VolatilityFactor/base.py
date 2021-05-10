from pandas import DataFrame
from factor.base import BaseFactor

class BaseVolatilityFactor(BaseFactor):
    """
    波动率因子基类
    """

    def __init__(self, **params) -> None:
        """Constructor"""
        super().__init__(**params)

    def compute_factor(self) -> DataFrame:
        """
        计算因子

        Returns
        -------
        波动率因子
        """
        pass