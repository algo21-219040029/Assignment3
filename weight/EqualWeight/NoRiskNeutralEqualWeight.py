import numpy as np
from pandas import DataFrame
from weight.base import BaseWeight

class NoRiskNeutralEqualWeight(BaseWeight):
    """
    等权重生成器,
    """
    def __init__(self) -> None:
        """Constructor"""
        super().__init__()

    def get_weight(self) -> DataFrame:
        """
        获取权重DataFrame

        Returns
        -------
        weight_df: DataFrame
                    权重DataFrame
        """

        # 预先检查
        if not isinstance(self.signal_df, DataFrame):
            raise ValueError("Please specify signal_df first!")

        signal_df = self.signal_df
        weight_df = (signal_df.T / np.abs(signal_df).sum(axis=1)).T
        self.weight_df = weight_df
        return weight_df