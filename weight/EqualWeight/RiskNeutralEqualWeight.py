import numpy as np
from pandas import DataFrame
from weight.base import BaseWeight

class RiskNeutralEqualWeight(BaseWeight):
    """
    等权重生成器
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
        long_signal_df = signal_df.copy()
        long_signal_df[long_signal_df < 0.0] = 0.0
        long_weight_df = (long_signal_df.T / np.abs(long_signal_df).sum(axis=1)).T / 2
        short_signal_df = signal_df.copy()
        short_signal_df[long_signal_df > 0.0] = 0.0
        short_weight_df = (short_signal_df.T / np.abs(short_signal_df).sum(axis=1)).T / 2
        weight_df = long_weight_df + short_weight_df
        self.weight_df = weight_df
        return weight_df