from pandas import DataFrame
from weight.base import BaseWeight

class TimeSeriesEqualWeight1(BaseWeight):
    """
    时间序列独立生成器
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

        # 原先检查
        if not isinstance(self.signal_df, DataFrame):
            raise ValueError("Please specify signal_df first!")

        weight_df = self.signal_df.copy()
        self.weight_df = weight_df
        return weight_df