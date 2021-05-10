import numpy as np
import pandas as pd
from pandas import DataFrame

from signals.TimeSeriesSignal import BaseTimeSeriesSignal

class TimeSeriesSignal1(BaseTimeSeriesSignal):
    """
    时间序列信号生成器1

    if rank == 'descending':
        做多因子值大于0的品种，做空因子值小于0的品种

    elif rank == 'ascending':
        做多因子值小于0的品种，做空因子值大于0的品种

    Attributes
    __________
    rank: str, default descending
            排序方法, descending表示因子值大的品种做多, ascending表示因子值小的品种做多

    """

    def __init__(self, long_threshold: float = 0.0, short_threshold: float = 0.0, rank: str = 'descending') -> None:
        """Constructor"""
        super().__init__(long_threshold=long_threshold, short_threshold=short_threshold, rank=rank)

    def transform(self) -> DataFrame:
        """
        生成信号, 1表示做多, -1表示做空,
        Returns
        -------
        signal_df: DataFrame
                    信号DataFrame, index为交易时间, columns为品种代码, data为信号值-
        """
        # 预先检查
        if not isinstance(self.factor_data, DataFrame):
            raise ValueError("Please specify the factor_data first!")
        else:
            factor_data = self.factor_data

        if not isinstance(self.commodity_pool, DataFrame):
            raise ValueError("Please specify the commodity pool first!")
        else:
            commodity_pool = self.commodity_pool

        factor_data[~commodity_pool] = np.nan

        rank = self.get_params()['rank']
        long_threshold = self.get_params()['long_threshold']
        short_threshold = self.get_params()['short_threshold']

        signal_df = pd.DataFrame(data=0.0, index=factor_data.index, columns=factor_data.columns)
        if rank == 'descending':
            signal_df[factor_data >= long_threshold] = 1.0
            signal_df[factor_data < short_threshold] = -1.0
        elif rank == 'ascending':
            signal_df[factor_data >= short_threshold] = -1.0
            signal_df[factor_data < long_threshold] = 1.0
        self.signal_df = signal_df
        return signal_df