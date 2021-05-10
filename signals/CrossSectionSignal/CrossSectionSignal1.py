import numpy as np
import pandas as pd
from pandas import DataFrame
from signals.CrossSectionSignal.base import BaseCrossSectionSignal

class CrossSectionSignal1(BaseCrossSectionSignal):
    """
    横截面信号生成器1

    因子值大于quantile(quantile>0.5)分位数的品种做多, 因子值小于1-quantile分位数的品种做空

    Attributes
    __________
    quantile: float, default 0.75
                分位数, 0.5 < quantile < 1.0

    rank: str, default descending
            排序方法, descending表示因子值大的品种做多, ascending表示因子值小的品种做多

    """
    def __init__(self, quantile: float = 0.75, rank: str = 'descending') -> None:
        """Constructor"""
        super().__init__(quantile=quantile, rank=rank)

    def transform(self) -> DataFrame:
        """
        生成信号, 1表示做多, -1表示做空,
        Returns
        -------
        signal_df: DataFrame
                    信号DataFrame, index为交易时间, columns为品种代码, data为信号值-
        """
        if not isinstance(self.factor_data, DataFrame):
            raise ValueError("Please specify the factor_data first!")
        else:
            factor_data = self.factor_data

        if not isinstance(self.commodity_pool, DataFrame):
            raise ValueError("Please specify the commodity pool first!")
        else:
            commodity_pool = self.commodity_pool

        factor_data[~commodity_pool] = np.nan

        params = self.get_params()
        rank = params['rank']
        quantile = params['quantile']

        if rank == 'ascending':
            factor_data = - factor_data

        def apply_quantile(series):
            quantile1 = series.quantile(q=quantile, interpolation='midpoint')
            quantile2 = series.quantile(q=1-quantile, interpolation='midpoint')
            signal_series = pd.Series(data=0.0, index=series.index)
            signal_series[series >= quantile1] = 1.0
            signal_series[series <= quantile2] = -1.0
            return signal_series

        signal_df = factor_data.apply(func=apply_quantile, axis=1)
        self.signal_df = signal_df
        return signal_df




