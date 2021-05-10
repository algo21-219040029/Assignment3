import numpy as np
import pandas as pd
from pandas import DataFrame
from signals.CompositeSignal.base import BaseCompositeSignal

class CompositeSignal2(BaseCompositeSignal):
    """
    复合信号生成器2

    1.先计算quantile分位数和1-quantile分位数(quantile必须大于0.5小于1.0)

    2.
    if rank == 'descending':
        大于0的组中取大于quantile分位数的品种做多, 小于0的组小于1-quantile分位数的品种做空

    if rank == 'ascending':
        大于0的组中取大于quantile分位数的品种做空, 小于0的组小于1-quantile分位数的品种做多

    Attributes
    __________
    quantile: float, default 0.5
                分位数, 0.5 < quantile <1

    rank: str, default descending
          排序方式, descending表示因子值大的做多, ascending表示因子值小的做多

    """
    def __init__(self, quantile: float = 0.5, rank: str = 'descending') -> None:
        """
        生成信号, 1表示做多, -1表示做空,
        Returns
        -------
        signal_df: DataFrame
                    信号DataFrame, index为交易时间, columns为品种代码, data为信号值
        """
        super().__init__(quantile=quantile, rank=rank)
        if not (quantile > 0.5 and quantile < 1.0):
            raise ValueError("quantile must be between 0.5 and 1.0")

    def transform(self) -> DataFrame:
        """
        生成信号, 1表示做多, -1表示做空,
        Returns
        -------
        signal_df: DataFrame
                    信号DataFrame, index为交易时间, columns为品种代码, data为信号值
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

        params = self.get_params()
        rank = params['rank']
        quantile = params['quantile']

        if rank == 'ascending':
            factor_data = - factor_data

        def apply_quantile(series):
            quantile1 = series.quantile(q=quantile, interpolation='midpoint')
            quantile2 = series.quantile(q=1 - quantile, interpolation='midpoint')
            signal_series = pd.Series(data=0.0, index=series.index)
            signal_series[(series > 0.0) & (series >= quantile1)] = 1.0
            signal_series[(series < 0.0) & (series <= quantile2)] = -1.0
            return signal_series

        signal_df = factor_data.apply(func=apply_quantile, axis=1)
        self.signal_df = signal_df
        return signal_df