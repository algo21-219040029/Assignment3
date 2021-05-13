import numpy as np
from pandas import DataFrame
from commodity_pool.base import BaseCommodityPool


class DynamicPool2(BaseCommodityPool):

    def __init__(self, q: float = 0.25, window: int = 126) -> None:
        super().__init__(q=q, window=window)

    def compute_commodity_pool(self) -> DataFrame:
        daily_data = self.daily_data_manager.get_daily_data()
        daily_volume = daily_data.groupby(['datetime', 'underlying_symbol'], as_index=False)['volume'].sum(). \
            set_index(['datetime', 'underlying_symbol'])['volume'].unstack(level=-1)

        daily_rolling_volume = daily_volume.rolling(window=self.window, min_periods=0).mean()
        daily_rolling_volume.loc[:, 'IF'] = np.nan
        daily_rolling_volume.loc[:, 'IH'] = np.nan
        daily_rolling_volume.loc[:, 'IC'] = np.nan
        daily_rolling_volume.loc[:, 'T'] = np.nan
        daily_rolling_volume.loc[:, 'TF'] = np.nan
        daily_rolling_volume.loc[:, 'TS'] = np.nan
        daily_rolling_volume.loc[:, 'SC'] = np.nan
        daily_rolling_volume.loc[:, 'NR'] = np.nan
        daily_rolling_volume.loc[:, 'LU'] = np.nan
        daily_rolling_volume.loc[:, 'BC'] = np.nan
        # daily_rolling_volume.loc[:, 'TA'] = np.nan
        daily_quantile = daily_rolling_volume.quantile(q=self.q, axis=1)
        commodity_pool_value = (daily_rolling_volume.T > daily_quantile).T
        commodity_pool_value.fillna(False, inplace=True)

        self.commodity_pool_value = commodity_pool_value
        return commodity_pool_value