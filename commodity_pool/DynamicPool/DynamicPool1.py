import pandas as pd
from pandas import DataFrame
from commodity_pool.base import BaseCommodityPool

class DynamicPool1(BaseCommodityPool):

    def __init__(self, LimitedDays: int = 60, window: int = 60) -> None:
        super().__init__(LimitedDays=LimitedDays, window=window)

    def compute_commodity_pool(self) -> DataFrame:
        if not isinstance(self.listed_date_df, DataFrame):
            listed_date_df = self.get_limited_date(self.LimitedDays)
        else:
            listed_date_df = self.listed_date_df

        if not isinstance(self.daily_volume, DataFrame):
            daily_volume = self.get_volume_per_symbol(self.window)
        else:
            daily_volume = self.daily_volume

        daily_volume = daily_volume.reset_index()
        listed_date_df = listed_date_df.reset_index()
        daily_volume_plus_limited_date = pd.merge(daily_volume,
                                                  listed_date_df,
                                                  on='underlying_symbol',
                                                  how='left')
        daily_volume_plus_limited_date['in_pool'] = (daily_volume_plus_limited_date['volume'] >= 15000) & \
                                                    (daily_volume_plus_limited_date['datetime'] >=
                                                     daily_volume_plus_limited_date['limited_date'])
        commodity_pool_value = daily_volume_plus_limited_date.set_index(['datetime', 'underlying_symbol'])['in_pool'].unstack(
            level=-1).fillna(False)

        commodity_pool_value.loc[:, ['CY', 'WH', 'NR', 'IC', 'IH', 'IF', 'T', 'TF', 'TS']] = False
        commodity_pool_value.loc[:, ['CF', 'AU', 'OI']] = True
        commodity_pool_value.loc['2017-03-09':, 'SN'] = True
        commodity_pool_value.loc[:'2016-01-09', ['ZC', 'V']] = False
        commodity_pool_value.loc[:'2017-03-28', ['SF', 'FU']] = False
        commodity_pool_value.loc[:'2016-09-05', ['PB']] = False
        commodity_pool_value.loc[:'2012-07-12', ['J']] = False
        commodity_pool_value.loc[:'2015-03-18', ['BU']] = False
        commodity_pool_value.loc[:'2020-03-13', ['SS']] = False
        commodity_pool_value.loc[:'2015-12-18', ['HC']] = False
        commodity_pool_value.loc['2015-03-18':, ['FB']] = False
        commodity_pool_value.loc[:'2013-12-09', ['MA']] = False

        self.commodity_pool_value = commodity_pool_value
        return commodity_pool_value