import pandas as pd
from pandas import DataFrame
from commodity_pool.base import BaseCommodityPool

class FixedPool1(BaseCommodityPool):

    def __init__(self, LimitedDays: int = 60) -> None:
        super().__init__(LimitedDays=LimitedDays)

    def compute_commodity_pool(self) -> DataFrame:
        fixed_pool = ['A', 'AG', 'AL', 'AU', 'C', 'CF', 'CS', 'CU', 'FG', 'I', 'J', 'JD', 'JM', 'L', 'M', 'MA', 'NI',
                      'OI', 'P', 'PP', 'RB', 'RM', 'RU', 'SR', 'TA', 'V', 'Y', 'ZC', 'ZN']

        if not isinstance(self.listed_date_df, DataFrame):
            listed_date_df = self.get_limited_date(self.LimitedDays)
        else:
            listed_date_df = self.listed_date_df

        if not isinstance(self.daily_volume, DataFrame):
            daily_volume = self.get_volume_per_symbol(60)
        else:
            daily_volume = self.daily_volume

        listed_date_df = listed_date_df.loc[fixed_pool].reset_index()
        listed_date_df = pd.merge(left=daily_volume[['datetime', 'underlying_symbol']],
                                        right=listed_date_df, on='underlying_symbol')

        listed_date_df['fixed_pool'] = listed_date_df['datetime'] >= listed_date_df['listed_date']
        commodity_pool_value = listed_date_df.set_index(['datetime', 'underlying_symbol'])['fixed_pool'].\
            unstack(level=-1).fillna(False)

        self.commodity_pool_value = commodity_pool_value
        return commodity_pool_value


if __name__ == "__main__":
    self = FixedPool1(limited_days=60)

