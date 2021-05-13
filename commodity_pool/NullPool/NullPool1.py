import pandas as pd
from pandas import DataFrame
from commodity_pool.base import BaseCommodityPool

class NullPool1(BaseCommodityPool):
    """
    不做筛选的商品池，仅筛选股指期货和国债期货
    """
    def __init__(self) -> None:
        """Constructor"""
        super().__init__()

    def compute_commodity_pool(self) -> DataFrame:
        """
        计算商品池

        Returns
        -------
        commodity_pool_value: DataFrame
                            商品池DataFrame
        """
        daily_data: DataFrame = self.daily_data_manager.get_daily_data()
        daily_volume: DataFrame = daily_data.groupby(['datetime', 'underlying_symbol'], as_index=True)['volume'].sum().unstack(level=-1)
        commodity_pool_value: DataFrame = pd.DataFrame(data=True, index=daily_volume.index, columns=daily_volume.columns)
        commodity_pool_value[daily_volume.isnull()] = False
        commodity_pool_value.loc[:, ['IH', 'IF', 'IC', 'T', 'TF', 'TS', 'FB', 'BB']] = False

        self.commodity_pool_value = commodity_pool_value
        return commodity_pool_value