from pathlib import Path

from data_manager import (CommodityPoolManager,
                          FactorDataManager)
from factor.factor_test import (get_q_later_quantile, FactorTest)


commodity_pool_manager = CommodityPoolManager()
pool = commodity_pool_manager.get_commodity_pool("dynamic_pool3")

factor_data_manager = FactorDataManager()
factor = factor_data_manager.get_factor("slope_factor1")
unstack_factor = factor.set_index(['datetime', 'underlying_symbol'])['factor'].unstack(level=-1)

rolling20_unstack_factor = unstack_factor.rolling(window=20, min_periods=0).mean()
rolling60_unstack_factor = unstack_factor.rolling(window=60, min_periods=0).mean()
rolling120_unstack_factor = unstack_factor.rolling(window=120, min_periods=0).mean()
rolling180_unstack_factor = unstack_factor.rolling(window=180, min_periods=0).mean()

factor_list = [rolling20_unstack_factor, rolling60_unstack_factor,
               rolling120_unstack_factor, rolling180_unstack_factor]
name_list = ['rolling20_slope_factor1', 'rolling_60_slope_factor1',
             'rolling120_slope_factor1', 'rolling180_slope_factor1']

file_path = Path("D:/LFProjects/CarryProject/结果输出/因子")
for i in range(2, len(factor_list)):
    factor_name = name_list[i]
    factor_data = factor_list[i]
    self = FactorTest(file_path.joinpath(factor_name))
    self.set_commodity_pool(pool)
    self.set_factor(factor_name, factor_data)
    self.set_factor_quantile(get_q_later_quantile, quantile=0.5)
    self.run_all()
