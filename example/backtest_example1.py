from signals import CarrySignal1
from data_manager import (CommodityPoolManager,
                          FactorDataManager)
from backtesting.monthly_backtesting import LongShortMonthlyBacktesting

commodity_pool_manager = CommodityPoolManager()
pool = commodity_pool_manager.get_commodity_pool("dynamic_pool3")

factor_data_manager = FactorDataManager()
factor = factor_data_manager.get_factor("rolling120_slope_factor1")

signal = CarrySignal1()
signal.set_commodity_pool(pool)
signal.set_factor_data(factor)
signal.set_quantile(0.5)
weight = signal.transform()

self = LongShortMonthlyBacktesting()
self.close_df = self.close_df['2010-01-04': '2018-09-21']
self.set_weight(weight)
self.run_backtesting(rate=0.0005)
self.output_backtest_result("D:/LFProjects/CarryProject/结果输出/因子/"
                            "rolling120_slope_factor1/"
                            "dynamic_pool3_q_later_risk_neutral_quantile_0.5_rate_0.0003_monthly_回测")
##
from signals import CarrySignal1
from data_manager import (CommodityPoolManager,
                          FactorDataManager)
from backtesting.period_backtesting import LongShortPeriodBacktesting

commodity_pool_manager = CommodityPoolManager()
pool = commodity_pool_manager.get_commodity_pool("dynamic_pool3")

factor_data_manager = FactorDataManager()
factor = factor_data_manager.get_factor("rolling20_slope_factor1")

signal = CarrySignal1()
signal.set_commodity_pool(pool)
signal.set_factor_data(factor)
signal.set_quantile(0.5)
weight = signal.transform()

self = LongShortPeriodBacktesting()
self.close_df = self.close_df['2010-01-04': '2018-09-21']
self.set_weight(weight)
self.run_backtesting(rate=0.0005, period=20)
self.output_backtest_result("D:/LFProjects/CarryProject/结果输出/因子/"
                            "rolling20_slope_factor1/"
                            "dynamic_pool3_q_later_risk_neutral_quantile_0.5_rate_0.0003_period20_回测")
##
from signals import CarrySignal1
from data_manager import (CommodityPoolManager,
                          FactorDataManager)
from backtesting.period_backtesting import LongShortPeriodBacktesting

commodity_pool_manager = CommodityPoolManager()
pool = commodity_pool_manager.get_commodity_pool("dynamic_pool3")

factor_data_manager = FactorDataManager()
factor = factor_data_manager.get_factor("rolling20_slope_factor1")

signal = CarrySignal1()
signal.set_commodity_pool(pool)
signal.set_factor_data(factor)
signal.set_quantile(0.5)
weight = signal.transform()

self = LongShortPeriodBacktesting()
self.close_df = self.close_df['2010-01-04': '2018-09-21']
self.set_weight(weight)
self.run_backtesting(rate=0.0005, period=60)
self.output_backtest_result("D:/LFProjects/CarryProject/结果输出/因子/"
                            "rolling20_slope_factor1/"
                            "dynamic_pool3_q_later_risk_neutral_quantile_0.5_rate_0.0003_period60_回测")
##
from signals import CarrySignal1
from data_manager import (CommodityPoolManager,
                          FactorDataManager)
from backtesting.period_backtesting import LongShortPeriodBacktesting

commodity_pool_manager = CommodityPoolManager()
pool = commodity_pool_manager.get_commodity_pool("dynamic_pool3")

factor_data_manager = FactorDataManager()
factor = factor_data_manager.get_factor("rolling60_slope_factor1")

signal = CarrySignal1()
signal.set_commodity_pool(pool)
signal.set_factor_data(factor)
signal.set_quantile(0.5)
weight = signal.transform()

self = LongShortPeriodBacktesting()
self.close_df = self.close_df['2010-01-04': '2018-09-21']
self.set_weight(weight)
self.run_backtesting(rate=0.0005, period=20)
self.output_backtest_result("D:/LFProjects/CarryProject/结果输出/因子/"
                            "rolling60_slope_factor1/"
                            "dynamic_pool3_q_later_risk_neutral_quantile_0.5_rate_0.0003_period20_回测")
##
from signals import CarrySignal1
from data_manager import (CommodityPoolManager,
                          FactorDataManager)
from backtesting.period_backtesting import LongShortPeriodBacktesting

commodity_pool_manager = CommodityPoolManager()
pool = commodity_pool_manager.get_commodity_pool("dynamic_pool3")

factor_data_manager = FactorDataManager()
factor = factor_data_manager.get_factor("rolling60_slope_factor1")

signal = CarrySignal1()
signal.set_commodity_pool(pool)
signal.set_factor_data(factor)
signal.set_quantile(0.5)
weight = signal.transform()

self = LongShortPeriodBacktesting()
self.close_df = self.close_df['2010-01-04': '2018-09-21']
self.set_weight(weight)
self.run_backtesting(rate=0.0005, period=60)
self.output_backtest_result("D:/LFProjects/CarryProject/结果输出/因子/"
                            "rolling60_slope_factor1/"
                            "dynamic_pool3_q_later_risk_neutral_quantile_0.5_rate_0.0003_period60_回测")
##
from signals import CarrySignal1
from data_manager import (CommodityPoolManager,
                          FactorDataManager)
from backtesting.period_backtesting import LongShortPeriodBacktesting

commodity_pool_manager = CommodityPoolManager()
pool = commodity_pool_manager.get_commodity_pool("dynamic_pool3")

factor_data_manager = FactorDataManager()
factor = factor_data_manager.get_factor("rolling120_slope_factor1")

signal = CarrySignal1()
signal.set_commodity_pool(pool)
signal.set_factor_data(factor)
signal.set_quantile(0.5)
weight = signal.transform()

self = LongShortPeriodBacktesting()
self.close_df = self.close_df['2010-01-04': '2018-09-21']
self.set_weight(weight)
self.run_backtesting(rate=0.0005, period=20)
self.output_backtest_result("D:/LFProjects/CarryProject/结果输出/因子/"
                            "rolling120_slope_factor1/"
                            "dynamic_pool3_q_later_risk_neutral_quantile_0.5_rate_0.0003_period20_回测")

##
from signals import CarrySignal1
from data_manager import (CommodityPoolManager,
                          FactorDataManager)
from backtesting.period_backtesting import LongShortPeriodBacktesting

commodity_pool_manager = CommodityPoolManager()
pool = commodity_pool_manager.get_commodity_pool("dynamic_pool3")

factor_data_manager = FactorDataManager()
factor = factor_data_manager.get_factor("rolling120_slope_factor1")

signal = CarrySignal1()
signal.set_commodity_pool(pool)
signal.set_factor_data(factor)
signal.set_quantile(0.5)
weight = signal.transform()

self = LongShortPeriodBacktesting()
self.close_df = self.close_df['2010-01-04': '2018-09-21']
self.set_weight(weight)
self.run_backtesting(rate=0.0005, period=60)
self.output_backtest_result("D:/LFProjects/CarryProject/结果输出/因子/"
                            "rolling120_slope_factor1/"
                            "dynamic_pool3_q_later_risk_neutral_quantile_0.5_rate_0.0003_period60_回测")
##
from signals import CarrySignal5
from data_manager import (CommodityPoolManager,
                          FactorDataManager)

from backtesting.period_backtesting import LongShortPeriodBacktesting

commodity_pool_manager = CommodityPoolManager()
pool = commodity_pool_manager.get_commodity_pool("dynamic_pool4")

factor_data_manager = FactorDataManager()
factor = factor_data_manager.get_factor("main_near_factor4")

signal = CarrySignal5()
signal.set_commodity_pool(pool)
signal.set_factor_data(factor)
signal.set_quantile(0.7)
weight = signal.transform()

self = LongShortPeriodBacktesting()
self.set_weight(weight)
self.close_df = self.close_df['2010-01-04': '2017-10-31']
self.run_backtesting(period=60, rate=0.0005)

##
from signals import CarrySignal1
from data_manager import (CommodityPoolManager,
                          FactorDataManager)

from backtesting.monthly_backtesting import LongShortMonthlyBacktesting

commodity_pool_manager = CommodityPoolManager()
pool = commodity_pool_manager.get_commodity_pool("dynamic_pool3")

factor_data_manager = FactorDataManager()
factor = factor_data_manager.get_factor("main_near_factor3")

signal = CarrySignal1()
signal.set_commodity_pool(pool)
signal.set_factor_data(factor)
signal.set_quantile(0.5)
weight = signal.transform()

self = LongShortMonthlyBacktesting()
self.set_weight(weight)
self.close_df = self.close_df['2010-01-04': '2018-09-21']
self.run_backtesting(rate=0.0003)
self.output_backtest_result("D:/LFProjects/CarryProject/结果输出/因子/main_near_factor3"
                            "/dynamic_pool3_q_later_risk_neutral_quantile_0.5_rate_0.0003_monthly_回测")
##
from signals import CarrySignal1
from data_manager import (CommodityPoolManager,
                          FactorDataManager)

from backtesting.period_backtesting import LongShortPeriodBacktesting

commodity_pool_manager = CommodityPoolManager()
pool = commodity_pool_manager.get_commodity_pool("dynamic_pool3")

factor_data_manager = FactorDataManager()
factor = factor_data_manager.get_factor("main_near_factor3")

signal = CarrySignal1()
signal.set_commodity_pool(pool)
signal.set_factor_data(factor)
signal.set_quantile(0.5)
weight = signal.transform()

self = LongShortPeriodBacktesting()
self.set_weight(weight)
self.close_df = self.close_df['2010-01-04': '2018-09-21']
self.run_backtesting(rate=0.0003, period=20)
