import pandas as pd
from typing import Dict, List
from pandas import DataFrame, Series

from backtesting.metrics import get_metrics
from backtesting.base import BaseBacktesting
from backtesting.backtest_utility import (execute_backtesting,
                                          rolling_backtest_result_analysis)

import matplotlib.pyplot as plt
plt.rcParams['font.sans-serif'] = ['KaiTi'] # 指定默认字体
plt.rcParams['axes.unicode_minus'] = False # 解决保存图像是负号'-'显示为方块的问题

class LongShortPeriodBacktesting(BaseBacktesting):
    """
    定期调仓、起始日平滑回测系统
    需要的数据:
        1.因子值: DataFrame, index为交易时间(datetime), columns为品种(underlying_symbol), data为因子值
        2.品种价格指数: DataFrame, index为交易时间(datetime), columns为品种(underlying_symbol), data为主力连续合约价格
    设置的参数:
        1.调仓周期period
        2.交易费用rate
        3.单利or复利interest
        4.用于计算收益的连续
    """

    def __init__(self,
                 rate: float = 0,
                 period: int = 1,
                 interest: str = 'simple',
                 contract: str = 'main',
                 price: str = 'close',
                 rebalance_num: int = 1,
                 start: str = None,
                 end: str = None,
                 **kwargs) -> None:

        """Constructor"""
        super().__init__(rate=rate,
                         period=period,
                         interest=interest,
                         contract=contract,
                         price=price,
                         rebalance_num=rebalance_num,
                         start=start,
                         end=end,
                         **kwargs)

    def run_backtesting(self, tqdm_flag: bool = True) -> None:
        """
        运行回测

        Parameters
        ----------
        tqdm_flag: bool, default True

        Returns
        -------
        None
        """

        # 预先检查
        if not isinstance(self.weights, DataFrame):
            try:
                self.prepare_weights()
            except:
                raise ValueError("init weights first!")

        params = self.get_params()
        rate = params['rate']
        period = params['period']
        interest = params['interest']
        contract = params['contract']
        price = params['price']
        rebalance_num = params['rebalance_num']
        start = params['start']
        end = params['end']

        # 初始资金
        init_total_value = 100000000
        self.init_total_value = init_total_value

        # 获取权重
        weights = self.weights

        # 获取收盘价
        price_df = self.get_continuous_field(contract, price, rebalance_num, 'continuous_price')
        if price == 'open':
            price_df = price_df.shift(-1)

        price_df = price_df[start: end]
        weights = weights[start: end]

        # 品种行业对应表
        industry_symbol_map = self.get_industry(group='actual_industry', name='actual_five_industry')
        self.industry_symbol_map = industry_symbol_map

        symbol_industry_map: Dict[str, str] = {}
        for industry in industry_symbol_map:
            for symbol in industry_symbol_map[industry]:
                symbol_industry_map[symbol] = industry
        symbol_industry_map: Series = pd.Series(symbol_industry_map)
        symbol_industry_map.index.names = ['underlying_symbol']
        symbol_industry_map: DataFrame = symbol_industry_map.to_frame("industry_name").reset_index()

        # 校正权重和收盘价
        common_index = weights.index.intersection(price_df.index)
        weights = weights.reindex_like(price_df)
        price_df = price_df.loc[common_index]
        weights = weights.loc[common_index]

        if start == None:
            start = price_df.index[0]

        if end == None:
            end = price_df.index[-1]

        self.set_params(start=start, end=end)

        price_df = price_df[start: end]
        weights = weights[start: end]

        rolling_metrics_result = {}
        rolling_long_metrics_result = {}
        rolling_short_metrics_result = {}

        rolling_cumsum_profit_series_dict = {}
        rolling_long_cumsum_profit_series_dict = {}
        rolling_short_cumsum_profit_series_dict = {}

        rolling_cumsum_profit_df_dict = {}
        rolling_long_cumsum_profit_df_dict = {}
        rolling_short_cumsum_profit_df_dict = {}

        for shift in range(period):

            # 生成hold_datetime_list
            hold_datetime_list = []
            for i in range(len(price_df)):
                if (i - shift) % period == 0:
                    date = price_df.index[i]
                    hold_datetime_list.append(date)

            # 执行回测

            df_dict: Dict[str, DataFrame] = execute_backtesting(weights,
                                            price_df,
                                            init_total_value,
                                            hold_datetime_list,
                                            rate,
                                            interest,
                                            'cross_section',
                                            tqdm_flag)

            weight_df = df_dict['weight']
            long_weight_df = df_dict['long_weight']
            short_weight_df = df_dict['short_weight']
            hold_weight_df = df_dict['hold_weight']
            long_hold_weight_df = df_dict['long_hold_weight']
            short_hold_weight_df = df_dict['short_hold_weight']
            turnover_df = df_dict['turnover']
            long_turnover_df = df_dict['long_turnover']
            short_turnover_df = df_dict['short_turnover']
            hold_turnover_df = df_dict['hold_turnover']
            long_hold_turnover_df = df_dict['long_hold_turnover']
            short_hold_turnover_df = df_dict['short_hold_turnover']
            profit_df = df_dict['profit']
            long_profit_df = df_dict['long_profit']
            short_profit_df = df_dict['short_profit']
            hold_profit_df = df_dict['hold_profit']
            long_hold_profit_df = df_dict['long_hold_profit']
            short_hold_profit_df = df_dict['short_hold_profit']

            # 生成总体的指标, 包括总体的all, industry, symbol metrics
            metrics_result = get_metrics(weight_df,
                                         hold_weight_df,
                                         symbol_industry_map,
                                         turnover_df,
                                         hold_turnover_df,
                                         init_total_value,
                                         profit_df,
                                         hold_profit_df,
                                         interest)

            # 生成long leg的指标, 包括多头的all, industry, symbol metrics
            long_metrics_result = get_metrics(long_weight_df,
                                              long_hold_weight_df,
                                              symbol_industry_map,
                                              long_turnover_df,
                                              long_hold_turnover_df,
                                              init_total_value,
                                              long_profit_df,
                                              long_hold_profit_df,
                                              interest)

            # 生成short leg指标, 包括空头的all, industry, symbol metrics
            short_metrics_result = get_metrics(short_weight_df,
                                              short_hold_weight_df,
                                              symbol_industry_map,
                                              short_turnover_df,
                                              short_hold_turnover_df,
                                              init_total_value,
                                              short_profit_df,
                                              short_hold_profit_df,
                                              interest)

            # 记录每次起始日调仓:
            # 总体metrics, 多头metrics, 空头metrics
            # 总体每日收益, 多头每日收益, 空头每日收益
            # 总体累积收益, 多头累积收益, 空头累积收益
            rolling_metrics_result[shift] = metrics_result
            rolling_long_metrics_result[shift] = long_metrics_result
            rolling_short_metrics_result[shift] = short_metrics_result

            rolling_cumsum_profit_series_dict[shift] = profit_df.sum(axis=1).fillna(0.0).cumsum()
            rolling_long_cumsum_profit_series_dict[shift] = long_profit_df.sum(axis=1).fillna(0.0).cumsum()
            rolling_short_cumsum_profit_series_dict[shift] = short_profit_df.sum(axis=1).fillna(0.0).cumsum()

            rolling_cumsum_profit_df_dict[shift] = profit_df.fillna(0.0).cumsum()
            rolling_long_cumsum_profit_df_dict[shift] = long_profit_df.fillna(0.0).cumsum()
            rolling_short_cumsum_profit_df_dict[shift] = short_profit_df.fillna(0.0).cumsum()

        # 得到平均总体metrics, 平均多头metrics, 空头metrics,
        metrics_result = rolling_backtest_result_analysis(rolling_metrics_result)
        long_metrics_result = rolling_backtest_result_analysis(rolling_long_metrics_result)
        short_metrics_result = rolling_backtest_result_analysis(rolling_short_metrics_result)

        self.backtest_result['metrics'] = metrics_result
        self.backtest_result['long_metrics'] = long_metrics_result
        self.backtest_result['short_metrics'] = short_metrics_result

        rolled_cumsum_profit_series: Series = pd.Series()
        rolled_long_cumsum_profit_series: Series = pd.Series()
        rolled_short_cumsum_profit_series: Series = pd.Series()
        rolled_cumsum_profit_df: DataFrame = pd.DataFrame()
        rolled_long_cumsum_profit_df: DataFrame = pd.DataFrame()
        rolled_short_cumsum_profit_df: DataFrame = pd.DataFrame()

        for shift in range(period):
            if shift == 0:
                rolled_cumsum_profit_series = rolling_cumsum_profit_series_dict[shift]
                rolled_long_cumsum_profit_series = rolling_long_cumsum_profit_series_dict[shift]
                rolled_short_cumsum_profit_series = rolling_short_cumsum_profit_series_dict[shift]
                rolled_cumsum_profit_df = rolling_cumsum_profit_df_dict[shift]
                rolled_long_cumsum_profit_df = rolling_long_cumsum_profit_df_dict[shift]
                rolled_short_cumsum_profit_df = rolling_short_cumsum_profit_df_dict[shift]
            elif shift > 0:
                rolled_cumsum_profit_series += rolling_cumsum_profit_series_dict[shift]
                rolled_long_cumsum_profit_series += rolling_long_cumsum_profit_series_dict[shift]
                rolled_short_cumsum_profit_series += rolling_short_cumsum_profit_series_dict[shift]
                rolled_cumsum_profit_df += rolling_cumsum_profit_df_dict[shift]
                rolled_long_cumsum_profit_df += rolling_long_cumsum_profit_df_dict[shift]
                rolled_short_cumsum_profit_df += rolling_short_cumsum_profit_df_dict[shift]

        rolled_cumsum_profit_series = rolled_cumsum_profit_series / len(rolling_cumsum_profit_series_dict)
        rolled_long_cumsum_profit_series = rolled_long_cumsum_profit_series / len(rolling_long_cumsum_profit_series_dict)
        rolled_short_cumsum_profit_series = rolled_short_cumsum_profit_series / len(rolling_short_cumsum_profit_series_dict)
        rolled_cumsum_profit_df = rolled_cumsum_profit_df / len(rolling_cumsum_profit_df_dict)
        rolled_long_cumsum_profit_df = rolled_long_cumsum_profit_df / len(rolling_long_cumsum_profit_df_dict)
        rolled_short_cumsum_profit_df = rolled_short_cumsum_profit_df / len(rolling_short_cumsum_profit_df_dict)

        ## 构建行业资金曲线
        # 初始化变量
        industry_rolled_cumsum_profit_series: Dict[str, Series] = {}
        industry_rolled_long_cumsum_profit_series: Dict[str, Series] = {}
        industry_rolled_short_cumsum_profit_series: Dict[str, Series] = {}
        industry_rolled_cumsum_profit_df: Dict[str, DataFrame] = {}
        industry_rolled_long_cumsum_profit_df: Dict[str, DataFrame] = {}
        industry_rolled_short_cumsum_profit_df: Dict[str, DataFrame] = {}

        # 行业列表
        industry_list = list(self.industry_symbol_map.keys())
        industry_symbol_map = self.industry_symbol_map

        for industry in industry_list:
            symbol_list = industry_symbol_map[industry]
            symbol_list = list(set(symbol_list).intersection(set(weights.columns.tolist())))
            industry_rolled_cumsum_profit_series[industry] = rolled_cumsum_profit_df[symbol_list].sum(axis=1)
            industry_rolled_long_cumsum_profit_series[industry] = rolled_long_cumsum_profit_df[symbol_list].sum(axis=1)
            industry_rolled_short_cumsum_profit_series[industry] = rolled_short_cumsum_profit_df[symbol_list].sum(axis=1)
            industry_rolled_cumsum_profit_df[industry] = rolled_cumsum_profit_df[symbol_list]
            industry_rolled_long_cumsum_profit_df[industry] = rolled_long_cumsum_profit_df[symbol_list]
            industry_rolled_short_cumsum_profit_df[industry] = rolled_short_cumsum_profit_df[symbol_list]

        # 保存曲线数据

        self.backtest_curve['all']['cumsum_profit'] = rolled_cumsum_profit_series
        self.backtest_curve['all']['long_cumsum_profit'] = rolled_long_cumsum_profit_series
        self.backtest_curve['all']['short_cumsum_profit'] = rolled_short_cumsum_profit_series

        self.backtest_curve['industry']['cumsum_profit'] = pd.DataFrame(industry_rolled_cumsum_profit_series)
        self.backtest_curve['industry']['long_cumsum_profit'] = pd.DataFrame(industry_rolled_long_cumsum_profit_series)
        self.backtest_curve['industry']['short_cumsum_profit'] = pd.DataFrame(industry_rolled_short_cumsum_profit_series)

        self.backtest_curve['symbol']['cumsum_profit'] = industry_rolled_cumsum_profit_df
        self.backtest_curve['symbol']['long_cumsum_profit'] = industry_rolled_long_cumsum_profit_df
        self.backtest_curve['symbol']['short_cumsum_profit'] = industry_rolled_short_cumsum_profit_df














