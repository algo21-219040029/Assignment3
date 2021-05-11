import pandas as pd
from collections import defaultdict
from typing import Tuple, Dict, Any
from pandas import DataFrame, Series

from backtesting.metrics import get_metrics
from backtesting.base import BaseBacktesting

from backtesting.backtest_utility import execute_backtesting

import matplotlib.pyplot as plt
plt.rcParams['font.sans-serif'] = ['KaiTi'] # 指定默认字体
plt.rcParams['axes.unicode_minus'] = False # 解决保存图像是负号'-'显示为方块的问题

class LongShortMonthlyBacktesting(BaseBacktesting):
    """
    多空组合，月末调仓回测系统
    """
    def __init__(self,
                 rate: float = 0,
                 period: str ='end',
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

    def run_backtesting(self, tqdm_flag: bool = True) -> Tuple[Dict]:
        """
        执行回测

        Parameters
        ----------


        Returns
        -------
        backtest_result: Tuple[Dict]
                        回测结果
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

        # 获取收盘价,如果扩展开盘价或收盘价，则需要进一步扩展
        price_df = self.get_continuous_field(contract, price, rebalance_num, 'continuous_price')
        if price == 'open':
            price_df = price_df.shift(-1)

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
        common_columns = weights.columns.intersection(price_df.columns)
        weights = weights.loc[common_index][common_columns]
        price_df = price_df.loc[common_index][common_columns]

        if start == None:
            start = price_df.index[0]

        if end == None:
            end = price_df.index[-1]

        self.set_params(start=start, end=end)

        price_df = price_df[start: end]
        weights = weights[start: end]

        dts = price_df.index.to_series(index=range(len(price_df))).to_frame('datetime')
        dts['year'] = dts['datetime'].dt.year
        dts['month'] = dts['datetime'].dt.month

        # 确定调仓日期，月初调仓或者月末调仓
        hold_datetime_list = []
        if period == 'end':
            hold_datetime_list = \
            dts.sort_values(by=['year', 'month'], ascending=True).groupby(['year', 'month'], as_index=False)[
                'datetime'].nth(-1).tolist()
        elif period == 'start':
            hold_datetime_list = \
                dts.sort_values(by=['year', 'month'], ascending=True).groupby(['year', 'month'], as_index=False)[
                    'datetime'].nth(0).tolist()

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
        profit_df: DataFrame = df_dict['profit']
        long_profit_df: DataFrame = df_dict['long_profit']
        short_profit_df: DataFrame = df_dict['short_profit']
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

        self.backtest_result = {}
        self.backtest_result['metrics'] = metrics_result
        self.backtest_result['long_metrics'] = long_metrics_result
        self.backtest_result['short_metrics'] = short_metrics_result

        cumsum_profit_df = profit_df.cumsum()
        long_cumsum_profit_df = long_profit_df.cumsum()
        short_cumsum_profit_df = short_profit_df.cumsum()

        profit_series = profit_df.sum(axis=1)
        long_profit_series = long_profit_df.sum(axis=1)
        short_profit_series = short_profit_df.sum(axis=1)

        cumsum_profit_series = profit_df.sum(axis=1).cumsum()
        long_cumsum_profit_series = long_profit_df.sum(axis=1).cumsum()
        short_cumsum_profit_series = short_profit_df.sum(axis=1).cumsum()

        ## 构建行业资金曲线
        # 初始化变量
        industry_profit_series: Dict[str, Series] = {}
        industry_long_profit_series: Dict[str, Series] = {}
        industry_short_profit_series: Dict[str, Series] = {}
        industry_cumsum_profit_series: Dict[str, Series] = {}
        industry_long_cumsum_profit_series: Dict[str, Series] = {}
        industry_short_cumsum_profit_series: Dict[str, Series] = {}
        industry_profit_df: Dict[str, DataFrame] = {}
        industry_long_profit_df: Dict[str, DataFrame] = {}
        industry_short_profit_df: Dict[str, DataFrame] = {}
        industry_cumsum_profit_df: Dict[str, DataFrame] = {}
        industry_long_cumsum_profit_df: Dict[str, DataFrame] = {}
        industry_short_cumsum_profit_df: Dict[str, DataFrame] = {}

        industry_list = list(self.industry_symbol_map.keys())
        industry_symbol_map = self.industry_symbol_map

        for industry in industry_list:
            symbol_list = industry_symbol_map[industry]
            symbol_list = list(set(symbol_list).intersection(set(weights.columns.tolist())))
            industry_profit_series[industry] = profit_df[symbol_list].sum(axis=1)
            industry_long_profit_series[industry] = long_profit_df[symbol_list].sum(axis=1)
            industry_short_profit_series[industry] = short_profit_df[symbol_list].sum(axis=1)
            industry_cumsum_profit_series[industry] = cumsum_profit_df[symbol_list].sum(axis=1)
            industry_long_cumsum_profit_series[industry] = long_cumsum_profit_df[symbol_list].sum(axis=1)
            industry_short_cumsum_profit_series[industry] = short_cumsum_profit_df[symbol_list].sum(axis=1)
            industry_profit_df[industry] = profit_df[symbol_list]
            industry_long_profit_df[industry] = profit_df[symbol_list]
            industry_short_profit_df[industry] = short_profit_df[symbol_list]
            industry_cumsum_profit_df[industry] = cumsum_profit_df[symbol_list]
            industry_long_cumsum_profit_df[industry] = long_cumsum_profit_df[symbol_list]
            industry_short_cumsum_profit_df[industry] = short_cumsum_profit_df[symbol_list]

        # 保存曲线数据
        self.backtest_curve: Dict[str, Dict[str, Any]] = defaultdict(dict)

        self.backtest_curve['all']['cumsum_profit'] = cumsum_profit_series
        self.backtest_curve['all']['long_cumsum_profit'] = long_cumsum_profit_series
        self.backtest_curve['all']['short_cumsum_profit'] = short_cumsum_profit_series

        self.backtest_curve['industry']['cumsum_profit'] = industry_cumsum_profit_series
        self.backtest_curve['industry']['long_cumsum_profit'] = industry_long_cumsum_profit_series
        self.backtest_curve['industry']['short_cumsum_profit'] = industry_short_cumsum_profit_series

        self.backtest_curve['symbol']['cumsum_profit'] = industry_cumsum_profit_df
        self.backtest_curve['symbol']['long_cumsum_profit'] = industry_long_cumsum_profit_df
        self.backtest_curve['symbol']['short_cumsum_profit'] = industry_short_cumsum_profit_df

