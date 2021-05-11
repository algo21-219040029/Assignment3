import numpy as np
import pandas as pd
from tqdm import tqdm
from pandas import (Series,
                    DataFrame)
from datetime import datetime
from typing import List, Dict, Any

def get_hold_profit_df(pos_df: DataFrame, price_df: DataFrame, hold_datetime_list: List[datetime]) -> DataFrame:
    """
    根据每日持仓、每日收盘价和换仓日期列表获取每次持仓的收益（时间戳为持仓开始日期）

    Parameters
    ----------
    pos_df: DataFrame
            每日持仓，index为交易日期，columns为品种代码，data为每日持仓（对应开盘调整后的持仓）

    price_df: DataFrame
            每日价格，index为交易日期，columns为品种代码，data为价格

    hold_datetime_list: List[datetime]
                        换仓日期列表

    Returns
    -------
    hold_profit_df: DataFrame
                    每次持仓数据
    """
    hold_profit_list = []
    for i in range(len(hold_datetime_list)):
        if i != len(hold_datetime_list)-1:
            start_date = hold_datetime_list[i]
            end_date = hold_datetime_list[i+1]
            pos = pos_df.loc[start_date]
            start_price = price_df.loc[start_date]
            end_price = price_df.loc[end_date]
            hold_profit = pos * (end_price - start_price)
            hold_profit_list.append(hold_profit)
        else:
            start_date = hold_datetime_list[i]
            pos = pos_df.loc[start_date]
            start_price = price_df.loc[start_date]
            end_price = price_df.iloc[-1]
            hold_profit = pos * (end_price - start_price)
            hold_profit_list.append(hold_profit)
    hold_profit_df = pd.concat(hold_profit_list, axis=1).T
    hold_profit_df.index = hold_datetime_list
    return hold_profit_df

def execute_backtesting(weight_df: DataFrame,
                        price_df: DataFrame,
                        init_total_value: int,
                        hold_datetime_list: List[datetime],
                        rate: float,
                        interest: str = 'simple',
                        type: str = 'cross_section',
                        tqdm_flag: bool = True)\
                        -> Dict[str, DataFrame]:
    """
    回测执行

    通过输入每日持仓权重、每日收盘价、初始资金量、换仓日期列表、交易费用执行回测

    Parameters
    ----------
    weight_df: DataFrame
                每日持仓权重

    price_df: DataFrame
                每日价格

    init_total_value: int
                    初始持仓金额
    hold_datetime_list: List[datetime]
                    换仓日期列表
    rate: float
            交易费用

    interest:  str, default simple
            单利还是复利执行, simple or compound

    type: str, default cross_section
            横截面还是时间序列

    tqdm_flag: bool, default True
            是否使用tqdm

    Returns
    -------
    result: Dict[str, DataFrame]
            回测结果信息

    Notes
    _____
    对于不是定期调仓、月末调仓、月初调仓等不规则调仓，可以自行构造hold_datetime_list执行回测

    See Also
    ________
    backtesting.monthly_backtesting.LongShortMonthlyBacktesting
    backtesting.period_backtesting.LongShortPeriodBacktesting
    """

    total_value_list = []  # 每日总持仓价值（注意持仓价值不等于各品种持仓价值绝对值相加）
    total_profit_list = [] # 每日总收益
    value_list = []  # 每日持仓价值（换仓日为换仓之后的）
    pos_list = []  # 每日持仓数量（换仓日为换仓之后的）
    turnover_list = []  # 每日成交额数量（除了换仓日都是0）
    profit_list = []  # 每日收益（换仓日为换仓之前的仓位的收益）
    long_profit_list = []
    short_profit_list = []
    group_list = []
    weight_list = []

    flag = 0  # flag = 0 表示从未开仓，flag = 1表示之前开仓过
    group = 0
    pos: Series = pd.Series()
    value: Series = pd.Series()
    weight: Series = pd.Series()
    total_value: float = 0.0
    new_value: Series = pd.Series()

    if tqdm_flag:
        iters = tqdm(range(len(weight_df)))
    else:
        iters = range(len(weight_df))
    for i in iters:
        date = weight_df.index[i]
        # 第一天
        if i == 0:
            weight = weight_df.iloc[i]
            price = price_df.iloc[i]
            value = weight * init_total_value
            pos = value / price
            profit = pd.Series(data=0.0, index=price.index)
            profit[weight.isnull()] = np.nan
            long_profit = profit.copy()
            short_profit = profit.copy()
            long_profit[pos < 0.0] = 0.0
            short_profit[pos > 0.0] = 0.0
            total_profit = profit.sum()
            total_value = init_total_value + total_profit
            turnover = value.copy()


            total_profit_list.append(total_profit)
            total_value_list.append(total_value)
            pos_list.append(pos)
            value_list.append(value)
            turnover_list.append(turnover)
            profit_list.append(profit)
            long_profit_list.append(long_profit)
            short_profit_list.append(short_profit)
            weight_list.append(weight)
            group_list.append(0)

        # 不是第一天
        else:
            if date in hold_datetime_list:
                # 换仓前对前一组收益进行统计
                price = price_df.iloc[i]
                last_price = price_df.iloc[i-1]
                profit = pos * (price - last_price)
                long_profit = profit.copy()
                short_profit = profit.copy()
                long_profit[pos < 0.0] = 0.0
                short_profit[pos > 0.0] = 0.0
                value = value + np.sign(value) * profit
                total_profit = profit.sum()
                total_value = total_value + total_profit
                profit_list.append(profit)
                long_profit_list.append(long_profit)
                short_profit_list.append(short_profit)
                total_profit_list.append(total_profit)
                total_value_list.append(total_value)

                # 更新仓位
                weight = weight_df.iloc[i]
                price = price_df.iloc[i]
                if type == 'time_series':
                    if interest == 'simple':
                        new_value = weight * init_total_value
                    elif interest == 'compound':
                        new_value = weight * np.abs(value)
                elif type == 'cross_section':
                    if interest == 'simple':
                        new_value = weight * init_total_value
                    elif interest == 'compound':
                        new_value = weight * total_value
                else:
                    raise ValueError("please specify correct type or interest")
                turnover = new_value - value
                value = new_value.copy()
                pos = value / price
                pos_list.append(pos)
                group_list.append(group)
                value_list.append(value)
                weight_list.append(weight)
                turnover_list.append(turnover)

                group += 1
                group_list.append(group)

            # 如果今天不是换仓日
            else:
                price = price_df.iloc[i]
                last_price = price_df.iloc[i-1]
                profit = pos * (price - last_price)
                long_profit = profit.copy()
                short_profit = profit.copy()
                long_profit[pos < 0.0] = 0.0
                short_profit[pos > 0.0] = 0.0
                value = value + np.sign(value) * profit
                total_profit = profit.sum()
                total_value = total_value + total_profit
                total_profit_list.append(total_profit)
                total_value_list.append(total_value)
                turnover = pd.Series(data=0.0, index=price.index)
                pos_list.append(pos)
                profit_list.append(profit)
                long_profit_list.append(long_profit)
                short_profit_list.append(short_profit)
                group_list.append(group)
                value_list.append(value)
                weight_list.append(weight)
                turnover_list.append(turnover)

    pos_df = pd.concat(pos_list, axis=1).T
    pos_df.index = price_df.index
    pos_df.columns = price_df.columns
    pos_df.index.names = ['datetime']
    pos_df.columns.names = ['underlying_symbol']

    long_pos_df = pos_df.copy()
    long_pos_df[pos_df < 0.0] = 0.0

    short_pos_df = pos_df.copy()
    short_pos_df[pos_df > 0.0] = 0.0

    turnover_df = pd.concat(turnover_list, axis=1).T
    turnover_df.index = price_df.index
    turnover_df.columns = price_df.columns
    turnover_df.index.names = ['datetime']
    turnover_df.columns.names = ['underlying_symbol']
    turnover_df = np.abs(turnover_df)

    long_turnover_df = turnover_df.copy()
    long_turnover_df[pos_df < 0.0] = 0.0

    short_turnover_df = turnover_df.copy()
    short_turnover_df[pos_df > 0.0] = 0.0

    profit_df = pd.concat(profit_list, axis=1).T
    profit_df.index = price_df.index
    profit_df.columns = price_df.columns
    profit_df.index.names = ['datetime']
    profit_df.columns.names = ['underlying_symbol']
    profit_df = profit_df - turnover_df * rate

    long_profit_df = pd.concat(long_profit_list, axis=1).T
    long_profit_df.index = price_df.index
    long_profit_df.columns = price_df.columns
    long_profit_df.index.names = ['datetime']
    long_profit_df.columns.names = ['underlying_symbol']
    long_profit_df = long_profit_df - turnover_df * rate

    short_profit_df = pd.concat(short_profit_list, axis=1).T
    short_profit_df.index = price_df.index
    short_profit_df.columns = price_df.columns
    short_profit_df.index.names = ['datetime']
    short_profit_df.columns.names = ['underlying_symbol']
    short_profit_df = short_profit_df - turnover_df * rate

    weight_df = pd.concat(weight_list, axis=1).T
    weight_df.index = price_df.index
    weight_df.columns = price_df.columns
    weight_df.index.names = ['datetime']
    weight_df.columns.names = ['underlying_symbol']

    long_weight_df = weight_df.copy()
    long_weight_df[pos_df < 0.0] = 0.0

    short_weight_df = weight_df.copy()
    short_weight_df[pos_df > 0.0] = 0.0

    hold_weight_df = weight_df.loc[hold_datetime_list]
    long_hold_weight_df = long_weight_df.loc[hold_datetime_list]
    short_hold_weight_df = short_weight_df.loc[hold_datetime_list]
    hold_turnover_df = turnover_df.loc[hold_datetime_list]
    long_hold_turnover_df = long_turnover_df.loc[hold_datetime_list]
    short_hold_turnover_df = short_turnover_df.loc[hold_datetime_list]
    hold_profit_df = get_hold_profit_df(pos_df, price_df, hold_datetime_list)
    long_hold_profit_df = get_hold_profit_df(long_pos_df, price_df, hold_datetime_list)
    short_hold_profit_df = get_hold_profit_df(short_pos_df, price_df, hold_datetime_list)
    hold_profit_df.index.names = ['datetime']
    long_hold_profit_df.index.names = ['datetime']
    short_hold_profit_df.index.names = ['datetime']

    result: Dict[str, DataFrame] = {}
    result['weight'] = weight_df
    result['long_weight'] = long_weight_df
    result['short_weight'] = short_weight_df
    result['hold_weight'] = hold_weight_df
    result['long_hold_weight'] = long_hold_weight_df
    result['short_hold_weight'] = short_hold_weight_df
    result['turnover'] = turnover_df
    result['long_turnover'] = long_turnover_df
    result['short_turnover'] = short_turnover_df
    result['hold_turnover'] = hold_turnover_df
    result['long_hold_turnover'] = long_hold_turnover_df
    result['short_hold_turnover'] = short_hold_turnover_df
    result['profit'] = profit_df
    result['long_profit'] = long_profit_df
    result['short_profit'] = short_profit_df
    result['hold_profit'] = hold_profit_df
    result['long_hold_profit'] = long_hold_profit_df
    result['short_hold_profit'] = short_hold_profit_df

    return result

def rolling_backtest_result_analysis(rolling_result: Dict[int, Any]) -> Dict[str, Any]:
    """
    生成起始日平滑回测结果

    Parameters
    ----------
    rolling_result: Dict[int, Any]
                    每次起始日定期调仓的回测结果, key为起始日shift, value为回测结果

    Returns
    -------
    result: Dict[str, Union[Series, DataFrame]]
            起始日平滑回测结果，包括三个键值对：
            1.symbol: 每个品种的指标
            2.industry: 分行业的指标
            3.all: 总体的指标
    """
    average_symbol_metrics = pd.Series()
    average_industry_metrics = pd.DataFrame()
    average_all_metrics = pd.DataFrame()
    for shift in rolling_result:
        metrics = rolling_result[shift]
        if shift == 0:
            average_symbol_metrics = metrics['symbol']
            average_industry_metrics = metrics['industry']
            average_all_metrics = metrics['all']
        else:
            average_symbol_metrics += metrics['symbol']
            average_industry_metrics += metrics['industry']
            average_all_metrics += metrics['all']
    average_symbol_metrics = average_symbol_metrics / len(rolling_result)
    average_industry_metrics = average_industry_metrics / len(rolling_result)
    average_all_metrics = average_all_metrics / len(rolling_result)
    result: Dict[str, Any] = {'symbol': average_symbol_metrics,
                              'industry': average_industry_metrics,
                              'all': average_all_metrics}
    return result