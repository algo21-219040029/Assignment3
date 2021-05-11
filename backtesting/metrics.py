import numpy as np
import pandas as pd
from pandas import DataFrame
from typing import Dict, List


def get_metrics(weight_df: DataFrame,
                hold_weight_df: DataFrame,
                symbol_industry_map: DataFrame,
                turnover_df: DataFrame,
                hold_turnover_df: DataFrame,
                init_total_value: int,
                profit_df: DataFrame,
                hold_profit_df: DataFrame,
                interest: str):
    turnover_df = np.abs(turnover_df)
    hold_turnover_df = np.abs(hold_turnover_df)

    # hold_result_df，第一列是交易时间datetime，第二列是品种underlying_symbol，第三列是权重weight，第四列是收益profit，第五列是行业industry_name
    hold_result_df = pd.concat([hold_weight_df.stack().to_frame('weight'),
                                hold_profit_df.stack().to_frame('profit'),
                                hold_turnover_df.stack().to_frame('turnover')],
                               axis=1).reset_index()

    hold_result_df = pd.merge(left=hold_result_df.reset_index(), right=symbol_industry_map.reset_index(),
                              on='underlying_symbol', how='left')

    # industry_hold_result_df，第一列是交易时间datetime，第二列是行业industry_name，第三列是权重，第四列是收益
    industry_hold_result_df = hold_result_df.groupby(['datetime', 'industry_name'], as_index=True)[[
        'weight', 'profit', 'turnover']].sum()

    # all_hold_result_df
    all_hold_result_df = hold_result_df.groupby('datetime')[['weight', 'profit', 'turnover']].sum()

    # result_df
    result_df = pd.concat([weight_df.stack().to_frame('weight'),
                           profit_df.stack().to_frame('profit'),
                           turnover_df.stack().to_frame('turnover')],
                          axis=1).reset_index()

    result_df = pd.merge(left=result_df.reset_index(), right=symbol_industry_map.reset_index(),
                         on='underlying_symbol', how='left')

    # industry_result_df
    industry_result_df = result_df.groupby(['datetime', 'industry_name'], as_index=True)[[
        'weight', 'profit', 'turnover']].sum()

    # all_result_df
    all_result_df = result_df.groupby('datetime')[['weight', 'profit', 'turnover']].sum()

    # 计算多空率
    # 计算多率
    def apply_long_rate(series):
        series = series.fillna(0.0)
        long_num = np.sum(series > 0.0)
        hold_num = np.sum(series != 0.0)
        if hold_num > 0:
            return long_num / hold_num
        else:
            return 0.0

    long_rate_series = hold_result_df.groupby('underlying_symbol')['weight'].apply(apply_long_rate)
    industry_long_rate_series = industry_hold_result_df.groupby('industry_name')['weight'].apply(apply_long_rate)
    all_long_rate = apply_long_rate(all_hold_result_df['weight'])

    # 计算空率
    def apply_short_rate(series):
        series = series.fillna(0.0)
        short_num = np.sum(series < 0.0)
        hold_num = np.sum(series != 0.0)
        if hold_num > 0:
            return short_num / hold_num
        else:
            return 0.0

    short_rate_series = hold_result_df.groupby('underlying_symbol')['weight'].apply(apply_short_rate)
    industry_short_rate_series = industry_hold_result_df.groupby('industry_name')['weight'].apply(apply_short_rate)
    all_short_rate = apply_short_rate(all_hold_result_df['weight'])

    # 计算参与率
    def apply_participate_rate(series):
        series = series.dropna()
        hold_num = np.sum(series != 0.0)
        total_num = len(series)
        if hold_num > 0:
            return hold_num / total_num
        else:
            return 0.0

    participate_rate_series = hold_result_df.groupby('underlying_symbol')['weight'].apply(apply_participate_rate)
    # 注意行业权重可能为0，但实际上有品种参与,因此对权重做取绝对值修正
    participate_hold_result_df = hold_result_df.copy()
    participate_hold_result_df['weight'] = np.abs(participate_hold_result_df['weight'])
    participate_industry_hold_result_df = participate_hold_result_df.groupby(['datetime', 'industry_name'],
                                                                             as_index=True)[[
        'weight', 'profit', 'turnover']].sum()
    industry_participate_rate_series = participate_industry_hold_result_df.groupby('industry_name')['weight']. \
        apply(apply_participate_rate)
    all_participate_rate = 1.0

    # 对数据做修正，考虑仅做多和仅做空
    long_hold_result_df = hold_result_df.copy()
    long_hold_result_df.loc[long_hold_result_df['weight'] < 0.0, 'profit'] = 0.0
    long_hold_result_df.loc[long_hold_result_df['weight'] < 0.0, 'turnover'] = 0.0
    long_hold_result_df.loc[long_hold_result_df['weight'] < 0.0, 'weight'] = 0.0
    long_industry_hold_result_df = long_hold_result_df.groupby(['datetime', 'industry_name'], as_index=True)[[
        'weight', 'profit', 'turnover']].sum()
    long_all_hold_result_df = long_hold_result_df.groupby('datetime')[['weight', 'profit', 'turnover']].sum()

    short_hold_result_df = hold_result_df.copy()
    short_hold_result_df.loc[short_hold_result_df['weight'] > 0.0, 'profit'] = 0.0
    short_hold_result_df.loc[short_hold_result_df['weight'] > 0.0, 'turnover'] = 0.0
    short_hold_result_df.loc[short_hold_result_df['weight'] > 0.0, 'weight'] = 0.0
    short_industry_hold_result_df = short_hold_result_df.groupby(['datetime', 'industry_name'], as_index=True)[[
        'weight', 'profit', 'turnover']].sum()
    short_all_hold_result_df = short_hold_result_df.groupby('datetime')[['weight', 'profit', 'turnover']].sum()

    long_result_df = result_df.copy()
    long_result_df.loc[long_result_df['weight'] < 0.0, 'profit'] = 0.0
    long_result_df.loc[long_result_df['weight'] < 0.0, 'turnover'] = 0.0
    long_result_df.loc[long_result_df['weight'] < 0.0, 'weight'] = 0.0
    long_industry_result_df = long_result_df.groupby(['datetime', 'industry_name'], as_index=True)[[
        'weight', 'profit', 'turnover']].sum()
    long_all_result_df = long_result_df.groupby('datetime')[['weight', 'profit', 'turnover']].sum()

    short_result_df = result_df.copy()
    short_result_df.loc[short_result_df['weight'] > 0.0, 'profit'] = 0.0
    short_result_df.loc[short_result_df['weight'] > 0.0, 'turnover'] = 0.0
    short_result_df.loc[short_result_df['weight'] > 0.0, 'weight'] = 0.0
    short_industry_result_df = short_result_df.groupby(['datetime', 'industry_name'], as_index=True)[[
        'weight', 'profit', 'turnover']].sum()
    short_all_result_df = short_result_df.groupby('datetime')[['weight', 'profit', 'turnover']].sum()

    # 计算换手率
    turnover_rate_series = hold_result_df.groupby('underlying_symbol')['turnover'].sum() * 252 / len(weight_df) / init_total_value
    industry_turnover_rate_series = industry_hold_result_df.groupby('industry_name')['turnover'].sum() * 252 / len(weight_df) / init_total_value
    all_turnover_rate = all_hold_result_df['turnover'].sum() * 252 / len(weight_df) / init_total_value

    long_turnover_rate_series = long_hold_result_df.groupby('underlying_symbol')['turnover'].sum() * 252 / len(weight_df)/ init_total_value
    long_industry_turnover_rate_series = long_industry_hold_result_df.groupby('industry_name')['turnover'].sum()* 252 / len(weight_df)/ init_total_value
    long_all_turnover_rate = long_all_hold_result_df['turnover'].sum() * 252 / len(weight_df)/ init_total_value

    short_turnover_rate_series = short_hold_result_df.groupby('underlying_symbol')['turnover'].sum()* 252 / len(weight_df)/ init_total_value
    short_industry_turnover_rate_series = short_industry_hold_result_df.groupby('industry_name')['turnover'].sum() * 252 / len(weight_df)/ init_total_value
    short_all_turnover_rate = short_all_hold_result_df['turnover'].sum() * 252 / len(weight_df)/ init_total_value

    # 计算胜率
    def apply_win_rate(series):

        win_num = np.sum(series > 0.0)
        hold_num = np.sum(series != 0.0)
        if hold_num > 0:
            win_rate = win_num / hold_num
        else:
            win_rate = 0.0
        return win_rate

    win_rate_series = hold_result_df.groupby('underlying_symbol')['profit'].apply(apply_win_rate)
    industry_win_rate_series = industry_hold_result_df.groupby('industry_name')['profit'].apply(apply_win_rate)
    all_win_rate = apply_win_rate(all_hold_result_df['profit'])

    long_win_rate_series = long_hold_result_df.groupby('underlying_symbol')['profit'].apply(apply_win_rate)
    long_industry_win_rate_series = long_industry_hold_result_df.groupby('industry_name')['profit'].apply(
        apply_win_rate)
    long_all_win_rate = apply_win_rate(long_all_hold_result_df['profit'])

    short_win_rate_series = short_hold_result_df.groupby('underlying_symbol')['profit'].apply(apply_win_rate)
    short_industry_win_rate_series = short_industry_hold_result_df. \
        groupby('industry_name')['profit'].apply(apply_win_rate)
    short_all_win_rate = apply_win_rate(short_all_hold_result_df['profit'])

    # 累计收益
    total_return_series = result_df.groupby('underlying_symbol')['profit'].sum() / init_total_value
    industry_total_return_series = industry_result_df.groupby('industry_name')['profit'].sum() / init_total_value
    all_total_return = all_result_df['profit'].sum() / init_total_value

    long_total_return_series = long_result_df.groupby('underlying_symbol')['profit'].sum() / init_total_value
    long_industry_total_return_series = long_industry_result_df. \
                                    groupby('industry_name')['profit'].sum() / init_total_value
    long_all_total_return = long_all_result_df['profit'].sum() / init_total_value

    short_total_return_series = short_result_df.groupby('underlying_symbol')['profit'].sum() / init_total_value
    short_industry_total_return_series = short_industry_result_df. \
                                     groupby('industry_name')['profit'].sum() / init_total_value
    short_all_total_return = short_all_result_df['profit'].sum() / init_total_value


    # 年化收益率
    if interest == 'simple':
        annual_return_series = total_return_series * 252 / len(weight_df)
        industry_annual_return_series = industry_total_return_series * 252 / len(weight_df)
        all_annual_return = all_total_return * 252 / len(weight_df)

        long_annual_return_series = long_total_return_series * 252 / len(weight_df)
        long_industry_annual_return_series = long_industry_total_return_series * 252 / len(weight_df)
        long_all_annual_return = long_all_total_return * 252 / len(weight_df)

        short_annual_return_series = short_total_return_series * 252 / len(weight_df)
        short_industry_annual_return_series = short_industry_total_return_series * 252 / len(weight_df)
        short_all_annual_return = short_all_total_return * 252 / len(weight_df)
    elif interest == 'compound':
        annual_return_series = (1+total_return_series)**(252/len(weight_df)) - 1
        industry_annual_return_series = (1+industry_total_return_series+1)**(252/len(weight_df)) - 1
        all_annual_return = (1+all_total_return) ** (252 / len(weight_df)) - 1

        long_annual_return_series = (1+long_total_return_series) ** (252 / len(weight_df)) - 1
        long_industry_annual_return_series = (1+long_industry_total_return_series) ** (252 / len(weight_df)) - 1
        long_all_annual_return = (1+long_all_total_return) ** (252 / len(weight_df)) - 1

        short_annual_return_series = (1+short_total_return_series) ** (252 / len(weight_df)) - 1
        short_industry_annual_return_series = (1+short_industry_total_return_series) ** (252 / len(weight_df)) - 1
        short_all_annual_return = (1+short_all_total_return) ** (252 / len(weight_df)) - 1
    # 盈亏比
    def apply_gain_loss_rate(series):

        series.fillna(0.0, inplace=True)
        total_profit = np.sum(series[series > 0.0])
        total_loss = np.sum(np.abs(series[series < 0.0]))
        if total_loss == 0 and total_profit > 0:
            return np.inf
        elif total_loss == 0 and total_profit == 0:
            return 0.0
        else:
            return total_profit / total_loss

    gain_loss_rate_series = hold_result_df.groupby('underlying_symbol')['profit'].apply(apply_gain_loss_rate)
    industry_gain_loss_rate_series = industry_hold_result_df.groupby('industry_name')['profit'].apply(
        apply_gain_loss_rate)
    all_gain_loss_rate = apply_gain_loss_rate(all_hold_result_df['profit'])

    long_gain_loss_rate_series = long_hold_result_df.groupby('underlying_symbol')['profit'].apply(apply_gain_loss_rate)
    long_industry_gain_loss_rate_series = long_industry_hold_result_df.groupby('industry_name')['profit'].apply(
        apply_gain_loss_rate)
    long_all_gain_loss_rate = apply_gain_loss_rate(long_all_hold_result_df['profit'])

    short_gain_loss_rate_series = short_hold_result_df.groupby('underlying_symbol')['profit'].apply(
        apply_gain_loss_rate)
    short_industry_gain_loss_rate_series = short_industry_hold_result_df.groupby('industry_name')['profit'].apply(
        apply_gain_loss_rate)
    short_all_gain_loss_rate = apply_gain_loss_rate(short_all_hold_result_df['profit'])

    # 夏普比率
    result_df['return'] = result_df['profit'] / init_total_value
    industry_result_df['return'] = industry_result_df['profit'] / init_total_value
    all_result_df['return'] = all_result_df['profit'] / init_total_value
    sharpe_series = result_df.groupby('underlying_symbol')['return'].mean() \
                    / result_df.groupby('underlying_symbol')['return'].std() * np.sqrt(252)
    industry_sharpe_series = industry_result_df.groupby('industry_name')['return'].mean() \
                             / industry_result_df.groupby('industry_name')['return'].std() * np.sqrt(252)
    all_sharpe = all_result_df['return'].mean() / all_result_df['return'].std() * np.sqrt(252)

    # 最大回撤
    def apply_max_drawdown(series):
        cum_series = series.cumsum() + 1
        return ((cum_series.cummax() - cum_series) / cum_series.cummax()).max()

    max_drawdown_series = result_df.groupby('underlying_symbol')['return'].apply(apply_max_drawdown)
    industry_max_drawdown_series = industry_result_df.groupby('industry_name')['return'].apply(apply_max_drawdown)
    all_max_drawdown = apply_max_drawdown(all_result_df['return'])

    symbol_metrics = pd.DataFrame({'long_rate': long_rate_series,
                                   'short_rate': short_rate_series,
                                   'participate_rate': participate_rate_series,
                                   'turnover_rate': turnover_rate_series,
                                   'long_turnover_rate': long_turnover_rate_series,
                                   'short_turnover_rate': short_turnover_rate_series,
                                   'win_rate': win_rate_series,
                                   'long_win_rate': long_win_rate_series,
                                   'short_win_rate': short_win_rate_series,
                                   'total_return': total_return_series,
                                   'long_total_return': long_total_return_series,
                                   'short_total_return': short_total_return_series,
                                   'annual_return': annual_return_series,
                                   'long_annual_return': long_annual_return_series,
                                   'short_annual_return': short_annual_return_series,
                                   'gain_loss_rate': gain_loss_rate_series,
                                   'long_gain_loss_rate': long_gain_loss_rate_series,
                                   'short_gain_loss_rate': short_gain_loss_rate_series,
                                   'sharpe': sharpe_series,
                                   'max_drawdown': max_drawdown_series})

    industry_metrics = pd.DataFrame({'long_rate': industry_long_rate_series,
                                     'short_rate': industry_short_rate_series,
                                     'participate_rate': industry_participate_rate_series,
                                     'turnover_rate': industry_turnover_rate_series,
                                     'long_turnover_rate': long_industry_turnover_rate_series,
                                     'short_turnover_rate': short_industry_turnover_rate_series,
                                     'win_rate': industry_win_rate_series,
                                     'long_win_rate': long_industry_win_rate_series,
                                     'short_win_rate': short_industry_win_rate_series,
                                     'total_return': industry_total_return_series,
                                     'long_total_return': long_industry_total_return_series,
                                     'short_total_return': short_industry_total_return_series,
                                     'annual_return': industry_annual_return_series,
                                     'long_annual_return': long_industry_annual_return_series,
                                     'short_annual_return': short_industry_annual_return_series,
                                     'gain_loss_rate': industry_gain_loss_rate_series,
                                     'long_gain_loss_rate': long_industry_gain_loss_rate_series,
                                     'short_gain_loss_rate': short_industry_gain_loss_rate_series,
                                     'sharpe': industry_sharpe_series,
                                     'max_drawdown': industry_max_drawdown_series})

    all_metrics = pd.Series({'long_rate': all_long_rate,
                             'short_rate': all_short_rate,
                             'participate_rate': all_participate_rate,
                             'turnover_rate': all_turnover_rate,
                             'long_turnover_rate': long_all_turnover_rate,
                             'short_turnover_rate': short_all_turnover_rate,
                             'win_rate': all_win_rate,
                             'long_win_rate': long_all_win_rate,
                             'short_win_rate': short_all_win_rate,
                             'total_return': all_total_return,
                             'long_total_return': long_all_total_return,
                             'short_total_return': short_all_total_return,
                             'annual_return': all_annual_return,
                             'long_annual_return': long_all_annual_return,
                             'short_annual_return': short_all_annual_return,
                             'gain_loss_rate': all_gain_loss_rate,
                             'long_gain_loss_rate': long_all_gain_loss_rate,
                             'short_gain_loss_rate': short_all_gain_loss_rate,
                             'sharpe': all_sharpe,
                             'max_drawdown': all_max_drawdown})

    metrics = {'symbol': symbol_metrics, 'industry': industry_metrics, 'all': all_metrics}
    return metrics
