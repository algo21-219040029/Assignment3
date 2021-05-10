import pandas as pd
from datetime import datetime, date
from typing import Dict, Union, List
from pandas import DataFrame, Series, DatetimeIndex


def Dev1(factor_df: DataFrame, R: int = 5) -> DataFrame:
    """
    计算因子的历史均值的偏离

    Parameters
    ----------
    factor_df: DataFrame
                因子值DataFrame, index为交易时间, columns为品种代码, data为因子值

    R: int, default 5
        回溯期

    Returns
    -------
    dev1_factor_df: DataFrame
                    dev1因子值DataFrame, index为交易时间, columns为品种代码, data为因子值
    """
    mean_factor_df: DataFrame = factor_df.rolling(window=R, min_periods=0).mean().shift(1)
    dev1_factor_df: DataFrame = (factor_df - mean_factor_df) / mean_factor_df
    return dev1_factor_df

def Dev2(factor_df: DataFrame, R: int = 5) -> DataFrame:
    """
    计算标准化因子值

    Parameters
    ----------
    factor_df: DataFrame
                因子值DataFrame, index为交易时间, columns为因子值

    R: int, default 5
        回溯期

    Returns
    -------
    dev2_factor_df: DataFrame
                    dev2因子值DataFrame, index为交易时间, columns为品种代码, data为因子值
    """
    mean_factor_df: DataFrame = factor_df.rolling(window=R, min_periods=0).mean().shift(1)
    std_factor_df: DataFrame = factor_df.rolling(window=R, min_periods=0).std().shift(1)
    dev2_factor_df: DataFrame = (factor_df - mean_factor_df) / std_factor_df
    return dev2_factor_df

def Dev3(factor_df: DataFrame, R: int = 5):
    """
    计算因子值的历史分位点

    Parameters
    ----------
    factor_df: DataFrame
                因子值DataFrame, index为交易时间, columns为因子值

    R: int, default 5
        回溯期

    Returns
    -------
    dev3_factor_df: DataFrame
                    dev3因子值DataFrame, index为交易时间, columns为品种代码, data为因子值
    """
    dev3_factor_df: DataFrame = factor_df.rolling(window=R, min_periods=0).apply(lambda x: x.dropna().values.argsort[-1]/len(x))
    return dev3_factor_df

def get_time_series_between_start_end(time_series: Union[list, DatetimeIndex],
                                      start: Union[datetime, date, str],
                                      end: Union[datetime, date, str],
                                      include_start_end: bool = True) -> DatetimeIndex:
    """
    获取一个时间序列介于两个时间点之间的部分
    :param time_series: 时间序列
    :param start: 开始时间点
    :param end: 结束时间点
    :param include_start_end: 是否包括开头和结束
    :return: 被截取之后的时间序列
    """
    if isinstance(time_series, list):
        time_series = DatetimeIndex(time_series)
    elif not isinstance(time_series, DatetimeIndex):
        raise TypeError("time_series should either be a list or a DatetimeIndex!")

    if isinstance(start, date) or isinstance(start, str):
        start = pd.to_datetime(start)
    elif not isinstance(start, datetime):
        raise TypeError("start must be a str, date or datetime!")

    if isinstance(end, date) or isinstance(end, str):
        end = pd.to_datetime(start)
    elif not isinstance(end, datetime):
        raise TypeError("end must be a str, date or datetime!")

    time_series = time_series.to_series()[time_series.to_series().between(start, end, include_start_end)].index
    return time_series


def stack_dataframe_by_fields(data: DataFrame, index_field: str, column_field: str, data_field: str) -> DataFrame:
    """
    将原来的DataFrame提取index_field, column_field, data_field三列，
    转换成以index_field为index, column_field为column, data_field为data的DataFrame
    :param data: 原始DataFrame
    :param index_field: 转换后作为index的字段名
    :param column_field: 转换后作为column的字段名
    :param data_field: 转换后作为data的字段名
    :type data: DataFrame
    :type index_field: str
    :type column_field: str
    :type data_field: str
    :return: 新的DataFrame, index_field为index，column_field为column，data_field为data
    """

    data = data[[index_field, column_field, data_field]].set_index([index_field, column_field]).unstack(level=-1)
    data.columns = data.columns.droplevel(level=0)
    return data


def get_month_delta(start: datetime, end: datetime) -> int:
    month_delta = (end.year - start.year) * 12 + (end.month - start.month)
    return month_delta


def output(msg: str) -> None:
    print(f"{datetime.now()}\t{msg}")

def flexible_dict_to_series(dic: Dict[str, str]) -> Series:
    """

    Parameters
    ----------
    dic

    Returns
    -------

    """
    result = {}
    for key in dic:
        for element in dic[key]:
            result[element] = key
    result = pd.Series(result)
    return result

def compute_period_return(price_df: DataFrame, period: int = 20, shift: int = 0) -> DataFrame:
    """
    根据价格计算period日的收益率

    Parameters
    ----------
    price_df: DataFrame
                价格DataFrame, index为交易时间, columns为品种代码, data为价格

    period: int, default 20
            多少日的收益率

    Returns
    -------
    hold_return_df:
    """
    hold_price_list: List[Series] = []
    hold_return_dict: Dict[datetime, Series] = {}
    for i in range(len(price_df)):
        if (i - shift) % period != 0:
            continue
        else:
            # 如果是第一次间隔
            date: datetime = price_df.index[i]
            if (i - shift) / period == 0:
                hold_price_list.append(price_df.iloc[i])
            else:
                last_price: Series = hold_price_list[-1]
                price: Series = price_df.iloc[i]
                hold_price_list.append(price)
                hold_return_dict[date] = (price-last_price)/last_price
    hold_return_df: DataFrame = pd.DataFrame(hold_return_dict).T
    return hold_return_df


