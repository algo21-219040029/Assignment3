"""
期货合约相关处理函数（除生成主力连续合约以外）
1.get_near_contract_except_main: 获取除主力合约以外的近月合约
2.get_maturity_date: 获取期货合约的
"""

import numpy as np
import pandas as pd
from pandas import DataFrame
from utils.utility import stack_dataframe_by_fields


def get_maturity_date(all_instruments: DataFrame, symbol: str) -> DataFrame:
    """
    获取某个品种所有合约的到期日
    :param all_instruments: 所有品种的合约基础信息
    :param symbol: 品种代码
    :return: 品种到期日期，DataFrame，一共两列，第一列为合约(contract)，第二列为到期日(maturity_date)
    """
    all_instruments = all_instruments[all_instruments['underlying_symbol'] == symbol]
    maturity_date = all_instruments[['contract', 'maturity_date']]
    maturity_date.index = range(len(maturity_date))
    return maturity_date


def get_near_contract_except_main(daily_data: DataFrame, main_contract_df: DataFrame) -> DataFrame:
    """
    获取单品种除主力合约以外的近月合约
    :param daily_data: 单品种日线行情数据
    :param main_contract_df: 单品种每日主力合约,注意是shift前的main_contract
    :type daily_data: DataFrame
    :type main_contract_df: DataFrame
    :return: 单品种每日近月合约，注意是shift前的近月合约，Series，index是时间，data是shift前的近月合约
    """
    try:
        close = stack_dataframe_by_fields(data=daily_data, index_field='datetime', column_field='contract',
                                      data_field='close')
    except KeyError:
        close = daily_data.copy()


    main_contract_list = main_contract_df['main_contract'].tolist()
    near_contract_list = []
    for i in range(len(close)):
        if i != len(close) - 1:
            # 可选合约池:今天有交易且排除掉主力合约
            if main_contract_list[i] in close.iloc[i].dropna().index:
                pool = close.iloc[i].dropna().index.drop(
                    main_contract_list[i])
                # 如果没有可选合约，则返回缺失值
                if len(pool) == 0:
                    print(main_contract_list[i], close.index[i])
                    near_contract_list.append(np.nan)
                else:
                    near_contract_list.append(pool.min())
            else:
                near_contract_list.append(np.nan)
        else:
            pool = close.iloc[i].dropna().index.drop(main_contract_list[i])
            near_contract_list.append(pool.min())
    near_contract_df = pd.Series(near_contract_list, index=close.index).reset_index()
    near_contract_df.columns = ['datetime', 'near_contract']
    return near_contract_df
