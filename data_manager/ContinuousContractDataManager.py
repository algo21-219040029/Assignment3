import pandas as pd
from typing import Dict
from pathlib import Path
from pandas import DataFrame
from collections import defaultdict

from utils.utility import stack_dataframe_by_fields

class ContinuousContractDataManager:
    """
    连续合约数据管理器，目前仅支持主力合约

    提取已生成的连续合约数据，不涉及连续合约数据的生成

    Attributes
    __________
    data_folder_path: pathlib.Path
                      连续合约数据的文件夹路径
    continuous_main_contract_series_dict: Dict[str, DataFrame]
                                          主力连续合约数据字典，key为几日换仓，value为主力连续合约数据
    continuous_active_near_contract_series_dict: Dict[str, DataFrame]
                                                活跃近月合约数据字典，key为几日换仓，value为活跃近月合约数据

    Examples
    ________
    1.主力连续合约数据示例:
        datetime main_contract_before_shift main_contract  flag old_main_contract  \
    0 2009-01-05                      A0905         A0905   0.0             A0905
    1 2009-01-06                      A0905         A0905   0.0             A0905
    2 2009-01-07                      A0905         A0905   0.0             A0905
    3 2009-01-08                      A0905         A0905   0.0             A0905
    4 2009-01-09                      A0905         A0905   0.0             A0905
        new_main_contract  old_weight  new_weight  old_return  new_return    return  \
    0             A0905         0.0         1.0    0.000000    0.000000  0.000000
    1             A0905         0.0         1.0    0.013478    0.013478  0.013478
    2             A0905         0.0         1.0   -0.003180   -0.003180 -0.003180
    3             A0905         0.0         1.0   -0.008121   -0.008121 -0.008121
    4             A0905         0.0         1.0    0.012281    0.012281  0.012281
        cum_return  continuous_close underlying_symbol
    0    1.000000            3413.0                 A
    1    1.013478            3459.0                 A
    2    1.010255            3448.0                 A
    3    1.002051            3420.0                 A
    4    1.014357            3462.0                 A

    See Also
    ________
    continuous_contract.ContunuousMainContract
    """
    def __init__(self) -> None:
        """Constructor"""
        self.data_folder_path: Path = Path(__file__).parent.parent.joinpath("data").\
            joinpath("continuous_contract_series")

        self.continuous_main_contract_series_dict: Dict[str, Dict[str, DataFrame]] = defaultdict(dict)
        self.continuous_active_near_contract_series_dict: Dict[str, Dict[str, DataFrame]] = defaultdict(dict)

    def get_continuous_contract_data(self, contract: str = 'main', price: str = 'close', rebalance_num: int = 1) -> DataFrame:
        """
        导入主力连续合约数据,有以下fields

        datetime: 交易日期

        main_contract_before_shift: shift(1)之前的期货主力合约，即时间戳实际为收盘时决定合约的时间

        main_contract: 主力连续合约代码

        old_main_contract: 旧主力合约，如果不换仓则为主力合约

        new_main_contract: 新主力合约，如果不换仓则为主力合约

        old_weight: 旧主力合约权重，不换仓时为0

        new_weight: 新主力合约权重，不换仓时为1

        old_return: 旧主力合约日收益率

        new_return: 新主力合约日收益率

        return: 主力连续合约日收益率

        cum_return: 主力连续合约累计收益率，第一天为1

        continuous_close: 主力连续合约收盘价，由累计收益率x第一天主力合约的收盘价得到

        underlying_symbol: 品种代码

        Parameters
        __________
        rebalance_num: int
                        换仓日期，可选1，3，5

        Returns
        _______
        DataFrame, 主力连续合约数据
        """
        if contract == 'main':
            try:
                df = self.continuous_main_contract_series_dict[price][rebalance_num]
            except KeyError:
                df = pd.read_pickle(self.data_folder_path.joinpath(f"{contract} {price} {rebalance_num}.pkl"))
                self.continuous_main_contract_series_dict[price][rebalance_num] = df
            return df

        elif contract == 'active_near':
            try:
                df = self.continuous_active_near_contract_series_dict[price][rebalance_num]
            except KeyError:
                df = pd.read_pickle(self.data_folder_path.joinpath(f"{contract} {price} {rebalance_num}.pkl"))
                self.continuous_active_near_contract_series_dict[price][rebalance_num] = df
            return df

    def get_field(self, contract: str = 'main', price: str = 'close', rebalance_num: int = 1, field: str = 'continuous_price') -> DataFrame:
        """
        获取连续合约指定字段的数据
        Parameters
        ----------
        contract: str
                合约种类，目前可选有main和active_near,main表示主力合约,active_near表示活跃近月
        rebalance_num: int, default = 1
                换仓天数，可选1，3，5
        field: str, default = 'close'
                字段，continuous_open或者continuous_close

        Returns
        -------
        df: DataFrame
            连续合约field字段数据，一般是开盘价或收盘价
        """
        data = self.get_continuous_contract_data(contract=contract, price=price, rebalance_num=rebalance_num)

        df = stack_dataframe_by_fields(data=data,
                                        index_field='datetime',
                                        column_field='underlying_symbol',
                                        data_field=field)
        return df

    def get_Fuwei_data(self) -> DataFrame:
        """
        获取复位数据

        Returns
        -------
        df: DataFrame
        """
        df: DataFrame = pd.read_pickle(self.data_folder_path.joinpath("Fuwei.pkl"))
        return df


