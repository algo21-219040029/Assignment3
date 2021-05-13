import os
import importlib
import pandas as pd
from pathlib import Path
from pandas import Series, \
                    DataFrame
import matplotlib.pyplot as plt
from typing import Dict, Any, Tuple

from commodity_pool.base import BaseCommodityPool
from collections import defaultdict

class CommodityPoolManager:
    """
    商品池处理器

    1.检查是否存在某个商品池
    2.获取商品池数据
    3.保存商品池数据
    4.保存商品池信息

    Attributes
    __________
    commodity_pool_file_folder_path: pathlib.Path
                                    商品池代码文件路径

    commodity_pool_data_folder_path: pathlib.Path
                                    商品池数据文件夹路径

    commodity_pool_dict: Dict[str, Dict[str, BaseCommodityPool]]
                        商品池字典，双层字典，外层为商品池名称: 该商品池的不同参数的具体商品池的字典，内层为商品池参数: 商品池DataFrame

    Notes
    _____
    CommodityPoolManager不涉及商品池构建的代码，商品池构建的代码详见commodity_pool.CommodityPool

    See Also
    ________
    commodity_pool.CommodityPool
    """

    def __init__(self) -> None:
        """Constructor"""
        self.commodity_pool_file_folder_path: Path = Path(__file__).parent.parent.joinpath("commodity_pool")
        self.commodity_pool_data_folder_path: Path = Path(__file__).parent.parent.joinpath("data").joinpath("commodity_pool")

        self.commodity_pool_dict: Dict[str, Dict[str, BaseCommodityPool]] = defaultdict(dict)

    def get_file_name(self, params: Dict[str, Any]) -> str:
        """
        将参数字典转化为文件名，即参数1_参数值1 参数2_参数值2 参数3_参数值3 xxx

        Parameters
        ----------
        params: Dict[str, Any]
                参数字典

        Returns
        -------
        string: str
                参数1_参数值1 参数2_参数值2 参数3_参数值3 xxx
        """
        string = ''
        for param in params:
            string += f"{param}_{params[param]}"
            string += ' '
        if string:
            string = string[:-1]
        return string

    def import_commodity_pool_class(self, group: str, name: str) -> BaseCommodityPool:
        """
        根据group和name导入商品池类

        Parameters
        ----------
        group: str
                商品池类别，即FixedPool或者DynamicPool

        name: str
                商品池名称

        Returns
        -------
        commodity_pool_class: 商品池类
        """
        commodity_pool_file_path = "commodity_pool"+"."+group+"."+name
        commodity_pool_module = importlib.import_module(commodity_pool_file_path)
        commodity_pool_class = getattr(commodity_pool_module, name)
        return commodity_pool_class

    def get_commodity_pool_in_out(self, group: str, name: str, **params) -> Dict[str, Any]:
        """
        获取商品池中品种每日进出及每个品种的进出次数
        Parameters
        ----------
        group
        name
        params

        Returns
        -------

        """
        commodity_pool_instance = self.get_commodity_pool(group=group, name=name, **params)
        commodity_pool_value: DataFrame = commodity_pool_instance.get_commodity_pool_value()
        commodity_pool_value.fillna(False, inplace=True)
        commodity_pool_value = commodity_pool_value.astype(int)
        commodity_pool_value_diff = commodity_pool_value.diff(1)
        commodity_pool_value_diff.iloc[0] = commodity_pool_value.iloc[0]
        commodity_pool_value_diff = commodity_pool_value_diff.astype(int)
        # 获取每日进入情况
        commodity_daily_in = {}
        commodity_daily_out = {}
        for date in commodity_pool_value_diff.index:
            commodity_pool_diff_series = commodity_pool_value_diff.loc[date]
            in_list = commodity_pool_diff_series[commodity_pool_diff_series == 1].index.tolist()
            out_list = commodity_pool_diff_series[commodity_pool_diff_series == -1].index.tolist()
            commodity_daily_in[date] = in_list
            commodity_daily_out[date] = out_list

        # 获取每个品种进入商品池几次
        commodity_in_num = {}
        commodity_out_num = {}
        for symbol in commodity_pool_value_diff.columns:
            value_count = commodity_pool_value_diff[symbol].value_counts().to_dict()
            if -1 not in value_count.keys():
                value_count[-1] = 0
            if 1 not in value_count.keys():
                value_count[1] = 0
            value_count.pop(0)
            commodity_in_num[symbol] = value_count[1]
            commodity_out_num[symbol] = value_count[-1]

        result = {'daily_in': commodity_daily_in,
                  'daily_out': commodity_daily_out,
                  'symbol_in_num': commodity_in_num,
                  'symbol_out_num': commodity_out_num}
        return result

    def get_commodity_pool_time_series_plot(self, group: str, name: str, **params) -> Series:
        """
        获取商品池中的品种数目变化图

        Parameters
        ----------
        group: str
                商品池类别，即FixedPool或者DynamicPool

        name: str
                商品池名称

        params: 可变参数
                商品池参数

        Returns
        -------
        None
        """
        commodity_pool_instance = self.get_commodity_pool(group=group, name=name, **params)
        commodity_pool_value: DataFrame = commodity_pool_instance.get_commodity_pool_value()
        commodity_pool_value.fillna(False, inplace=True)
        commodity_pool_num_series:Series = commodity_pool_value.sum(axis=1)
        plt.figure(figsize=(20, 8))
        fig, ax = plt.subplots(figsize=(20, 8))
        ax.step(commodity_pool_num_series.index, commodity_pool_num_series.values)
        # for i, j in zip(commodity_pool_num_series.index, commodity_pool_num_series.values):
        #     ax.text(x=i, y=j+1, s=str(int(j)))
        fig.tight_layout()
        plt.grid()
        plt.title(commodity_pool_instance.__repr__())
        plt.show()
        return commodity_pool_num_series

    def get_commodity_pool(self, group: str, name: str, overwrite: bool = False, **params) -> BaseCommodityPool:
        """
        获取商品池

        Parameters
        ----------
        group: str
                商品池类别，即FixedPool或者DynamicPool

        name: str
                商品池名称

        overwrite: bool, default False
                    是否覆盖目前已有的商品池数据

        params: 可变参数
                商品池参数

        Returns
        -------
        commodity_pool_instance: BaseCommodityPool
                                商品池实例，可获取商品池参数，商品池数值DataFrame
        """
        # 参数检测和生成文件名
        commodity_pool_class = self.import_commodity_pool_class(group=group, name=name)
        commodity_pool_instance = commodity_pool_class(**params)
        string = self.get_file_name(commodity_pool_instance.get_params())

        # 如果overwrite=True
        if overwrite:
            # 计算商品池
            commodity_pool_instance.compute_commodity_pool()
            self.commodity_pool_dict[f"{group}_{name}"][string] = commodity_pool_instance
            self.save_commodity_pool(group=group, name=name, overwrite=True, **params)
            return commodity_pool_instance

        # 如果overwrite=False
        elif not overwrite:
            # 如果commodity_pool_dict中该因子
            if f"{group}_{name}" in self.commodity_pool_dict:
                # 如果有该商品池参数下的商品池实例，则直接调取commodity_pool_dict中的商品池实例
                if string in self.commodity_pool_dict:
                    self.save_commodity_pool(group=group, name=name, overwrite=True, **params)
                    return self.commodity_pool_dict[f"{group}_{name}"][string]
                # 如果没有该商品池参数下的商品池实例，则通过本地调取或直接计算生成商品池实例
                else:
                    # 首先尝试通过本地调取
                    try:
                        commodity_pool_value = pd.read_pickle(self.commodity_pool_data_folder_path.joinpath(group).
                                                    joinpath(name).joinpath(f"{string}.pkl"))
                        commodity_pool_instance.set_commodity_pool_value(commodity_pool_value)
                        self.commodity_pool_dict[f"{group}_{name}"][string] = commodity_pool_instance
                    # 如果本地调取失败, 则直接计算商品池
                    except:
                        commodity_pool_instance.compute_commodity_pool()
                        self.commodity_pool_dict[f"{group}_{name}"][string] = commodity_pool_instance
                        self.save_commodity_pool(group=group, name=name, overwrite=True, **params)

                    return commodity_pool_instance

            else:
                # 首先尝试通过本地调取
                try:
                    commodity_pool_value = pd.read_pickle(self.commodity_pool_data_folder_path.joinpath(group).
                                                          joinpath(name).joinpath(f"{string}.pkl"))
                    commodity_pool_instance.set_commodity_pool_value(commodity_pool_value)
                    self.commodity_pool_dict[f"{group}_{name}"][string] = commodity_pool_instance
                # 如果本地调取失败, 则直接计算商品池
                except:
                    commodity_pool_instance.compute_commodity_pool()
                    self.commodity_pool_dict[f"{group}_{name}"][string] = commodity_pool_instance
                    self.save_commodity_pool(group=group, name=name, overwrite=True, **params)

                return commodity_pool_instance

    def compute_commodity_pool(self, group: str, name: str, overwrite: bool = False, **params) -> BaseCommodityPool:
        """
        计算商品池数据DataFrame

        Parameters
        ----------
        group: str
                商品池类别, FixedPool或者DynamicPool

        name: str
                商品池名称

        overwrite: bool, default False
                是否覆盖

        params: 可变参数
                商品池参数

        Returns
        -------
        commodity_pool_instance: BaseCommodityPool
                                商品池实例，可获取商品池参数，商品池数值DataFrame
        """
        commodity_pool_class = self.import_commodity_pool_class(group=group, name=name)
        commodity_pool_instance = commodity_pool_class(**params)
        string = self.get_file_name(commodity_pool_instance.get_params())

        if overwrite:
            commodity_pool_instance.compute_commodity_pool()
            self.commodity_pool_dict[f"{group}_{name}"][string] = commodity_pool_instance
            return commodity_pool_instance

        elif not overwrite:
            if f"{group}_{name}" in self.commodity_pool_dict:
                # 如果有该商品池参数下的商品池实例，则直接调取commodity_pool_dict中的商品池实例
                if string in self.commodity_pool_dict:
                    return self.commodity_pool_dict[f"{group}_{name}"][string]
                else:
                    commodity_pool_instance.compute_commodity_pool()
                    self.commodity_pool_dict[f"{group}_{name}"][string] = commodity_pool_instance
                    return commodity_pool_instance
            else:
                commodity_pool_instance.compute_commodity_pool()
                self.commodity_pool_dict[f"{group}_{name}"][string] = commodity_pool_instance
                return commodity_pool_instance

    def save_commodity_pool(self, group: str, name: str, overwrite: bool = False, **params) -> None:
        """
        保存商品池数据

        Parameters
        ----------
        group: str
                商品池类别, FixedPool或者DynamicPool

        name: str
                商品池名称

        overwrite: bool, default False
                    是否覆盖本地已有的商品池数据

        params:
                商品池参数

        Returns
        -------
        None
        """
        commodity_pool_class = self.import_commodity_pool_class(group=group, name=name)
        commodity_pool_instance = commodity_pool_class(**params)
        string = self.get_file_name(commodity_pool_instance.get_params())

        commodity_pool_data_folder_path = self.commodity_pool_data_folder_path.joinpath(group).joinpath(name)
        # 如果没有商品池文件路径，则创建
        if not os.path.exists(commodity_pool_data_folder_path):
            os.makedirs(commodity_pool_data_folder_path)

        # 如果不覆盖
        if not overwrite:
            if os.path.exists(str(commodity_pool_data_folder_path.joinpath(f"{string}.pkl"))):
                return
            else:
                # 如果有商品池类
                if f"{group}_{name}" in self.commodity_pool_dict:
                    # 如果已经计算该商品池该参数下的商品池值, 则直接保存商品池值
                    if string in self.commodity_pool_dict[f"{group}_{name}"]:
                        commodity_pool_value = self.commodity_pool_dict[f"{group}_{name}"][string].get_commodity_pool_value()
                        commodity_pool_value.to_pickle(str(commodity_pool_data_folder_path.joinpath(f"{string}.pkl")))
                    # 如果没有计算该商品池该参数下的商品池值, 则先计算商品池值, 再保存商品池值
                    else:
                        commodity_pool_instance.compute_commodity_pool()
                        commodity_pool_value = commodity_pool_instance.get_commodity_pool_value()
                        self.commodity_pool_dict[f"{group}_{name}"][string] = commodity_pool_instance
                        commodity_pool_value.to_pickle(str(commodity_pool_data_folder_path.joinpath(f"{string}.pkl")))
                # 如果没有计算商品池值
                else:
                    commodity_pool_instance.compute_commodity_pool()
                    commodity_pool_value = commodity_pool_instance.get_commodity_pool_value()
                    self.commodity_pool_dict[f"{group}_{name}"][string] = commodity_pool_instance
                    commodity_pool_value.to_pickle(str(commodity_pool_data_folder_path.joinpath(f"{string}.pkl")))
        elif overwrite:
            commodity_pool_instance.compute_commodity_pool()
            commodity_pool_value = commodity_pool_instance.get_commodity_pool_value()
            self.commodity_pool_dict[f"{group}_{name}"][string] = commodity_pool_instance
            commodity_pool_value.to_pickle(str(commodity_pool_data_folder_path.joinpath(f"{string}.pkl")))



















