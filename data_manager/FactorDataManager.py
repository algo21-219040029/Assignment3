import os
import importlib
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any
from collections import defaultdict

from factor.base import BaseFactor

class FactorDataManager:
    """
    因子数据管理器

    Attributes
    __________
    factor_file_folder_path: pathlib.Path
                            因子代码文件夹路径

    factor_data_folder_path: pathlib.Path
                            因子数据文件夹路径

    factor_group_list: List[str]
                        因子类别列表

    factor_dict: Dict[str, DataFrame]
                因子数据字典，key为因子名称，value为因子实例

    Notes
    _____
    1.因子代码的路径为factor

    2.因子数据的路径为data/factor

    """

    def __init__(self) -> None:
        """Constructor"""
        self.factor_file_folder_path: Path = Path(__file__).parent.parent.joinpath("factor")
        self.factor_data_folder_path: Path = Path(__file__).parent.parent.joinpath("data").joinpath("factor")

        self.factor_group_list: List[str] = None

        self.factor_dict: Dict[str, Dict[str, BaseFactor]] = defaultdict(dict)

        self.init_factor_group_list()

    def init_factor_group_list(self) -> None:
        """
        初始化因子类别列表

        Returns
        -------
        None
        """
        factor_group_list = [file for file in os.listdir(self.factor_file_folder_path) if "." not in file]
        self.factor_group_list = factor_group_list

    def import_factor_class(self, group: str, name: str) -> BaseFactor:
        """
        根据group和name导入因子类

        Parameters
        ----------
        group: str
                因子类别，如CarryFactor, MomentumFactor

        name: str
                因子名称

        Returns
        -------
        factor_class: 因子类
        """
        factor_file_path = "factor" + "." + group + "." + name
        factor_module = importlib.import_module(factor_file_path)
        factor_class = getattr(factor_module, name)
        return factor_class

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

    def check_local_factor_data(self, group: str, name: str, **params: Dict[str, Any]) -> bool:
        """
        检查本地是否有因子值

        Parameters
        ----------
        Parameters
        ----------
        group: str
                因子类别

        name: str
                因子名称

        params: Dict[str, Any]
                因子参数

        Returns
        -------
        True ro False
        """
        factor_class = self.import_factor_class(group=group, name=name)
        factor_instance = factor_class(**params)
        params = factor_instance.get_params()
        string = self.get_file_name(params)
        factor_file_path = self.factor_data_folder_path.joinpath(group).joinpath(name).joinpath(f"{string}.pkl")
        if os.path.exists(factor_file_path):
            return True
        else:
            return False

    def get_factor(self, group: str, name: str, overwrite=False, **params) -> BaseFactor:
        """
        获取因子值

        Parameters
        __________
        group: str
                因子组

        name: str
                因子名称

        overwrite: bool, default False
                    是否覆盖，即在本地有因子值时是否覆盖更新

        params:
                因子参数，字典形式，{参数1: 参数1值, 参数2: 参数2值, 参数3: 参数3值...}

        Returns
        _______
        factor: BaseFactor
                因子实例
        """

        # 生成文件名
        factor_class = self.import_factor_class(group=group, name=name)
        factor_instance = factor_class(**params)
        string = self.get_file_name(factor_instance.get_params())

        # 如果overwrite=True
        if overwrite:
            # 导入因子实例
            factor_instance.compute_factor()
            self.factor_dict[f"{group}_{name}"][string] = factor_instance
            self.save_factor(group=group, name=name, overwrite=True, **params)
            return factor_instance

        # 如果overwrite=False
        elif not overwrite:
            # 如果factor_dict中有该因子
            if f"{group}_{name}" in self.factor_dict:
                # 如果有该因子该参数下的因子实例，则直接调取factor_dict中的因子实例
                if string in self.factor_dict[f"{group}_{name}"]:
                    self.save_factor(group=group, name=name, overwrite=False, **params)
                    return self.factor_dict[f"{group}_{name}"][string]
                # 如果没有该因子该参数下的因子实例，则通过本地调取或直接计算生成因子实例
                else:
                    # 检查本地是否有该因子该参数下的因子值
                    flag = self.check_local_factor_data(group=group, name=name, **factor_instance.get_params())

                    # 如果本地有该因子该参数下的因子值，则从本地导入因子数据
                    if flag:
                        factor_data = pd.read_pickle(self.factor_data_folder_path.joinpath(group).
                                                     joinpath(name).joinpath(f"{string}.pkl"))
                        factor_instance.set_factor_value(factor_data)
                        self.factor_dict[f"{group}_{name}"][string] = factor_instance

                    # 如果本地没有该因子该参数下的因子值，则需要直接计算，并且保存因子值
                    else:
                        factor_instance.compute_factor()
                        self.factor_dict[f"{group}_{name}"][string] = factor_instance
                        self.save_factor(group=group, name=name, **params)

                    return factor_instance
            # 如果factor_dict中没有该因子
            else:

                # 检查本地是否有该因子该参数下的因子值
                flag = self.check_local_factor_data(group=group, name=name, **factor_instance.get_params())

                # 如果本地有该因子该参数下的因子值，则从本地导入因子数据
                if flag:
                    factor_data = pd.read_pickle(self.factor_data_folder_path.joinpath(group).
                                                 joinpath(name).joinpath(f"{string}.pkl"))
                    factor_instance.set_factor_value(factor_data)
                    self.factor_dict[f"{group}_{name}"][string] = factor_instance
                # 如果本地没有该因子该参数下的因子值，则需要直接计算
                else:
                    factor_instance.compute_factor()
                    self.factor_dict[f"{group}_{name}"][string] = factor_instance
                    self.save_factor(group=group, name=name, **params)

                return factor_instance

    def compute_factor(self, group: str, name: str, overwrite: bool = False, **params) -> BaseFactor:
        """
        计算因子数据DataFrame

        Parameters
        ----------
        group: str
                因子类别

        name: str
                因子名称

        overwrite: bool, default False
                    是否覆盖

        params:
                因子参数

        Returns
        -------
        factor_instance: BaseCommodityPool
                        因子实例，可获取因子参数，因子数值DataFrame
        """
        # 创建因子实例
        factor_class = self.import_factor_class(group, name)
        factor_instance = factor_class(**params)
        # 创建参数字符串
        string = self.get_file_name(factor_instance.get_params())

        # 如果覆盖, 则直接计算因子值
        if overwrite:
            factor_instance.compute_factor()
            self.factor_dict[f"{group}_{name}"][string] = factor_instance
            return factor_instance

        # 如果不覆盖
        elif not overwrite:
            # 如果factor_dict有因子类
            if f"{group}_{name}" in self.factor_dict:
                # 如果factor_dict有因子实例
                if string in self.factor_dict[f"{group}_{name}"]:
                    return self.factor_dict[f"{group}_{name}"][string]
                # 如果factor_dict没有因子实例
                else:
                    factor_instance.compute_factor()
                    self.factor_dict[f"{group}_{name}"][string] = factor_instance
                    return factor_instance
            # 如果factor_dict没有因子类
            else:
                factor_instance.compute_factor()
                self.factor_dict[f"{group}_{name}"][string] = factor_instance
                return factor_instance

    def save_factor(self, group: str, name: str, overwrite: bool = False, **params) -> None:
        """
        保存因子数据

        Parameters
        ----------
        group: str
                因子类别

        name: str
                因子名称

        overwrite: bool, default False
                    是否覆盖本地已有的因子数据

        params:
                因子参数

        Returns
        -------
        None
        """
        # 创建因子实例
        factor_class = self.import_factor_class(group=group, name=name)
        factor_instance = factor_class(**params)
        # 创建因子参数字符串
        string = self.get_file_name(factor_instance.get_params())

        # 因子文件路径
        factor_data_folder_path = self.factor_data_folder_path.joinpath(group).joinpath(name)
        # 如果没有因子文件路径，则创建
        if not os.path.exists(factor_data_folder_path):
            os.makedirs(factor_data_folder_path)

        # 如果不覆盖
        if not overwrite:
            if os.path.exists(str(factor_data_folder_path.joinpath(f"{string}.pkl"))):
                return
            else:
                # 如果有因子类
                if f"{group}_{name}" in self.factor_dict:
                    # 如果已经计算该因子该参数下的因子值, 则直接保存因子值
                    if string in self.factor_dict[f"{group}_{name}"]:
                        factor_data = self.factor_dict[f"{group}_{name}"][string].factor_value
                        factor_data.to_pickle(str(factor_data_folder_path.joinpath(f"{string}.pkl")))
                    # 如果没有计算该因子值该参数下的因子值, 则先计算因子值, 再保存因子值
                    else:
                        factor_instance.compute_factor()
                        factor_value = factor_instance.factor_value
                        self.factor_dict[f"{group}_{name}"][string] = factor_instance
                        factor_value.to_pickle(str(factor_data_folder_path.joinpath(f"{string}.pkl")))
                # 如果没有计算因子
                else:
                    factor_instance.compute_factor()
                    factor_value = factor_instance.factor_value
                    self.factor_dict[f"{group}_{name}"][string] = factor_instance
                    factor_value.to_pickle(str(factor_data_folder_path.joinpath(f"{string}.pkl")))
        elif overwrite:
            factor_instance.compute_factor()
            factor_value = factor_instance.factor_value
            self.factor_dict[f"{group}_{name}"][string] = factor_instance
            factor_value.to_pickle(str(factor_data_folder_path.joinpath(f"{string}.pkl")))



