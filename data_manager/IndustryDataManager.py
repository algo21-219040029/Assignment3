import os
import json
from pathlib import Path
from typing import Dict, List

class IndustryDataManager:
    """
    行业数据管理器
    """
    def __init__(self) -> None:
        """Constructor"""
        self.industry_data_folder_path: Path = Path(__file__).parent.parent.joinpath("data").joinpath("industry")

        self.industry_dict: Dict[str, List[str]] = {}
        self.industry_symbol_map_dict: Dict[str, Dict[str, List[str]]] = {}

    def get_all_industry_list(self) -> Dict[str, List[str]]:
        """
        获取所有的行业列表

        Returns
        -------
        industry_dict: Dict[str, List[str]]
                        行业列表,key为group,value为该group下的行业分类的name名称
        """
        if self.industry_dict:
            return self.industry_dict
        else:
            group_list: List[str] = os.listdir(self.industry_data_folder_path)
            industry_dict: Dict[str, List[str]] = {}
            for group in group_list:
                file_path: Path = self.industry_data_folder_path.joinpath(group)
                name_list: List[str] = [file.replace(".json", "") for file in os.listdir(file_path)]
                industry_dict[group] = name_list
            self.industry_dict = industry_dict
            return industry_dict

    def set_industry_symbol_map(self, group: str, name: str, industry_symbol_map: Dict[str, List[str]]):
        """
        设置行业

        Parameters
        ----------
        group: str
                行业表组

        name: str
                行业分组名

        industry_symbol_map: Dict[str, List[str]]
                            行业品种对应表

        Returns
        -------
        None
        """
        file_path = self.industry_data_folder_path.joinpath(group).joinpath(f"{name}.json")
        with open(str(file_path), "w") as f:
            json_industry_symbol_map = json.dumps(industry_symbol_map)
            f.write(json_industry_symbol_map)

    def get_all_industry_symbol_maps(self) -> Dict[str, Dict[str, List[str]]]:
        """
        获取所有行业品种映射表

        Returns
        -------

        """
        if self.industry_dict:
            industry_dict = self.industry_dict
        else:
            industry_dict = self.get_all_industry_list()

        for group in industry_dict:
            for name in industry_dict[group]:
                self.get_industry_symbol_map(group=group, name=name)
        industry_symbol_map_dict: Dict[str, Dict[str, List[str]]] = self.industry_symbol_map_dict
        return industry_symbol_map_dict


    def get_industry_symbol_map(self, group: str, name: str) -> Dict[str, List[str]]:
        """
        获取行业品种映射表

        Parameters
        ----------
        group: str
                行业表组

        name: str
                行业分组名

        Returns
        -------
        industry_symbol_map: Dict[str, List[str]]
                            行业品种映射表
        """
        file_path = self.industry_data_folder_path.joinpath(group).joinpath(f"{name}.json")
        with open(file_path, "rb") as f:
            industry_symbol_map = json.load(f)
        self.industry_symbol_map_dict[f"{group}_{name}"] = industry_symbol_map
        return industry_symbol_map