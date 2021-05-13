import os
import pandas as pd
from pathlib import Path
from typing import List, Dict
import matplotlib.pyplot as plt
from pandas import Series, DataFrame

from data_manager.IndustryDataManager import IndustryDataManager
from data_manager.ContinuousContractDataManager import ContinuousContractDataManager

plt.rcParams['font.sans-serif'] = ['KaiTi'] # 指定默认字体
plt.rcParams['axes.unicode_minus'] = False # 解决保存图像是负号'-'显示为方块的问题

class IndexDataManager:
    """
    指数数据提取器
    """
    def __init__(self) -> None:
        """"""
        self.index_data_path: Path = Path(__file__).parent.parent.joinpath("data").joinpath("index")

        self.industry_data_manager: IndustryDataManager = IndustryDataManager()
        self.continuous_contract_data_manager: ContinuousContractDataManager = ContinuousContractDataManager()

    def get_commodity_index(self) -> Series:
        """
        获取商品指数

        Returns
        -------
        commodity_index: Series
                        商品指数,index是交易时间, data是指数数值
        """
        commodity_index: Series = pd.read_pickle(self.index_data_path.joinpath("commodity_index.pkl"))
        commodity_index = commodity_index['2010':]
        commodity_index = commodity_index / commodity_index.iloc[0]
        return commodity_index

    def get_industry_list(self) -> List[str]:
        """
        获取行业列表

        Returns
        -------
        industry_list: List[str]
                        行业列表
        """
        industry_list: List[str] = [file.replace(".pkl","") for file in os.listdir(str(self.index_data_path)) if 'commodity_index' not in file]
        return industry_list

    def get_industry_index(self, industry_name: str) -> Series:
        """
        获取行业指数

        Parameters
        ----------
        industry_name: str
                        行业名称

        Returns
        -------
        industry_index: Series
                        行业指数
        """
        industry_index: Series = pd.read_pickle(self.index_data_path.joinpath(f"{industry_name}.pkl")).dropna()
        industry_index = industry_index['2010': ]
        industry_index = industry_index / industry_index.iloc[0]
        return industry_index

    def get_all_industry_index(self) -> DataFrame:
        """
        获取所有行业的指数

        Returns
        -------

        """
        industry_list: List[str] = self.get_industry_list()
        industry_index_list: List[Series] = []
        for industry in industry_list:
            industry_index_list.append(self.get_industry_index(industry).to_frame(industry))
        industry_index_df: DataFrame = pd.concat(industry_index_list, axis=1)
        return industry_index_df

    def get_symbol_index(self, symbol: str) -> Series:
        """
        获取品种指数

        Parameters
        ----------
        symbol: str
                品种代码

        Returns
        -------
        symbol_index: Series
                        品种指数
        """
        symbol_index: Series = self.continuous_contract_data_manager.get_field(field='cum_return')[symbol].dropna()
        symbol_index = symbol_index['2010': ]
        symbol_index = symbol_index / symbol_index.iloc[0]
        return symbol_index

    def get_industry_symbol_map(self) -> Dict[str, List[str]]:
        """
        获取行业品种映射表

        Returns
        -------

        """
        industry_symbol_map: Dict[str, List[str]] = self.industry_data_manager.get_industry_symbol_map(group='actual_industry', name='actual_five_industry')
        return industry_symbol_map

    def plot_all_industry(self) -> None:
        """
        画总商品指数和行业指数

        Returns
        -------
        None
        """
        all_index_df: DataFrame = self.get_commodity_index().to_frame("all")
        industry_index_df: DataFrame = self.get_all_industry_index()
        fig, ax = plt.subplots(figsize=(20, 8))
        all_index_df.plot(ax=ax, figsize=(20, 8), linewidth=7)
        industry_index_df.plot(ax=ax, figsize=(20, 8))
        plt.title("总商品指数和各行业指数")
        plt.legend()
        plt.grid()
        plt.show()

    def plot_industry_symbol(self, industry_name: str) -> None:
        """
        画行业及该行业品种指数

        Parameters
        __________
        industry_name: str
                        行业名称

        Returns
        -------

        """
        industry_index_df: DataFrame = self.get_industry_index(industry_name).to_frame("industry")
        industry_symbol_map: Dict[str, List[str]] = self.get_industry_symbol_map()
        symbol_list: List[str] = industry_symbol_map[industry_name]
        symbol_index_list: List[Series] = []
        for symbol in symbol_list:
            symbol_index_list.append(self.get_symbol_index(symbol).to_frame(symbol))
        symbol_index_df: DataFrame = pd.concat(symbol_index_list, axis=1)
        fig, ax = plt.subplots(figsize=(20, 8))
        industry_index_df.plot(ax=ax, figsize=(20, 8), linewidth=5)
        symbol_index_df.plot(ax=ax, figsize=(20, 8))
        plt.title(f"{industry_name}指数和各品种指数")
        plt.legend()
        plt.grid()
        plt.show()

if __name__ == "__main__":
    self = IndexDataManager()
    self.plot_all_industry()
    self.plot_industry_symbol("农产品_软商品")
    self.plot_industry_symbol("化工能源")
    self.plot_industry_symbol("有色_贵金属")
    self.plot_industry_symbol("油脂油料")
    self.plot_industry_symbol("黑色")