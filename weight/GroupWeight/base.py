import numpy as np
from typing import Dict
from pandas import DataFrame
from abc import abstractmethod
from weight.base import BaseWeight

class BaseGroupWeight(BaseWeight):

    def __init__(self, **params) -> None:
        """Constructor"""
        super().__init__(**params)

    def get_weight(self) -> Dict[int, DataFrame]:
        """
        获取因子的权重DataFrame字典

        Returns
        -------
        weight_df_dict: Dict[int, DataFrame]
                        因子的权重DataFrame字典
        """
        if not isinstance(self.signal_df, DataFrame):
            raise ValueError("Please specify signal df first!")

        signal_df = self.signal_df
        new_signal_df = signal_df.copy()
        new_signal_df[signal_df <= 0.0] = np.nan
        max_num = signal_df.max().max()

        weight_df_dict: Dict[int, DataFrame] = {}
        for i in range(1, int(max_num)+1):
            df = new_signal_df.copy()
            df[df != i] = np.nan
            single_weight_df = self.get_single_weight(df)
            weight_df_dict[i] = single_weight_df

        self.weight_df_dict = weight_df_dict
        return weight_df_dict

    @abstractmethod
    def get_single_weight(self, single_signal_df: DataFrame) -> DataFrame:
        """
        获取因子各组的权重DataFrame

        Parameters
        __________
        signle_signal_df: DataFrame
                            单一组别的信号DataFrame, index为交易时间,columns为品种代码，data为信号

        Returns
        -------
        因子各组的权重DataFrame
        """
        raise NotImplementedError