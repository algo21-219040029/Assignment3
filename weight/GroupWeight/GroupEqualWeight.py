from pandas import DataFrame
from weight.GroupWeight.base import BaseGroupWeight

class GroupEqualWeight(BaseGroupWeight):

    def __init__(self, **params) -> None:
        """Constructor"""
        super().__init__(**params)

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
        single_weight_df = (single_signal_df.T / single_signal_df.sum(axis=1)).T
        return single_weight_df


