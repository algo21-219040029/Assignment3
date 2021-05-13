import numpy as np
from typing import Dict, Any
from pandas import DataFrame

from weight.base import BaseWeight
from data_manager.ContinuousContractDataManager import ContinuousContractDataManager

class BaseVolWeight(BaseWeight):
    """
    波动率相关权重生成器
    """

    def __init__(self, contract: str = 'main', price: str = 'close', rebalance_num: int = 1, **params) -> None:
        """Constructor"""
        super().__init__(contract=contract, price=price, rebalance_num=rebalance_num, **params)
        self.continuous_contract_data_manager: ContinuousContractDataManager = ContinuousContractDataManager()

    def get_rolling_std(self, window: int) -> DataFrame:
        """
        获取滚动N日的标准差

        Parameters
        ----------
        window: int
                时间窗口


        Returns
        -------
    `   std_df: DataFrame
                window日滚动标准差
        """
        # 提取参数
        params: Dict[str, Any] = self.get_params()
        contract: str = params['contract']
        price: str = params['price']
        rebalance_num: int = params['rebalance_num']

        # 获取连续合约价格
        return_df: DataFrame = self.continuous_contract_data_manager.get_field(contract=contract, price=price,
                                                                              rebalance_num=rebalance_num, field='return')

        std_df: DataFrame = return_df.rolling(window=window, min_periods=0).std()
        return std_df


    def get_weight(self) -> DataFrame:
        """
        获取权重DataFrame

        Returns
        -------
        weight_df: DataFrame
                权重DataFrame
        """
        pass