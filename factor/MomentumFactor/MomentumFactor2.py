from pandas import DataFrame
from typing import Dict, Any
from factor.MomentumFactor.base import BaseMomentumFactor

class MomentumFactor2(BaseMomentumFactor):
    """
    将过去N个交易日的日平均日收益率/过去N个交易日的日收益率的标准差作为的动量因子

    Attributes
    __________
    R: int, default 5
        回溯期，即过去R天的平均收益率

    contract: str, default main
              连续合约以什么合约为基础，main or active_near

    price: str, default close
            选取连续合约的什么价格, close or settlement

    rebalance_num: int, default 1
                    选择几日换仓, 1 or 3 or 5

    """
    def __init__(self, R1: int = 5, R2: int = 5, contract: str = 'main', price: str = 'close', rebalance_num: int = 1) -> None:
        """
        Constructor

        Parameters
        ----------
        R1: int, default 5
            收益率回溯期，即过去R1天的平均收益率

        R2: int, default 5
            标准差回溯期，即过去R2天的收益率标准差

        contract: str, default main
                连续合约以什么合约为基础，main or active_near

        price: str, default close
                选取连续合约的什么价格, close or settlement

        rebalance_num: int, default 1
                    选择几日换仓, 1 or 3 or 5
        """
        super().__init__(R1=R1, R2=R2, contract=contract, price=price, rebalance_num=rebalance_num)

    def compute_factor(self) -> DataFrame:
        """
        计算因子值

        Parameters
        ----------

        Returns
        -------
        momentum_df: DataFrame
                    动量因子
        """
        # 获取收盘价
        params: Dict[str, Any] = self.get_params()
        R1: int = params['R1']
        R2: int = params['R2']
        contract: str = params['contract']
        price: str = params['price']
        rebalance_num: int = params['rebalance_num']
        price_df: DataFrame = self.get_continuous_field(contract=contract, price=price, rebalance_num=rebalance_num, field='continuous_price')
        return_df: DataFrame = price_df.pct_change()
        momentum_df: DataFrame = return_df.rolling(window=R1, min_periods=0).mean() / return_df.rolling(window=R2, min_periods=0).std()
        self.factor_value = momentum_df
        return momentum_df