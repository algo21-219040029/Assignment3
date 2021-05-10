from pandas import DataFrame
from factor.MomentumFactor.base import BaseMomentumFactor

class MomentumFactor1(BaseMomentumFactor):

    """
    将过去一段时间的日平均日收益率作为的动量因子

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

    def __init__(self, R: int = 5, contract: str = 'main', price: str = 'close', rebalance_num: int = 1) -> None:
        """
        Constructor

        Parameters
        ----------
        R: int, default 5
            回溯期，即过去R天的平均收益率

        contract: str, default main
                连续合约以什么合约为基础，main or active_near

        price: str, default close
                选取连续合约的什么价格, close or settlement

        rebalance_num: int, default 1
                    选择几日换仓, 1 or 3 or 5
        """
        super().__init__(R=R, contract=contract, price=price, rebalance_num=rebalance_num)

    def compute_factor(self) -> DataFrame:
        """
        计算因子值

        Parameters
        ----------

        Returns
        -------
        momentum: DataFrame
                    动量因子
        """
        # 获取收盘价
        params = self.get_params()
        R = params['R']
        contract = params['contract']
        price = params['price']
        rebalance_num = params['rebalance_num']
        price_df = self.get_continuous_field(contract=contract, price=price, rebalance_num=rebalance_num, field='continuous_price')
        return_df = price_df.pct_change()
        momentum = return_df.rolling(window=R, min_periods=0).mean()
        self.factor_value = momentum
        return momentum
