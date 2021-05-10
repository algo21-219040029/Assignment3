from pandas import DataFrame
from factor.MomentumFactor.base import BaseMomentumFactor

class MomentumFactor3(BaseMomentumFactor):

    """
    将过去一段时间的R1日收益率之和与其平均R2日平滑之差作为的动量因子

    Attributes
    __________
    R1: int, default 5
        回溯期，即过去R1天的日收益率之和

    R2: int, default 5
        平滑回溯期，即过去R2天的日收益率之和

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
            回溯期1，即过去R1天的收益率之和

        R2: int, default 5
            回溯期2，即过去R2天的收益率之和的平均

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
        momentum: DataFrame
                    动量因子
        """
        # 获取收盘价
        params = self.get_params()
        R1 = params['R1']
        R2 = params['R2']
        contract = params['contract']
        price = params['price']
        rebalance_num = params['rebalance_num']
        price_df = self.get_continuous_field(contract=contract, price=price, rebalance_num=rebalance_num, field='continuous_price')
        return_df = price_df.pct_change()
        momentum = return_df.rolling(window=R1, min_periods=0).sum()
        mean_momentum = momentum.rolling(window=R2, min_periods=0).mean()
        factor_value = momentum - mean_momentum
        self.factor_value = factor_value
        return factor_value