from pandas import DataFrame
from typing import Dict, Any, Callable, Union

from utils.utility import (Dev1, Dev2, Dev3)
from factor.VolatilityFactor.base import BaseVolatilityFactor

class VolatilityFactor1(BaseVolatilityFactor):
    """
    基于标准差的波动率因子

    Attributes
    __________
    contract: str, default main
            连续合约基于的合约

    price: str, default close
            连续合约基于的价格

    rebalance_num: int, default 1
                连续合约几日换仓

    func: Union[str, Callable], default None
            在因子值基础上叠加的函数
    """

    def __init__(self,
                 contract: str = 'main',
                 price: str = 'close',
                 rebalance_num: int = 1,
                 window: int = 5,
                 func: Union[str, Callable] = None,
                 R: int = 5) -> None:
        """Constructor"""
        super().__init__(contract=contract,
                         price=price,
                         rebalance_num=rebalance_num,
                         window=window,
                         func=func,
                         R=R)

    def compute_factor(self) -> DataFrame:
        """
        计算因子

        Returns
        -------
        rolling_std_df: DataFrame
                        window日滚动收益率标准差
        """
        params: Dict[str, Any]  = self.get_params()
        contract: str = params['contract']
        price: str = params['price']
        rebalance_num: int = params['rebalance_num']
        window: int = params['window']
        func: Union[str, Callable] = params['func']

        return_df: DataFrame = self.get_continuous_field(contract=contract, price=price, rebalance_num=rebalance_num, field='return')
        rolling_std_df: DataFrame = return_df.rolling(window=window, min_periods=0).std()

        # 提取参数
        R = self.get_params()['R']

        # 叠加函数
        if not func:
            pass
        elif func == 'Dev1':
            rolling_std_df = Dev1(rolling_std_df, R)
        elif func == 'Dev2':
            rolling_std_df = Dev2(rolling_std_df, R)
        elif func == 'Dev3':
            rolling_std_df = Dev3(rolling_std_df, R)
        elif isinstance(func, Callable):
            rolling_std_df = func(rolling_std_df, R)
        else:
            raise NameError("func is not expected!")

        self.factor_value: DataFrame = rolling_std_df
        return rolling_std_df

if __name__ == "__main__":
    self = VolatilityFactor1()

