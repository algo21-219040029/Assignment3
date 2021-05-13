import numpy as np
import pandas as pd
from typing import Dict, Any
from pandas import DataFrame
from commodity_pool.base import BaseCommodityPool

class DynamicPool4(BaseCommodityPool):
    """
    动态商品池4

    1.计算各品种主力合约过去60天平均成交金额的时间序列
    2.在建仓日, 计算各合约成交金额的40分位点, 以此为筛选标准
    3.在建仓日, 剔除掉平均成交金额小于40分位数的合约
    """
    def __init__(self, contract: str = 'main', price: str = 'close',
                 rebalance_num: int = 1, window: int = 60, q: float = 0.4) -> None:
        super().__init__(contract=contract, price=price, rebalance_num=rebalance_num, window=window,q=q)

    def compute_commodity_pool(self) -> DataFrame:
        """
        计算商品池

        Returns
        -------
        commodity_pool_value: DataFrame
                                商品池DataFrame
        """
        # 导入参数
        params: Dict[str, Any] = self.get_params()
        contract: str = params['contract']
        price: str = params['price']
        rebalance_num: int = params['rebalance_num']

        # 处理数据
        daily_data = self.daily_data_manager.get_daily_data()
        continuous_main_contract_df = self.continuous_contract_data_manager.\
                                    get_continuous_contract_data(contract=contract,
                                                                 price=price,
                                                                 rebalance_num=rebalance_num)
        turnover = pd.merge(left=daily_data[['datetime', 'contract', 'turnover', 'underlying_symbol']],
                            right=continuous_main_contract_df[['datetime', 'contract']],
                            on=['datetime', 'contract'], how='right')
        turnover = turnover.set_index(['datetime', 'underlying_symbol'])['turnover'].unstack(level=-1)

        symbol_list = ['C', 'CS', 'JR', 'RI', 'LR', 'PM', 'WH',
                       'A', 'B', 'M', 'Y', 'P', 'OI', 'RM', 'RS',
                       'SR', 'CF', 'JD', 'CU', 'AL', 'ZN', 'PB',
                       'NI', 'SN', 'AU', 'AG', 'RB', 'HC', 'WR', 'I',
                       'SF', 'SM', 'JM', 'J', 'BB', 'FB', 'V', 'FG',
                       'FU', 'ZC', 'RU', 'TA', 'PP', 'L', 'BU', 'MA']

        symbol_flag_list = [[True if symbol in symbol_list else False for symbol in turnover.columns]]*len(turnover)
        basic_pool = pd.DataFrame(symbol_flag_list, index=turnover.index, columns=turnover.columns)

        rolling_turnover = turnover.rolling(window=self.window, min_periods=0).mean()
        rolling_turnover[~basic_pool] = np.nan
        rolling_turnover_quantile = rolling_turnover.quantile(q=self.q, axis=1)
        commodity_pool_value = (rolling_turnover.T > rolling_turnover_quantile).T
        commodity_pool_value.fillna(False, inplace=True)
        self.commodity_pool_value = commodity_pool_value
        return commodity_pool_value

if __name__ == "__main__":
    self = DynamicPool4()
    self.compute_commodity_pool()
