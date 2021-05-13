import numpy as np
import pandas as pd
from pandas import DataFrame
from commodity_pool.base import BaseCommodityPool

class DynamicPool5(BaseCommodityPool):

    def __init__(self, contract: str = 'main', price: str = 'close',
                 rebalance_num: int = 1, window: int = 60, q: float = 0.4) -> None:
        super().__init__(contract=contract, price=price, rebalance_num=rebalance_num, window=window,q=q)

    def compute_commodity_pool(self) -> DataFrame:
        daily_data = self.daily_data_manager.get_daily_data()
        continuous_main_contract_df = self.continuous_contract_data_manager.\
                                    get_continuous_contract_data(contract=self.contract,
                                                                 price=self.price,
                                                                 rebalance_num=self.rebalance_num)
        turnover = pd.merge(left=daily_data[['datetime', 'contract', 'turnover', 'underlying_symbol']],
                            right=continuous_main_contract_df[['datetime', 'main_contract']].
                            rename(columns={'main_contract': 'contract'}),
                            on=['datetime', 'contract'], how='right')
        turnover = turnover.set_index(['datetime', 'underlying_symbol'])['turnover'].unstack(level=-1)

        symbol_list = ['M', 'Y', 'P', 'CS', 'C', 'FB', 'BB', 'JD', 'A', 'B', 'RI', 'LR', 'WH', 'PM',
                       'CF', 'JR', 'CJ', 'RS', 'OI', 'RM', 'SR', 'AP', 'CU', 'AL', 'ZN', 'PB', 'NI',
                       'RB', 'HC', 'WR', 'I', 'RU', 'FU', 'BU', 'JM', 'J', 'V', 'L', 'TA', 'FG', 'SP', 'PP', 'EG']

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
    self = DynamicPool5()
    self.compute_commodity_pool()