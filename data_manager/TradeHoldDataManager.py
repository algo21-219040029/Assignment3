import pandas as pd
from pathlib import Path
from typing import Dict
from pandas import DataFrame

class TradeHoldDataManager:

    def __init__(self) -> None:
        """Constructor"""
        self.trade_hold_data_folder_path: Path = Path(__file__).parent.parent.joinpath("data").joinpath("trade_hold")

        self.trade_hold_data_dict: Dict[str, DataFrame] = {}

    def get_trade_hold_data_by_rank(self, rank_by: str = 'volume', group_by_symbol: bool = True) -> DataFrame:
        """
        根据排名标准获取成交持仓数据

        Parameters
        ----------
        rank_by: str, default volume
                排名标准, volume, long,short

        group_by_symbol: bool, default True
                        是否根据品种聚合

        Returns
        -------
        data: 成交持仓数据
        """
        if rank_by in self.trade_hold_data_dict:
            return self.trade_hold_data_dict[rank_by]
        else:
            file_name = f"trade_hold_{rank_by}.pkl"
            data: DataFrame = pd.read_pickle(list(self.trade_hold_data_folder_path.glob(file_name))[0])
            symbol_exchange_map = data[['underlying_symbol', 'exchange']].drop_duplicates()

            # 如果有按品种为单位转化
            if group_by_symbol:
                data = data.set_index(['datetime', 'underlying_symbol', 'member_name']).groupby(['datetime', 'underlying_symbol', 'member_name'])[['volume', 'volume_change']].sum()
                data = data.reset_index()
                data['rank'] = rank_by
                data = pd.merge(left=data, right=symbol_exchange_map, on='underlying_symbol', how='left')
                data.sort_values(by=['datetime', 'underlying_symbol', 'volume'], ascending=[True, True, False], inplace=True)
            # 添加到数据字典
            self.trade_hold_data_dict[rank_by] = data
            return data

if __name__ == '__main__':
    self = TradeHoldDataManager()
    self.get_trade_hold_data_by_rank()
