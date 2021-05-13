import inspect
import pandas as pd
from pathlib import Path
from pandas import DataFrame
from datetime import timedelta
from abc import ABC, abstractmethod

from bases.base import BaseClass
from data_manager.DailyDataManager import DailyDataManager
from data_manager.BasicsDataManager import BasicsDataManager
from data_manager.ContinuousContractDataManager import ContinuousContractDataManager

class BaseCommodityPool(BaseClass):

    def __init__(self, **params) -> None:
        """Constructor"""
        super().__init__(**params)

        self.group: str = Path(inspect.getfile(self.__class__)).parent.name
        self.name: str = self.__class__.__name__

        self.daily_data_manager: DailyDataManager = DailyDataManager()
        self.basics_data_manager: BasicsDataManager = BasicsDataManager()
        self.continuous_contract_data_manager: ContinuousContractDataManager = ContinuousContractDataManager()

        self.daily_volume: DataFrame = None
        self.listed_date_df: DataFrame = None
        self.all_instruments: DataFrame = None
        self.commodity_pool_value: DataFrame = None

    def get_limited_date(self, days: int = 60) -> DataFrame:
        if not isinstance(self.all_instruments, DataFrame):
            all_instruments = self.basics_data_manager.get_all_instruments()
        else:
            all_instruments = self.all_instruments
        listed_date_df = all_instruments.sort_values(by='contract').\
            groupby('underlying_symbol', as_index=True)['listed_date'].nth(0).to_frame('listed_date')
        listed_date_df['listed_date'] = pd.to_datetime(listed_date_df['listed_date'])
        listed_date_df['limited_date'] = pd.DatetimeIndex(listed_date_df['listed_date']) + timedelta(days=days)
        self.listed_date_df = listed_date_df
        return listed_date_df

    def get_volume_per_symbol(self, window: int = 60) -> DataFrame:
        daily_data = self.daily_data_manager.get_daily_data()
        daily_volume = daily_data.groupby(['datetime', 'underlying_symbol'], as_index=True)['volume'].sum()
        daily_volume = daily_volume.unstack(level=-1).rolling(window=60, min_periods=0).mean().stack().\
            to_frame('volume').reset_index()
        self.daily_volume = daily_volume
        return daily_volume

    def set_commodity_pool_value(self, commodity_pool_value: DataFrame) -> None:
        self.commodity_pool_value = commodity_pool_value

    def get_commodity_pool_value(self) -> DataFrame:
        return self.commodity_pool_value

    @abstractmethod
    def compute_commodity_pool(self) -> DataFrame:
        raise NotImplementedError

    def __repr__(self):
        group = self.group
        name = self.name
        title = ''
        title += f"commodity_pool(group={group}, name={name}, "
        # 添加因子参数
        for key, value in self.get_params().items():
            title += f"{key}={value}, "
        title = title[:-2]
        title += ")"
        return title

    def get_string(self) -> str:
        return self.__repr__()
