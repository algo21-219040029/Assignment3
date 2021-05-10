import os
import numpy as np
import pandas as pd
from pathlib import Path

from pandas import (Series,
                    DataFrame)
import matplotlib.pyplot as plt
from typing import Callable, Tuple
from collections import defaultdict

from utils.utility import stack_dataframe_by_fields

from bases.base import BaseClass
from data_manager.ContinuousContractDataManager import ContinuousContractDataManager

plt.rcParams['font.family'] = ['sans-serif']
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False


def get_q_later_quantile(factor_data_with_pool: DataFrame, quantile: float) -> Tuple[DataFrame]:
    """"""
    def apply_quantile1(series):
        series.fillna(0.0, inplace=True)
        quantile1 = series[series > 0].quantile(q=quantile, interpolation='midpoint')
        return quantile1

    def apply_quantile2(series):
        series.fillna(0.0, inplace=True)
        quantile2 = series[series < 0].quantile(q=1 - quantile, interpolation='midpoint')
        return quantile2

    quantile1 = factor_data_with_pool.apply(func=apply_quantile1, axis=1)
    quantile2 = factor_data_with_pool.apply(func=apply_quantile2, axis=1)
    return quantile1, quantile2


class FactorTest(BaseClass):

    def __init__(self, file_path: str) -> None:
        """Constructor"""
        self.file_path: Path = Path(file_path)

        self.factor_name: str = None
        self.factor_data: DataFrame = None
        self.factor_data_with_pool: DataFrame = None

        self.commodity_pool: DataFrame = None

        self.long_short_quantile_func: Callable = None

        self.quantile1: Series = None
        self.quantile2: Series = None

        self.continuous_contract_data_manager: ContinuousContractDataManager = ContinuousContractDataManager()

        # 如果不存在因子绩效保存路径，则需要先创建
        self.init_file_path()

    def init_file_path(self) -> None:
        """
        初始化各类文件夹
        :return: None
        """
        if not os.path.exists(self.file_path):
            os.makedirs(self.file_path)

        distribution_file_path = self.file_path.joinpath("factor_distribution")
        if not os.path.exists(distribution_file_path):
            os.makedirs(distribution_file_path)

        time_series_file_path = self.file_path.joinpath('time_series')
        if not os.path.exists(time_series_file_path):
            os.makedirs(time_series_file_path)

        all_time_series_file_path = time_series_file_path.joinpath("all_time_series")
        if not os.path.exists(all_time_series_file_path):
            os.makedirs(all_time_series_file_path)

        time_series_per_symbol_file_path = self.file_path.joinpath("time_series").joinpath("time_series_per_symbol")
        if not os.path.exists(time_series_per_symbol_file_path):
            os.makedirs(time_series_per_symbol_file_path)

        time_series_per_symbol_2016_file_path = self.file_path.joinpath('time_series').\
            joinpath("time_series_per_symbol_2016")
        if not os.path.exists(time_series_per_symbol_2016_file_path):
            os.makedirs(time_series_per_symbol_2016_file_path)

        autocorrelation_file_path = self.file_path.joinpath("autocorrelation")
        if not os.path.exists(autocorrelation_file_path):
            os.makedirs(autocorrelation_file_path)
        autocorrelation_all_symbols_file_path = autocorrelation_file_path.joinpath("all_symbols")
        if not os.path.exists(autocorrelation_all_symbols_file_path):
            os.makedirs(autocorrelation_all_symbols_file_path)

        average_autocorrelation_file_path = autocorrelation_file_path.joinpath("average_autocorrelation")
        if not os.path.exists(average_autocorrelation_file_path):
            os.makedirs(average_autocorrelation_file_path)

    def set_factor(self, factor_name: str, factor_data: DataFrame) -> None:
        """
        设置因子值DataFrame
        :param factor_name: 因子名，主要用于保存因子情况相关稳健时使用
        :param factor_data: 因子值，index为交易日期，columns为合约代码
        :return: None
        """
        self.factor_name = factor_name
        self.factor_data = factor_data

    def set_commodity_pool(self, commodity_pool: DataFrame) -> None:
        """
        设置商品池
        :param commodity_pool: 商品池，index为交易日期，columns为合约代码
        :return: None
        """
        self.commodity_pool = commodity_pool

    def set_factor_quantile(self, func: Callable, **params) -> None:
        """
        设置多空分位数
        :param func: 设置多空分位数的函数
        :param params: long_short_quantile_func函数的参数
        :return: None
        """
        if not isinstance(self.factor_data, DataFrame):
            raise ValueError("Please specify factor data first!")
        else:
            factor_data = self.factor_data
        if hasattr(self, "commodity_pool"):
            commodity_pool = self.commodity_pool
            factor_data_with_pool = factor_data.copy()
            factor_data_with_pool[~commodity_pool] = np.nan
        else:
            factor_data_with_pool = factor_data.copy()
        self.factor_data_with_pool = factor_data_with_pool

        quantile1, quantile2 = func(factor_data_with_pool, **params)
        self.quantile1 = quantile1
        self.quantile2 = quantile2

    def get_continuous_field(self, contract: str = 'main', price: str = 'close', rebalance_num: int = 1, field: str = 'continuous_price') -> DataFrame:
        """
        获取连续合约指定字段的数据

        Parameters
        ----------
        contract: str
                合约种类，目前可选有main和active_near, main表示主力合约, active_near表示活跃近月

        price: str
                选择以什么价格为基础的连续数据, close为收盘价, settlement结算价

        rebalance_num: int, default = 1
                换仓天数, 可选天数1,3,5
        field: str, default = 'continuous_price'
                字典，continuous_price

        Returns
        -------
        df: DataFrame
            连续合约field字段数据, 一般是开盘价或收盘价
        """
        return self.continuous_contract_data_manager.get_field(contract=contract,
                                                               price=price,
                                                               rebalance_num=rebalance_num,
                                                               field=field
                                                               )

    def get_rankX_y_plot(self):
        """rankX_y因子收益率图"""

        # 预先检查
        if not isinstance(self.factor_data_with_pool, DataFrame):
            raise ValueError("factor data with pool is not defined!")

        factor_data_with_pool = self.factor_data_with_pool


        factor_data_with_pool_rank = factor_data_with_pool.rank(axis=1, ascending=True)
        factor_data_with_pool_rank = factor_data_with_pool_rank.stack(dropna=True).to_frame("factor")

        # return_df = return_df.stack(dropna=True).to_frame("return")

        # data = pd.concat([factor_data_with_pool, return_df], axis=1)
        # plt.scatter(x=data["factor"], y=data['return'])



    def get_factor_distribution(self, **params) -> None:
        """获取因子值分布直方图"""
        if not isinstance(self.factor_data, DataFrame):
            raise ValueError("Please specify factor data first!")
        else:
            factor_data = self.factor_data

        if 'figsize' in params:
            plt.figure(params['figsize'])
        else:
            plt.figure(figsize=(20, 8))
        if 'bins' in params:
            plt.hist(x=factor_data.values.flatten(), bins=params['bins'])
        else:
            plt.hist(x=factor_data.values.flatten(), bins=500)
        if 'xlim' in params:
            plt.xlim(params['xlim'][0], params['xlim'][1])

        distribution_file_path = self.file_path.joinpath("factor_distribution")
        if not os.path.exists(distribution_file_path):
            os.makedirs(distribution_file_path)
        plt.title(f"{self.factor_name} distribution")
        plt.grid()
        plt.savefig(distribution_file_path.joinpath(f"{self.factor_name} distribution.png"))
        plt.show()

        stack_factor_data = factor_data.stack()
        factor_description = stack_factor_data.describe()
        factor_description['skew'] = stack_factor_data.skew()
        factor_description['kurt'] = stack_factor_data.kurt()

        factor_description.to_csv(distribution_file_path.joinpath(f"{self.factor_name} description.csv"))

    def get_factor_time_series(self, **params) -> None:
        """获取因子时序图"""
        if not isinstance(self.factor_data, DataFrame):
            raise ValueError("Please specify factor data first!")
        else:
            factor_data = self.factor_data

        if 'figsize' in params:
            plt.figure(figsize=params['figsize'])
        else:
            plt.figure(figsize=(15, 8))

        time_series_file_path = self.file_path.joinpath('time_series')
        if not os.path.exists(time_series_file_path):
            os.makedirs(time_series_file_path)

        all_time_series_file_path = time_series_file_path.joinpath("all_time_series")
        if not os.path.exists(all_time_series_file_path):
            os.makedirs(all_time_series_file_path)
        factor_data.plot(legend=False)
        plt.title(f"{self.factor_name}_all_time_series")
        plt.grid()
        plt.savefig(all_time_series_file_path.joinpath(f"{self.factor_name}_all_time_series.png"))
        plt.show()

    def get_factor_time_series_per_symbol(self, **params):
        """
        获取每个品种的时间序列图，配上分位数曲线
        :param params: None
        :return: None
        """
        if not isinstance(self.factor_data_with_pool, DataFrame):
            raise ValueError("Please specify factor data with pool first!")
        else:
            factor_data_with_pool = self.factor_data_with_pool

        if not hasattr(self, "quantile1") or not hasattr(self, "quantile2"):
            raise ValueError("Please specify quantile1 and quantile2 first!")
        else:
            quantile1 = self.quantile1
            quantile2 = self.quantile2

        if 'figsize' in params:
            plt.figure(figsize=params['figsize'])
        else:
            plt.figure(figsize=(15, 8))
        time_series_per_symbol_file_path = self.file_path.joinpath("time_series").joinpath("time_series_per_symbol")
        if not os.path.exists(time_series_per_symbol_file_path):
            os.makedirs(time_series_per_symbol_file_path)
        for symbol in factor_data_with_pool.columns:
            if 'figsize' in params:
                plt.figure(figsize=params['figsize'])
            else:
                plt.figure(figsize=(20, 8))
            factor_data_with_pool[symbol].plot(label=f'{symbol}_{self.factor_name}')
            quantile1.plot(label='quantile1')
            quantile2.plot(label='quantile2')
            plt.legend()
            plt.title(f"{self.factor_name}_{symbol}_time_series")
            plt.grid()
            plt.savefig(time_series_per_symbol_file_path.joinpath(f"{symbol}_{self.factor_name}_time_series.png"))
            plt.show()

    def get_factor_time_series_per_symbol_2016(self, **params):
        """
        获取每个品种的时间序列图，配上分位数曲线
        :param params: None
        :return: None
        """
        if not isinstance(self.factor_data_with_pool, DataFrame):
            raise ValueError("Please specify factor data with pool first!")
        else:
            factor_data_with_pool = self.factor_data_with_pool

        if not hasattr(self, "quantile1") or not hasattr(self, "quantile2"):
            raise ValueError("Please specify quantile1 and quantile2 first!")
        else:
            quantile1 = self.quantile1
            quantile2 = self.quantile2

        if 'figsize' in params:
            plt.figure(figsize=params['figsize'])
        else:
            plt.figure(figsize=(15, 8))
        time_series_per_symbol_2016_file_path = self.file_path.joinpath("time_series").joinpath("time_series_per_symbol_2016")
        if not os.path.exists(time_series_per_symbol_2016_file_path):
            os.makedirs(time_series_per_symbol_2016_file_path)
        for symbol in factor_data_with_pool.columns:
            if 'figsize' in params:
                plt.figure(figsize=params['figsize'])
            else:
                plt.figure(figsize=(20, 8))
            factor_data_with_pool[symbol]['2016'].plot(label=f'2016年日度{symbol}_{self.factor_name}')
            quantile1['2016'].plot(label='quantile1')
            quantile2['2016'].plot(label='quantile2')
            plt.legend()
            plt.title(f"2016年日度_{self.factor_name}_{symbol}_time_series")
            plt.grid()
            plt.savefig(time_series_per_symbol_2016_file_path.joinpath(f"2016年日度_{symbol}_{self.factor_name}_time_series.png"))
            plt.show()

    def get_factor_autocorrelation(self, **params):
        if not isinstance(self.factor_data, DataFrame):
            raise ValueError("Please specify factor data first!")
        else:
            factor_data = self.factor_data

        if 'lags' in params:
            lags = params['lag']
        else:
            lags = 100

        autocorrelation_file_path = self.file_path.joinpath("autocorrelation")
        if not os.path.exists(autocorrelation_file_path):
            os.makedirs(autocorrelation_file_path)
        autocorrelation_all_symbols_file_path = autocorrelation_file_path.joinpath("all_symbols")
        if not os.path.exists(autocorrelation_all_symbols_file_path):
            os.makedirs(autocorrelation_all_symbols_file_path)
        autocorr_dict = defaultdict(list)
        for symbol in factor_data.columns:
            factor_series = factor_data[symbol]
            for lag in range(1, lags+1, 1):
                autocorr_dict[symbol].append(factor_series.autocorr(lag=lag))
        autocorr_df = pd.DataFrame(autocorr_dict, index=range(1, lag+1, 1))

        if 'figsize' in params:
            plt.figure(figsize=params['figsize'])
        else:
            plt.figure(figsize=(20, 8))
        autocorr_df.plot(legend=False)
        plt.title(f"{self.factor_name}_autocorrelation_all_symbols")
        plt.grid()
        plt.savefig(autocorrelation_all_symbols_file_path.joinpath("all_symbols_autocorrelation.png"))
        plt.show()

        average_autocorrelation_file_path = autocorrelation_file_path.joinpath("average_autocorrelation")
        if not os.path.exists(average_autocorrelation_file_path):
            os.makedirs(average_autocorrelation_file_path)
        average_autocorr_series = autocorr_df.mean(axis=1)

        if 'figsize' in params:
            plt.figure(figsize=params['figsize'])
        else:
            plt.figure(figsize=(20, 8))

        average_autocorr_series.plot()
        plt.title(f"{self.factor_name}_average_autocorrelation")
        plt.grid()
        plt.savefig(average_autocorrelation_file_path.joinpath("average_autocorrelation.png"))
        plt.show()

    def run_all(self):
        self.get_factor_distribution()
        self.get_factor_time_series_per_symbol()
        self.get_factor_time_series_per_symbol_2016()
        self.get_factor_time_series()
        self.get_factor_autocorrelation()
        





