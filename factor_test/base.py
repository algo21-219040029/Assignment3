import os
import pandas as pd
from pathlib import Path
from pandas import (Index,
                    Series,
                    DataFrame)
import matplotlib.pyplot as plt
from typing import Dict, Any, List
from collections import defaultdict

from bases.base import BaseClass
from factor.base import BaseFactor
from data_manager.FactorDataManager import FactorDataManager
from data_manager.IndustryDataManager import IndustryDataManager
from data_manager.ContinuousContractDataManager import ContinuousContractDataManager

#解决中文显示问题
plt.rcParams['font.sans-serif'] = ['KaiTi'] # 指定默认字体
plt.rcParams['axes.unicode_minus'] = False # 解决保存图像是负号'-'显示为方块的问题

class BaseFactorTest(BaseClass):

    def __init__(self) -> None:
        """Constructor"""
        super().__init__()
        self.factor_data_manager: FactorDataManager = FactorDataManager()
        self.industry_data_manager: IndustryDataManager = IndustryDataManager()
        self.continuous_contract_data_manager: ContinuousContractDataManager = ContinuousContractDataManager()
        self.factor_test_result_path: Path = Path(__file__).parent.parent.joinpath("output_result")

        self.factor_info: Dict[str, str] = {}
        self.factor: BaseFactor = None
        self.factor_params: Dict[str, Any] = {}

    def set_factor(self, group: str, name: str, **params) -> None:
        """
        设置因子

        Parameters
        ----------
        group: str
                因子类别

        name: str
                因子名称

        params: 因子参数

        Returns
        -------
        None
        """
        self.factor_info = {'group': group, 'name': name}
        self.factor = self.factor_data_manager.get_factor(group=group, name=name, **params)
        self.factor_params = self.factor.get_params()

    def get_factor_autocorrelation(self, **params) -> None:
        """
        获取因子自相关性图

        Parameters
        ----------
        params: 参数, 包括输出图的figsize, 滞后期数lags

        Returns
        -------
        None
        """

        # 预先检查, 如果没有因子, 则报错
        if not isinstance(self.factor, BaseFactor):
            raise ValueError("Please specify factor first!")
        else:
            factor = self.factor
        factor_value = factor.factor_value

        # 确定滞后阶数参数
        if 'lags' in params:
            lags = params['lags']
        else:
            lags = 100

        # 生成自相关性数据
        autocorr_dict = defaultdict(list)
        for symbol in factor_value.columns:
            factor_series = factor_value[symbol]
            for lag in range(1, lags+1, 1):
                autocorr_dict[symbol].append(factor_series.autocorr(lag=lag))
        autocorr_df = pd.DataFrame(autocorr_dict, index=range(1, lags+1, 1))
        autocorr_series = autocorr_df.mean(axis=1)

        picture_name = factor.__repr__()

        # 输出的路径
        factor_folder_path = self.factor_test_result_path.joinpath(factor.group).joinpath(factor.name)
        if not os.path.exists(factor_folder_path):
            os.makedirs(factor_folder_path)
        autocorrelation_file_path = factor_folder_path.joinpath(f"{picture_name} autocorrelation.png")

        # 图片大小
        if 'figsize' in params:
            plt.figure(figsize=params['figsize'])
        else:
            plt.figure(figsize=(20, 8))

        autocorr_series.plot()

        plt.title(f"{picture_name} autocorrelation")
        plt.grid()
        plt.savefig(str(autocorrelation_file_path))

    def get_Frank_vs_Rrank(self,
                           group_num: int = 5,
                           period: int = 1,
                           start: str = None,
                           end: str = None,
                           industry: str = None):
        """
        获取因子rank分组与收益率rank的violin plot

        Attributes
        __________
        group_num: int, default 5
                    分组组数

        period: int, default 1
                收益率周期长度

        start: str, default None
                起始日期

        end: str, default None
                结束日期

        Returns
        -------
        None
        """
        # 获取收益率及收益率排序
        price_df: DataFrame = self.continuous_contract_data_manager.get_field(field='continuous_price')
        if start:
            price_df = price_df[start:]
        if end:
            price_df = price_df[:end]
        if industry:
            industry_symbol_map: Dict[str, List[str]] = self.industry_data_manager. \
                get_industry_symbol_map(group='actual_industry', name='actual_five_industry')
            symbol_index = pd.Index(industry_symbol_map[industry])
            symbol_index = symbol_index.intersection(price_df.columns)
            price_df = price_df[symbol_index]

        # index = pd.Index(range(len(price_df)))
        # selected_num_index = index[(index - shift) % period == 0]
        # price_df = price_df.iloc[selected_num_index]
        return_df = price_df.pct_change(period).shift(-period)
        return_rank_df: DataFrame = return_df.rank(axis=1, ascending=False, method='first', na_option='keep')
        return_rank_df = (return_rank_df.T - return_rank_df.min(axis=1)) / (return_rank_df.max(axis=1)-return_rank_df.min(axis=1))
        selected_index = return_df.index

        # 预先检查, 如果没有因子, 则报错
        if not isinstance(self.factor, BaseFactor):
            raise ValueError("Please specify factor first!")
        else:
            factor = self.factor

        # 获取因子值
        factor_df: DataFrame = factor.factor_value
        selected_index = selected_index.intersection(factor_df.index)
        factor_df = factor_df.loc[selected_index]
        factor_rank_df: DataFrame = factor_df.rank(axis=1, ascending=False, method='first', na_option='keep')
        factor_rank_df = (factor_rank_df.T - factor_rank_df.min(axis=1)) / (factor_rank_df.max(axis=1)-factor_rank_df.min(axis=1))

        # 因子值排序和收益率排序做交集处理
        common_index = return_rank_df.index.intersection(factor_rank_df.index)
        common_columns = return_rank_df.columns.intersection(factor_rank_df.columns)
        return_rank_df = return_rank_df.loc[common_index, common_columns]
        factor_rank_df = factor_rank_df.loc[common_index, common_columns]

        stack_return_rank_df: DataFrame = return_rank_df.stack().to_frame('return').reset_index()
        stack_factor_rank_df: DataFrame = factor_rank_df.stack().to_frame('factor').reset_index()

        stack_df = pd.merge(left=stack_return_rank_df, right=stack_factor_rank_df, on=['datetime', 'underlying_symbol'], how='inner')

        def modified_qcut(series: Series, q: int, labels: List[Any]):
            """
            修正的qcut

            Parameters
            ----------
            series: Series
                    因子排序值rank

            q: int
               分组组数

            labels: List[Any]
                    标签, 如分为5组, [1,2,3,4,5]

            Returns
            -------
            组别: Series
            """

            # 如果有效数据个数小于组别, 则全部为0, 即不持仓
            if series.count() < q:
                new_series = pd.Series([0.0]*len(series))
                new_series.index = series.index
                return new_series
            else:
                return pd.qcut(x=series, q=q, labels=labels)

        stack_df['factor_group'] = stack_df.groupby(by='datetime')['factor'].apply(modified_qcut, q=group_num, labels=range(1, group_num+1))

        group_list: List[int] = list(range(1, group_num+1, 1))
        factor_return_df = stack_df[['factor_group', 'return']]
        data = []
        for i in group_list:
            data.append(factor_return_df[factor_return_df['factor_group']==i]['return'].dropna().tolist())

        fig, ax = plt.subplots(figsize=(20, 8))
        ax.violinplot(data, showmeans=True, showextrema=True)

        factor_string = self.factor.get_string()
        ax.set_title(f"{factor_string}\n Return rankIC vs Factor rankIC group_num={group_num} period={period}")

        # ax.set_xticklabels([(y)/len(data) for y in range(len(data)+1)])
        plt.xticks(ticks=[y for y in range(1, len(data)+1)], labels=[y/group_num for y in range(1, len(data)+1)])
        plt.yticks(ticks=[y/group_num for y in range(1, len(data)+1)])
        plt.show()

    def get_factor_distribution_per_symbol(self):
        """
        画每个品种的因子分布violin图

        Returns
        -------
        None
        """
        # 预先检查, 如果没有因子, 则报错
        if not isinstance(self.factor, BaseFactor):
            raise ValueError("Please specify factor first!")
        else:
            factor = self.factor

        # 获取因子值
        factor_value: DataFrame = factor.factor_value

        industry_symbol_map: Dict[str, List[str]] = self.industry_data_manager.\
            get_industry_symbol_map(group='actual_industry', name='actual_five_industry')

        for industry in industry_symbol_map:
            fig, ax = plt.subplots(figsize=(20, 8))
            symbol_index: Index  = pd.Index(industry_symbol_map[industry])
            symbol_index = symbol_index.intersection(factor_value.columns)
            industry_factor_value = factor_value[symbol_index]
            industry_factor_75q_value = industry_factor_value.quantile(q=0.75, axis=0)
            industry_factor_50q_value = industry_factor_value.quantile(q=0.5, axis=0)
            industry_factor_25q_value = industry_factor_value.quantile(q=0.25, axis=0)
            industry_factor_value = factor_value[symbol_index].to_dict(orient='Series')
            industry_symbol_list = list(industry_factor_value.keys())
            industry_factor_value = [industry_factor_value[key].dropna().values for key in industry_factor_value.keys()]
            ax.violinplot(dataset=industry_factor_value, positions=range(0, len(industry_factor_value)), showmeans=True, showextrema=True)
            industry_factor_75q_value.plot(ax=ax, label='75 quantile')
            industry_factor_50q_value.plot(ax=ax, label='50 quantile')
            industry_factor_25q_value.plot(ax=ax, label='25 quantile')
            ax.set_title(industry)
            fig.legend()
            plt.setp(ax,xticks=[y for y in range(len(industry_factor_value))],
                      xticklabels=industry_symbol_list)
            plt.grid()
            plt.show()

        fig, ax = plt.subplots(figsize=(20, 8))
        symbol_list = factor_value.columns.tolist()
        symbol_list.remove('FB')
        symbol_list.remove('BB')
        factor_value = factor_value[symbol_list]
        factor_75q_value = factor_value.quantile(q=0.75, axis=0)
        factor_50q_value = factor_value.quantile(q=0.5, axis=0)
        factor_25q_value = factor_value.quantile(q=0.25, axis=0)
        factor_value_list = [factor_value[key].dropna().values for key in factor_value.to_dict(orient='Series') if key not in ('FB', 'BB')]
        ax.violinplot(factor_value_list, positions=range(len(factor_value_list)), showmeans=True, showextrema=True)
        factor_75q_value.plot(ax=ax, label='75 quantile')
        factor_50q_value.plot(ax=ax, label='50 quantile')
        factor_25q_value.plot(ax=ax, label='25 quantile')
        ax.set_title("all")
        fig.legend()
        plt.setp(ax, xticks=[y for y in range(len(factor_value_list))],
                 xticklabels=symbol_list)
        plt.grid()
        plt.show()

    def get_factor_PN_stats(self):
        """
        统计每个品种:
        1.因子值大于0的天数占总天数的比例
        2.因子值小于0的天数占总天数的比例
        3.上穿天数的比例
        4.下穿天数的比例

        Returns
        -------

        """
        # 预先检查, 如果没有因子, 则报错
        if not isinstance(self.factor, BaseFactor):
            raise ValueError("Please specify factor first!")
        else:
            factor = self.factor

        # 获取因子值
        factor_value = factor.factor_value

        # 根据大于0小于打标签
        df: DataFrame = factor_value.mask(factor_value > 0.0, 1.0).mask(factor_value < 0.0, -1.0)

        result_dict = defaultdict(dict)

        for symbol in df.columns:
            series = df[symbol].value_counts()
            if 1.0 not in series.index:
                series.loc[1.0] = 0
            if -1.0 not in series.index:
                series.loc[-1.0] = 0
            # 统计因子值大于0的天数
            result_dict[symbol]['pos'] = series.loc[1.0]
            # 统计因子值小于0的天数
            result_dict[symbol]['neg'] = series.loc[-1.0]
            # 统计上穿天数
            result_dict[symbol]['upper_cross'] = len(df[symbol][(df[symbol].shift(1)==-1.0)&(df[symbol]==1.0)])
            # 统计下穿天数
            result_dict[symbol]['lower_cross'] = len(df[symbol][(df[symbol].shift(1)==1.0)&(df[symbol]==-1.0)])
            result_dict[symbol]['all'] = result_dict[symbol]['pos'] + result_dict[symbol]['neg']
            result_dict[symbol]['pos_pct'] = result_dict[symbol]['pos'] / result_dict[symbol]['all']
            result_dict[symbol]['neg_pct'] = result_dict[symbol]['neg'] / result_dict[symbol]['all']
            result_dict[symbol]['upper_cross_pct'] = result_dict[symbol]['upper_cross'] / result_dict[symbol]['all']
            result_dict[symbol]['lower_cross_pct'] = result_dict[symbol]['lower_cross'] / result_dict[symbol]['all']
        result_df: DataFrame = pd.DataFrame(result_dict)

        industry_symbol_map: Dict[str, List[str]] = self.industry_data_manager.\
            get_industry_symbol_map(group='actual_industry', name='actual_five_industry')

        for industry in industry_symbol_map:
            fig, ax = plt.subplots(figsize=(20, 8))
            symbol_index: Index  = pd.Index(industry_symbol_map[industry])
            symbol_index = symbol_index.intersection(result_df.columns)
            industry_result_df: DataFrame = result_df[symbol_index].loc[['pos_pct', 'neg_pct', 'upper_cross_pct', 'lower_cross_pct']]
            industry_result_df.plot.bar(ax=ax, legend=False)
            for tick in ax.get_xticklabels():
                tick.set_rotation(360)
            fig.legend()
            plt.title(industry)
            plt.axhline(y=0.5,c='red')
            plt.grid()
            plt.show()

    def get_factor_time_series(self, period: int = 1) -> None:
        """
        获取因子时序图

        Parameters
        __________
        period: int, default 1
                持有期
        """

        # 预先检查, 如果没有因子, 则报错
        if not isinstance(self.factor, BaseFactor):
            raise ValueError("Please specify factor first!")
        else:
            factor = self.factor

        # 获取因子值
        factor_value = factor.factor_value
        # 获取因子信息
        picture_name = f"{factor.__repr__()} period={period}"

        # 采样
        range_index = pd.Index(range(len(factor_value)))
        selected_index = range_index[range_index % 5 == 0]
        factor_value = factor_value.iloc[selected_index]

        # 输出的路径
        factor_folder_path = self.factor_test_result_path.joinpath(factor.group).joinpath(factor.name)
        if not os.path.exists(factor_folder_path):
            os.makedirs(factor_folder_path)
        time_series_file_path = factor_folder_path.joinpath(f"{picture_name} time_series.png")

        # 画因子时序图
        fig, ax = plt.subplots(figsize=(20, 8))
        ax1 = fig.add_subplot(211)
        factor_value.plot(ax=ax1, legend=False, sharey=True)
        ax1.set_title(f"{picture_name}\ntime_series")
        ax2 = fig.add_subplot(212)
        factor_value.mean(axis=1).plot(ax=ax2, legend=False, )
        ax2.set_title("avg_time_series")
        plt.grid()
        plt.savefig(time_series_file_path)
        plt.show()

if __name__ == "__main__":
    self = BaseFactorTest()
    self.set_factor(group='TradeHoldFactor', name='BilateralTradeHoldFactor1')
    self.get_Frank_vs_Rrank()




