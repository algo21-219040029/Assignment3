import os
import json
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from typing import Any, Dict, Union
from collections import defaultdict
from pandas import DataFrame, Series

from backtesting.base import BaseBacktesting
from signals.GroupSignal.base import GroupSignalType
from backtesting.period_backtesting import LongShortPeriodBacktesting
from backtesting.monthly_backtesting import LongShortMonthlyBacktesting

class GroupBacktesting(BaseBacktesting):
    """
    分组回测系统

    分组回测系统会根据因子值将期货品种等分成若干组，做多每一组的品种，得到每一组的回测结果

    Attributes
    __________
    group_df: DataFrame
                根据因子值大小得到的分组结果，index为交易时间，columns为品种代码，分组为1，2，3，4，5，0表示不属于任何一组（不可交易）
    weight_df_dict: Dict[str, DataFrame]
                    分组权重DataFrame的字典。为方便进行分组回测，将每组的权重DataFrame分成若干DataFrame，
                    每个DataFrame中仅有租内的品种权重不为0，其他品种均为0。
    backtesting: Union[LongShortPeriodBacktesting, LongShortPeriodBacktesting]
                    作为执行回测的回测引擎
    backtesting_result_dict: Dict[str, Any]
                                每组回测的结果字典
    profit_series_dict: Dict[str, Series]
                        每组回测的日收益时间序列
    return_series_dict: Dict[str, Series]
                        每组回测的日收益率序列
    cum_profit_series_dict: Dict[str, Series]
                            每组回测的累积收益序列
    cum_return_series_dict: Dict[str, Series]
                            每组回测的累积收益率序列

    See Also
    ________
    backtesting.monthly_backtesting.LongShortMonthlyBacktesting
    backtesting.period_backtesting.LongShortPeriodBacktesting
    """

    def __init__(self,
                 rate: float = 0,
                 period: Union[str,int] = 'end',
                 interest: str = 'simple',
                 contract: str = 'main',
                 price: str = 'close',
                 rebalance_num: int = 1,
                 group_num: int = 5,
                 **kwargs) -> None:
        """Constructor"""
        super().__init__(rate=rate,
                         period=period,
                         interest=interest,
                         contract=contract,
                         price=price,
                         rebalance_num=rebalance_num,
                         group_num=group_num,
                         **kwargs)

        self.group_df: DataFrame = None  # 分组的DataFrame,index为交易时间,columns为品种代码，分组为1，2，3，4，5.0表示不属于任何一组
        # 分组权重DataFrame的字典，key为组label，如1，2，3，4，5.value为df
        # index为交易时间，columns为品种代码，data为权重
        self.weight_df_dict: Dict[str, DataFrame] = None

        self.backtesting: Union[LongShortPeriodBacktesting, LongShortPeriodBacktesting] = None
        self.backtest_result_dict: Dict[str, Any] = {}

        self.profit_series_dict: Dict[str, Series] = {}
        self.cum_profit_series_dict: Dict[str, Series] = {}

    def get_groupby_pool_in_out(self, period: int = None, shift: int = 0, figsize=(30, 8), heatmap_rotation1=False, heatmap_rotation2=False, annot_fontsize1=30, annot_fontsize2=30) -> None:
        """
        获取商品池进出每个组的情况，返回

        Returns
        -------

        """
        self.signal.set_factor_data(self.factor.factor_value)
        self.signal.set_commodity_pool(self.commodity_pool.commodity_pool_value)

        # 修正group_num
        group_num: int = self.get_params()['group_num']
        if not period:
            period: int = self.get_params()['period']
        self.signal.set_params(group_num=group_num)

        signal_df: DataFrame = self.signal.transform()
        # avg_group_in_pct_dict = {}
        # avg_group_out_pct_dict = {}
        # for shift in range(period):
        #     index = pd.Index(range(len(signal_df)))
        #     index = index[(index - shift) % period == 0]
        #     new_signal_df = signal_df.copy()
        #     new_signal_df = new_signal_df.iloc[index]
        #
        #     if self.signal.__class__.group_signal_type == GroupSignalType.AllGroupSignal:
        #         other_group_list = [-2, -1, 0]
        #     else:
        #         other_group_list = [-3, -2, -1, 0]
        #
        #     group_in_num_dict = defaultdict(dict)
        #     group_out_num_dict = defaultdict(dict)
        #
        #     for i in range(1, group_num+1):
        #         for j in other_group_list:
        #             in_df: DataFrame = pd.DataFrame(data=False,
        #                             index=new_signal_df.index,
        #                             columns=new_signal_df.columns)
        #             out_df: DataFrame = pd.DataFrame(data=False,
        #                             index=new_signal_df.index,
        #                             columns=new_signal_df.columns)
        #             # 考虑进入
        #             in_df[(new_signal_df.shift(1)==j)&(new_signal_df==i)] = True
        #             if j != 0:
        #                 pass
        #             else:
        #                 in_df.loc[new_signal_df.index[0], new_signal_df.iloc[0]==i] = True
        #
        #             # 考虑退出
        #             out_df[(new_signal_df.shift(1)==i)&(new_signal_df==j)] = True
        #
        #             group_in_num_dict[j][i] = in_df.sum().sum()
        #             group_out_num_dict[i][j] = out_df.sum().sum()
        #
        #     group_in_num_df: DataFrame = pd.DataFrame(group_in_num_dict)
        #     group_out_num_df: DataFrame = pd.DataFrame(group_out_num_dict)
        #     group_in_pct_df: DataFrame = group_in_num_df / group_in_num_df.sum(axis=0)
        #     group_out_pct_df: DataFrame = group_out_num_df / group_out_num_df.sum(axis=0)
        #
        #     avg_group_in_pct_dict[shift] = group_in_pct_df
        #     avg_group_out_pct_dict[shift] = group_out_pct_df
        #
        # avg_group_in_pct_df = pd.DataFrame()
        # avg_group_out_pct_df = pd.DataFrame()
        # for i in avg_group_in_pct_dict:
        #     if i == 0:
        #         avg_group_in_pct_df = avg_group_in_pct_dict[i]
        #         avg_group_out_pct_df = avg_group_out_pct_dict[i]
        #     else:
        #         avg_group_in_pct_df += avg_group_in_pct_dict[i]
        #         avg_group_out_pct_df += avg_group_out_pct_dict[i]
        # avg_group_in_pct_df = avg_group_in_pct_df / len(avg_group_in_pct_dict)
        # avg_group_out_pct_df = avg_group_out_pct_df / len(avg_group_out_pct_dict)

        index = pd.Series(range(len(signal_df)))
        index = index[((index - float(shift)) % float(period)) == 0]
        index = pd.Index(index.values.tolist())
        new_signal_df = signal_df.copy()
        new_signal_df = new_signal_df.iloc[index]

        if self.signal.__class__.group_signal_type == GroupSignalType.AllGroupSignal:
            other_group_list = [-2, -1, 0]
        else:
            other_group_list = [-3, -2, -1, 0]

        group_in_num_dict = defaultdict(dict)
        group_out_num_dict = defaultdict(dict)

        for i in range(1, group_num+1):
            for j in other_group_list:
                in_df: DataFrame = pd.DataFrame(data=False,
                                index=new_signal_df.index,
                                columns=new_signal_df.columns)
                out_df: DataFrame = pd.DataFrame(data=False,
                                index=new_signal_df.index,
                                columns=new_signal_df.columns)
                # 考虑进入
                in_df[(new_signal_df.shift(1)==j)&(new_signal_df==i)] = True
                if j != 0:
                    pass
                else:
                    in_df.loc[new_signal_df.index[0], new_signal_df.iloc[0]==i] = True

                # 考虑退出
                out_df[(new_signal_df.shift(1)==i)&(new_signal_df==j)] = True

                group_in_num_dict[j][i] = in_df.sum().sum()
                group_out_num_dict[i][j] = out_df.sum().sum()

        group_in_num_df: DataFrame = pd.DataFrame(group_in_num_dict)
        group_out_num_df: DataFrame = pd.DataFrame(group_out_num_dict)
        cond_avg_group_in_pct_df: DataFrame = group_in_num_df / group_in_num_df.sum(axis=0)
        cond_avg_group_out_pct_df: DataFrame = group_out_num_df / group_out_num_df.sum(axis=0)

        # 统计从-2, -1, 0到其他状态的情况
        start_in_pct_series: Series = group_in_num_df.sum(axis=0) / group_in_num_df.sum(axis=0).sum()

        # 统计从持仓状态到0，-1，-2的情况
        start_out_pct_series: Series = group_out_num_df.sum(axis=0) / group_out_num_df.sum(axis=0).sum()

        uncond_avg_group_in_pct_df = cond_avg_group_in_pct_df * start_in_pct_series
        uncond_avg_group_out_pct_df = cond_avg_group_out_pct_df * start_out_pct_series

        # 总天数
        total_days_num = len(new_signal_df)

        # 总进入次数
        total_in_num = int(group_in_num_df.sum().sum())

        # 总退出次数
        total_out_num = int(group_out_num_df.sum().sum())

        # 总进入比例
        total_in_pct = (total_in_num / group_num) / total_days_num

        # 总退出比例
        total_out_pct = (total_out_num / group_num) / total_days_num

        # 第一张图
        fig, axes = plt.subplots(figsize=figsize, nrows=1, ncols=3)
        # 第一张子图
        if heatmap_rotation1:
            cond_avg_group_in_pct_df = cond_avg_group_in_pct_df.T
        sns.heatmap(data=np.round(cond_avg_group_in_pct_df,2), vmin=0, vmax=1, annot=True, annot_kws={'fontsize': annot_fontsize1}, ax=axes[0])
        axes[0].set_title("cond commodity in pct", fontsize=25)
        axes[0].tick_params(axis='both', labelsize=30)
        # 第二张子图
        # start_in_pct_series.plot.bar(ax=axes[1])
        # if len(start_in_pct_series) == 3:
        #     for x, y in start_in_pct_series.to_dict().items():
        #         axes[1].text(x+1.7, y, np.round(y, 2), fontdict={'fontsize': 30})
        # elif len(start_in_pct_series) == 4:
        #     for x, y in start_in_pct_series.to_dict().items():
        #         axes[1].text(x+2.7, y, np.round(y, 2), fontdict={'fontsize': 30})
        # 第二张子图
        labels = start_in_pct_series.sort_index().index.tolist()
        start_in_pct_series.index = start_in_pct_series.index - start_in_pct_series.index.min()+1
        start_in_pct_series.plot.bar(ax=axes[1])
        for i, x in enumerate(start_in_pct_series.tolist()):
            axes[1].text(i-0.1, x, np.round(x, 2), fontdict={'fontsize': 30})
        axes[1].set_xticklabels(labels=labels, fontsize=25)
        # 第三张子图
        if heatmap_rotation1:
            uncond_avg_group_in_pct_df = uncond_avg_group_in_pct_df.T
        sns.heatmap(data=np.round(uncond_avg_group_in_pct_df, 2), vmin=0, vmax=1, annot=True, annot_kws={'fontsize': annot_fontsize1}, ax=axes[2])
        axes[2].set_title("uncond commodity in pct", fontsize=25)
        axes[2].tick_params(axis='both', labelsize=30)
        # 汇总
        fig.suptitle(f"group commodity in info 总次数={total_in_num} 组数={group_num} 总天数={total_days_num} 比例={round(total_in_num/group_num/total_days_num,3)}", fontsize=36)
        fig.subplots_adjust(wspace=0.3)
        fig.subplots_adjust(hspace=0.2)
        plt.xticks(fontsize=36)
        plt.show()

        # 第二张图
        fig, axes = plt.subplots(figsize=(30, 8), nrows=1, ncols=3)
        # 第一张子图
        if heatmap_rotation2:
            cond_avg_group_out_pct_df = cond_avg_group_out_pct_df.T
        sns.heatmap(data=np.round(cond_avg_group_out_pct_df,2), vmin=0, vmax=1, annot=True, annot_kws={'fontsize': annot_fontsize2}, ax=axes[0])
        axes[0].set_title("cond commodity out pct", fontsize=25)
        axes[0].tick_params(axis='both', labelsize=30)
        # 第二张子图
        # start_out_pct_series.plot.bar(ax=axes[1])
        # for x, y in start_out_pct_series.to_dict().items():
        #     axes[1].text(x+1.7, y, np.round(y, 2), fontdict={'fontsize': 30})
        # 第二张子图
        labels = start_out_pct_series.sort_index().index.tolist()
        start_out_pct_series.index = start_out_pct_series.index - start_out_pct_series.index.min()+1
        start_out_pct_series.plot.bar(ax=axes[1])
        for i, x in enumerate(start_out_pct_series.tolist()):
            axes[1].text(i-0.1, x, np.round(x, 2), fontdict={'fontsize': 20})
        axes[1].set_xticklabels(labels=labels, fontsize=25)
        # 第三张子图
        if heatmap_rotation2:
            uncond_avg_group_out_pct_df = uncond_avg_group_out_pct_df.T
        sns.heatmap(data=np.round(uncond_avg_group_out_pct_df, 2), vmin=0, vmax=1, annot=True, annot_kws={'fontsize': annot_fontsize2}, ax=axes[2])
        axes[2].set_title("uncond commodity out pct", fontsize=25)
        axes[2].tick_params(axis='both', labelsize=30)
        # 汇总
        fig.suptitle(f"group commodity out info 总次数={total_out_num} 组数={group_num} 总天数={total_days_num} 比例={round(total_out_num/group_num/total_days_num,3)}", fontsize=36)
        fig.subplots_adjust(wspace=0.3)
        fig.subplots_adjust(hspace=0.2)
        plt.xticks(fontsize=36)
        plt.show()

    def get_group_distribution_per_symbol(self, period: int = None, shift: int = 0, start: str = None, end: str = None):
        """
        获取各品种在各组的分布（包括已上市但被商品池排除的组）

        Attributes
        __________
        period: int, default None
                采样间隔多少个交易日

        shift: int, default 0
                从第几个交易日开始采样。如果shift=0, period=20，则取第1, 21, 41, ...个交易日

        start: str, default None
                起始日期

        end: str, default None
             结束日期

        Returns
        -------
        None
        """
        # 获取signal_df
        self.signal.set_factor_data(self.factor.factor_value)
        self.signal.set_commodity_pool(self.commodity_pool.commodity_pool_value)

        # 修正group_num
        group_num: int = self.get_params()['group_num']
        if not period:
            period: int = self.get_params()['period']
        self.signal.set_params(group_num=group_num)

        signal_df: DataFrame = self.signal.transform()
        if start:
            signal_df = signal_df[start:]
        if end:
            signal_df = signal_df[:end]
        index = pd.Index(range(len(signal_df)))
        index = index[(index - shift) % period == 0]
        new_signal_df = signal_df.copy()
        new_signal_df = new_signal_df.iloc[index]

        min_num = int(signal_df.min().min())
        max_num = int(signal_df.max().max())
        num_list = list(range(min_num, max_num+1))

        industry_symbol_map = self.get_industry(group='actual_industry', name='actual_five_industry')

        # industry_list = list(industry_symbol_map.keys())
        # for i in range(len(industry_symbol_map)):
        #     fig, ax = plt.subplots(figsize=(20, 8))
        #     industry = industry_list[i]
        #     symbol_list = industry_symbol_map[industry]
        #     industry_signal_df = new_signal_df[symbol_list]
        #     industry_signal_df.plot(ax=ax, legend=False)
        #     ax.set_title(industry)
        #     fig.suptitle("Group Distribution per Symbol")
        #     fig.legend()
        #     plt.grid()
        #     plt.show()

        minus_one_pct_per_symbol = {}
        minus_two_pct_per_symbol = {}
        for industry in industry_symbol_map:
            symbol_list = industry_symbol_map[industry]

            fig, axes = plt.subplots(figsize=(30, 50), nrows=len(symbol_list), ncols=1)
            for symbol in symbol_list:
                i = symbol_list.index(symbol)
                symbol_signal_series = new_signal_df[symbol]
                symbol_signal_series = symbol_signal_series[symbol_signal_series != 0.0]
                # sns.distplot(symbol_signal_series[symbol_signal_series != 0.0], ax=axes[i])
                symbol_signal_series_value_counts = symbol_signal_series.dropna().value_counts()
                symbol_signal_series_value_counts.index = symbol_signal_series_value_counts.index.astype(int)
                for num in num_list:
                    if num not in symbol_signal_series_value_counts.index:
                        symbol_signal_series_value_counts.loc[num] = 0
                symbol_signal_series_pct = (symbol_signal_series_value_counts / symbol_signal_series_value_counts.sum()).sort_index(ascending=True)
                symbol_signal_series_pct.plot.bar(ax=axes[i])
                minus_one_pct_per_symbol[symbol] = symbol_signal_series_pct.loc[-1]
                minus_two_pct_per_symbol[symbol] = symbol_signal_series_pct.loc[-2]
                axes[i].set_title(label=symbol, fontsize=30)
                # axes[i].set_xticks(num_list)
                xticks_delta = num_list[0] - axes[i].get_xticks()[0]
                axes[i].set_xticklabels(labels=axes[i].get_xticks()+xticks_delta, fontsize=30)
                axes[i].set_yticklabels(labels=np.round(axes[i].get_yticks(), 2), fontsize=30)
                # for x, y in symbol_signal_series_value_counts.to_dict().items():
                #     axes[i].text(x, y, np.round(y, 2))
                for tick in axes[i].get_xticklabels():
                    tick.set_rotation(360)
            # plt.xticks(ticks=list(range(min_num, max_num+1)), labels=list(range(min_num, max_num+1)))
            fig.subplots_adjust(hspace=0.7)
            fig.suptitle(industry, fontsize=30)
            plt.show()

        minus_one_pct_per_symbol = np.round(pd.Series(minus_one_pct_per_symbol), 2).sort_values(ascending=False)
        minus_two_pct_per_symbol = np.round(pd.Series(minus_two_pct_per_symbol), 2).sort_values(ascending=False)

        fig, axes = plt.subplots(figsize=(20, 8), nrows=2, ncols=1)
        minus_one_pct_per_symbol.plot.bar(ax=axes[0], figsize=(20, 8))
        axes[0].set_title('各品种已上市但未被纳入商品池的比例')
        axes[0].grid()
        for tick in axes[0].get_xticklabels():
            tick.set_rotation(360)

        minus_two_pct_per_symbol.plot.bar(ax=axes[1], figsize=(20, 8))
        axes[1].set_title('各品种已上市且被纳入商品池但无因子值的比例')
        axes[1].grid()
        for tick in axes[1].get_xticklabels():
            tick.set_rotation(360)
        plt.show()

    def run_backtesting(self) -> None:
        """
        运行分组回测
        """
        # 检查是否有weight
        self.prepare_weights()
        if not isinstance(self.weights, dict):
            raise ValueError("Init weight first!")

        params = self.get_params()
        rate = params['rate']
        period = params['period']
        interest = params['interest']
        contract = params['contract']
        price = params['price']
        rebalance_num = params['rebalance_num']
        group_num = params['group_num']

        # 如果是月末调仓
        if isinstance(period, str):
            backtesting = LongShortMonthlyBacktesting(rate=rate,
                                                      period=period,
                                                      interest=interest,
                                                      contract=contract,
                                                      price=price,
                                                      rebalance_num=rebalance_num,
                                                      )

        # 如果是固定天数调仓
        elif isinstance(period, int):
            backtesting = LongShortPeriodBacktesting(rate=rate,
                                                    period=period,
                                                    interest=interest,
                                                    contract=contract,
                                                     price=price,
                                                    rebalance_num=rebalance_num)

        else:
            raise TypeError("period must be an integer or a string")

        for i in self.weights:
            weight_df = self.weights[i]
            backtesting.set_weight_df(weight_df)
            backtesting.run_backtesting()

            self.cum_profit_series_dict[i] = backtesting.backtest_curve['all']['cumsum_profit']
            self.backtest_result_dict[i] = backtesting.backtest_result['metrics']

            self.backtesting = backtesting

    def output_backtest_result(self, overwrite: bool = True) -> None:
        """
        输出回测结果

        Parameters
        ----------
        overwrite: bool, default True
                    是否覆盖已有的

        Returns
        -------
        None
        """
        # 因子信息
        factor_info = self.factor_info
        factor_group, factor_name = factor_info['group'], factor_info['name']
        factor_folder_path = self.backtest_result_path.joinpath(factor_group).joinpath(factor_name)
        if not os.path.exists(factor_folder_path):
            os.makedirs(factor_folder_path)

        # 商品池信息
        commodity_pool_info = self.commodity_pool_info
        commodity_pool_group, commodity_pool_name = commodity_pool_info['group'], commodity_pool_info['name']

        # 信号信息
        signal_info = self.signal_info
        signal_group, signal_name = signal_info['group'], signal_info['name']

        # 权重名称
        weight_info = self.weight_info
        weight_group, weight_name = weight_info['group'], weight_info['name']

        # 回测参数
        backtest_params = self.get_params()

        info_dict = {}
        info_dict['factor_group'] = factor_group
        info_dict['factor_name'] = factor_name
        info_dict['factor_params'] = self.factor_params
        info_dict['commodity_pool_group'] = commodity_pool_group
        info_dict['commodity_pool_name'] = commodity_pool_name
        info_dict['commodity_pool_params'] = self.commodity_pool_params
        info_dict['signal_group'] = signal_group
        info_dict['signal_name'] = signal_name
        info_dict['signal_params'] = self.signal_params
        info_dict['weight_group'] = weight_group
        info_dict['weight_name'] = weight_name
        info_dict['weight_params'] = self.weight_params
        info_dict['backtest_params'] = backtest_params
        str_info_dict = str(info_dict)

        factor_group_analysis_folder_path = factor_folder_path.joinpath("group_analysis")
        if not os.path.exists(factor_group_analysis_folder_path):
            os.makedirs(factor_group_analysis_folder_path)

        settings = self.load_setting()
        if str_info_dict in settings:
            if not overwrite:
                return
            else:
                group_analysis_id = settings[str_info_dict]
        else:
            group_analysis_id = len(os.listdir(factor_group_analysis_folder_path)) +1
            settings[str_info_dict] = group_analysis_id
            self.save_setting(settings)

        single_factor_group_analysis_folder_path = factor_group_analysis_folder_path.joinpath(f"{group_analysis_id}")
        if not os.path.exists(single_factor_group_analysis_folder_path):
            os.makedirs(single_factor_group_analysis_folder_path)
        with open(single_factor_group_analysis_folder_path.joinpath("setting.json"), "w") as f:
            json_info_dict = json.dumps(info_dict)
            f.write(json_info_dict)

        # 指标保存
        symbol_result_list = []
        industry_result_list = []
        all_result_list = []
        for i in self.backtest_result_dict:
            backtest_result = self.backtest_result_dict[i]

            symbol_result = backtest_result['symbol']
            symbol_result = symbol_result.stack().to_frame("value")
            symbol_result['group'] = i
            symbol_result.index.names = ['underlying_symbol', 'metrics']
            symbol_result.reset_index(inplace=True)
            symbol_result_list.append(symbol_result)

            industry_result = backtest_result['industry']
            industry_result = industry_result.stack().to_frame("value")
            industry_result['group'] = i
            industry_result.index.names = ['underlying_symbol', 'metrics']
            industry_result.reset_index(inplace=True)
            industry_result_list.append(industry_result)

            all_result = backtest_result['all'].to_frame("value")
            all_result['group'] = i
            all_result.index.names = ['metrics']
            all_result.reset_index(inplace=True)
            all_result_list.append(all_result)

        symbol_result_df = pd.concat(symbol_result_list, axis=0)
        symbol_result_df = symbol_result_df.set_index(['underlying_symbol', 'metrics', 'group']).\
            unstack(level=[2, 1])
        symbol_result_df.columns = symbol_result_df.columns.droplevel(level=0)
        industry_result_df = pd.concat(industry_result_list, axis=0)
        industry_result_df = industry_result_df.set_index(['underlying_symbol', 'metrics', 'group']).\
            unstack(level=[2, 1])
        industry_result_df.columns = industry_result_df.columns.droplevel(level=0)
        all_result_df = pd.concat(all_result_list, axis=0)
        all_result_df = all_result_df.set_index(['metrics', 'group']).unstack(level=-1)
        all_result_df.columns = all_result_df.columns.droplevel(level=0)
        all_result_df.columns.names = [None]
        all_result_df.index.names = [None]

        all_result_df.to_csv(single_factor_group_analysis_folder_path.joinpath("all.csv"))
        industry_result_df.to_csv(single_factor_group_analysis_folder_path.joinpath("industry.csv"))
        symbol_result_df.to_csv(single_factor_group_analysis_folder_path.joinpath("symbol.csv"))

        title = ''
        # 添加因子
        title += f"{self.factor.get_string()}\n"
        # 添加商品池
        title += f"{self.commodity_pool.get_string()}\n"
        # 添加信号
        title += f"{self.signal.get_string()}\n"
        # 添加权重
        title += f"{self.weight.get_string()}\n"
        # 添加回测
        title += f"{self.get_string()}"

        init_total_value = 100000000
        cum_return_df = pd.DataFrame(self.cum_profit_series_dict)/ init_total_value
        plt.figure(figsize=(20, 8))
        cum_return_df.plot(figsize=(20, 8))
        plt.title(title, fontdict={'horizontalalignment': 'center', 'verticalalignment': 'center'})
        plt.grid()
        plt.savefig(single_factor_group_analysis_folder_path.joinpath("curve.png"))
        plt.show()

    def save_setting(self, settings):
        setting_file_path = self.backtest_result_path.joinpath("group_analysis_setting.json")

        with open(setting_file_path, "w") as f:
            json_settings = json.dumps(settings)
            f.write(json_settings)

    def load_setting(self):
        setting_file_path = self.backtest_result_path.joinpath("group_analysis_setting.json")
        if not os.path.exists(setting_file_path):
            with open(setting_file_path, "w") as f:
                json_settings = json.dumps({})
                f.write(json_settings)
            return {}
        else:
            with open(setting_file_path, "rb") as f:
                settings = json.load(f)
        return settings

if __name__ == "__main__":
    self = GroupBacktesting(rate=0.0003,
                            period=10,
                            price='open',
                            group_num=5)
    self.set_factor(group='TradeHoldFactor', name='UnilateralTradeHoldFactor1', N=20, window=20)
    self.set_commodity_pool(group='DynamicPool', name='DynamicPool3')
    self.set_signal(group='GroupSignal', name='GroupLongSignal1', group_num=5)
    # self.get_groupby_pool_in_out()
    self.get_group_distribution_per_symbol()



