import warnings
import pandas as pd
from tqdm import tqdm
from pathlib import Path
from copy import deepcopy
from typing import Dict, List
from pandas import DataFrame, DatetimeIndex

from data_manager.BasicsDataManager import BasicsDataManager
from data_manager.DailyDataManager import DailyDataManager

warnings.filterwarnings("ignore")

class ContinuousMainContract:
    """
    生成主力连续合约数据的代码
    """
    def __init__(self) -> None:
        """Constructor"""
        self.data_folder_path = Path(__file__).parent.parent.joinpath("data")  # 数据文件夹路径

        self.basics_data_manager: BasicsDataManager = BasicsDataManager()      # 基础数据管理器
        self.daily_data_manager: DailyDataManager = DailyDataManager()         # 日线数据管理器

        self.trade_cal: DatetimeIndex = None  # 交易日历，DatetimeIndex格式，每个元素datetime格式，方便做时间序列处理
        self.all_instruments: DataFrame = None  # 所有合约基础数据

        self.maturity_date_dict: Dict[str, DataFrame] = {}  # 每个品种到期日数据

        self.main_contract_df_dict: Dict[str, DataFrame] = {}  # 每个品种主力合约代码数据

        self.init_basics_data()

    def init_basics_data(self) -> None:
        """
        初始化基础数据：包括交易日历和all_instruments
        :return: None
        """
        if not isinstance(self.trade_cal, DatetimeIndex):
            self.trade_cal = self.basics_data_manager.get_trade_cal()
        if not isinstance(self.all_instruments, DataFrame):
            self.all_instruments = self.basics_data_manager.get_all_instruments()

    def get_symbol_list(self) -> List[str]:
        """
        获取品种列表

        Returns
        -------
        symbol_List: List[str]
                    期货品种列表
        """
        return self.daily_data_manager.get_symbol_list()

    def get_open_interest(self, symbol: str) -> DataFrame:
        """
        获取每日持仓量数据
        :param symbol: 品种代码
        :return: 持仓量数据DataFrame
        """
        return self.daily_data_manager.get_field(symbol, "open_interest")

    def get_field(self, symbol: str, field: str) -> DataFrame:
        """
        获取收盘价DataFrame
        :param symbol: 品种代码
        :return: 单品种不同合约的收盘价
        """
        return self.daily_data_manager.get_field(symbol, field)

    def get_maturity_date(self, symbol: str) -> DataFrame:
        """
        获取单一品种每个合约的到期日信息，包括到期日、到期月上一个月的最后一个交易日、到期日上一个月的倒数第三个的交易日和倒数第五个交易日
        :param symbol: 品种代码
        :return:
        """
        basics_info = self.all_instruments[self.all_instruments.underlying_symbol == symbol]
        maturity_date = basics_info[['contract', 'de_listed_date']]
        maturity_date.loc[:, 'year'] = maturity_date['de_listed_date'].dt.year
        maturity_date.loc[:, 'month'] = maturity_date['de_listed_date'].dt.month

        trade_cal = self.trade_cal.to_frame(name='trade_date')
        trade_cal.loc[:, 'year'] = trade_cal['trade_date'].dt.year
        trade_cal.loc[:, 'month'] = trade_cal['trade_date'].dt.month
        trade_cal.index = range(len(trade_cal))

        last_business_date_per_month = trade_cal.sort_values(by='trade_date'). \
            groupby(['year', 'month'], as_index=False).nth(-1).sort_values(by='trade_date')
        last_business_date_per_month['trade_date'] = last_business_date_per_month['trade_date'].shift(1)
        last_three_business_date_per_month = trade_cal.sort_values(by='trade_date'). \
            groupby(['year', 'month'], as_index=False).nth(-3).sort_values(by='trade_date')
        last_three_business_date_per_month['trade_date'] = last_three_business_date_per_month['trade_date'].shift(1)
        last_five_business_date_per_month = trade_cal.sort_values(by='trade_date'). \
            groupby(['year', 'month'], as_index=False).nth(-5).sort_values(by='trade_date')
        last_five_business_date_per_month['trade_date'] = last_five_business_date_per_month['trade_date'].shift(1)

        maturity_date = pd.merge(left=maturity_date, right=last_business_date_per_month,
                                 on=['year', 'month'], how='left'). \
            rename(columns={'trade_date': 'last_business_date_per_month'})
        maturity_date = pd.merge(left=maturity_date, right=last_three_business_date_per_month,
                                 on=['year', 'month'], how='left'). \
            rename(columns={"trade_date": 'last_three_business_date_per_month'})
        maturity_date = pd.merge(left=maturity_date, right=last_five_business_date_per_month,
                                 on=['year', 'month'], how='left'). \
            rename(columns={'trade_date': 'last_five_business_date_per_month'})

        self.maturity_date_dict[symbol] = maturity_date
        return maturity_date

    def get_main_contract_df(self, symbol: str, rebalance_date_num: int, field: str) -> DataFrame:
        """
        生成主力合约代码列表
        :param symbol: 品种代码
        :param rebalance_date_num: 几日换仓
        :return: 主力连续合约数据，具体见上面
        """

        if symbol not in self.maturity_date_dict:
            self.get_maturity_date(symbol)

        open_interest = self.get_open_interest(symbol)
        price = self.get_field(symbol, field)
        maturity_date = self.maturity_date_dict[symbol]

        # 交易日列表
        date_list = open_interest.index.tolist()
        # 合约交割月前一个月的倒数第N个交易日
        if rebalance_date_num == 1:
            maturity = maturity_date[['contract', 'last_business_date_per_month']].set_index('contract').iloc[:, 0]
        elif rebalance_date_num == 3:
            maturity = maturity_date[['contract', 'last_three_business_date_per_month']].set_index('contract').iloc[:, 0]
        elif rebalance_date_num == 5:
            maturity = maturity_date[['contract', 'last_five_business_date_per_month']].set_index('contract').iloc[:, 0]

        # 主力合约列表
        main_contracts = []
        flag_list = []  # 0表示不换仓，1表示一次换仓，n表示n次换仓

        for i in range(len(open_interest)):
            date = date_list[i]
            # 如果是第一天，选择当天持仓量最大的合约为主力合约
            if i == 0:
                # 首先选出可选合约：
                # 1.今天可交易且明天不交割的合约
                contract_pool = open_interest.iloc[i].dropna().index. \
                    intersection(open_interest.iloc[i + 1].dropna().index)

                # 获取可选合约当天的持仓量数据
                pool_open_interest = open_interest.iloc[i][contract_pool].sort_values(ascending=False)

                # 确定主力合约
                main_contract = pool_open_interest.index[0]
                main_contracts.append(main_contract)
                flag_list.append(0)
            # 如果是第二天
            elif i == 1:
                # 首先选出可选合约:
                # 1.今天可交易且明天不交割的合约
                main_contract = main_contracts[-1]
                contract_pool = open_interest.iloc[i].dropna().index. \
                    intersection(open_interest.iloc[i + 1].dropna().index)
                # 2.是当前主力合约或比主力合约更新
                contract_pool = contract_pool[contract_pool >= main_contract]
                # 3.没有到到期日
                contract_pool = maturity.loc[contract_pool][maturity.loc[contract_pool] > date].index

                pool_open_interest = open_interest.iloc[i][contract_pool]

                # 更新主力合约
                new_main_contract = pool_open_interest.sort_values(ascending=False).index[0]

                if new_main_contract != main_contract:
                    flag_list.append(1.0)
                else:
                    flag_list.append(0.0)

                main_contracts.append(main_contract)
            # 从第三天开始但不是最后一天
            elif i != len(open_interest) - 1:
                # 首先选出可选合约
                # 1.今天可交易，没有进入交割月
                main_contract = main_contracts[-1]
                contract_pool = open_interest.iloc[i].dropna().index. \
                    intersection(open_interest.iloc[i + 1].dropna().index)
                # 如果在极端情况下出现没有今天和下一个交易日都交易的情况，则仅选取下一个交易日
                if len(contract_pool) == 0:
                    contract_pool = open_interest.iloc[i + 1].dropna().index

                # 2.是当前主力合约或比主力合约更新
                contract_pool = contract_pool[contract_pool >= main_contract]
                # 3.没有进入交割月
                if len(maturity.loc[contract_pool][maturity.loc[contract_pool] > date].index) > 0:
                    contract_pool = maturity.loc[contract_pool][maturity.loc[contract_pool] > date].index

                # 如果当前主力合约在contract_pool，则需要检查是否有其他的contract_pool中的合约连续三天持仓量最大
                if main_contract in contract_pool:
                    # 可选合约池中三日持仓量排名之和
                    open_interest_rank_by_three = open_interest.iloc[i - 2:i + 1][contract_pool].dropna(axis=1). \
                        rank(axis=1, ascending=False).sum()
                    # 如果可选合约中没有合约连续三日持仓量排名第一，则保持原主力合约
                    if open_interest_rank_by_three[open_interest_rank_by_three == 3.0].empty:
                        main_contracts.append(main_contract)
                        flag_list.append(0.0)
                    else:
                        # 如果可选合约中连续三日持仓量排名第一的合约是主力合约，则保持原主力合约
                        if open_interest_rank_by_three[open_interest_rank_by_three == 3.0].index[0] == main_contract:
                            main_contracts.append(main_contract)
                            flag_list.append(0.0)
                        else:
                            main_contract = open_interest_rank_by_three[open_interest_rank_by_three == 3.0].index[0]
                            main_contracts.append(main_contract)
                            flag_list.append(rebalance_date_num)
                # 如果当前主力合约不在contract_pool，则直接换成contract_pool中持仓量最大的合约
                elif main_contract not in contract_pool:
                    main_contract = open_interest.iloc[i][contract_pool].sort_values(ascending=False).index[0]
                    main_contracts.append(main_contract)
                    flag_list.append(1.0)
            # 数据中的最后一个交易日，主力合约保持不变，因为事实上也用不到
            else:
                main_contract = main_contracts[-1]
                main_contracts.append(main_contract)
                flag_list.append(0.0)

        main_contract_df = pd.Series(data=main_contracts, index=date_list). \
            to_frame("contract_before_shift")
        main_contract_df.index.names = ['datetime']
        main_contract_df.reset_index(inplace=True)
        main_contract_df['contract'] = main_contract_df['contract_before_shift'].shift(1)
        main_contract_df.loc[0, 'contract'] = main_contract_df['contract'].iloc[1]
        main_contract_df['flag'] = flag_list
        main_contract_df['flag'] = main_contract_df['flag'].shift(1)
        main_contract_df.loc[0, 'flag'] = 0.0

        flag_list = main_contract_df['flag']
        main_contract_list = main_contract_df['contract']
        cum_flag_list = deepcopy(flag_list)
        old_main_contract_list = []
        new_main_contract_list = []

        total_flag = 0
        total = 0

        for i in range(len(main_contract_df)):
            if i == 0:
                old_main_contract_list.append(main_contract_list[i])
                new_main_contract_list.append(main_contract_list[i])
            elif total_flag == 0:
                if main_contract_list[i] != main_contract_list[i - 1]:
                    total_flag = 1
                    total = flag_list[i]
                    cum_flag_list[i] = (total - total_flag) / total
                    old_main_contract_list.append(main_contract_list[i - 1])
                    new_main_contract_list.append(main_contract_list[i])
                    if total - total_flag == 0:
                        total = 0
                        total_flag = 0
                else:
                    old_main_contract_list.append(main_contract_list[i])
                    new_main_contract_list.append(main_contract_list[i])
            else:
                total_flag += 1
                cum_flag_list[i] = (total - total_flag) / total
                old_main_contract_list.append(old_main_contract_list[-1])
                new_main_contract_list.append(new_main_contract_list[-1])
                if total - total_flag == 0:
                    total = 0
                    total_flag = 0

        main_contract_df['old_contract'] = old_main_contract_list
        main_contract_df['new_contract'] = new_main_contract_list
        main_contract_df['old_weight'] = cum_flag_list
        main_contract_df['new_weight'] = 1 - main_contract_df['old_weight']
        self.main_contract_df_dict[symbol] = main_contract_df

        return main_contract_df

    def get_continuous_main_contract_series(self, symbol: str, rebalance_date_num: int, field: str) -> DataFrame:
        """
        获取主力连续合约收益序列
        :param symbol: 品种代码
        :param rebalance_date_num: 换仓日期
        :return:
        """
        # 导入收益率
        price = self.get_field(symbol, field)
        returns = price.pct_change()
        returns.iloc[0] = 0.0
        stack_returns = returns.stack().to_frame("return").reset_index()
        # 生成主力合约列表
        main_contract_df = self.get_main_contract_df(symbol, rebalance_date_num, field)

        # 数据拼接
        main_contract_df = pd.merge(left=main_contract_df,
                                    right=stack_returns.rename(columns={'contract': 'old_contract',
                                                                        'return': 'old_return'}),
                                    on=['datetime', 'old_contract'], how='left')

        main_contract_df = pd.merge(left=main_contract_df,
                                    right=stack_returns.rename(columns={'contract': 'new_contract',
                                                                        'return': 'new_return'}),
                                    on=['datetime', 'new_contract'], how='left')
        main_contract_df['return'] = main_contract_df['old_weight'] * main_contract_df['old_return'] + \
                                        main_contract_df['new_weight'] * main_contract_df['new_return']
        main_contract_df['cum_return'] = (1+main_contract_df['return']).cumprod()

        start_price = price.iloc[0][main_contract_df['contract'].iloc[0]]
        main_contract_df['continuous_price'] = main_contract_df['cum_return'] * start_price
        main_contract_df['underlying_symbol'] = symbol

        self.main_contract_df_dict[symbol] = main_contract_df
        return main_contract_df
