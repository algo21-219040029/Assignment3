import pandas as pd
from pathlib import Path
from pandas import DataFrame
from typing import Dict, List
from collections import defaultdict

from utils.utility import stack_dataframe_by_fields

class DailyDataManager:
    """
    期货日线行情管理器

    DailyDataManager提取单一品种期货日线行情数据的某个字段的数据，包括:
    结算价settlement, 前结算价prev_settlement, 开盘价open, 最高价high,
    最低价low, 收盘价close, 成交量volume, 持仓量open_interest,
    成交额turnover, 涨停价upper_limit, 跌停价lower_limit

    Attributes
    __________
    data_folder_path: pathlib.Path
        数据文件夹路径
    daily_data: DataFrame
        期货日线数据
    fields_dict: dictionary
        双层字典，结构为{字段1:{品种1: 字段数据1, 品种2: 字段数据2, ...}, 字段2: {品种1: 字段数据1, ...}}
        每次运行完get_field的数据会保存在这里。

    """
    def __init__(self) -> None:
        """Constructor"""
        self.data_folder_path: Path = Path(__file__).parent.parent.joinpath("data")  # 数据文件夹
        self.daily_data: DataFrame = None                                            # 所有日线数据

        self.fields_dict: Dict[str, Dict[str, DataFrame]] = defaultdict(dict)        # {字段:{品种: 字段数据}}

        self.init_daily_data()

    def init_daily_data(self) -> None:
        """
        初始化期货日线数据，构造函数中自动运行

        Returns
        __________
        None， 无返回值

        Examples
        __________
        >>> import pandas as pd
        >>> df = pd.read_pickle("D:/LFProjects/PythonProject/data/futures_daily_20210213_1.pkl")
        >>> df
                contract   datetime  settlement  prev_settlement    close      low  ...     open  lower_limit   turnover  underlying_symbol  upper_limit_tag  lower_limit_tag
        0          A0901 2009-01-05      3696.0           3699.0   3660.0   3560.0  ...   3736.0       3478.0  9168100.0                  A              0.0              0.0
        1          A0901 2009-01-06      3586.0           3696.0   3679.0   3580.0  ...   3583.0       3475.0  2582200.0                  A              0.0              0.0
        2          A0901 2009-01-07      3599.0           3586.0   3599.0   3599.0  ...   3600.0       3371.0  8926300.0                  A              0.0              0.0
        3          A0901 2009-01-08      3599.0           3599.0   3600.0   3599.0  ...   3599.0       3384.0  2591900.0                  A              0.0              0.0
        4          A0901 2009-01-09      3599.0           3599.0   3600.0   3599.0  ...   3600.0       3384.0  1584000.0                  A              0.0              0.0
        ...          ...        ...         ...              ...      ...      ...  ...      ...          ...        ...                ...              ...              ...
        1158138   ZN2112 2020-12-16     21280.0          21060.0  21145.0  21060.0  ...  21060.0      18530.0  2234675.0                 ZN              0.0              0.0
        1158139   ZN2112 2020-12-17     21280.0          21280.0  21280.0  21280.0  ...  21280.0      20000.0        0.0                 ZN              0.0              0.0
        1158140   ZN2112 2020-12-18     21280.0          21280.0  21280.0  21280.0  ...  21280.0      20000.0        0.0                 ZN              0.0              0.0
        1158141   ZN2112 2020-12-21     21480.0          21280.0  21465.0  21280.0  ...  21280.0      20000.0   859200.0                 ZN              1.0              0.0
        1158142   ZN2112 2020-12-22     21445.0          21480.0  21390.0  21390.0  ...  21465.0      20190.0   214475.0                 ZN              1.0              0.0

        [1158143 rows x 16 columns]
        """
        self.daily_data = pd.read_pickle(list(self.data_folder_path.glob('futures_daily[0-9_]*.pkl'))[0])

    def get_symbol_list(self) -> List[str]:
        """
        获取期货品种列表

        Returns
        -------
        symbol_list: List[str]
                    期货品种列表
        """
        symbol_list = self.daily_data.underlying_symbol.unique().tolist()
        return symbol_list

    def get_daily_data(self) -> DataFrame:
        return self.daily_data

    def get_field(self, symbol: str, field: str) -> DataFrame:
        """
        获取单品种收盘价DataFrame

        Parameters
        ________
        symbol: string
                品种代码

        field:  string
                要获取的字段，如open, high, low, close

        Returns
        -------
        单品种不同合约的收盘价，index是交易日期，columns是合约代码，data是字段值

        Example:
        ________
        >>> self = DailyDataManager()
        >>> df = self.get_field("A", "close")
        >>>>print(df)

        contract     A0901   A0903   A0905   A0907  ...   A2105   A2107   A2109   A2111
        datetime                                    ...
        2009-01-05  3660.0  3598.0  3413.0  3308.0  ...     NaN     NaN     NaN     NaN
        2009-01-06  3679.0  3630.0  3459.0  3360.0  ...     NaN     NaN     NaN     NaN
        2009-01-07  3599.0  3610.0  3448.0  3360.0  ...     NaN     NaN     NaN     NaN
        2009-01-08  3600.0  3623.0  3420.0  3348.0  ...     NaN     NaN     NaN     NaN
        2009-01-09  3600.0  3623.0  3462.0  3407.0  ...     NaN     NaN     NaN     NaN
        ...
        """
        # 预先检查
        if field not in self.daily_data.columns.drop(['contract', 'datetime']):
            raise NameError(f"No field named {field}")
        if symbol not in self.daily_data.underlying_symbol.tolist():
            raise NameError(f"No symbol named {symbol}")

        # 如果已有数据，则直接返回
        if field in self.fields_dict:
            if symbol in self.fields_dict[field]:
                return self.fields_dict[field][symbol]

        daily_data = self.daily_data
        daily_data = daily_data[daily_data['underlying_symbol'] == symbol]
        data = stack_dataframe_by_fields(data=daily_data, index_field='datetime',
                                          column_field='contract', data_field=field)
        self.fields_dict[field][symbol] = data
        return data


if __name__ == "__main__":
    self = DailyDataManager()
    df = self.get_field("A", "close")
    print(df)