import warnings
import pandas as pd
from pathlib import Path
from typing import List, Dict
from pandas import DatetimeIndex, DataFrame

warnings.filterwarnings('ignore')

class BasicsDataManager:
    """
    基础信息数据管理器
    
    BasicsDataManager用于提取期货基础数据，包括交易日历、合约基础数据，以及在此基础上衍生的
    品种列表、单一品种的所有列表、单一品种所有合约到期日。

    Attributes:
    ____________
    data_folder_path: pathlib.Path
                      数据文件文件夹路径。

    trade_cal: DatetimeIndex
               交易日历

    all_instruments: DataFrame
                     期货合约基础信息

    symbol_list: 列表
                 品种代码列表

    contract_list_dict: dictionary
                        {品种1:[合约1, 合约2, ...}, 品种2:[合约1, 合约2, ...]...}
                        每个品种的合约列表

    maturity_date_dict: Dict[str, DataFrame]
                        每个品种的每个合约的到期日，数据示例:
                        {'A':     contract maturity_date
                            0      A0303    2003-03-14
                            1      A0305    2003-05-23
                            2      A0307    2003-07-14
                            3      A0309    2003-09-12
                            4      A0311    2003-11-14
                            ..       ...           ...
                            128    A0211    2002-11-14
                            129    A0301    2003-01-15
                            130    A2109    2021-09-14
                            131    A2111    2021-11-12
                            132    A2201    2022-01-17
                            [133 rows x 2 columns]}
    """

    def __init__(self) -> None:
        """Constructor"""
        self.data_folder_path: Path = Path(__file__).parent.parent.joinpath("data")

        self.trade_cal: DatetimeIndex = None
        self.all_instruments: DataFrame = None

        self.symbol_list: List[str] = None
        self.contract_list_dict: Dict[List[str]] = {}

        self.maturity_date_dict: Dict[str, DataFrame] = {}

        self.get_trade_cal()
        self.get_all_instruments()

    def get_trade_cal(self) -> DatetimeIndex:
        """
        获取交易日历

        Returns
        __________
        交易日历，DatetimeIndex

        Examples
        ________
        >>> self = BasicsDataManager()
        >>> print(self.get_trade_cal())
        DatetimeIndex(['2008-01-02', '2008-01-03', '2008-01-04', '2008-01-07',
               '2008-01-08', '2008-01-09', '2008-01-10', '2008-01-11',
               '2008-01-14', '2008-01-15',
               ...
               '2021-12-20', '2021-12-21', '2021-12-22', '2021-12-23',
               '2021-12-24', '2021-12-27', '2021-12-28', '2021-12-29',
               '2021-12-30', '2021-12-31'],
              dtype='datetime64[ns]', length=3409, freq=None)
        """
        if not isinstance(self.trade_cal, DatetimeIndex):
            self.trade_cal = pd.to_datetime(
                pd.read_json(self.data_folder_path.joinpath("trade_cal.json")).iloc[:, 0].tolist()
            )
            return self.trade_cal
        else:
            return self.trade_cal

    def get_all_instruments(self) -> DataFrame:
        """
        获取所有合约基础信息，合约基础信息包括以下fields:

        contract: 合约代码

        underlying_symbol: 品种代码

        market_tplus: 未知

        symbol: 期货简称

        margin rate: 期货合约的最低合约保证金率, maturity_date: 到期日

        type: 合约类型，'Future', trading_code: 未知

        exchange: 交易所，DCE: 大商所，SHFE:上期所，CZCE: 郑商所，CFFEX: 中金所，INE: 能源交易所

        product: 合约种类，'Commodity'-商品期货，'Index'-股指期货，'Government'-国债期货

        contract_multiplier: 合约乘数

        round_lot: 期货全部为1.0

        trading_hours: 合约交易时间

        listed_date: 期货的上市日期

        industry_name: 行业

        de_listed_date: 期货到期日

        underlying_order_book_id: \

        :return: 所有合约基础信息 DataFrame

        Examples
        ________
        >>> self = BasicsDataManager()
        >>> print(self.get_all_instruments())
             contract underlying_symbol  ...  de_listed_date underlying_order_book_id
        0       A0303                 A  ...      2003-03-14                     null
        1       A0305                 A  ...      2003-05-23                     null
        2       A0307                 A  ...      2003-07-14                     null
        3       A0309                 A  ...      2003-09-12                     null
        4       A0311                 A  ...      2003-11-14                     null
            ...               ...  ...             ...                      ...
        6616   EB2201                EB  ...      2022-01-26                     null
        6617   PG2201                PG  ...      2022-01-26                     null
        6620   FU2202                FU  ...      2022-01-31                     null
        6623   SC2202                SC  ...      2022-01-31                     null
        6624   LU2202                LU  ...      2022-01-31                     null
        [6262 rows x 17 columns]

        >>>print(self.get_all_instruments().columns)

        Index(['contract', 'underlying_symbol', 'market_tplus', 'symbol',
                'margin_rate', 'maturity_date', 'type', 'trading_code', 'exchange',
                'product', 'contract_multiplier', 'round_lot', 'trading_hours',
                'listed_date', 'industry_name', 'de_listed_date',
                'underlying_order_book_id'],
                dtype='object')
        """
        if not isinstance(self.all_instruments, DataFrame):
            all_instruments = pd.read_pickle(list(self.data_folder_path.glob('all_instruments[0-9_]*.pkl'))[0])
            all_instruments = all_instruments[~all_instruments['contract'].str.contains("88|888|889|99")]
            all_instruments['maturity_date'] = pd.to_datetime(all_instruments['maturity_date'])
            all_instruments['listed_date'] = pd.to_datetime(all_instruments['listed_date'])
            all_instruments['de_listed_date'] = pd.to_datetime(all_instruments['de_listed_date'])
            self.all_instruments = all_instruments
            return all_instruments
        else:
            return self.all_instruments

    def get_symbol_list(self) -> List[str]:
        """
        获取品种代码列表

        Returns
        -------
        品种代码列表

        Examples
        ________
        >>> self = BasicsDataManager()
        >>> print(self.get_symbol_list())
        ['A', 'AG', 'AL', 'AP', 'AU', 'B', 'BB', 'BU', 'C', 'CF', 'CJ', 'CS', 'CU', 'CY',...
        """

        if isinstance(self.symbol_list, list):
            return self.symbol_list
        else:
            all_instruments = self.get_all_instruments()
            symbol_list: List[str] = all_instruments.underlying_symbol.unique().tolist()
            self.symbol_list = symbol_list
            return symbol_list

    def get_contract_list(self, symbol: str) -> List[str]:
        """
        获取单一品种的合约列表

        Parameters
        ----------
        symbol: string
                品种代码

        Returns
        __________
        品种的合约列表

        Examples
        ________
        >>> self = BasicsDataManager()
        >>> print(self.get_contract_list('A'))
        ['A0303', 'A0305', 'A0307', 'A0309', 'A0311', 'A0401', 'A0403', 'A0405', 'A0407', 'A0409',...
        """
        if symbol in self.contract_list_dict:
            return self.contract_list_dict[symbol]
        else:
            all_instruments = self.get_all_instruments()
            contract_list = all_instruments[all_instruments.underlying_symbol == symbol][
                'contract'].unique().tolist()
            self.contract_list_dict[symbol] = contract_list
            return contract_list

    def get_maturity_date(self, symbol: str) -> DataFrame:
        """
        获取某个品种所有合约的到期日

        Parameters
        __________
        symbol: string
                品种代码

        Returns
        __________
        品种到期日期，DataFrame，一共两列，第一列为合约(contract)，第二列为到期日(maturity_date)

        Examples
        ________
        >>> self = BasicsDataManager()
        >>> print(self.get_maturity_date('A'))
            contract maturity_date
        0      A0303    2003-03-14
        1      A0305    2003-05-23
        2      A0307    2003-07-14
        3      A0309    2003-09-12
        4      A0311    2003-11-14
        ..       ...           ...
        """
        if symbol in self.maturity_date_dict:
            return self.maturity_date_dict[symbol]
        else:
            all_instruments = self.all_instruments[self.all_instruments['underlying_symbol'] == symbol]
            maturity_date = all_instruments[['contract', 'maturity_date']]
            maturity_date.index = range(len(maturity_date))
            self.maturity_date_dict[symbol] = maturity_date
        return maturity_date

if __name__ == "__main__":
    self = BasicsDataManager()
    print(self.get_trade_cal())
    print(self.get_all_instruments())
    print(self.get_symbol_list())
    print(self.get_contract_list('A'))
    print(self.get_maturity_date('A'))







