import pandas as pd
from tqdm import tqdm
from pandas import DataFrame
from abc import abstractmethod
from factor.base import BaseFactor

from utils.utility import stack_dataframe_by_fields

class BaseCarryFactor(BaseFactor):

    """
    期限结构因子基类

    See Also
    ________
    bases.bases.BaseClass
    factor.bases.BaseFactor
    """

    def __init__(self, **params) -> None:
        """
        Constructor

        Parameters
        __________
        **params: 关键字可变参数

        See Also
        ________
        factor.bases.BaseFactor
        """
        super().__init__(**params)

    @abstractmethod
    def compute_single_factor(self, symbol: str) -> DataFrame:
        """
        计算单品种的期限结构因子, 需要各期限结构因子重写

        Parameters
        ----------
        symbol: str
                品种代码

        Returns
        -------
        因子值: DataFrame
                默认的index, 三列, datetime, underlying_symbol(均为symbol), factor
        """
        raise NotImplementedError

    def compute_factor(self) -> DataFrame:
        """
        计算因子，通过对compute_single_factor实现

        Returns
        -------
        因子值: DataFrame
                index为datetime, columns为underlying_symbol, data为factor
        """
        symbol_list = self.get_symbol_list()
        factor_list = []
        for symbol in tqdm(symbol_list):
            factor = self.compute_single_factor(symbol)
            factor_list.append(factor)
        factor = pd.concat(factor_list, axis=0)
        factor = stack_dataframe_by_fields(data=factor,
                                           index_field='datetime',
                                           column_field='underlying_symbol',
                                           data_field='factor')
        factor = factor.rolling(window=self.window, min_periods=1).mean()
        self.factor_value = factor
        return factor

