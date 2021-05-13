import os
import numpy as np
import seaborn as sns
from typing import (Dict,
                    List)
from pathlib import Path
from pandas import DataFrame
import matplotlib.pyplot as plt

import matplotlib as mpl
mpl.rcParams['font.sans-serif'] = ['KaiTi', 'SimHei', 'FangSong']  # 汉字字体,优先使用楷体，如果找不到楷体，则使用黑体
mpl.rcParams['font.size'] = 12  # 字体大小
mpl.rcParams['axes.unicode_minus'] = False  # 正常显示负号

from utils.utility import compute_period_return

def get_industry_correlation_result(data: DataFrame,
                                    industry_symbol_map: Dict[str, List[str]],
                                    output_file_path: str
                                    ) -> DataFrame:
    """
    获取与行业相关的相关系数

    Parameters
    ----------
    data: DataFrame
            变量,index是交易日期,columns是品种,data是变量值

    industry_symbol_map: Dict[str, List[str]]
                行业中的品种字典

    output_file_path: str
                输出路径

    Returns
    -------
    None
    """
    output_file_path = Path(output_file_path)
    if not os.path.exists(output_file_path):
        os.makedirs(output_file_path)

    corr = data.corr()
    corr.fillna(0.0, inplace=True)

    for industry in industry_symbol_map:
        print(industry)
        symbol_list = industry_symbol_map[industry]
        symbol_list = sorted(list(set(symbol_list).intersection(set(corr.columns))))
        industry_corr = corr.loc[symbol_list, symbol_list]
        if len(industry_corr) == 1:
            continue
        plt.figure(figsize=(20, 15))
        sns.heatmap(data=np.round(industry_corr, 2), annot=True, vmin=0, vmax=1)
        plt.title(industry)
        plt.savefig(output_file_path.joinpath(f"{industry}.png"))
        plt.show()
    return corr

def get_period_industry_correlation_result(price_df: DataFrame,
                                           industry_symbol_map: Dict[str, List[str]],
                                           output_file_path: str,
                                           period: int = 20
                                           ) -> DataFrame:
    output_file_path = Path(output_file_path)
    if not os.path.exists(output_file_path):
        os.makedirs(output_file_path)

    corr_list = []
    for shift in range(period):
        return_df = compute_period_return(price_df=price_df, period=period, shift=shift)
        corr_df = return_df.corr().fillna(0.0)
        corr_list.append(corr_df)

    for i in range(len(corr_list)):
        if i == 0:
            avg_corr = corr_list[i].copy()
        else:
            avg_corr += corr_list[i]

    avg_corr = avg_corr / len(corr_list)

    for industry in industry_symbol_map:
        print(industry)
        symbol_list = industry_symbol_map[industry]
        symbol_list = sorted(list(set(symbol_list).intersection(set(avg_corr.columns))))
        industry_corr = avg_corr.loc[symbol_list, symbol_list]
        if len(industry_corr) == 1:
            continue
        plt.figure(figsize=(20, 15))
        sns.heatmap(data=np.round(industry_corr, 2), annot=True, vmin=0, vmax=1)
        plt.title(industry)
        plt.savefig(output_file_path.joinpath(f"{industry}.png"))
        plt.show()
    return avg_corr



