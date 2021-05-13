import os
import json
import pandas as pd
from pathlib import Path
from typing import Dict, Any

from display.word_template import BacktestDocument

class Display:

    def __init__(self) -> None:
        self.backtest_document: BacktestDocument = BacktestDocument()
        self.output_result_folder_path: Path = Path(__file__).parent.parent.joinpath("output_result")

    def get_backtest_result(self, info_dict):
        return self.backtest_document.backtest_result_dict[str(info_dict)]

    def load_single_backtest_result(self,
                                    factor_group: str,
                                    factor_name: str,
                                    factor_params: Dict[str, Any],
                                    commodity_pool_group: str,
                                    commodity_pool_name: str,
                                    commodity_pool_params: Dict[str, Any],
                                    signal_group: str,
                                    signal_name: str,
                                    signal_params: Dict[str, Any],
                                    weight_group: str,
                                    weight_name: str,
                                    weight_params: Dict[str, Any],
                                    backtest_params: Dict[str, Any]
                                    ):

        factor_output_result_folder_path = self.output_result_folder_path.\
            joinpath(factor_group).joinpath(factor_name).joinpath("backtest_result")

        info_dict = {}
        info_dict['factor_group'] = factor_group
        info_dict['factor_name'] = factor_name
        info_dict['factor_params'] = factor_params
        info_dict['commodity_pool_group'] = commodity_pool_group
        info_dict['commodity_pool_name'] = commodity_pool_name
        info_dict['commodity_pool_params'] = commodity_pool_params
        info_dict['signal_group'] = signal_group
        info_dict['signal_name'] = signal_name
        info_dict['signal_params'] = signal_params
        info_dict['weight_group'] = weight_group
        info_dict['weight_name'] = weight_name
        info_dict['weight_params'] = weight_params
        info_dict['backtest_params'] = backtest_params

        str_info_dict = str(info_dict)
        backtest_id = self.load_setting()[str_info_dict]

        backtest_output_result_folder_path = factor_output_result_folder_path.joinpath(str(backtest_id))

        all_metrics = pd.read_csv(backtest_output_result_folder_path.joinpath("all.csv"))
        industry_metrics = pd.read_csv(backtest_output_result_folder_path.joinpath("industry.csv"))
        symbol_metrics = pd.read_csv(backtest_output_result_folder_path.joinpath("symbol.csv"))
        cum_profit_series = pd.read_csv(backtest_output_result_folder_path.joinpath("cum_profit.csv"))
        curve = str(backtest_output_result_folder_path.joinpath("curve.png"))

        self.add_backtest_result(info_dict, curve, all_metrics, industry_metrics, symbol_metrics, cum_profit_series)

    def add_backtest_result(self, info_dict, curve, all_metrics, industry_metrics, symbol_metrics, cum_profit_series):
        self.backtest_document.add_backtest_result(info_dict, curve, all_metrics, industry_metrics, symbol_metrics, cum_profit_series)

    def display_backtest_result(self, info_list, output_folder_path, doc_name):
        self.backtest_document.display_backtest_result(info_list, output_folder_path, doc_name)

    def load_setting(self):
        setting_file_path = self.output_result_folder_path.joinpath("backtest_setting.json")
        if not os.path.exists(setting_file_path):
            with open(setting_file_path, "w") as f:
                json_settings = json.dumps({})
                f.write(json_settings)
            return {}
        else:
            with open(setting_file_path, "rb") as f:
                settings = json.load(f)
        return settings
