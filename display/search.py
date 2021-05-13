import json
from pathlib import Path

class Search:

    def __init__(self) -> None:
        """Constructor"""
        self.backtest_result_path: Path = Path(__file__).parent.parent.joinpath("output_result").joinpath("backtest_setting.json")
        self.init_setting()

    def init_setting(self):
        with open(str(self.backtest_result_path), "rb") as f:
            backtest_settings = json.load(f)
        self.backtest_settings = backtest_settings

    def get_all_factor_info(self):
        factor_info = []
        backtest_settings = self.backtest_settings
        for info in backtest_settings:
            info = eval(info)
            factor_info.append({'group': info['group'], 'name': info['name'], 'factor_params': info['factor_params']})
        return factor_info

    def get_info(self, factor_group: str, factor_name: str):
        backtest_settings = self.backtest_settings
        info_list = []
        for info in backtest_settings:
            info = eval(info)
            if info['factor_group'] == factor_group and info['factor_name'] == factor_name:
                info_list.append(info)
        return info_list

