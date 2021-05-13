##
# 导入库
from tqdm import tqdm
from collections import defaultdict
from data_manager.FactorDataManager import FactorDataManager

# 导入因子
factor_data_manager = FactorDataManager()

factor_dict = defaultdict(dict)
for name in ['FarNearFactor', 'MainNearFactor3', 'SlopeFactor1']:
    for price in ['close', 'settlement']:
        for window in [1, 5, 20, 40, 60, 80, 120, 140, 160, 180]:
            factor = factor_data_manager.get_factor(group='CarryFactor', name=name, price=price, window=window)
            factor_data_manager.save_factor(group='CarryFactor', name=name, price=price, window=window)