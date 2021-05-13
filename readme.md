# Commodity Futures Factor Commodity Analysis Framework

This framework is used to analyse commodity futures factors, including correlation analysis, group analysis, industry analysis and factor backtesting.

## documentation

Besides visiting the following website directly, you can also refer to the **tutorial** folder directly.

**grasping data**:\
    [commodity_pool_data](https://github.com/algo21-219040029/Assignment3/blob/master/tutorial/%E6%95%B0%E6%8D%AE%E8%8E%B7%E5%8F%96/%E5%95%86%E5%93%81%E6%B1%A0%E6%95%B0%E6%8D%AE.ipynb)\
    [factor_data](https://github.com/algo21-219040029/Assignment3/blob/master/tutorial/%E6%95%B0%E6%8D%AE%E8%8E%B7%E5%8F%96/%E5%9B%A0%E5%AD%90%E6%95%B0%E6%8D%AE.ipynb)\
    [commodity futures basics data](https://github.com/algo21-219040029/Assignment3/blob/master/tutorial/%E6%95%B0%E6%8D%AE%E8%8E%B7%E5%8F%96/%E6%9C%9F%E8%B4%A7%E5%9F%BA%E7%A1%80%E4%BF%A1%E6%81%AF%E6%95%B0%E6%8D%AE.ipynb)\
    [commodity futures daily market data](https://github.com/algo21-219040029/Assignment3/blob/master/tutorial/%E6%95%B0%E6%8D%AE%E8%8E%B7%E5%8F%96/%E6%9C%9F%E8%B4%A7%E6%97%A5%E7%BA%BF%E8%A1%8C%E6%83%85%E6%95%B0%E6%8D%AE.ipynb)\
    [commodity futures industry data](https://github.com/algo21-219040029/Assignment3/blob/master/tutorial/%E6%95%B0%E6%8D%AE%E8%8E%B7%E5%8F%96/%E6%9C%9F%E8%B4%A7%E8%A1%8C%E4%B8%9A%E5%88%86%E7%B1%BB%E6%95%B0%E6%8D%AE.ipynb)\
    [continuous futures contract data](https://github.com/algo21-219040029/Assignment3/blob/master/tutorial/%E6%95%B0%E6%8D%AE%E8%8E%B7%E5%8F%96/%E8%BF%9E%E7%BB%AD%E5%90%88%E7%BA%A6%E6%95%B0%E6%8D%AE.ipynb)
    
**construct factor, commodity pool, signal, weight**:\
    [construct factor](https://github.com/algo21-219040029/Assignment3/blob/master/tutorial/%E5%9B%A0%E5%AD%90%E3%80%81%E5%95%86%E5%93%81%E6%B1%A0%E3%80%81%E4%BF%A1%E5%8F%B7%E3%80%81%E6%9D%83%E9%87%8D%E6%9E%84%E5%BB%BA/%E5%9B%A0%E5%AD%90%E6%9E%84%E5%BB%BA.ipynb)\
    [construct commodity pool](https://github.com/algo21-219040029/Assignment3/blob/master/tutorial/%E5%9B%A0%E5%AD%90%E3%80%81%E5%95%86%E5%93%81%E6%B1%A0%E3%80%81%E4%BF%A1%E5%8F%B7%E3%80%81%E6%9D%83%E9%87%8D%E6%9E%84%E5%BB%BA/%E5%95%86%E5%93%81%E6%B1%A0%E6%9E%84%E5%BB%BA.ipynb)\
    [construct signal](https://github.com/algo21-219040029/Assignment3/blob/master/tutorial/%E5%9B%A0%E5%AD%90%E3%80%81%E5%95%86%E5%93%81%E6%B1%A0%E3%80%81%E4%BF%A1%E5%8F%B7%E3%80%81%E6%9D%83%E9%87%8D%E6%9E%84%E5%BB%BA/%E4%BF%A1%E5%8F%B7%E6%9E%84%E5%BB%BA.ipynb)\
    [construct weight](https://github.com/algo21-219040029/Assignment3/blob/master/tutorial/%E5%9B%A0%E5%AD%90%E3%80%81%E5%95%86%E5%93%81%E6%B1%A0%E3%80%81%E4%BF%A1%E5%8F%B7%E3%80%81%E6%9D%83%E9%87%8D%E6%9E%84%E5%BB%BA/%E6%9D%83%E9%87%8D%E6%9E%84%E5%BB%BA.ipynb)

**factor test**:\
    [factor test](https://github.com/algo21-219040029/Assignment3/tree/master/tutorial/%E5%9B%A0%E5%AD%90%E6%B5%8B%E8%AF%95)

**backtesting**:\
    [backtesting](https://github.com/algo21-219040029/Assignment3/blob/master/tutorial/%E5%9B%9E%E6%B5%8B/%E5%9B%9E%E6%B5%8B.ipynb)

**group backtesting and analysis**:\
    [group_backtesting and analysis](https://github.com/algo21-219040029/Assignment3/blob/master/tutorial/%E5%88%86%E7%BB%84%E5%9B%9E%E6%B5%8B%E5%8F%8A%E5%85%B6%E5%88%86%E6%9E%90/%E5%88%86%E7%BB%84%E5%9B%9E%E6%B5%8B%E5%8F%8A%E5%85%B6%E5%88%86%E6%9E%90.ipynb)

## API References

The following is the API reference documentation. You can refer to it:\
**/docs/build/html/index.rst**,\
in which you can see:
![image.png](/image/image.png)



# Data

The Data can be grasped from the following url:

链接：https://pan.baidu.com/s/1z8RBAI8E7gB9tuLKsYGCyw 
提取码：l7fz 

After downloading the data, you need to put the data folder in the root path.



# Example

## Import packages


```python
import os
from pathlib import Path
os.chdir(Path(os.getcwd()).parent.parent)
os.getcwd()
```




    'D:\\LFProjects\\NewPythonProject'




```python
from factor_test.base import BaseFactorTest
from backtesting.group_backtesting import GroupBacktesting
from backtesting.period_backtesting import LongShortPeriodBacktesting
```

## Factor Test

### BilateralTradeHoldFactor1, N=20, window=1

####  H=1, q=0.6, CrossSection


```python
self = LongShortPeriodBacktesting(rate=0.0000, period=1, price='open')
self.set_factor(group='TradeHoldFactor', name='BilateralTradeHoldFactor1', N=20, window=1)
self.set_commodity_pool(group='DynamicPool', name='DynamicPool3')
self.set_signal(group='CrossSectionSignal', name='CrossSectionSignal1', quantile=0.6)
self.set_weight(group='EqualWeight', name='NoRiskNeutralEqualWeight')
self.run_backtesting()
```

    100%|█████████████████████████████████████████████████████████████████████████████████████████████████████████████| 2830/2830 [00:10<00:00, 266.22it/s]



```python
self.output_backtest_result()
```


![png](/image/output_8_0.png)
​    




![png](/image/output_8_1.png)
    




![png](/image/output_8_2.png)
    




![png](/image/output_8_3.png)
    




![png](/image/output_8_4.png)
    




![png](/image/output_8_5.png)
    




![png](/image/output_8_6.png)
    




![png](/image/output_8_7.png)
    




![png](/image/output_8_8.png)
    




![png](/image/output_8_9.png)
    




![png](/image/output_8_10.png)
    




![png](/image/output_8_11.png)
    




![png](/image/output_8_12.png)
    




![png](/image/output_8_13.png)
    




![png](/image/output_8_14.png)
    




![png](/image/output_8_15.png)
    




![png](/image/output_8_16.png)
    




![png](/image/output_8_17.png)
    




![png](/image/output_8_18.png)





![png](/image/output_8_19.png)
    




![png](/image/output_8_20.png)
    




![png](/image/output_8_21.png)
    




![png](/image/output_8_22.png)
    




![png](/image/output_8_23.png)
    


### ### Initialize Factor Test Module


```python
factor_test = BaseFactorTest()
factor_test.set_factor(group='TradeHoldFactor', name='BilateralTradeHoldFactor1',N=20,window=1)
```

### Factor rank vs Return rank


```python
factor_test.get_Frank_vs_Rrank(group_num=10, period=1)
```


![png](/image/output_15_0.png)
    



```python
factor_test.get_Frank_vs_Rrank(group_num=10, period=5)
```


![png](/image/output_16_0.png)
    



```python
factor_test.get_Frank_vs_Rrank(group_num=10, period=10)
```


![png](/image/output_17_0.png)
    



```python
factor_test.get_Frank_vs_Rrank(group_num=10, period=20)
```


![png](/image/output_18_0.png)
    



```python
factor_test.get_Frank_vs_Rrank(group_num=5, period=1, industry='化工能源')
```


![png](/image/output_19_0.png)
    



```python
factor_test.get_Frank_vs_Rrank(group_num=5, period=1, industry='有色_贵金属')
```


![png](/image/output_20_0.png)
    



```python
factor_test.get_Frank_vs_Rrank(group_num=5, period=1, industry='农产品_软商品')
```


![png](/image/output_21_0.png)
    



```python
factor_test.get_Frank_vs_Rrank(group_num=5, period=1, industry='油脂油料')
```


![png](/image/output_22_0.png)
    



```python
factor_test.get_Frank_vs_Rrank(group_num=5, period=5, industry='油脂油料')
```


![png](/image/output_23_0.png)
    



```python
factor_test.get_Frank_vs_Rrank(group_num=5, period=10, industry='油脂油料')
```


![png](/image/output_24_0.png)
    



```python
factor_test.get_Frank_vs_Rrank(group_num=5, period=20, industry='油脂油料')
```


![png](/image/output_25_0.png)
    



```python
factor_test.get_Frank_vs_Rrank(group_num=5, period=1, industry='黑色')
```


![png](/image/output_26_0.png)
    



```python
factor_test.get_Frank_vs_Rrank(group_num=5, period=20, industry='黑色')
```


![png](/image/output_27_0.png)
    


### Factor Sign and Cross with zero


```python
self.get_factor_PN_stats()
```


![png](/image/output_31_0.png)
    




![png](/image/output_31_1.png)
    




![png](/image/output_31_2.png)
    




![png](/image/output_31_3.png)
    




![png](/image/output_31_4.png)
    




![png](/image/output_31_5.png)
    



```python
group_test = GroupBacktesting(group_num=10, period=1)
group_test.set_factor(group='TradeHoldFactor', name='BilateralTradeHoldFactor1', N=20, window=1)
group_test.set_commodity_pool(group='DynamicPool', name='DynamicPool3')
group_test.set_signal(group='GroupSignal', name='GroupLongSignal1')
```

### Factor Group Distribution


```python
group_test.get_group_distribution_per_symbol(period=1, shift=0)
```


![png](/image/output_34_0.png)
    




![png](/image/output_34_1.png)
    




![png](/image/output_34_2.png)
    




![png](/image/output_34_3.png)
    




![png](/image/output_34_4.png)
    




![png](/image/output_34_5.png)
    




![png](/image/output_34_6.png)
    


### Symbol Group Changes


```python
group_test.get_groupby_pool_in_out(figsize=(30,30), annot_fontsize2=18, heatmap_rotation2=True)
```


![png](/image/output_36_0.png)
    




![png](/image/output_36_1.png)
    


## BilateralTradeHoldFactor1, N=20, window=10

### Initialize Factor Test Module


```python
factor_test = BaseFactorTest()
factor_test.set_factor(group='TradeHoldFactor', name='BilateralTradeHoldFactor1',N=20,window=10)
```

### Factor rank vs Return rank


```python
factor_test.get_Frank_vs_Rrank(group_num=10, period=1)
```


![png](/image/output_41_0.png)
    



```python
factor_test.get_Frank_vs_Rrank(group_num=10, period=5)
```


![png](/image/output_42_0.png)
    



```python
factor_test.get_Frank_vs_Rrank(group_num=10, period=10)
```


![png](/image/output_43_0.png)
    



```python
factor_test.get_Frank_vs_Rrank(group_num=10, period=20)
```


![png](/image/output_44_0.png)
    



```python
factor_test.get_Frank_vs_Rrank(group_num=10, period=40)
```


![png](/image/output_45_0.png)
    



```python
factor_test.get_Frank_vs_Rrank(group_num=10, period=60)
```


![png](/image/output_46_0.png)
    


### Factor Distribution per Symbol


```python
factor_test.get_factor_distribution_per_symbol()
```


![png](/image/output_48_0.png)
    




![png](/image/output_48_1.png)
    




![png](/image/output_48_2.png)
    




![png](/image/output_48_3.png)
    




![png](/image/output_48_4.png)
    




![png](/image/output_48_5.png)
    




![png](/image/output_48_6.png)
    
