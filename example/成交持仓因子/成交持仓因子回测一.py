from backtesting.period_backtesting import LongShortPeriodBacktesting
self = LongShortPeriodBacktesting(rate=0.0000,
                                  period=1,
                                  interest='compound',
                                  contract='main',
                                  price='close',
                                  rebalance_num=1)
self.set_factor(group='TradeHoldFactor', name='BilateralTradeHoldFactor1')
self.set_commodity_pool(group='DynamicPool', name='DynamicPool3')
self.set_signal(group='CrossSectionSignal', name='CrossSectionSignal1')
self.set_weight(group='EqualWeight', name='NoRiskNeutralEqualWeight')
self.prepare_weights()
self.run_backtesting()
self.output_backtest_result(overwrite=True)