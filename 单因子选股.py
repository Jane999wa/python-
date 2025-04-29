import numpy as np
import pandas as pd


def init(context):
    # 在context中保存全局变量
    context.hs300 = index_components("000300.XSHG")
    scheduler.run_monthly(filter_data, tradingday=4)
    context.top_10_stock_codes = ['300122.XSHE']
    logger.info("RunInfo: {}".format(context.run_info))


# before_trading此函数会在每天策略交易开始前被调用，当天只会被调用一次
def before_trading(context):
    pass


def filter_data(context, bar_dict):
    context.fundamentals_df = get_factor(context.hs300, 'return_on_invested_capital_lyr', count=30, universe=None,
                                         expect_df=True)
    logger.info("\n" + str(context.fundamentals_df.tail(30)))
    # 更新最新日期
    latest_date = context.fundamentals_df.index.get_level_values('date').max()
    # 筛选最新日期的数据
    latest_data = context.fundamentals_df.xs(latest_date, level='date').dropna()
    # 按市值从大到小排序
    sorted_stocks = latest_data.sort_values('return_on_invested_capital_lyr', ascending=False)
    # 只获取股票代码列表
    top_10_stock_codes = list(sorted_stocks.head(10).index)
    # context.fundamentals_df.T
    context.top_10_stock_codes = top_10_stock_codes


# 你选择的证券的数据更新将会触发此段逻辑，例如日或分钟历史数据切片或者是实时数据切片更新
def handle_bar(context, bar_dict):
    # 交易池中有股票
    if len(context.portfolio.positions.keys()) != 0:
        for stock in context.portfolio.positions.keys():
            if stock not in context.top_10_stock_codes:
                order_target_percent(stock, 0)

    for stock in context.top_10_stock_codes:
        order_target_percent(stock, 1 / len(context.top_10_stock_codes))


# after_trading函数会在每天交易结束后被调用，当天只会被调用一次
def after_trading(context):
    pass