import numpy as np
import pandas as pd
import talib
import matplotlib.pyplot as plt


def init(context):
    # 在context中保存全局变量
    context.hs300 = index_components("000300.XSHG")
    scheduler.run_monthly(filter_data, tradingday=4)
    context.top_10_stock_codes = []
    # 创建空DataFrame来存储最新因子分数
    context.factor_scores = pd.DataFrame(index=context.hs300)
    logger.info("RunInfo: {}".format(context.run_info))


# before_trading此函数会在每天策略交易开始前被调用，当天只会被调用一次
def before_trading(context):
    pass


def filter_data(context, bar_dict):
    # 正相关因子列表
    factor_up = [
        'basic_earnings_per_share',
        'return_on_invested_capital_lyr',
        'return_on_equity_lyr'
    ]
    # 负相关的因子列表
    factor_down = [
        'market_cap',
        'debt_to_asset_ratio_lyr',
        'pb_ratio_lyr'
    ]

    # 获取正相关因子的数据
    for factor in factor_up:
        factor_data_up = get_factor(
            context.hs300,  # 股票列表
            factor,  # 因子名称
            count=10,  # 获取最近10个交易日的数据
            universe=None,  # 不指定特定universe
            expect_df=True  # 返回DataFrame格式
        )
        # 获取最新日期
        latest_date = factor_data_up.index.get_level_values('date').max()
        # 提取最新日期的数据
        latest_data = factor_data_up.xs(latest_date, level='date', drop_level=False)
        latest_data = latest_data.reset_index(level='date', drop=True)

        # 首先创建一个包含股票代码和因子值的Series
        factor_series = latest_data[factor].dropna()
        # 对因子值从大到小进行排名
        factor_rank = factor_series.rank(method='min', ascending=False)
        # 将排名转换为分数（排名第1的得300分，最后一名得1分）
        n_stocks = len(factor_rank)
        factor_score = n_stocks + 1 - factor_rank

        # 将分数添加到分数DataFrame
        context.factor_scores[factor + '_score'] = factor_score

    # 获取负相关因子的数据
    for factor in factor_down:
        factor_data_down = get_factor(
            context.hs300,  # 股票列表
            factor,  # 因子名称
            count=10,  # 获取最近10个交易日的数据
            universe=None,  # 不指定特定universe
            expect_df=True  # 返回DataFrame格式
        )
        # 获取最新日期
        latest_date = factor_data_down.index.get_level_values('date').max()
        # 提取最新日期的数据
        latest_data = factor_data_down.xs(latest_date, level='date', drop_level=False)
        latest_data = latest_data.reset_index(level='date', drop=True)

        # 首先创建一个包含股票代码和因子值的Series
        factor_series = latest_data[factor].dropna()
        # 对因子值从小到大进行排名
        factor_rank = factor_series.rank(method='min', ascending=True)
        # 将排名转换为分数（排名第1的得300分，最后一名得1分）
        n_stocks = len(factor_rank)
        factor_score = n_stocks + 1 - factor_rank
        # 将分数添加到分数DataFrame
        context.factor_scores[factor + '_score'] = factor_score

    # 计算所有因子得分的总和
    context.factor_scores['total_score'] = context.factor_scores.sum(axis=1)

    # 按总分从大到小排序
    context.factor_scores = context.factor_scores.sort_values('total_score', ascending=False)

    # 选出得分最高的前十个股票
    top_10_stocks = context.factor_scores.head(10).index.tolist()
    context.top_10_stock_codes = top_10_stocks


# 你选择的证券的数据更新将会触发此段逻辑，例如日或分钟历史数据切片或者是实时数据切片更新
def handle_bar(context, bar_dict):
    # 获取当前日期
    current_date = context.now.strftime("%Y-%m-%d")
    # 仅在每月 4 号运行交易逻辑
    if context.now.day == 4:
        # 如果股票池为空 , 先买入
        if (len(context.portfolio.positions.keys()) == 0):
            for stock in context.top_10_stock_codes:
                order_target_percent(stock, 1 / len(context.top_10_stock_codes))

        # 股票池不为空
        else:
            for stock_code in context.portfolio.positions.keys():
                if stock_code not in context.top_10_stock_codes:
                    order_target_percent(stock_code, 0)

        for stock in context.top_10_stock_codes:
            order_target_percent(stock, 1 / len(context.top_10_stock_codes))


# after_trading函数会在每天交易结束后被调用，当天只会被调用一次
def after_trading(context):
    pass