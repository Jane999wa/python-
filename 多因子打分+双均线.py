import numpy as np
import pandas as pd
import talib


def init(context):
    # 在context中保存全局变量
    context.hs300 = index_components("000300.XSHG")
    scheduler.run_monthly(filter_data, tradingday=4)
    # 短周期均线，5 日
    context.SHORT_MA_PERIOD = 12
    # 长周期均线，20 日
    context.LONG_MA_PERIOD = 26
    # K线历史数据
    context.OBSERVATION = 50
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

        # 股票池不为空 , 按照双均线择时策略检查买卖
        else:
            for stock_code in context.portfolio.positions.keys():
                # 读取该股票的历史收盘价序列，使用sma方式计算均线
                prices = history_bars(stock_code, context.OBSERVATION, '1d', 'close')

                if len(prices) < context.LONG_MA_PERIOD:
                    logger.info(f"股票 {stock_code} 死亡交叉历史数据不足")

                # 使用 talib 计算短期和长期移动平均线 (SMA)
                short_ma = talib.SMA(prices, context.SHORT_MA_PERIOD)
                long_ma = talib.SMA(prices, context.LONG_MA_PERIOD)
                # 获取最新的均线值
                current_short_ma = short_ma[-1]
                current_long_ma = long_ma[-1]
                # 昨日短期和长期均线值
                prev_short_ma = short_ma[-2]
                prev_long_ma = long_ma[-2]

                # 卖出条件1 : 均线背离 - 短期均线已经在长期均线下方，且差距扩大到一定程度
                ma_divergence = (current_short_ma < current_long_ma and
                                 (current_long_ma - current_short_ma) / current_long_ma > 0.03)

                # 卖出条件 2 : 价格跌破关键均线且短期趋势向下
                death_cross = prices[-1] < current_short_ma and current_short_ma < prev_short_ma

                # 综合卖出信号
                if death_cross or ma_divergence:
                    logger.info(f"卖出信号触发，股票: {stock_code}")
                    logger.info(f"短期均线: {current_short_ma}, 长期均线: {current_long_ma}")

                    # 全部卖出该股票
                    order_target_percent(stock_code, 0)
                    logger.info(f"已卖出全部 {stock_code} 股票")

        # 根据“黄金交叉”判断是否买入
        for stock_code in context.top_10_stock_codes:
            prices = history_bars(stock_code, context.OBSERVATION, '1d', 'close')
            if len(prices) < context.LONG_MA_PERIOD:
                logger.info(f"股票 {stock_code} 黄金交叉数据历史不足")
            short_ma = talib.SMA(prices, context.SHORT_MA_PERIOD)
            long_ma = talib.SMA(prices, context.LONG_MA_PERIOD)
            logger.info(short_ma)
            logger.info(long_ma)
            # 获取最新的均线值
            current_short_ma = short_ma[-1]
            current_long_ma = long_ma[-1]
            # 买入条件1: 黄金信号 -- 短期均线 > 长期均线
            golden_state = current_short_ma > current_long_ma
            # 辅助条件2 ：短期均线仍在上升
            short_ma_rising = current_short_ma > short_ma[-2]
            # 辅助条件3 ：价格在短期均线之上
            price_above_short_ma = prices[-1] > current_short_ma
            logger.info(
                f"golden_state: {golden_state}, short_ma_rising: {short_ma_rising}, price_above_short_ma: {price_above_short_ma}")
            buy_signal = golden_state and short_ma_rising and price_above_short_ma
            if buy_signal:
                logger.info(f"买入信号触发，股票: {stock_code}")
                logger.info(f"当前 短期均线: {current_short_ma:.2f}, 长期均线: {current_long_ma:.2f}")
                # 买入股票
                order_target_percent(stock_code, 1 / len(context.top_10_stock_codes))


# after_trading函数会在每天交易结束后被调用，当天只会被调用一次
def after_trading(context):
    pass

