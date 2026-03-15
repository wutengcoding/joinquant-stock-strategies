# 导入函数库
from jqdata import *
from datetime import datetime

'''
============================================================
日线级别动量策略
策略逻辑：
1. 每日收盘前调仓（14:50）
2. 选择过去 20 日涨幅前 10 的股票
3. 考虑 A 股 T+1 限制
4. 添加止损和仓位管理
============================================================
'''

# ==================== 全局参数配置 ====================
g.STOCK_COUNT = 10              # 持有股票数量
g.MOMENTUM_DAYS = 20            # 动量计算周期（20 日）
g.STOP_LOSS = 0.08              # 止损线 8%
g.TAKE_PROFIT = 0.20            # 止盈线 20%
g.MAX_POSITION = 0.10           # 单只股票最大仓位 10%


# ==================== 初始化函数 ====================
def initialize(context):
    # 设定基准
    set_benchmark('000300.XSHG')
    
    # 开启动态复权
    set_option('use_real_price', True)
    
    # 设置滑点
    set_slippage(PriceRelatedSlippage(0.002))
    
    # 手续费设置
    set_order_cost(OrderCost(
        close_tax=0.001,          # 印花税 0.1%
        open_commission=0.0003,   # 买入佣金 0.03%
        close_commission=0.0003,  # 卖出佣金 0.03%
        min_commission=5          # 最低 5 元
    ), type='stock')
    
    # 初始化全局变量
    g.buy_date = {}           # 记录每只股票的买入日期
    g.buy_price = {}          # 记录每只股票的买入价格
    g.trading_day = None      # 当前交易日
    
    log.info('='*60)
    log.info('日线级别动量策略启动')
    log.info(f'持有股票数：{g.STOCK_COUNT}')
    log.info(f'动量周期：{g.MOMENTUM_DAYS}日')
    log.info(f'止损线：{g.STOP_LOSS*100}%')
    log.info(f'止盈线：{g.TAKE_PROFIT*100}%')
    log.info(f'单只仓位：{g.MAX_POSITION*100}%')
    log.info('='*60)
    
    # 每日运行一次（14:50，收盘前）
    run_daily(trade, time='14:50', reference_security='000300.XSHG')


# ==================== 选股函数 ====================
def select_stocks(context):
    """选择动量最强的股票"""
    
    # 获取沪深 300 成分股
    stocks = get_index_stocks('000300.XSHG')
    
    # 过滤股票
    filtered_stocks = []
    current_data = get_current_data()
    
    for stock in stocks:
        # 跳过 ST 股票
        if 'ST' in get_security_info(stock).display_name:
            continue
        # 跳过停牌
        if current_data[stock].paused:
            continue
        # 跳过科创板
        if stock.startswith('688'):
            continue
        # 跳过上市不足 60 天的新股
        info = get_security_info(stock)
        if (context.current_dt.date() - info.start_date).days < 60:
            continue
        # 跳过涨停无法买入的
        if current_data[stock].high_limit == current_data[stock].last_price:
            continue
        filtered_stocks.append(stock)
    
    # 计算动量（20 日收益率）
    momentum_data = {}
    for stock in filtered_stocks:
        try:
            # 获取历史收盘价
            prices = attribute_history(stock, g.MOMENTUM_DAYS, '1d', ['close'])
            if len(prices) < g.MOMENTUM_DAYS:
                continue
            
            # 计算 20 日收益率
            momentum = (prices['close'].iloc[-1] - prices['close'].iloc[0]) / prices['close'].iloc[0]
            momentum_data[stock] = momentum
        except:
            continue
    
    # 按动量排序，取前 N 只
    sorted_stocks = sorted(momentum_data.items(), key=lambda x: x[1], reverse=True)
    top_stocks = [stock for stock, _ in sorted_stocks[:g.STOCK_COUNT]]
    
    return top_stocks


# ==================== T+1 判断 ====================
def can_sell(stock, context):
    """检查是否可卖出（T+1 判断）"""
    if stock in g.buy_date:
        buy_day = g.buy_date[stock]
        today = context.current_dt.date()
        if buy_day == today:  # 今天买的不能卖
            return False
    return True


# ==================== 止损止盈检查 ====================
def check_stop_loss_profit(stock, context):
    """检查止损止盈"""
    if stock not in g.buy_price:
        return 'hold'
    
    buy_price = g.buy_price[stock]
    current_price = get_current_data()[stock].last_price
    
    if current_price is None or current_price <= 0:
        return 'hold'
    
    # 计算收益率
    return_rate = (current_price - buy_price) / buy_price
    
    # 止损检查
    if return_rate <= -g.STOP_LOSS:
        log.info(f'【止损】{stock} 买入价:{buy_price:.2f} 现价:{current_price:.2f} 收益:{return_rate*100:.2f}%')
        return 'sell'
    
    # 止盈检查
    if return_rate >= g.TAKE_PROFIT:
        log.info(f'【止盈】{stock} 买入价:{buy_price:.2f} 现价:{current_price:.2f} 收益:{return_rate*100:.2f}%')
        return 'sell'
    
    return 'hold'


# ==================== 主交易函数 ====================
def trade(context):
    """每日交易逻辑"""
    
    today = context.current_dt.date()
    
    # 更新交易日
    if g.trading_day != today:
        g.trading_day = today
        log.info(f'\n{"="*60}')
        log.info(f'交易日：{today}')
        log.info(f'{"="*60}')
    
    # 1. 先检查止损止盈
    log.info('\n【检查止损止盈】')
    for stock in list(context.portfolio.positions.keys()):
        if stock not in g.buy_price:
            continue
        
        action = check_stop_loss_profit(stock, context)
        if action == 'sell' and can_sell(stock, context):
            order_target(stock, 0)
            log.info(f'  → 卖出 {stock}')
            if stock in g.buy_price:
                del g.buy_price[stock]
            if stock in g.buy_date:
                del g.buy_date[stock]
    
    # 2. 选股
    log.info('\n【选股】')
    target_stocks = select_stocks(context)
    log.info(f'选中股票：{target_stocks}')
    
    # 3. 卖出不在目标列表的股票（T+1 允许的情况下）
    log.info('\n【调仓卖出】')
    for stock in list(context.portfolio.positions.keys()):
        if stock not in target_stocks and can_sell(stock, context):
            order_target(stock, 0)
            log.info(f'  → 调仓卖出 {stock}')
            if stock in g.buy_price:
                del g.buy_price[stock]
            if stock in g.buy_date:
                del g.buy_date[stock]
    
    # 4. 买入目标股票
    log.info('\n【买入股票】')
    available_cash = context.portfolio.available_cash
    cash_per_stock = available_cash * g.MAX_POSITION  # 每只股票 10% 仓位
    
    buy_count = 0
    for stock in target_stocks:
        if stock in context.portfolio.positions:
            continue  # 已持有，跳过
        
        if cash_per_stock < 5000:  # 最少 5000 元才交易
            continue
        
        current_price = get_current_data()[stock].last_price
        if current_price is None or current_price <= 0:
            continue
        
        # 检查是否跌停（跌停无法卖出，但可以买入）
        if current_price == get_current_data()[stock].low_limit:
            continue
        
        # 计算买入数量（100 股的整数倍）
        buy_amount = int((cash_per_stock / current_price / 100)) * 100
        if buy_amount < 100:
            continue
        
        # 买入
        order(stock, buy_amount)
        g.buy_date[stock] = today
        g.buy_price[stock] = current_price
        log.info(f'  → 买入 {stock} 价格:{current_price:.2f} 数量:{buy_amount}股')
        buy_count += 1
    
    # 5. 输出持仓信息
    log.info('\n【当前持仓】')
    for stock, position in context.portfolio.positions.items():
        if stock in g.buy_price:
            return_rate = (position.last_sale_price - g.buy_price[stock]) / g.buy_price[stock] * 100
            log.info(f'  {stock}: {position.sold_amount}股 盈亏:{return_rate:+.2f}%')
    
    log.info(f'\n总资金：{context.portfolio.total_value:.2f}')
    log.info(f'可用现金：{context.portfolio.available_cash:.2f}')
    log.info(f'持仓数量：{len(context.portfolio.positions)}')
    log.info(f'{"="*60}\n')


# ==================== 收盘后总结 ====================
def after_market_close(context):
    """收盘后总结"""
    log.info(f'\n{"="*60}')
    log.info('【收盘总结】')
    log.info(f'日期：{context.current_dt.date()}')
    log.info(f'总资产：{context.portfolio.total_value:.2f}')
    log.info(f'可用现金：{context.portfolio.available_cash:.2f}')
    log.info(f'仓位：{100 - context.portfolio.available_cash/context.portfolio.total_value*100:.1f}%')
    log.info(f'持仓数量：{len(context.portfolio.positions)}')
    
    # 计算当日盈亏
    for stock, position in context.portfolio.positions.items():
        if stock in g.buy_price:
            return_rate = (position.last_sale_price - g.buy_price[stock]) / g.buy_price[stock] * 100
            log.info(f'  {stock}: {position.sold_amount}股 盈亏:{return_rate:+.2f}%')
    
    log.info(f'{"="*60}\n')
