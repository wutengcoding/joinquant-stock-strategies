# 导入函数库
from jqdata import *
from datetime import datetime

'''
============================================================
分钟级别动量策略（优化版）
策略逻辑：
1. 每 30 分钟调仓一次
2. 选择过去 60 分钟涨幅最高的股票
3. 考虑 T+1 限制，当天买入的股票不能卖出
4. 添加止损和仓位管理
============================================================
'''

# 全局参数配置
g.STOCK_POOL_SIZE = 10          # 股票池大小（选前 10 只）
g.HOLD_PERIOD = 60              # 动量计算周期（60 分钟）
g.REBALANCE_MINUTES = 30        # 调仓间隔（30 分钟）
g.STOP_LOSS = 0.05              # 止损线 5%
g.TAKE_PROFIT = 0.10            # 止盈线 10%
g.MAX_POSITION = 0.25           # 单只股票最大仓位 25%

# 初始化函数
def initialize(context):
    # 设定沪深 300 作为基准
    set_benchmark('000300.XSHG')
    
    # 开启动态复权模式
    set_option('use_real_price', True)
    
    # 设置滑点（减少回测与实盘差距）
    set_slippage(PriceRelatedSlippage(0.002))  # 0.2% 滑点
    
    # 手续费设置
    set_order_cost(OrderCost(
        close_tax=0.001,          # 印花税 0.1%
        open_commission=0.0003,   # 买入佣金 0.03%
        close_commission=0.0003,  # 卖出佣金 0.03%
        min_commission=5          # 最低 5 元
    ), type='stock')
    
    # 初始化全局变量
    g.buy_time = {}           # 记录每只股票的买入时间
    g.buy_price = {}          # 记录每只股票的买入价格
    g.last_rebalance = None   # 上次调仓时间
    g.trading_day = None      # 当前交易日
    
    log.info('='*60)
    log.info('分钟级别动量策略启动')
    log.info(f'股票池大小：{g.STOCK_POOL_SIZE}')
    log.info(f'动量周期：{g.HOLD_PERIOD}分钟')
    log.info(f'调仓间隔：{g.REBALANCE_MINUTES}分钟')
    log.info(f'止损线：{g.STOP_LOSS*100}%')
    log.info(f'止盈线：{g.TAKE_PROFIT*100}%')
    log.info('='*60)
    
    # 每分钟运行一次
    run_time_interval(check_market, 1, unit='minutes')


# 选股函数 - 获取动量最强的股票
def select_momentum_stocks(context):
    # 获取沪深 300 成分股（可替换为自己关注的股票池）
    stocks = get_index_stocks('000300.XSHG')
    
    # 过滤 ST 股票、停牌股票、科创板（可选）
    filtered_stocks = []
    for stock in stocks:
        # 跳过 ST 股票
        if 'ST' in get_security_info(stock).display_name:
            continue
        # 跳过停牌
        if get_current_data()[stock].paused:
            continue
        # 跳过科创板（可选，根据权限决定）
        if stock.startswith('688'):
            continue
        # 跳过上市不足 5 天的新股
        info = get_security_info(stock)
        if (context.current_dt.date() - info.start_date).days < 5:
            continue
        filtered_stocks.append(stock)
    
    # 限制股票池大小
    if len(filtered_stocks) > 100:
        filtered_stocks = filtered_stocks[:100]
    
    # 获取过去 N 分钟的收盘价
    momentum_data = {}
    for stock in filtered_stocks:
        try:
            # 获取分钟线数据
            bars = get_bars(stock, count=g.HOLD_PERIOD, unit='1m', fields=['close'])
            if bars is None or len(bars) < g.HOLD_PERIOD:
                continue
            
            # 计算动量（收益率）
            momentum = (bars['close'].iloc[-1] - bars['close'].iloc[0]) / bars['close'].iloc[0]
            momentum_data[stock] = momentum
        except:
            continue
    
    # 按动量排序，取前 N 只
    sorted_stocks = sorted(momentum_data.items(), key=lambda x: x[1], reverse=True)
    top_stocks = [stock for stock, _ in sorted_stocks[:g.STOCK_POOL_SIZE]]
    
    return top_stocks


# 检查是否可卖出（T+1 判断）
def can_sell(stock, context):
    # 如果是今天买入的，不能卖出（T+1）
    if stock in g.buy_time:
        buy_date = g.buy_time[stock].date()
        today = context.current_dt.date()
        if buy_date == today:
            return False
    return True


# 检查止损止盈
def check_stop_loss_profit(stock, context):
    if stock not in g.buy_price:
        return 'hold'
    
    buy_price = g.buy_price[stock]
    current_price = get_current_data()[stock].last_price
    
    # 计算收益率
    return_rate = (current_price - buy_price) / buy_price
    
    # 止损检查
    if return_rate <= -g.STOP_LOSS:
        log.info(f'【止损】{stock} 当前收益 {return_rate*100:.2f}%，触发止损线 -{g.STOP_LOSS*100}%')
        return 'sell'
    
    # 止盈检查
    if return_rate >= g.TAKE_PROFIT:
        log.info(f'【止盈】{stock} 当前收益 {return_rate*100:.2f}%，触发止盈线 +{g.TAKE_PROFIT*100}%')
        return 'sell'
    
    return 'hold'


# 主策略函数 - 每分钟检查
def check_market(context):
    today = context.current_dt.date()
    current_time = context.current_dt.time()
    
    # 只在交易时间运行（9:35-14:55）
    if current_time.hour < 9 or (current_time.hour == 9 and current_time.minute < 35):
        return
    if current_time.hour >= 15:
        return
    if current_time.hour == 14 and current_time.minute >= 55:
        return
    
    # 更新交易日
    if g.trading_day != today:
        g.trading_day = today
        log.info(f'\n{"="*60}')
        log.info(f'新交易日：{today}')
        log.info(f'{"="*60}')
    
    # 检查持仓的止损止盈
    for stock in list(context.portfolio.positions.keys()):
        if stock not in g.buy_price:
            continue
        
        action = check_stop_loss_profit(stock, context)
        if action == 'sell' and can_sell(stock, context):
            # 卖出持仓
            position = context.portfolio.positions[stock]
            order_target(stock, 0)
            log.info(f'【卖出】{stock} 数量：{position.closeable_amount}')
            # 清除记录
            if stock in g.buy_price:
                del g.buy_price[stock]
            if stock in g.buy_time:
                del g.buy_time[stock]
    
    # 判断是否到调仓时间（每 30 分钟）
    minutes_since_open = (current_time.hour - 9) * 60 + current_time.minute - 30
    if minutes_since_open % g.REBALANCE_MINUTES != 0:
        return
    
    # 防止重复调仓
    if g.last_rebalance == minutes_since_open:
        return
    g.last_rebalance = minutes_since_open
    
    log.info(f'\n【调仓检查】{current_time}')
    
    # 选股
    target_stocks = select_momentum_stocks(context)
    log.info(f'选中股票：{target_stocks}')
    
    # 计算可用资金
    available_cash = context.portfolio.available_cash
    position_count = len(context.portfolio.positions)
    
    # 卖出不在目标列表中的股票（T+1 允许的情况下）
    for stock in list(context.portfolio.positions.keys()):
        if stock not in target_stocks and can_sell(stock, context):
            order_target(stock, 0)
            log.info(f'【调仓卖出】{stock}')
            if stock in g.buy_price:
                del g.buy_price[stock]
            if stock in g.buy_time:
                del g.buy_time[stock]
    
    # 刷新可用资金
    available_cash = context.portfolio.available_cash
    
    # 买入目标股票
    target_position_size = len(target_stocks)
    cash_per_stock = available_cash * g.MAX_POSITION  # 每只股票最多 25% 仓位
    
    for stock in target_stocks:
        if stock in context.portfolio.positions:
            # 已持有，跳过
            continue
        
        # 检查资金
        if cash_per_stock < 5000:  # 最少 5000 元才交易
            continue
        
        current_price = get_current_data()[stock].last_price
        if current_price is None or current_price <= 0:
            continue
        
        # 买入
        order_value(stock, cash_per_stock)
        g.buy_time[stock] = context.current_dt
        g.buy_price[stock] = current_price
        log.info(f'【买入】{stock} 价格：{current_price:.2f} 金额：{cash_per_stock:.2f}')


# 收盘后总结
def after_market_close(context):
    log.info(f'\n{"="*60}')
    log.info('【收盘总结】')
    log.info(f'总资产：{context.portfolio.total_value:.2f}')
    log.info(f'可用现金：{context.portfolio.available_cash:.2f}')
    log.info(f'持仓数量：{len(context.portfolio.positions)}')
    
    for stock, position in context.portfolio.positions.items():
        if stock in g.buy_price:
            return_rate = (position.last_sale_price - g.buy_price[stock]) / g.buy_price[stock] * 100
            log.info(f'  {stock}: 持仓 {position.sold_amount} 股 盈亏 {return_rate:+.2f}%')
    
    log.info(f'{"="*60}\n')
