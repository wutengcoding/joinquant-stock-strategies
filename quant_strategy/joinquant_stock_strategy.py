# -*- coding: utf-8 -*-
"""
聚宽平台选股策略 - 基本面 + 技术面结合策略

策略逻辑：
1. 基本面选股：
   - 营业收入同比增长 > 0
   - 毛利率同比增长 > 0
   - 三费费率同比下降
2. 技术面筛选：
   - 近期涨势较好（近 20 日涨幅排名前）
   - 选出 10 只候选股票
3. 交易信号：
   - 日线级别 MACD 金叉买入
   - MACD 死叉卖出

回测周期：近一年
"""

import pandas as pd
import numpy as np
from jqdata import *

# ==================== 全局配置 ====================

# 回测配置
__version__ = '1.0.0'
__author__ = 'Assistant'

# 股票池配置
STOCK_POOL_SIZE = 10  # 候选池股票数量
MIN_MARKET_CAP = 50   # 最小市值 (亿)

# 基本面筛选条件
REVENUE_GROWTH_MIN = 0      # 营收同比增长最小值
GROSS_MARGIN_GROWTH_MIN = 0 # 毛利率同比增长最小值
FEE_RATIO_DECLINE_MIN = 0   # 费率同比下降最小值

# 技术面参数
LOOKBACK_DAYS = 20          # 近期涨势观察天数
MACD_FAST = 12              # MACD 快线
MACD_SLOW = 26              # MACD 慢线
MACD_SIGNAL = 9             # MACD 信号线

# 交易参数
POSITION_PER_STOCK = 0.2    # 单只股票最大仓位 (20%)
MAX_POSITIONS = 5           # 最大持仓数量
STOP_LOSS = 0.15            # 止损比例 15%
TAKE_PROFIT = 0.30          # 止盈比例 30%


# ==================== 初始化函数 ====================

def initialize(context):
    """
    策略初始化函数
    """
    # 设置基准
    set_benchmark('000300.XSHG')  # 沪深 300
    
    # 开启动态复权
    set_option('use_real_price', True)
    
    # 设置滑点和手续费
    set_slippage(PriceRelatedSlippage(0.002))  # 0.2% 滑点
    set_commission(PerTrade(buy_cost=0.0003, sell_cost=0.0013, min_cost=5))
    
    # 全局变量
    g.stock_pool = []           # 候选股票池
    g.holding_stocks = []       # 当前持仓
    g.buy_price = {}            # 买入价格记录
    g.stock_data = {}           # 股票数据缓存
    
    # 交易日志
    log.info('策略初始化完成')
    log.info(f'候选池大小：{STOCK_POOL_SIZE}')
    log.info(f'最大持仓数：{MAX_POSITIONS}')
    log.info(f'止损比例：{STOP_LOSS*100}%')
    log.info(f'止盈比例：{TAKE_PROFIT*100}%')


# ==================== 定时运行函数 ====================

def before_trading_start(context):
    """
    盘前运行：更新股票池
    每月第一个交易日更新一次股票池
    """
    # 每月 1 号更新股票池
    if context.current_dt.day == 1:
        log.info('=== 开始更新股票池 ===')
        g.stock_pool = select_stocks(context)
        log.info(f'股票池更新完成，共 {len(g.stock_pool)} 只股票')
        log.info(f'股票池：{g.stock_pool[:5]}...')
    
    # 清空昨日数据缓存
    g.stock_data = {}


def handle_data(context, data):
    """
    每分钟运行：交易逻辑
    """
    # 只在开盘后 30 分钟开始交易，避免开盘波动
    current_time = context.current_dt.time()
    if current_time.hour < 9 or (current_time.hour == 9 and current_time.minute < 45):
        return
    
    # 获取候选股票池
    if not g.stock_pool:
        return
    
    # 获取当前持仓
    positions = get_positions()
    g.holding_stocks = [p.security for p in positions]
    
    # 检查止盈止损
    check_stop_loss_take_profit(context, data)
    
    # 检查 MACD 交易信号
    check_macd_signals(context, data)


# ==================== 基本面选股函数 ====================

def select_stocks(context):
    """
    基本面选股：选择收入和毛利率同比上升，费率同比下降的公司
    """
    # 获取全部 A 股
    all_stocks = get_all_securities(['stock'], date=context.current_dt).index.tolist()
    
    # 过滤 ST 股票和新股
    all_stocks = filter_st_and_new_stocks(all_stocks, context.current_dt)
    
    # 获取基本面数据
    fundamental_data = get_fundamental_data(all_stocks, context.current_dt)
    
    if fundamental_data is None or len(fundamental_data) == 0:
        log.warning('未获取到基本面数据')
        return []
    
    # 筛选条件 1：营业收入同比增长 > 0
    condition1 = fundamental_data['revenue_yoy'] > REVENUE_GROWTH_MIN
    
    # 筛选条件 2：毛利率同比增长 > 0
    condition2 = fundamental_data['gross_margin_yoy'] > GROSS_MARGIN_GROWTH_MIN
    
    # 筛选条件 3：三费费率同比下降
    condition3 = fundamental_data['fee_ratio_yoy'] < -FEE_RATIO_DECLINE_MIN
    
    # 筛选条件 4：市值大于最小值
    condition4 = fundamental_data['market_cap'] > MIN_MARKET_CAP
    
    # 综合筛选
    selected = fundamental_data[condition1 & condition2 & condition3 & condition4]
    
    if len(selected) == 0:
        log.warning('基本面筛选后无股票符合条件')
        return []
    
    log.info(f'基本面筛选通过：{len(selected)} 只股票')
    
    # 技术面筛选：选择近期涨势较好的股票
    technical_selected = filter_by_momentum(selected.index.tolist(), context)
    
    # 取前 N 只
    final_pool = technical_selected[:STOCK_POOL_SIZE]
    
    log.info(f'技术面筛选后：{len(final_pool)} 只股票')
    
    return final_pool


def get_fundamental_data(stock_list, date):
    """
    获取基本面数据
    """
    try:
        # 获取最新财报数据
        q = query(
            valuation.code,
            valuation.market_cap,
            income.total_operating_revenue,
            income.total_operating_revenue_yoy,
            indicator.gross_profit_margin,
            indicator.roe,
            income.operating_expense,
            income.sales_expense,
            income.administrative_expense,
            income.finance_expense
        ).filter(
            valuation.code.in_(stock_list)
        ).order_by(
            valuation.market_cap.desc()
        )
        
        # 获取当前季度的财报
        current_quarter = get_current_quarter(date)
        last_quarter = get_last_quarter(date)
        
        df = get_fundamentals(q, date=date)
        
        if df is None or len(df) == 0:
            return None
        
        # 计算同比增长率
        df['revenue_yoy'] = df['total_operating_revenue_yoy'] / 100.0
        
        # 获取去年同期数据计算毛利率和费率变化
        df_last = get_fundamentals(q, date=last_quarter)
        
        if df_last is not None and len(df_last) > 0:
            # 合并数据
            df = df.merge(df_last[['code', 'gross_profit_margin']], 
                         on='code', 
                         suffixes=('', '_last'),
                         how='left')
            
            # 计算毛利率同比增长
            df['gross_margin_yoy'] = (df['gross_profit_margin'] - df['gross_profit_margin_last']) / (df['gross_profit_margin_last'] + 0.01)
            
            # 计算三费费率 (销售 + 管理 + 财务费用) / 营业收入
            df['fee_ratio'] = (df['operating_expense'] + df['sales_expense'] + 
                              df['administrative_expense'] + df['finance_expense']) / (df['total_operating_revenue'] + 0.01)
            
            df_last['fee_ratio'] = (df_last['operating_expense'] + df_last['sales_expense'] + 
                                   df_last['administrative_expense'] + df_last['finance_expense']) / (df_last['total_operating_revenue'] + 0.01)
            
            df = df.merge(df_last[['code', 'fee_ratio']], 
                         on='code', 
                         suffixes=('', '_last'),
                         how='left')
            
            # 计算费率同比变化
            df['fee_ratio_yoy'] = df['fee_ratio'] - df['fee_ratio_last']
        else:
            df['gross_margin_yoy'] = 0
            df['fee_ratio_yoy'] = 0
        
        # 保留需要的列
        result = df[['code', 'market_cap', 'revenue_yoy', 'gross_margin_yoy', 'fee_ratio_yoy']]
        result = result.set_index('code')
        
        return result
        
    except Exception as e:
        log.error(f'获取基本面数据失败：{str(e)}')
        return None


def get_current_quarter(date):
    """获取当前季度末日期"""
    month = date.month
    if month <= 3:
        return pd.Timestamp(f'{date.year}-03-31')
    elif month <= 6:
        return pd.Timestamp(f'{date.year}-06-30')
    elif month <= 9:
        return pd.Timestamp(f'{date.year}-09-30')
    else:
        return pd.Timestamp(f'{date.year}-12-31')


def get_last_quarter(date):
    """获取去年同期季度末日期"""
    year = date.year - 1
    month = date.month
    if month <= 3:
        return pd.Timestamp(f'{year}-03-31')
    elif month <= 6:
        return pd.Timestamp(f'{year}-06-30')
    elif month <= 9:
        return pd.Timestamp(f'{year}-09-30')
    else:
        return pd.Timestamp(f'{year}-12-31')


def filter_st_and_new_stocks(stock_list, date):
    """
    过滤 ST 股票和新上市股票
    """
    # 过滤 ST 股票
    st_stocks = get_stocks_by_status('ST', date=date)
    stock_list = [s for s in stock_list if s not in st_stocks]
    
    # 过滤新股 (上市不满一年)
    filtered = []
    for stock in stock_list:
        info = get_security_info(stock, date)
        if info and info.start_date:
            days_since_ipo = (date - info.start_date).days
            if days_since_ipo > 365:  # 上市满一年
                filtered.append(stock)
    
    return filtered


def filter_by_momentum(stock_list, context):
    """
    技术面筛选：按近期涨幅排序
    """
    momentum_data = []
    
    for stock in stock_list:
        try:
            # 获取近 LOOKBACK_DAYS 天的收盘价
            prices = attribute_history(stock, LOOKBACK_DAYS, '1d', ['close'])
            
            if prices is None or len(prices) < LOOKBACK_DAYS:
                continue
            
            # 计算近期涨幅
            momentum = (prices['close'].iloc[-1] - prices['close'].iloc[0]) / prices['close'].iloc[0]
            
            momentum_data.append({
                'code': stock,
                'momentum': momentum
            })
        except Exception as e:
            log.warning(f'获取 {stock} 动量数据失败：{str(e)}')
            continue
    
    if not momentum_data:
        return []
    
    # 按涨幅排序
    df_momentum = pd.DataFrame(momentum_data)
    df_momentum = df_momentum.sort_values('momentum', ascending=False)
    
    return df_momentum['code'].tolist()


# ==================== MACD 交易信号函数 ====================

def check_macd_signals(context, data):
    """
    检查 MACD 交易信号
    """
    # 检查买入信号
    check_buy_signals(context, data)
    
    # 检查卖出信号
    check_sell_signals(context, data)


def check_buy_signals(context, data):
    """
    检查 MACD 金叉买入信号
    """
    # 获取当前持仓数量
    current_positions = len(get_positions())
    
    if current_positions >= MAX_POSITIONS:
        return  # 已达到最大持仓数
    
    # 遍历候选股票池
    for stock in g.stock_pool:
        # 跳过已持仓股票
        if stock in g.holding_stocks:
            continue
        
        # 跳过停牌股票
        if is_paused(stock):
            continue
        
        # 获取 MACD 数据
        macd_data = get_macd_data(stock, context)
        
        if macd_data is None:
            continue
        
        # 判断金叉信号
        if is_golden_cross(macd_data):
            # 计算可买入金额
            cash = context.portfolio.cash
            position_value = cash * POSITION_PER_STOCK
            
            # 买入
            order_value(stock, position_value)
            
            # 记录买入价格
            g.buy_price[stock] = data[stock].close
            
            log.info(f'【买入信号】{stock} MACD 金叉，买入价格：{data[stock].close:.2f}')


def check_sell_signals(context, data):
    """
    检查 MACD 死叉卖出信号
    """
    positions = get_positions()
    
    for position in positions:
        stock = position.security
        
        # 获取 MACD 数据
        macd_data = get_macd_data(stock, context)
        
        if macd_data is None:
            continue
        
        # 判断死叉信号
        if is_dead_cross(macd_data):
            # 卖出
            order_target(stock, 0)
            
            # 清除买入价格记录
            if stock in g.buy_price:
                del g.buy_price[stock]
            
            log.info(f'【卖出信号】{stock} MACD 死叉，卖出价格：{data[stock].close:.2f}')


def get_macd_data(stock, context):
    """
    获取 MACD 指标数据
    """
    try:
        # 获取历史数据
        prices = attribute_history(stock, MACD_SLOW + MACD_SIGNAL + 10, '1d', ['close'])
        
        if prices is None or len(prices) < MACD_SLOW:
            return None
        
        # 计算 MACD
        close = prices['close'].values
        
        # 计算 EMA
        ema_fast = calculate_ema(close, MACD_FAST)
        ema_slow = calculate_ema(close, MACD_SLOW)
        
        # 计算 DIF
        dif = ema_fast - ema_slow
        
        # 计算 DEA (DIF 的 EMA)
        dea = calculate_ema(dif, MACD_SIGNAL)
        
        # 计算 MACD 柱
        macd_bar = (dif - dea) * 2
        
        return {
            'dif': dif,
            'dea': dea,
            'macd': macd_bar,
            'close': close
        }
        
    except Exception as e:
        log.warning(f'获取 {stock} MACD 数据失败：{str(e)}')
        return None


def calculate_ema(data, period):
    """
    计算指数移动平均线
    """
    ema = np.zeros_like(data)
    ema[0] = data[0]
    multiplier = 2 / (period + 1)
    
    for i in range(1, len(data)):
        ema[i] = (data[i] - ema[i-1]) * multiplier + ema[i-1]
    
    return ema


def is_golden_cross(macd_data):
    """
    判断 MACD 金叉信号
    条件：DIF 从下向上穿越 DEA
    """
    dif = macd_data['dif']
    dea = macd_data['dea']
    
    if len(dif) < 2:
        return False
    
    # 昨日 DIF < DEA，今日 DIF > DEA
    yesterday_cross = dif[-2] < dea[-2]
    today_cross = dif[-1] > dea[-1]
    
    # 且 MACD 柱由负转正
    macd_positive = macd_data['macd'][-1] > 0
    
    return yesterday_cross and today_cross and macd_positive


def is_dead_cross(macd_data):
    """
    判断 MACD 死叉信号
    条件：DIF 从上向下穿越 DEA
    """
    dif = macd_data['dif']
    dea = macd_data['dea']
    
    if len(dif) < 2:
        return False
    
    # 昨日 DIF > DEA，今日 DIF < DEA
    yesterday_cross = dif[-2] > dea[-2]
    today_cross = dif[-1] < dea[-1]
    
    # 且 MACD 柱由正转负
    macd_negative = macd_data['macd'][-1] < 0
    
    return yesterday_cross and today_cross and macd_negative


# ==================== 止盈止损函数 ====================

def check_stop_loss_take_profit(context, data):
    """
    检查止盈止损条件
    """
    positions = get_positions()
    
    for position in positions:
        stock = position.security
        current_price = data[stock].close
        
        # 获取买入价格
        if stock not in g.buy_price:
            g.buy_price[stock] = position.avg_cost
        
        buy_price = g.buy_price[stock]
        
        # 计算盈亏比例
        profit_rate = (current_price - buy_price) / buy_price
        
        # 止损检查
        if profit_rate <= -STOP_LOSS:
            order_target(stock, 0)
            if stock in g.buy_price:
                del g.buy_price[stock]
            log.info(f'【止损卖出】{stock} 当前价：{current_price:.2f}, 买入价：{buy_price:.2f}, 亏损：{profit_rate*100:.2f}%')
            continue
        
        # 止盈检查
        if profit_rate >= TAKE_PROFIT:
            order_target(stock, 0)
            if stock in g.buy_price:
                del g.buy_price[stock]
            log.info(f'【止盈卖出】{stock} 当前价：{current_price:.2f}, 买入价：{buy_price:.2f}, 盈利：{profit_rate*100:.2f}%')


# ==================== 辅助函数 ====================

def get_positions():
    """
    获取当前持仓列表
    """
    return [p for p in context.portfolio.positions.values() if p.amount > 0]


def is_paused(stock):
    """
    判断股票是否停牌
    """
    try:
        # 获取当日数据
        current_data = get_current_data()
        return current_data[stock].paused
    except:
        return False


# ==================== 分析函数 ====================

def analyze(context):
    """
    策略分析函数（回测结束后调用）
    """
    log.info('=== 策略回测结束 ===')
    log.info(f'最终资产：{context.portfolio.total_value:.2f}')
    log.info(f'初始资产：{context.portfolio.starting_cash:.2f}')
    
    # 计算收益率
    total_return = (context.portfolio.total_value - context.portfolio.starting_cash) / context.portfolio.starting_cash
    log.info(f'总收益率：{total_return*100:.2f}%')


# ==================== 回测配置说明 ====================

"""
回测配置（在聚宽平台设置）：

1. 回测时间：2025-09-15 至 2026-03-15（近半年）
2. 初始资金：1000000（100 万）
3. 交易频率：日线
4. 基准：沪深 300 (000300.XSHG)
5. 滑点：0.2%
6. 手续费：买入 0.03%，卖出 0.13%，最低 5 元

使用方法：
1. 登录聚宽平台 (https://www.joinquant.com)
2. 创建新策略，选择 Python3
3. 复制本代码到策略编辑器
4. 设置回测参数
5. 运行回测

策略优化建议：
1. 可根据实际情况调整基本面筛选阈值
2. 可尝试不同的 MACD 参数组合
3. 可加入更多技术指标（如 RSI、KDJ 等）
4. 可优化仓位管理策略
5. 可加入大盘择时逻辑（熊市降低仓位）
"""
