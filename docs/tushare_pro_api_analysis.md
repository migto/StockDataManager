# Tushare Pro API接口分析

## 概述

基于用户提供的文档链接 https://tushare.pro/document/2?doc_id=27 和Tushare Pro官方文档，本文档详细分析了我们项目需要使用的核心API接口。

## 核心接口分析

### 1. A股日线行情接口 (daily)

**接口名称**: `daily`
**接口说明**: 获取A股日线行情数据
**数据说明**: 
- 交易日每天15点～16点之间入库
- 未复权行情数据
- 停牌期间不提供数据

**调用限制**:
- 基础积分：每分钟最多2次调用
- 免费账户：每日120点数限制
- 每次最多获取6000条数据
- 一次请求相当于提取一个股票23年历史数据

**输入参数**:
| 参数名 | 类型 | 必选 | 描述 |
|--------|------|------|------|
| ts_code | str | N | 股票代码（支持多个股票同时提取，逗号分隔）|
| trade_date | str | N | 交易日期（YYYYMMDD格式）|
| start_date | str | N | 开始日期（YYYYMMDD格式）|
| end_date | str | N | 结束日期（YYYYMMDD格式）|

**输出参数**:
| 参数名 | 类型 | 描述 |
|--------|------|------|
| ts_code | str | 股票代码 |
| trade_date | str | 交易日期 |
| open | float | 开盘价 |
| high | float | 最高价 |
| low | float | 最低价 |
| close | float | 收盘价 |
| pre_close | float | 昨收价【除权价，前复权】|
| change | float | 涨跌额 |
| pct_chg | float | 涨跌幅 |
| vol | float | 成交量（手）|
| amount | float | 成交额（千元）|

**调用示例**:
```python
import tushare as ts

# 初始化API
pro = ts.pro_api('YOUR_TOKEN')

# 获取单只股票的历史数据
df = pro.daily(ts_code='000001.SZ', start_date='20230101', end_date='20231231')

# 获取指定日期全部股票数据（推荐方式）
df = pro.daily(trade_date='20240101')

# 获取多只股票数据
df = pro.daily(ts_code='000001.SZ,000002.SZ', start_date='20230101', end_date='20231231')
```

### 2. 股票基础信息接口 (stock_basic)

**接口名称**: `stock_basic`
**接口说明**: 获取A股基础信息数据
**数据说明**: 
- 包含股票代码、名称、上市日期等基本信息
- 数据相对稳定，建议本地缓存
- 每日最多更新一次

**调用限制**:
- 基础积分：每小时最多调用1次
- 免费账户：消耗1点积分

**输入参数**:
| 参数名 | 类型 | 必选 | 描述 |
|--------|------|------|------|
| is_hs | str | N | 是否沪深港通标的，N否 H沪股通 S深股通 |
| list_status | str | N | 上市状态： L上市 D退市 P暂停上市，默认L |
| exchange | str | N | 交易所 SSE上交所 SZSE深交所 |
| ts_code | str | N | 股票代码 |
| market | str | N | 市场类别 |
| limit | int | N | 单次返回数据长度 |
| offset | int | N | 数据偏移量 |

**输出参数**:
| 参数名 | 类型 | 描述 |
|--------|------|------|
| ts_code | str | TS代码 |
| symbol | str | 股票代码 |
| name | str | 股票名称 |
| area | str | 所在地域 |
| industry | str | 所属行业 |
| market | str | 市场类型 |
| exchange | str | 交易所代码 |
| curr_type | str | 交易货币 |
| list_date | str | 上市日期 |
| list_status | str | 上市状态 |
| delist_date | str | 退市日期 |
| is_hs | str | 是否沪深港通标的 |

**调用示例**:
```python
# 获取全部A股基础信息
df = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')

# 获取上交所股票信息
df = pro.stock_basic(exchange='SSE')

# 获取深交所股票信息
df = pro.stock_basic(exchange='SZSE')
```

### 3. 交易日历接口 (trade_cal)

**接口名称**: `trade_cal`
**接口说明**: 获取交易日历数据
**数据说明**: 
- 包含交易日期、是否开市等信息
- 用于判断交易日和节假日
- 数据稳定，建议本地缓存

**调用限制**:
- 基础积分：每小时最多调用1次
- 免费账户：消耗1点积分

**输入参数**:
| 参数名 | 类型 | 必选 | 描述 |
|--------|------|------|------|
| exchange | str | N | 交易所 SSE上交所 SZSE深交所 |
| start_date | str | N | 开始日期 |
| end_date | str | N | 结束日期 |
| is_open | str | N | 是否交易 '0'休市 '1'交易 |

**输出参数**:
| 参数名 | 类型 | 描述 |
|--------|------|------|
| exchange | str | 交易所 |
| cal_date | str | 日历日期 |
| is_open | int | 是否交易 0休市 1交易 |
| pretrade_date | str | 上一个交易日 |

**调用示例**:
```python
# 获取交易日历
df = pro.trade_cal(exchange='', start_date='20230101', end_date='20231231')

# 获取交易日
df = pro.trade_cal(exchange='', start_date='20230101', end_date='20231231', is_open='1')
```

## 免费账户限制分析

### 积分限制
- **每日总积分**: 120点
- **每分钟调用**: 最多2次
- **每小时调用**: 最多60次
- **每日调用**: 最多120次

### 接口消耗分析
| 接口名称 | 积分消耗 | 备注 |
|----------|----------|------|
| daily | 1点/次 | 每次最多6000条数据 |
| stock_basic | 1点/次 | 建议每日最多调用1次 |
| trade_cal | 1点/次 | 建议每日最多调用1次 |

### 优化策略
1. **按日期批量获取**: 使用`trade_date`参数一次获取全部股票某日数据
2. **本地缓存**: 股票基础信息和交易日历数据本地缓存
3. **增量更新**: 只下载缺失的交易日数据
4. **频率控制**: 严格控制API调用频率

## 最佳实践建议

### 1. 数据获取顺序
1. 首先获取交易日历数据
2. 获取股票基础信息
3. 按交易日期批量获取日线数据

### 2. 错误处理
- 网络超时重试
- API限制处理
- 数据缺失处理

### 3. 数据存储
- 使用SQLite本地存储
- 建立适当的索引
- 实现数据去重逻辑

### 4. 性能优化
- 避免重复下载
- 实现增量更新
- 合理使用缓存

## 技术实现要点

### 1. API初始化
```python
import tushare as ts

# 设置token
ts.set_token('YOUR_TOKEN')

# 初始化API
pro = ts.pro_api()
```

### 2. 频率控制
```python
import time
from datetime import datetime

class RateLimiter:
    def __init__(self, max_calls_per_minute=2):
        self.max_calls_per_minute = max_calls_per_minute
        self.call_times = []
    
    def wait_if_needed(self):
        now = datetime.now()
        # 移除一分钟前的记录
        self.call_times = [t for t in self.call_times if (now - t).seconds < 60]
        
        if len(self.call_times) >= self.max_calls_per_minute:
            sleep_time = 60 - (now - self.call_times[0]).seconds
            time.sleep(sleep_time)
        
        self.call_times.append(now)
```

### 3. 数据验证
```python
def validate_daily_data(df):
    """验证日线数据完整性"""
    required_columns = ['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'vol']
    
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")
    
    # 检查数据类型
    numeric_columns = ['open', 'high', 'low', 'close', 'vol', 'amount']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df
```

## 项目配置更新建议

基于API分析，建议更新项目配置：

### 1. API限制配置
```json
{
    "api_limits": {
        "free_account": {
            "total_points": 120,
            "calls_per_minute": 2,
            "calls_per_hour": 60,
            "calls_per_day": 120
        },
        "interface_costs": {
            "daily": 1,
            "stock_basic": 1,
            "trade_cal": 1
        }
    }
}
```

### 2. 下载策略配置
```json
{
    "download_strategy": {
        "prefer_by_date": true,
        "batch_size": 1,
        "max_retries": 3,
        "retry_delay": 60,
        "cache_basic_info": true,
        "cache_trade_calendar": true
    }
}
```

## 总结

1. **daily接口是核心**: 用于获取日线行情数据
2. **免费账户限制严格**: 每日120点数，需精确控制
3. **按日期批量获取最优**: 避免按股票代码循环
4. **本地缓存重要**: 减少API调用次数
5. **严格频率控制**: 每分钟最多2次调用

这个分析为下一步实现TushareAPIManager类提供了详细的技术基础。 