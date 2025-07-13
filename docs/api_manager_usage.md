# TushareAPIManager 使用说明

## 概述

本项目提供了两个版本的Tushare API管理器：

1. **完整版** (`tushare_api_manager.py`) - 功能完整，包含数据库集成
2. **优化版** (`optimized_tushare_api_manager.py`) - 专为免费账户优化，更简单易用

推荐使用**优化版**，特别是对于免费账户用户。

## 优化版API管理器特性

### 核心功能
- ✅ 专注于daily接口（免费账户主要可用接口）
- ✅ 智能频率控制（每分钟最多2次，30秒最小间隔）
- ✅ 本地缓存策略（自动缓存下载的数据）
- ✅ 内置交易日历（预配置26个节假日）
- ✅ 批量下载支持
- ✅ 完整的命令行接口

### 权限限制解决方案
| 接口 | 免费账户限制 | 解决方案 |
|------|-------------|----------|
| daily | ✅ 可用 | 主要数据源，智能频率控制 |
| stock_basic | ⚠️ 每日5次限制 | 本地缓存策略（计划中） |
| trade_cal | ❌ 无权限 | 使用内置交易日历 |

## 安装和配置

### 1. 确保配置正确
```bash
# 检查配置文件
python src/config_manager.py validate

# 如果需要设置Token
python src/config_manager.py set tushare.token YOUR_TOKEN_HERE
```

### 2. 验证API连接
```bash
# 检查API状态
python src/optimized_tushare_api_manager.py --status
```

## 基本使用

### 1. 获取单日数据
```bash
# 获取指定日期的日线数据
python src/optimized_tushare_api_manager.py --daily 2025-07-11

# 获取最近交易日的数据
python src/optimized_tushare_api_manager.py --recent 1
```

### 2. 批量下载数据
```bash
# 下载单日数据
python src/optimized_tushare_api_manager.py --batch 2025-07-11

# 下载多日数据
python src/optimized_tushare_api_manager.py --batch 2025-07-09,2025-07-11

# 下载最近几个交易日
python src/optimized_tushare_api_manager.py --recent 3
```

### 3. 查看交易日历
```bash
# 显示最近10个交易日
python src/optimized_tushare_api_manager.py --trade-dates 10

# 显示最近5个交易日
python src/optimized_tushare_api_manager.py --trade-dates 5
```

### 4. 缓存管理
```bash
# 清理30天前的缓存
python src/optimized_tushare_api_manager.py --clear-cache 30

# 清理7天前的缓存
python src/optimized_tushare_api_manager.py --clear-cache 7
```

## 编程接口使用

### 基本示例
```python
from optimized_tushare_api_manager import OptimizedTushareAPIManager

# 初始化API管理器
api = OptimizedTushareAPIManager()

# 获取最近交易日的数据
df = api.get_daily_data()
print(f"获取到 {len(df)} 条记录")

# 获取指定日期的数据
df = api.get_daily_data(trade_date='2025-07-11')

# 检查API状态
status = api.get_api_status()
print(f"今日已调用 {status['calls_today']} 次")
```

### 批量下载示例
```python
# 批量下载多日数据
result = api.batch_download_daily_data('2025-07-09', '2025-07-11')
print(f"成功率: {result['success_rate']}%")
print(f"总记录数: {result['total_records']}")
```

### 交易日历示例
```python
# 检查是否为交易日
is_trade = api.is_trade_date('2025-07-11')
print(f"2025-07-11 是交易日: {is_trade}")

# 获取最近的交易日
trade_dates = api.get_recent_trade_dates(5)
print(f"最近5个交易日: {trade_dates}")
```

## 数据格式

### daily接口返回的数据字段
| 字段名 | 类型 | 描述 |
|--------|------|------|
| ts_code | str | 股票代码 |
| trade_date | str | 交易日期（YYYY-MM-DD格式） |
| open | float | 开盘价 |
| high | float | 最高价 |
| low | float | 最低价 |
| close | float | 收盘价 |
| pre_close | float | 昨收价 |
| change | float | 涨跌额 |
| pct_chg | float | 涨跌幅（%） |
| vol | float | 成交量（手） |
| amount | float | 成交额（千元） |

### 数据示例
```csv
ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount
000001.SZ,2025-07-11,13.19,13.30,12.89,12.91,13.18,-0.27,-2.0486,2443743.50,3200219.566
000002.SZ,2025-07-11,6.75,6.79,6.68,6.76,6.76,0.00,0.0000,1628706.59,1099511.183
```

## 缓存机制

### 缓存目录结构
```
data/cache/
├── daily_20250709.csv    # 2025-07-09的日线数据
├── daily_20250710.csv    # 2025-07-10的日线数据
└── daily_20250711.csv    # 2025-07-11的日线数据
```

### 缓存特性
- 自动缓存所有下载的数据
- 避免重复API调用
- 支持手动清理过期缓存
- CSV格式，便于Excel等工具打开

## 频率控制策略

### 安全限制
- **每分钟最多2次调用** - 符合Tushare免费账户限制
- **30秒最小间隔** - 避免频繁调用
- **每日最多100次调用** - 保守的安全限制
- **智能等待机制** - 自动等待到可以调用的时间

### 调用统计
```python
# 查看今日调用统计
status = api.get_api_status()
print(f"今日已调用: {status['calls_today']} 次")
print(f"今日剩余: {status['calls_remaining_today']} 次")
print(f"本分钟剩余: {status['calls_this_minute']} 次")
```

## 错误处理

### 常见错误及解决方案

1. **Token未配置**
   ```
   错误: Tushare Token未配置，请先配置API Token
   解决: python src/config_manager.py set tushare.token YOUR_TOKEN
   ```

2. **权限不足**
   ```
   错误: 抱歉，您没有接口访问权限
   解决: 该接口免费账户无权限，使用daily接口替代
   ```

3. **频率限制**
   ```
   错误: 抱歉，您每分钟最多访问该接口1次
   解决: 等待或使用缓存数据
   ```

4. **无数据返回**
   ```
   情况: 指定日期返回空数据
   原因: 可能不是交易日或数据未更新
   解决: 检查交易日历或选择其他日期
   ```

## 性能建议

### 最佳实践
1. **优先使用缓存** - 避免重复下载相同数据
2. **批量下载** - 一次下载多天数据更高效
3. **避免频繁调用** - 利用30秒间隔充分使用缓存
4. **合理安排时间** - 避免在开盘时间大量调用API

### 典型使用场景
```python
# 场景1: 每日更新（推荐）
api = OptimizedTushareAPIManager()
df = api.get_daily_data()  # 获取最新交易日数据

# 场景2: 历史数据补全
result = api.batch_download_daily_data('2025-07-01', '2025-07-10')

# 场景3: 数据分析前的检查
trade_dates = api.get_recent_trade_dates(10)
for date in trade_dates:
    df = api.get_daily_data(date, use_cache=True)  # 优先使用缓存
```

## 与数据库集成

虽然优化版API管理器不直接集成数据库，但可以轻松与数据库系统结合：

```python
from database_manager import DatabaseManager
from optimized_tushare_api_manager import OptimizedTushareAPIManager

# 初始化
api = OptimizedTushareAPIManager()
db = DatabaseManager()

# 获取数据并存储到数据库
df = api.get_daily_data('2025-07-11')
if df is not None:
    # 这里可以添加数据库存储逻辑
    print(f"可以存储 {len(df)} 条记录到数据库")
```

## 总结

OptimizedTushareAPIManager为免费账户用户提供了一个实用、可靠的股票数据获取解决方案：

- 🎯 **专注核心功能** - daily接口满足主要需求
- 🛡️ **智能保护** - 频率控制避免超限
- 💾 **本地缓存** - 减少API调用，提高效率
- 📅 **内置日历** - 无需额外权限即可判断交易日
- 🔄 **批量支持** - 高效下载历史数据
- 📋 **完整接口** - 命令行和编程两种使用方式

这个设计在免费账户限制下实现了项目的核心功能，为后续的数据分析和处理奠定了坚实基础。 