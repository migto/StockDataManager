# 数据库结构总结

## 概述

本数据库设计用于A股日线数据下载系统，包含6个核心表、13个索引、3个视图和3个触发器，支持完整的数据存储和管理功能。

## 数据库信息

- **数据库类型**: SQLite 3
- **文件路径**: `data/stock_data.db`
- **创建时间**: 2025-07-13
- **版本**: 1.0.0
- **特性**: WAL模式，外键约束，自动时间戳

## 表结构

### 1. stocks - 股票基本信息表
存储所有A股股票的基本信息。

| 字段名 | 类型 | 约束 | 说明 |
|--------|------|------|------|
| ts_code | TEXT | PRIMARY KEY | 股票代码 (如: 000001.SZ) |
| symbol | TEXT | NOT NULL | 股票简称 (如: 000001) |
| name | TEXT | NOT NULL | 股票名称 (如: 平安银行) |
| area | TEXT | | 所在地区 |
| industry | TEXT | | 所属行业 |
| list_date | DATE | | 上市日期 |
| market | TEXT | | 市场类型 |
| exchange | TEXT | | 交易所代码 |
| curr_type | TEXT | | 交易货币 |
| list_status | TEXT | | 上市状态 |
| delist_date | DATE | | 退市日期 |
| created_at | TIMESTAMP | DEFAULT CURRENT_TIMESTAMP | 创建时间 |
| updated_at | TIMESTAMP | DEFAULT CURRENT_TIMESTAMP | 更新时间 |

### 2. daily_data - 日线数据表
存储股票的日线交易数据。

| 字段名 | 类型 | 约束 | 说明 |
|--------|------|------|------|
| id | INTEGER | PRIMARY KEY AUTOINCREMENT | 主键ID |
| ts_code | TEXT | NOT NULL | 股票代码 |
| trade_date | DATE | NOT NULL | 交易日期 |
| open | REAL | | 开盘价 |
| high | REAL | | 最高价 |
| low | REAL | | 最低价 |
| close | REAL | | 收盘价 |
| pre_close | REAL | | 前收盘价 |
| change | REAL | | 涨跌额 |
| pct_chg | REAL | | 涨跌幅 (%) |
| vol | REAL | | 成交量 (手) |
| amount | REAL | | 成交额 (千元) |
| created_at | TIMESTAMP | DEFAULT CURRENT_TIMESTAMP | 创建时间 |

**约束**: UNIQUE(ts_code, trade_date) - 联合唯一约束

### 3. download_status - 下载状态跟踪表
跟踪每只股票的下载状态和进度。

| 字段名 | 类型 | 约束 | 说明 |
|--------|------|------|------|
| ts_code | TEXT | PRIMARY KEY | 股票代码 |
| last_download_date | DATE | | 最后下载日期 |
| total_records | INTEGER | DEFAULT 0 | 总记录数 |
| status | TEXT | DEFAULT 'pending' | 下载状态 |
| error_message | TEXT | | 错误信息 |
| retry_count | INTEGER | DEFAULT 0 | 重试次数 |
| updated_at | TIMESTAMP | DEFAULT CURRENT_TIMESTAMP | 更新时间 |

### 4. system_config - 系统配置表
存储系统运行的各种配置参数。

| 字段名 | 类型 | 约束 | 说明 |
|--------|------|------|------|
| key | TEXT | PRIMARY KEY | 配置键 |
| value | TEXT | | 配置值 |
| description | TEXT | | 配置说明 |
| data_type | TEXT | DEFAULT 'string' | 数据类型 |
| updated_at | TIMESTAMP | DEFAULT CURRENT_TIMESTAMP | 更新时间 |

### 5. api_call_log - API调用记录表
记录API调用历史，用于频率控制。

| 字段名 | 类型 | 约束 | 说明 |
|--------|------|------|------|
| id | INTEGER | PRIMARY KEY AUTOINCREMENT | 主键ID |
| api_name | TEXT | NOT NULL | API名称 |
| call_time | TIMESTAMP | DEFAULT CURRENT_TIMESTAMP | 调用时间 |
| success | BOOLEAN | DEFAULT 0 | 是否成功 |
| response_time | INTEGER | | 响应时间(ms) |
| records_count | INTEGER | DEFAULT 0 | 返回记录数 |
| error_message | TEXT | | 错误信息 |
| request_params | TEXT | | 请求参数 |

### 6. trade_calendar - 交易日历表
存储交易日历信息，用于确定交易日。

| 字段名 | 类型 | 约束 | 说明 |
|--------|------|------|------|
| cal_date | DATE | PRIMARY KEY | 日期 |
| is_open | BOOLEAN | DEFAULT 0 | 是否开市 |
| pretrade_date | DATE | | 上一交易日 |
| created_at | TIMESTAMP | DEFAULT CURRENT_TIMESTAMP | 创建时间 |

## 索引

### 日线数据表索引
- `idx_daily_data_ts_code`: 股票代码索引
- `idx_daily_data_trade_date`: 交易日期索引
- `idx_daily_data_ts_code_date`: 股票代码+交易日期复合索引

### 股票基本信息表索引
- `idx_stocks_symbol`: 股票简称索引
- `idx_stocks_name`: 股票名称索引
- `idx_stocks_industry`: 行业索引
- `idx_stocks_list_date`: 上市日期索引

### API调用记录表索引
- `idx_api_call_log_api_name`: API名称索引
- `idx_api_call_log_call_time`: 调用时间索引
- `idx_api_call_log_success`: 成功状态索引

### 其他索引
- `idx_download_status_status`: 下载状态索引
- `idx_download_status_last_download_date`: 最后下载日期索引
- `idx_trade_calendar_is_open`: 开市状态索引

## 视图

### 1. v_active_stocks - 活跃股票视图
显示所有上市且未退市的股票。

```sql
SELECT ts_code, symbol, name, area, industry, list_date, market, exchange
FROM stocks
WHERE list_status = 'L' AND delist_date IS NULL;
```

### 2. v_latest_daily_data - 最新日线数据视图
显示每只股票的最新交易数据。

```sql
SELECT d.ts_code, s.name, d.trade_date, d.close, d.pct_chg, d.vol, d.amount
FROM daily_data d
JOIN stocks s ON d.ts_code = s.ts_code
WHERE d.trade_date = (
    SELECT MAX(trade_date)
    FROM daily_data d2
    WHERE d2.ts_code = d.ts_code
);
```

### 3. v_download_progress - 下载进度统计视图
显示下载进度统计信息。

```sql
SELECT 
    COUNT(*) as total_stocks,
    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_stocks,
    SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as error_stocks,
    SUM(CASE WHEN status = 'downloading' THEN 1 ELSE 0 END) as downloading_stocks,
    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending_stocks,
    SUM(total_records) as total_records
FROM download_status;
```

## 触发器

### 1. update_stocks_timestamp
自动更新stocks表的updated_at字段。

### 2. update_download_status_timestamp
自动更新download_status表的updated_at字段。

### 3. update_system_config_timestamp
自动更新system_config表的updated_at字段。

## 初始系统配置

系统预置了以下配置项：

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| tushare_token | (用户提供) | Tushare Pro API Token |
| db_version | 1.0.0 | 数据库版本 |
| api_call_limit_per_minute | 60 | 每分钟API调用限制 |
| api_call_limit_per_hour | 500 | 每小时API调用限制 |
| api_call_limit_per_day | 3000 | 每天API调用限制 |
| download_batch_size | 200 | 批量下载大小 |
| retry_max_count | 3 | 最大重试次数 |
| retry_delay_seconds | 1 | 重试延迟秒数 |
| log_level | INFO | 日志级别 |
| auto_download_enabled | 1 | 是否启用自动下载 |
| download_start_time | 09:00 | 下载开始时间 |
| download_end_time | 18:00 | 下载结束时间 |

## 设计特点

1. **日期类型优化**: 所有日期字段使用DATE类型，便于查询和排序
2. **性能优化**: 创建了全面的索引，提高查询效率
3. **数据完整性**: 使用外键约束和联合唯一约束
4. **自动化**: 触发器自动更新时间戳
5. **扩展性**: 视图提供便捷的数据访问方式
6. **监控**: API调用记录表支持频率控制
7. **配置化**: 系统配置表支持灵活的参数管理

## 使用示例

### 基本查询
```sql
-- 查询所有上市股票
SELECT * FROM v_active_stocks;

-- 查询特定股票的日线数据
SELECT * FROM daily_data WHERE ts_code = '000001.SZ' ORDER BY trade_date DESC;

-- 查看下载进度
SELECT * FROM v_download_progress;
```

### 管理操作
```bash
# 初始化数据库
python src/database_manager.py --init

# 查看数据库信息
python src/database_manager.py --info

# 验证数据库结构
python src/database_schema_validator.py

# 备份数据库
python src/database_manager.py --backup
``` 