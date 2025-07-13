# 数据存储和查询功能使用指南

## 概述

本文档介绍第三阶段开发的数据存储和查询功能，包括完整的数据库操作和日线数据管理。

## 功能特性

### 1. 数据库管理器 (DatabaseManager)

#### 完整的CRUD操作
- **查询**: `execute_query()` - 执行查询语句
- **插入**: `execute_insert()` - 单条插入，`execute_batch_insert()` - 批量插入
- **更新**: `execute_update()` - 更新操作
- **删除**: `execute_delete()` - 删除操作

#### 高级操作
- **智能插入**: `insert_or_update()` - 插入或更新单条记录
- **批量智能插入**: `bulk_insert_or_update()` - 批量插入或更新
- **事务处理**: `execute_transaction()` - 事务操作

#### 数据库分析
- **表信息**: `get_table_info()` - 获取表结构信息
- **统计信息**: `get_table_statistics()` - 获取表统计数据
- **数据库大小**: `get_database_size()` - 获取数据库大小信息

#### 维护功能
- **数据库优化**: `vacuum_database()` - 清理和优化数据库
- **备份**: `backup_database()` - 备份数据库

### 2. 数据存储管理器 (DataStorageManager)

#### 批量数据插入
- **智能去重**: 自动检查并过滤重复数据
- **数据验证**: 验证数据完整性和合理性
- **批量处理**: 支持大量数据的高效插入

#### 数据查询
- **多种查询方式**: 按股票代码、日期范围、记录数限制
- **高性能查询**: 优化的查询语句和索引使用
- **结果格式化**: 返回标准DataFrame格式

#### 数据分析
- **缺失数据检查**: 识别指定时间段内的缺失交易日
- **覆盖度报告**: 分析数据覆盖情况和完整性
- **统计信息**: 提供详细的数据统计

#### 数据维护
- **重复数据清理**: 自动识别并清理重复记录
- **数据完整性检查**: 验证数据的一致性
- **状态监控**: 实时显示操作统计和数据状态

## 使用示例

### 1. 基本数据库操作

```python
from src.database_manager import DatabaseManager
from src.config_manager import ConfigManager

# 初始化
config = ConfigManager()
db_manager = DatabaseManager(config.get('database_path'))

# 插入数据
stock_data = {
    'ts_code': '000001.SZ',
    'symbol': '000001',
    'name': '平安银行',
    'list_status': 'L'
}
db_manager.insert_or_update('stocks', stock_data, ['ts_code'])

# 查询数据
results = db_manager.execute_query("SELECT * FROM stocks WHERE ts_code = ?", ('000001.SZ',))

# 更新数据
db_manager.execute_update("UPDATE stocks SET name = ? WHERE ts_code = ?", ('新名称', '000001.SZ'))

# 删除数据
db_manager.execute_delete("DELETE FROM stocks WHERE ts_code = ?", ('000001.SZ',))
```

### 2. 数据存储管理

```python
from src.data_storage_manager import DataStorageManager
from src.config_manager import ConfigManager
import pandas as pd

# 初始化
config = ConfigManager()
storage_manager = DataStorageManager(config)

# 批量插入日线数据
daily_data = pd.DataFrame({
    'ts_code': ['000001.SZ', '000002.SZ'],
    'trade_date': ['2025-01-10', '2025-01-10'],
    'open': [10.50, 20.30],
    'high': [11.20, 21.00],
    'low': [10.30, 20.00],
    'close': [10.80, 20.60],
    'vol': [1000000, 800000],
    'amount': [10800000, 16480000]
})

result = storage_manager.bulk_insert_daily_data(daily_data)
print(f"插入结果: {result}")

# 查询数据
# 查询所有数据
all_data = storage_manager.query_daily_data(limit=100)

# 查询特定股票
stock_data = storage_manager.query_daily_data(ts_code='000001.SZ')

# 查询日期范围
date_data = storage_manager.query_daily_data(
    start_date='2025-01-01', 
    end_date='2025-01-31'
)

# 检查缺失数据
missing_dates = storage_manager.get_missing_data_dates(
    '000001.SZ', '2025-01-01', '2025-01-31'
)

# 获取覆盖度报告
coverage_report = storage_manager.get_data_coverage_report()
```

### 3. 命令行使用

#### 数据库管理器
```bash
# 显示数据库信息
python -m src.database_manager --info

# 备份数据库
python -m src.database_manager --backup

# 初始化数据库
python -m src.database_manager --init
```

#### 数据存储管理器
```bash
# 显示状态
python -m src.data_storage_manager --status

# 查询数据
python -m src.data_storage_manager --query "000001.SZ,2025-01-01,2025-01-31"

# 检查缺失数据
python -m src.data_storage_manager --missing "000001.SZ,2025-01-01,2025-01-31"

# 数据覆盖度报告
python -m src.data_storage_manager --coverage

# 清理重复数据
python -m src.data_storage_manager --clean
```

## 高级功能

### 1. 事务处理

```python
# 定义事务操作
operations = [
    {
        'type': 'insert',
        'sql': 'INSERT INTO stocks (ts_code, name) VALUES (?, ?)',
        'params': ('000001.SZ', '平安银行')
    },
    {
        'type': 'update',
        'sql': 'UPDATE stocks SET list_status = ? WHERE ts_code = ?',
        'params': ('L', '000001.SZ')
    }
]

# 执行事务
success = db_manager.execute_transaction(operations)
```

### 2. 数据分析和报告

```python
# 获取表信息
table_info = db_manager.get_table_info('daily_data')
print(f"表结构: {table_info}")

# 获取统计信息
stats = db_manager.get_table_statistics('daily_data')
print(f"统计信息: {stats}")

# 数据库大小
size_info = db_manager.get_database_size()
print(f"数据库大小: {size_info['file_size_mb']} MB")
```

### 3. 数据验证和清理

```python
# 数据验证会自动进行：
# - 价格数据合理性检查
# - 高低价格关系验证
# - 成交量和成交额检查
# - 移除无效数据

# 清理重复数据
deleted_count = storage_manager.clean_duplicate_data()
print(f"清理了 {deleted_count} 条重复数据")

# 获取操作统计
stats = storage_manager.get_statistics()
print(f"操作统计: {stats}")
```

## 错误处理

所有操作都包含完善的错误处理：

```python
try:
    result = storage_manager.bulk_insert_daily_data(data)
    if result['errors'] > 0:
        print(f"插入时发生 {result['errors']} 个错误")
except Exception as e:
    print(f"操作失败: {e}")
```

## 性能优化

### 1. 批量操作
- 使用 `bulk_insert_or_update()` 而不是循环插入
- 使用事务处理大量操作

### 2. 查询优化
- 使用索引字段进行查询
- 适当使用 `limit` 参数限制结果数量

### 3. 数据库维护
- 定期执行 `vacuum_database()` 优化数据库
- 监控数据库大小和性能

## 最佳实践

1. **数据验证**: 始终验证数据完整性
2. **错误处理**: 处理所有可能的异常情况
3. **性能监控**: 监控操作统计和数据库性能
4. **定期维护**: 定期清理重复数据和优化数据库
5. **备份策略**: 制定数据备份计划

## 技术特点

- **高性能**: 优化的批量操作和查询
- **可靠性**: 完善的错误处理和事务支持
- **易用性**: 简单直观的API设计
- **可扩展**: 模块化设计，易于扩展
- **可维护**: 完整的日志和监控功能

## 注意事项

1. 大量数据插入时建议分批处理
2. 定期监控数据库大小和性能
3. 使用事务处理相关操作
4. 注意外键约束和数据完整性
5. 合理设置查询限制避免内存溢出 