# 增量更新策略使用指南

## 概述

增量更新管理器（IncrementalUpdateManager）提供智能的增量更新策略，只更新缺失或变化的数据，避免重复下载，提高效率。

## 核心功能

### 1. 缺失交易日检测
- 自动检测数据库中缺失的交易日
- 智能交易日历生成（排除周末和节假日）
- 支持自定义日期范围分析

### 2. 股票数据覆盖率分析
- 分析每只股票的数据覆盖情况
- 计算覆盖率统计（完全覆盖、部分覆盖、无覆盖）
- 识别低覆盖率股票

### 3. 增量更新计划生成
- 支持多种更新策略：
  - `missing_days`: 更新缺失的交易日
  - `recent_days`: 更新最近几天的数据
  - `specific_stocks`: 更新特定股票的数据
- 智能任务规划和优先级设置

### 4. 更新计划执行
- 支持试运行模式（dry_run）
- 任务执行进度跟踪
- 成功率统计和错误处理

### 5. 更新历史记录
- 记录所有更新操作
- 提供历史查询功能
- 支持更新状态管理

## 使用方法

### 命令行使用

#### 1. 检查缺失交易日
```bash
python src/incremental_update_manager.py --missing-days
```

#### 2. 检查特定日期缺失数据的股票
```bash
python src/incremental_update_manager.py --missing-stocks 20250110
```

#### 3. 分析数据覆盖情况
```bash
python src/incremental_update_manager.py --coverage
```

#### 4. 规划增量更新
```bash
# 规划缺失日期更新
python src/incremental_update_manager.py --plan-update missing_days --max-days 30

# 规划最近日期更新
python src/incremental_update_manager.py --plan-update recent_days --max-days 7

# 规划特定股票更新
python src/incremental_update_manager.py --plan-update specific_stocks
```

#### 5. 执行更新计划
```bash
# 试运行模式
python src/incremental_update_manager.py --execute-plan plan.json --dry-run

# 实际执行
python src/incremental_update_manager.py --execute-plan plan.json
```

#### 6. 查看更新历史
```bash
python src/incremental_update_manager.py --history
```

### 编程接口使用

#### 1. 基本初始化
```python
from src.incremental_update_manager import IncrementalUpdateManager

manager = IncrementalUpdateManager()
```

#### 2. 获取缺失交易日
```python
result = manager.get_missing_trading_days('20250101', '20250131')
print(f"缺失交易日: {result['missing_trading_days']}")
```

#### 3. 分析数据覆盖情况
```python
coverage = manager.get_stocks_data_coverage('20250101', '20250131')
stats = coverage['coverage_statistics']
print(f"完全覆盖: {stats['full_coverage']}")
print(f"部分覆盖: {stats['partial_coverage']}")
```

#### 4. 规划增量更新
```python
plan_result = manager.plan_incremental_update('missing_days', max_days=10)
if plan_result['success']:
    plan = plan_result['update_plan']
    print(f"规划任务数: {plan['statistics']['total_tasks']}")
    print(f"预计API调用数: {plan['statistics']['estimated_api_calls']}")
```

#### 5. 执行更新计划
```python
execution_result = manager.execute_incremental_update(plan, dry_run=True)
if execution_result['success']:
    result = execution_result['execution_result']
    print(f"成功率: {result['success_rate']:.1f}%")
```

## 配置说明

### 交易日历配置
增量更新管理器内置了中国股市的交易日历，包括：
- 自动排除周末（周六、周日）
- 预配置主要节假日（元旦、春节、清明、劳动节、端午、中秋、国庆）
- 支持自定义节假日配置

### 更新策略配置
- `max_days`: 最大更新天数限制
- `update_type`: 更新类型选择
- `priority`: 任务优先级设置

## 最佳实践

### 1. 定期数据完整性检查
```python
# 每日运行完整性检查
missing_days = manager.get_missing_trading_days()
if missing_days['missing_trading_days'] > 0:
    print(f"发现 {missing_days['missing_trading_days']} 个缺失交易日")
```

### 2. 增量更新策略选择
- **missing_days**: 适合修复历史数据缺失
- **recent_days**: 适合日常数据更新
- **specific_stocks**: 适合特定股票数据补充

### 3. 资源控制
- 使用 `max_days` 参数控制单次更新量
- 在免费账户限制下，建议每次更新不超过30天
- 使用试运行模式验证更新计划

### 4. 监控和日志
- 定期查看更新历史记录
- 关注成功率统计
- 保存重要更新计划供后续使用

## 错误处理

### 常见错误及解决方案

1. **交易日历错误**
   - 检查日期格式是否正确（YYYYMMDD）
   - 确认日期范围是否合理

2. **数据库连接错误**
   - 检查数据库文件是否存在
   - 确认数据库路径配置正确

3. **更新计划执行失败**
   - 检查API权限和频率限制
   - 验证网络连接状态
   - 查看详细错误日志

## 性能优化建议

1. **批量处理**: 优先选择批量更新策略
2. **时间分散**: 避免在高峰时段执行大量更新
3. **缓存利用**: 充分利用本地缓存减少API调用
4. **增量优先**: 优先使用增量更新而非全量更新

## 注意事项

- 增量更新依赖于准确的交易日历
- 免费账户需要控制API调用频率
- 建议在非交易时间执行大量更新操作
- 定期备份数据库以防数据丢失 