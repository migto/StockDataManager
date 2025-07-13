# A股日线数据下载系统 - 用户使用指南

## 目录

1. [系统概述](#系统概述)
2. [快速开始](#快速开始)
3. [系统架构](#系统架构)
4. [配置管理](#配置管理)
5. [命令行界面](#命令行界面)
6. [调度管理](#调度管理)
7. [数据管理](#数据管理)
8. [监控报告](#监控报告)
9. [故障排除](#故障排除)
10. [高级功能](#高级功能)

---

## 系统概述

### 功能特性

本系统是一个完整的A股日线数据下载和管理系统，主要功能包括：

- **自动化下载**：支持每日自动下载A股市场日线数据
- **智能调度**：基于时间的任务调度，支持多种调度策略
- **数据完整性**：完善的数据验证和修复机制
- **增量更新**：智能增量更新，避免重复下载
- **监控报告**：全面的系统监控和报告功能
- **命令行界面**：友好的命令行工具，支持各种操作
- **错误处理**：智能错误处理和重试机制

### 技术架构

- **数据源**：Tushare Pro API
- **数据库**：SQLite 3
- **语言**：Python 3.8+
- **调度**：Schedule 库
- **数据处理**：Pandas, NumPy

---

## 快速开始

### 1. 环境准备

```bash
# 1. 创建虚拟环境
python -m venv venv

# 2. 激活虚拟环境
# Windows
venv\Scripts\activate
# Linux/Mac
source venv/bin/activate

# 3. 安装依赖
pip install -r requirements.txt
```

### 2. 系统初始化

```bash
# 初始化系统
python -m src.command_line_interface init

# 查看系统状态
python -m src.command_line_interface status
```

### 3. 配置Tushare Token

编辑 `config/config.json` 文件，设置您的Tushare Pro Token：

```json
{
  "tushare": {
    "token": "your_tushare_token_here"
  }
}
```

### 4. 启动调度器

```bash
# 启动调度器
python -m src.command_line_interface scheduler start
```

### 5. 手动下载数据

```bash
# 下载指定股票数据
python -m src.command_line_interface data download --stocks 000001 000002

# 下载最近几天的数据
python -m src.command_line_interface data download --type recent_days --max-days 7
```

---

## 系统架构

### 核心模块

```
src/
├── config_manager.py              # 配置管理
├── database_manager.py            # 数据库管理
├── optimized_tushare_api_manager.py  # API管理
├── stock_basic_manager.py         # 股票基本信息管理
├── daily_data_manager.py          # 日线数据管理
├── data_storage_manager.py        # 数据存储管理
├── data_integrity_manager.py      # 数据完整性管理
├── incremental_update_manager.py  # 增量更新管理
├── smart_download_manager.py      # 智能下载管理
├── download_status_manager.py     # 下载状态管理
├── error_handler_retry_manager.py # 错误处理和重试管理
├── logging_manager.py             # 日志管理
├── schedule_manager.py            # 调度管理
├── monitoring_report_manager.py   # 监控报告管理
└── command_line_interface.py      # 命令行界面
```

### 数据流

```
Tushare API → API管理器 → 数据处理 → 存储管理 → SQLite数据库
     ↓              ↓         ↓         ↓
   错误处理    →  日志记录  →  监控报告  →  告警系统
```

---

## 配置管理

### 配置文件结构

配置文件位于 `config/config.json`，主要包含以下部分：

```json
{
  "tushare": {
    "token": "your_token",
    "api_url": "http://api.tushare.pro",
    "timeout": 30,
    "retry_count": 3
  },
  "database": {
    "path": "data/stock_data.db",
    "backup_path": "data/backups/",
    "enable_wal_mode": true
  },
  "api_limits": {
    "free_account": {
      "calls_per_minute": 2,
      "calls_per_hour": 60,
      "calls_per_day": 120
    }
  },
  "download": {
    "batch_size": 100,
    "max_workers": 1,
    "enable_incremental": true
  },
  "logging": {
    "level": "INFO",
    "file_path": "logs/stock_downloader.log",
    "max_file_size": "10MB"
  },
  "scheduler": {
    "enabled": true,
    "run_time": "09:00",
    "timezone": "Asia/Shanghai"
  }
}
```

### 配置命令

```bash
# 查看配置
python -m src.command_line_interface config get database.path

# 设置配置
python -m src.command_line_interface config set database.path "data/new_stock_data.db"

# 列出所有配置
python -m src.command_line_interface config list

# 备份配置
python -m src.command_line_interface config backup

# 验证配置
python -m src.command_line_interface config validate
```

---

## 命令行界面

### 主要命令

#### 1. 系统管理

```bash
# 初始化系统
python -m src.command_line_interface init

# 查看系统状态
python -m src.command_line_interface status

# 查看详细状态
python -m src.command_line_interface status --detailed

# 查看指定股票状态
python -m src.command_line_interface status --stocks 000001 000002
```

#### 2. 调度管理

```bash
# 启动调度器
python -m src.command_line_interface scheduler start

# 停止调度器
python -m src.command_line_interface scheduler stop

# 重启调度器
python -m src.command_line_interface scheduler restart

# 查看调度器状态
python -m src.command_line_interface scheduler status

# 暂停调度器
python -m src.command_line_interface scheduler pause

# 恢复调度器
python -m src.command_line_interface scheduler resume
```

#### 3. 任务管理

```bash
# 手动运行任务
python -m src.command_line_interface task run daily_download

# 查看任务历史
python -m src.command_line_interface task history --limit 20

# 查看特定类型任务历史
python -m src.command_line_interface task history --type daily_download

# 配置任务
python -m src.command_line_interface task config daily_download --time 10:00 --enabled true
```

#### 4. 数据管理

```bash
# 下载数据
python -m src.command_line_interface data download --stocks 000001 000002

# 按日期范围下载
python -m src.command_line_interface data download --start-date 20250101 --end-date 20250114

# 增量更新
python -m src.command_line_interface data update --days 7

# 数据完整性检查
python -m src.command_line_interface data integrity

# 修复数据问题
python -m src.command_line_interface data integrity --repair
```

#### 5. 日志管理

```bash
# 查看日志
python -m src.command_line_interface logs --limit 50

# 查看特定类型日志
python -m src.command_line_interface logs --type system --level error

# 按时间范围查看日志
python -m src.command_line_interface logs --start-time "2025-01-01 00:00:00" --end-time "2025-01-14 23:59:59"

# 导出日志
python -m src.command_line_interface logs --export logs_export.json
```

#### 6. 报告生成

```bash
# 生成日报
python -m src.command_line_interface report --type daily

# 生成周报
python -m src.command_line_interface report --type weekly

# 生成月报
python -m src.command_line_interface report --type monthly

# 生成自定义报告
python -m src.command_line_interface report --type custom --start-date 2025-01-01 --end-date 2025-01-14

# 保存报告到文件
python -m src.command_line_interface report --type daily --output daily_report.json
```

#### 7. 数据库管理

```bash
# 数据库信息
python -m src.command_line_interface database info

# 备份数据库
python -m src.command_line_interface database backup

# 优化数据库
python -m src.command_line_interface database vacuum

# 数据库统计
python -m src.command_line_interface database stats

# 执行SQL查询
python -m src.command_line_interface database query "SELECT COUNT(*) FROM daily_data"
```

### 预演模式

使用 `--dry-run` 参数可以预演命令执行，不会实际执行操作：

```bash
# 预演启动调度器
python -m src.command_line_interface --dry-run scheduler start

# 预演数据下载
python -m src.command_line_interface --dry-run data download --stocks 000001
```

---

## 调度管理

### 任务类型

系统支持以下任务类型：

1. **每日下载任务** (`daily_download`)
   - 默认时间：09:00
   - 功能：下载最新交易日数据

2. **数据完整性检查** (`integrity_check`)
   - 默认时间：01:00
   - 功能：检查数据完整性，自动修复问题

3. **每周清理任务** (`weekly_cleanup`)
   - 默认时间：星期一 02:00
   - 功能：清理过期日志和数据

4. **月度报告任务** (`monthly_report`)
   - 默认时间：每月1号 03:00
   - 功能：生成月度统计报告

### 调度配置

```python
# 使用Python API配置调度
from src.schedule_manager import ScheduleManager, TaskType
from src.config_manager import ConfigManager

config = ConfigManager()
scheduler = ScheduleManager(config)

# 更新任务配置
scheduler.update_task_config(TaskType.DAILY_DOWNLOAD, {
    'time': '10:00',
    'enabled': True
})

# 立即运行任务
scheduler.run_task_immediately(TaskType.DAILY_DOWNLOAD)
```

### 监控调度状态

```bash
# 查看调度状态
python -m src.command_line_interface scheduler status

# 查看任务历史
python -m src.command_line_interface task history

# 查看下次运行时间
python -m src.command_line_interface scheduler status | grep "下次运行"
```

---

## 数据管理

### 数据结构

系统使用SQLite数据库存储数据，主要表结构：

- **stocks**：股票基本信息
- **daily_data**：日线数据
- **download_status**：下载状态
- **api_call_log**：API调用记录
- **system_config**：系统配置

### 数据下载策略

#### 1. 智能下载

```python
from src.smart_download_manager import SmartDownloadManager

download_manager = SmartDownloadManager(config)

# 分析下载需求
analysis = download_manager.analyze_download_requirements()

# 创建下载计划
plan = download_manager.create_download_plan(
    download_type='missing_days',
    max_days=30
)

# 执行下载
result = download_manager.execute_download_plan(plan)
```

#### 2. 增量更新

```python
from src.incremental_update_manager import IncrementalUpdateManager

update_manager = IncrementalUpdateManager(config)

# 创建增量更新计划
plan = update_manager.create_update_plan(
    plan_type='recent_days',
    days=7
)

# 执行更新
result = update_manager.execute_update_plan(plan)
```

#### 3. 数据完整性检查

```python
from src.data_integrity_manager import DataIntegrityManager

integrity_manager = DataIntegrityManager(config)

# 检查数据完整性
report = integrity_manager.check_data_integrity()

# 修复数据问题
repair_result = integrity_manager.repair_data_issues()
```

### 数据查询

```python
from src.database_manager import DatabaseManager

db_manager = DatabaseManager('data/stock_data.db')

# 查询日线数据
data = db_manager.execute_query("""
    SELECT * FROM daily_data 
    WHERE ts_code = ? AND trade_date >= ?
    ORDER BY trade_date DESC
    LIMIT 100
""", ('000001.SZ', '20250101'))

# 查询股票基本信息
stocks = db_manager.execute_query("""
    SELECT ts_code, name, industry, list_date 
    FROM stocks 
    WHERE list_status = 'L'
""")
```

---

## 监控报告

### 报告类型

系统支持多种报告类型：

1. **下载进度报告**
2. **错误统计报告**
3. **数据完整性报告**
4. **系统性能报告**
5. **综合报告**

### 生成报告

```python
from src.monitoring_report_manager import MonitoringReportManager

monitor = MonitoringReportManager(config)

# 生成下载进度报告
download_report = monitor.generate_download_progress_report(
    start_date='2025-01-01',
    end_date='2025-01-14'
)

# 生成错误统计报告
error_report = monitor.generate_error_statistics_report()

# 生成综合报告
comprehensive_report = monitor.generate_comprehensive_report()
```

### 监控指标

```python
from src.monitoring_report_manager import MetricType

# 记录监控指标
monitor.record_metric(
    MetricType.DOWNLOAD_PROGRESS,
    "records_downloaded",
    1000.0,
    "records",
    {"stock_code": "000001"}
)

# 创建告警
monitor.create_alert(
    AlertLevel.WARNING,
    "HIGH_ERROR_RATE",
    "错误率过高",
    {"error_rate": 0.15}
)
```

---

## 故障排除

### 常见问题

#### 1. API调用失败

**问题**：API调用返回错误
**解决方案**：
- 检查Token是否正确
- 检查网络连接
- 查看API调用频率限制

```bash
# 查看API调用日志
python -m src.command_line_interface logs --type api --level error

# 检查配置
python -m src.command_line_interface config get tushare.token
```

#### 2. 数据库连接失败

**问题**：数据库连接超时或失败
**解决方案**：
- 检查数据库文件路径
- 检查磁盘空间
- 重新初始化数据库

```bash
# 检查数据库状态
python -m src.command_line_interface database info

# 重新初始化
python -m src.command_line_interface init --force
```

#### 3. 调度器异常

**问题**：调度器停止工作
**解决方案**：
- 检查调度器状态
- 查看错误日志
- 重启调度器

```bash
# 查看调度器状态
python -m src.command_line_interface scheduler status

# 重启调度器
python -m src.command_line_interface scheduler restart
```

#### 4. 数据完整性问题

**问题**：数据缺失或不一致
**解决方案**：
- 运行完整性检查
- 执行数据修复
- 重新下载数据

```bash
# 完整性检查
python -m src.command_line_interface data integrity --repair

# 重新下载
python -m src.command_line_interface data download --type missing_days
```

### 日志分析

```bash
# 查看错误日志
python -m src.command_line_interface logs --level error --limit 100

# 查看系统日志
python -m src.command_line_interface logs --type system --limit 50

# 导出日志进行分析
python -m src.command_line_interface logs --export error_logs.json --level error
```

---

## 高级功能

### 1. 批量操作

```python
from src.command_line_interface import CommandLineInterface

cli = CommandLineInterface()

# 批量下载多只股票
stocks = ['000001.SZ', '000002.SZ', '600000.SH']
for stock in stocks:
    cli._handle_data({'data_action': 'download', 'stocks': [stock]})
```

### 2. 自定义调度

```python
from src.schedule_manager import ScheduleManager
import schedule

scheduler = ScheduleManager(config)

# 添加自定义任务
def custom_task():
    print("执行自定义任务")
    return {"status": "success"}

# 每小时执行一次
schedule.every().hour.do(custom_task)
```

### 3. 数据导出

```python
from src.database_manager import DatabaseManager
import pandas as pd

db_manager = DatabaseManager('data/stock_data.db')

# 导出数据到CSV
data = db_manager.execute_query("""
    SELECT ts_code, trade_date, open, high, low, close, vol 
    FROM daily_data 
    WHERE trade_date >= '20250101'
""")

df = pd.DataFrame(data, columns=['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'vol'])
df.to_csv('stock_data_export.csv', index=False)
```

### 4. 性能优化

```python
# 数据库优化
db_manager.vacuum_database()

# 清理过期数据
monitor.cleanup_old_reports(days=30)

# 批量插入优化
data_storage.bulk_insert_daily_data(data_list)
```

---

## 附录

### A. API接口参考

详细的API接口文档请参考各个模块的源代码注释。

### B. 配置参数参考

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| tushare.token | string | "" | Tushare Pro Token |
| database.path | string | "data/stock_data.db" | 数据库文件路径 |
| api_limits.calls_per_minute | int | 2 | 每分钟API调用限制 |
| download.batch_size | int | 100 | 批量下载大小 |
| logging.level | string | "INFO" | 日志级别 |
| scheduler.run_time | string | "09:00" | 调度运行时间 |

### C. 错误代码参考

| 错误代码 | 说明 | 解决方案 |
|----------|------|----------|
| API_001 | Token无效 | 检查Token配置 |
| DB_001 | 数据库连接失败 | 检查数据库路径 |
| SCHEDULE_001 | 调度器启动失败 | 检查权限和配置 |
| DATA_001 | 数据完整性检查失败 | 运行修复工具 |

### D. 性能基准

| 指标 | 期望值 | 说明 |
|------|--------|------|
| 下载速度 | 1000条/分钟 | 日线数据下载速度 |
| 数据库大小 | <1GB | 5000只股票5年数据 |
| 内存占用 | <500MB | 正常运行内存占用 |
| 响应时间 | <100ms | API调用平均响应时间 |

---

## 联系支持

如果您在使用过程中遇到问题，可以通过以下方式获取帮助：

1. 查看系统日志：`python -m src.command_line_interface logs --level error`
2. 生成诊断报告：`python -m src.command_line_interface report --type comprehensive`
3. 检查系统状态：`python -m src.command_line_interface status --detailed`

---

**版本信息**：v1.0.0  
**最后更新**：2025-01-14  
**文档版本**：1.0 