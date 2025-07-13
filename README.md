# A股日线数据下载系统

一个完整的A股市场日线数据自动下载和管理系统，基于Tushare Pro API，提供智能调度、数据完整性检查、监控报告等功能。

## 🎯 项目特色

- **🔄 自动化调度**：支持每日自动下载，智能避重，增量更新
- **📊 完整性保障**：数据验证、修复、去重机制
- **🎛️ 智能管理**：下载状态追踪、错误处理、重试机制
- **📈 监控报告**：实时监控、统计分析、告警通知
- **🛠️ 命令行界面**：友好的CLI工具，支持各种操作
- **📋 详细日志**：结构化日志记录，便于分析和调试

## 🚀 快速开始

### 1. 环境准备

```bash
# 克隆项目
git clone <repository-url>
cd cursor_stock

# 创建虚拟环境
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

# 安装依赖
pip install -r requirements.txt
```

### 2. 配置设置

```bash
# 初始化系统
python -m src.command_line_interface init

# 配置Tushare Token
python -m src.command_line_interface config set tushare.token "your_token_here"
```

### 3. 启动运行

```bash
# 启动调度器
python -m src.command_line_interface scheduler start

# 或手动下载数据
python -m src.command_line_interface data download --stocks 000001 000002
```

## 📁 项目结构

```
cursor_stock/
├── src/                           # 源代码目录
│   ├── config_manager.py          # 配置管理
│   ├── database_manager.py        # 数据库管理
│   ├── optimized_tushare_api_manager.py  # API管理
│   ├── stock_basic_manager.py     # 股票基本信息管理
│   ├── daily_data_manager.py      # 日线数据管理
│   ├── data_storage_manager.py    # 数据存储管理
│   ├── data_integrity_manager.py  # 数据完整性管理
│   ├── incremental_update_manager.py  # 增量更新管理
│   ├── smart_download_manager.py  # 智能下载管理
│   ├── download_status_manager.py # 下载状态管理
│   ├── error_handler_retry_manager.py  # 错误处理和重试管理
│   ├── logging_manager.py         # 日志管理
│   ├── schedule_manager.py        # 调度管理
│   ├── monitoring_report_manager.py  # 监控报告管理
│   ├── command_line_interface.py  # 命令行界面
│   └── database_init.sql          # 数据库初始化脚本
├── config/                        # 配置文件目录
│   └── config.json                # 主配置文件
├── data/                          # 数据文件目录
│   ├── stock_data.db              # SQLite数据库
│   └── cache/                     # 缓存文件
├── logs/                          # 日志文件目录
├── docs/                          # 文档目录
│   ├── user_guide.md              # 用户指南
│   └── deployment_guide.md        # 部署指南
├── requirements.txt               # Python依赖
└── README.md                      # 项目说明
```

## 🔧 核心功能

### 1. 数据下载管理

- **智能下载策略**：自动识别缺失数据，按需下载
- **批量下载**：支持多股票、多日期范围批量下载
- **增量更新**：只下载新增和变化的数据
- **断点续传**：支持下载中断后恢复

### 2. 数据完整性保障

- **数据验证**：价格逻辑检查、空值检测
- **自动修复**：无效数据修复、重复数据清理
- **完整性报告**：数据质量统计和分析
- **一致性检查**：跨表数据一致性验证

### 3. 任务调度系统

- **定时任务**：每日、每周、每月任务调度
- **任务管理**：任务状态追踪、历史记录
- **智能重试**：任务失败自动重试机制
- **调度监控**：调度状态实时监控

### 4. 监控报告系统

- **实时监控**：系统状态、性能指标监控
- **统计报告**：下载进度、错误统计、性能报告
- **告警机制**：异常情况自动告警
- **历史分析**：趋势分析、问题诊断

## 🛠️ 命令行工具

### 系统管理

```bash
# 系统初始化
python -m src.command_line_interface init

# 查看系统状态
python -m src.command_line_interface status

# 配置管理
python -m src.command_line_interface config get database.path
python -m src.command_line_interface config set logging.level DEBUG
```

### 调度管理

```bash
# 启动调度器
python -m src.command_line_interface scheduler start

# 查看调度状态
python -m src.command_line_interface scheduler status

# 停止调度器
python -m src.command_line_interface scheduler stop
```

### 任务管理

```bash
# 手动运行任务
python -m src.command_line_interface task run daily_download

# 查看任务历史
python -m src.command_line_interface task history --limit 20

# 配置任务
python -m src.command_line_interface task config daily_download --time 10:00
```

### 数据管理

```bash
# 下载数据
python -m src.command_line_interface data download --stocks 000001 000002

# 增量更新
python -m src.command_line_interface data update --days 7

# 数据完整性检查
python -m src.command_line_interface data integrity --repair
```

### 监控报告

```bash
# 生成报告
python -m src.command_line_interface report --type daily

# 查看日志
python -m src.command_line_interface logs --level error --limit 50

# 数据库管理
python -m src.command_line_interface database info
```

## 📊 数据库设计

### 核心表结构

```sql
-- 股票基本信息表
CREATE TABLE stocks (
    ts_code TEXT PRIMARY KEY,
    symbol TEXT,
    name TEXT,
    area TEXT,
    industry TEXT,
    list_date DATE,
    market TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 日线数据表
CREATE TABLE daily_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_code TEXT,
    trade_date DATE,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    pre_close REAL,
    change REAL,
    pct_chg REAL,
    vol REAL,
    amount REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ts_code, trade_date)
);

-- 下载状态表
CREATE TABLE download_status (
    ts_code TEXT PRIMARY KEY,
    last_download_date DATE,
    total_records INTEGER DEFAULT 0,
    status TEXT DEFAULT 'pending',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## 🔧 配置说明

### 主要配置项

```json
{
  "tushare": {
    "token": "your_tushare_token",
    "api_url": "http://api.tushare.pro"
  },
  "database": {
    "path": "data/stock_data.db",
    "backup_path": "data/backups/"
  },
  "api_limits": {
    "free_account": {
      "calls_per_minute": 2,
      "calls_per_day": 120
    }
  },
  "scheduler": {
    "enabled": true,
    "run_time": "09:00",
    "timezone": "Asia/Shanghai"
  },
  "logging": {
    "level": "INFO",
    "file_path": "logs/stock_downloader.log"
  }
}
```

## 🚀 部署指南

### 标准部署

```bash
# 1. 克隆项目
git clone <repository-url>
cd cursor_stock

# 2. 安装依赖
pip install -r requirements.txt

# 3. 配置系统
python -m src.command_line_interface init
python -m src.command_line_interface config set tushare.token "your_token"

# 4. 启动服务
python -m src.command_line_interface scheduler start
```

### Docker部署

```bash
# 构建镜像
docker build -t stock-downloader .

# 运行容器
docker run -d \
  --name stock-downloader \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/config:/app/config \
  stock-downloader
```

### 系统服务部署

```bash
# 创建系统服务
sudo cp scripts/stock-downloader.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable stock-downloader
sudo systemctl start stock-downloader
```

## 📈 性能特点

- **高效下载**：批量处理，智能避重
- **内存优化**：分块处理，内存占用低
- **并发控制**：合理控制并发，避免API限制
- **存储优化**：SQLite WAL模式，提高并发性能
- **缓存机制**：本地缓存，减少API调用

## 🛡️ 错误处理

- **智能重试**：网络错误、API限制自动重试
- **错误分类**：不同类型错误采用不同策略
- **错误记录**：详细错误日志，便于问题定位
- **告警机制**：严重错误自动告警通知

## 📋 监控指标

- **下载进度**：完成率、下载速度、数据覆盖率
- **错误统计**：错误类型、错误率、解决率
- **系统性能**：CPU、内存、磁盘使用率
- **数据质量**：数据完整性、准确性分数

## 🔍 故障排除

### 常见问题

1. **API调用失败**
   - 检查Token配置
   - 确认网络连接
   - 查看API调用频率

2. **数据库错误**
   - 检查磁盘空间
   - 验证数据库文件权限
   - 运行数据库完整性检查

3. **调度器异常**
   - 查看调度器状态
   - 检查错误日志
   - 重启调度器服务

### 日志分析

```bash
# 查看错误日志
python -m src.command_line_interface logs --level error

# 生成诊断报告
python -m src.command_line_interface report --type comprehensive

# 检查系统状态
python -m src.command_line_interface status --detailed
```

## 🤝 贡献指南

1. Fork 项目
2. 创建功能分支 (`git checkout -b feature/new-feature`)
3. 提交更改 (`git commit -am 'Add new feature'`)
4. 推送到分支 (`git push origin feature/new-feature`)
5. 创建 Pull Request

## 📄 许可证

本项目采用 MIT 许可证 - 详见 [LICENSE](LICENSE) 文件

## 🙏 致谢

- [Tushare Pro](https://tushare.pro/) - 提供股票数据API
- [SQLite](https://sqlite.org/) - 轻量级数据库
- [Schedule](https://github.com/dbader/schedule) - Python任务调度库

## 📞 联系方式

- 项目主页：[GitHub Repository](https://github.com/your-username/cursor_stock)
- 问题反馈：[GitHub Issues](https://github.com/your-username/cursor_stock/issues)
- 邮箱：your-email@example.com

---

**版本信息**：v1.0.0  
**最后更新**：2025-01-14  
**Python版本**：3.8+  
**数据库**：SQLite 3.35+ 