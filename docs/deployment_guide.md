# A股日线数据下载系统 - 部署指南

## 目录

1. [系统要求](#系统要求)
2. [安装部署](#安装部署)
3. [配置说明](#配置说明)
4. [服务配置](#服务配置)
5. [监控设置](#监控设置)
6. [备份策略](#备份策略)
7. [安全配置](#安全配置)
8. [性能优化](#性能优化)
9. [故障恢复](#故障恢复)
10. [维护操作](#维护操作)

---

## 系统要求

### 硬件要求

| 配置项 | 最低要求 | 推荐配置 |
|--------|----------|----------|
| CPU | 1核 | 2核+ |
| 内存 | 1GB | 4GB+ |
| 存储 | 10GB | 50GB+ SSD |
| 网络 | 10Mbps | 100Mbps+ |

### 软件要求

- **操作系统**：Windows 10+, Linux (Ubuntu 18.04+, CentOS 7+), macOS 10.14+
- **Python版本**：3.8+
- **数据库**：SQLite 3.35+
- **其他依赖**：见 requirements.txt

---

## 安装部署

### 方式一：标准安装

#### 1. 下载源码

```bash
# 克隆仓库
git clone <repository-url>
cd cursor_stock

# 或者下载压缩包并解压
wget <archive-url>
unzip cursor_stock.zip
cd cursor_stock
```

#### 2. 创建虚拟环境

```bash
# 创建虚拟环境
python -m venv venv

# 激活虚拟环境
# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate
```

#### 3. 安装依赖

```bash
# 安装Python依赖
pip install -r requirements.txt

# 验证安装
python -c "import tushare; print('Tushare installed successfully')"
```

#### 4. 系统初始化

```bash
# 初始化系统
python -m src.command_line_interface init

# 验证初始化
python -m src.command_line_interface status
```

### 方式二：Docker部署

#### 1. 构建Docker镜像

```dockerfile
# Dockerfile
FROM python:3.9-slim

WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖文件
COPY requirements.txt .
RUN pip install -r requirements.txt

# 复制应用代码
COPY src/ src/
COPY config/ config/

# 创建数据目录
RUN mkdir -p data logs

# 暴露端口（如果需要）
EXPOSE 8000

# 启动命令
CMD ["python", "-m", "src.command_line_interface", "scheduler", "start"]
```

#### 2. 构建和运行

```bash
# 构建镜像
docker build -t stock-downloader .

# 运行容器
docker run -d \
  --name stock-downloader \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/logs:/app/logs \
  -v $(pwd)/config:/app/config \
  stock-downloader
```

#### 3. Docker Compose

```yaml
# docker-compose.yml
version: '3.8'

services:
  stock-downloader:
    build: .
    container_name: stock-downloader
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
      - ./config:/app/config
    environment:
      - TZ=Asia/Shanghai
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "python", "-m", "src.command_line_interface", "status"]
      interval: 30s
      timeout: 10s
      retries: 3
```

---

## 配置说明

### 基础配置

#### 1. 创建配置文件

```bash
# 创建配置目录
mkdir -p config

# 复制示例配置
cp config/config.example.json config/config.json
```

#### 2. 配置Tushare Token

```json
{
  "tushare": {
    "token": "your_tushare_token_here",
    "api_url": "http://api.tushare.pro",
    "timeout": 30,
    "retry_count": 3,
    "retry_delay": 1
  }
}
```

#### 3. 数据库配置

```json
{
  "database": {
    "path": "data/stock_data.db",
    "backup_path": "data/backups/",
    "connection_timeout": 30,
    "enable_foreign_keys": true,
    "enable_wal_mode": true
  }
}
```

#### 4. 调度配置

```json
{
  "scheduler": {
    "enabled": true,
    "run_time": "09:00",
    "timezone": "Asia/Shanghai",
    "weekends_enabled": false,
    "holidays_enabled": false
  }
}
```

### 高级配置

#### 1. API限制配置

```json
{
  "api_limits": {
    "free_account": {
      "total_points": 120,
      "calls_per_minute": 2,
      "calls_per_hour": 60,
      "calls_per_day": 120
    },
    "rate_limit_buffer": 0.1,
    "monitor_window_hours": 24
  }
}
```

#### 2. 下载配置

```json
{
  "download": {
    "batch_size": 100,
    "max_workers": 1,
    "enable_incremental": true,
    "auto_retry": true,
    "max_retry_attempts": 3
  }
}
```

#### 3. 日志配置

```json
{
  "logging": {
    "level": "INFO",
    "file_path": "logs/stock_downloader.log",
    "max_file_size": "10MB",
    "backup_count": 5,
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  }
}
```

---

## 服务配置

### Linux Systemd服务

#### 1. 创建服务文件

```ini
# /etc/systemd/system/stock-downloader.service
[Unit]
Description=Stock Data Downloader Service
After=network.target

[Service]
Type=simple
User=stockuser
Group=stockuser
WorkingDirectory=/opt/stock-downloader
ExecStart=/opt/stock-downloader/venv/bin/python -m src.command_line_interface scheduler start
ExecStop=/opt/stock-downloader/venv/bin/python -m src.command_line_interface scheduler stop
Restart=on-failure
RestartSec=10
Environment=PYTHONPATH=/opt/stock-downloader

[Install]
WantedBy=multi-user.target
```

#### 2. 启用服务

```bash
# 重载systemd配置
sudo systemctl daemon-reload

# 启用服务
sudo systemctl enable stock-downloader

# 启动服务
sudo systemctl start stock-downloader

# 查看状态
sudo systemctl status stock-downloader
```

### Windows服务

#### 1. 使用NSSM

```bash
# 下载NSSM
# https://nssm.cc/download

# 安装服务
nssm install StockDownloader

# 配置服务
# Application path: C:\path\to\venv\Scripts\python.exe
# Startup directory: C:\path\to\stock-downloader
# Arguments: -m src.command_line_interface scheduler start
```

#### 2. 使用任务计划程序

```bash
# 创建任务
schtasks /create /tn "StockDownloader" /tr "C:\path\to\venv\Scripts\python.exe -m src.command_line_interface scheduler start" /sc onstart /ru SYSTEM
```

---

## 监控设置

### 系统监控

#### 1. 健康检查脚本

```bash
#!/bin/bash
# health_check.sh

# 检查进程
if pgrep -f "command_line_interface scheduler" > /dev/null; then
    echo "Scheduler is running"
else
    echo "Scheduler is not running"
    exit 1
fi

# 检查数据库
if [ -f "data/stock_data.db" ]; then
    echo "Database exists"
else
    echo "Database not found"
    exit 1
fi

# 检查最近的日志
if [ -f "logs/stock_downloader.log" ]; then
    # 检查最近1小时内是否有日志
    if [ $(find logs/stock_downloader.log -mmin -60) ]; then
        echo "Recent logs found"
    else
        echo "No recent logs"
        exit 1
    fi
fi

echo "Health check passed"
```

#### 2. 监控指标

```python
# monitoring.py
import psutil
import sqlite3
from datetime import datetime, timedelta

def get_system_metrics():
    return {
        'cpu_usage': psutil.cpu_percent(),
        'memory_usage': psutil.virtual_memory().percent,
        'disk_usage': psutil.disk_usage('/').percent,
        'process_count': len(psutil.pids())
    }

def get_database_metrics():
    conn = sqlite3.connect('data/stock_data.db')
    cursor = conn.cursor()
    
    # 获取表大小
    cursor.execute("SELECT COUNT(*) FROM daily_data")
    record_count = cursor.fetchone()[0]
    
    # 获取最新数据时间
    cursor.execute("SELECT MAX(trade_date) FROM daily_data")
    latest_date = cursor.fetchone()[0]
    
    conn.close()
    
    return {
        'total_records': record_count,
        'latest_date': latest_date
    }
```

### 告警配置

#### 1. 邮件告警

```python
# email_alert.py
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def send_alert(subject, message, to_email):
    smtp_server = "smtp.gmail.com"
    smtp_port = 587
    username = "your_email@gmail.com"
    password = "your_app_password"
    
    msg = MIMEMultipart()
    msg['From'] = username
    msg['To'] = to_email
    msg['Subject'] = subject
    
    msg.attach(MIMEText(message, 'plain'))
    
    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()
    server.login(username, password)
    server.send_message(msg)
    server.quit()
```

#### 2. 钉钉告警

```python
# dingtalk_alert.py
import requests
import json

def send_dingtalk_alert(message, webhook_url):
    headers = {'Content-Type': 'application/json'}
    data = {
        "msgtype": "text",
        "text": {
            "content": message
        }
    }
    
    response = requests.post(webhook_url, headers=headers, data=json.dumps(data))
    return response.json()
```

---

## 备份策略

### 数据备份

#### 1. 自动备份脚本

```bash
#!/bin/bash
# backup.sh

DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="/backup/stock_data"
SOURCE_DB="data/stock_data.db"

# 创建备份目录
mkdir -p $BACKUP_DIR

# 备份数据库
sqlite3 $SOURCE_DB ".backup $BACKUP_DIR/stock_data_$DATE.db"

# 压缩备份
gzip "$BACKUP_DIR/stock_data_$DATE.db"

# 删除7天前的备份
find $BACKUP_DIR -name "*.gz" -mtime +7 -delete

echo "Backup completed: stock_data_$DATE.db.gz"
```

#### 2. 增量备份

```python
# incremental_backup.py
import sqlite3
import shutil
from datetime import datetime

def incremental_backup():
    source_db = "data/stock_data.db"
    backup_dir = f"backups/incremental/{datetime.now().strftime('%Y%m%d')}"
    
    os.makedirs(backup_dir, exist_ok=True)
    
    # 备份数据库
    shutil.copy2(source_db, f"{backup_dir}/stock_data.db")
    
    # 备份配置文件
    shutil.copy2("config/config.json", f"{backup_dir}/config.json")
    
    # 备份日志
    shutil.copytree("logs", f"{backup_dir}/logs", dirs_exist_ok=True)
    
    print(f"Incremental backup completed: {backup_dir}")
```

### 配置备份

#### 1. 配置文件备份

```bash
# config_backup.sh
#!/bin/bash

DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="/backup/config"

mkdir -p $BACKUP_DIR

# 备份配置文件
cp config/config.json "$BACKUP_DIR/config_$DATE.json"

# 备份脚本
tar -czf "$BACKUP_DIR/scripts_$DATE.tar.gz" *.sh *.py

echo "Configuration backup completed"
```

---

## 安全配置

### 访问控制

#### 1. 文件权限

```bash
# 设置文件权限
chmod 600 config/config.json
chmod 700 data/
chmod 644 logs/*.log

# 设置用户组
chown -R stockuser:stockuser /opt/stock-downloader
```

#### 2. 网络安全

```bash
# 防火墙配置
ufw allow ssh
ufw allow 8000/tcp
ufw enable

# 限制访问
echo "restrict access to trusted IPs only" >> /etc/hosts.allow
```

### 敏感信息保护

#### 1. 环境变量

```bash
# 设置环境变量
export TUSHARE_TOKEN="your_token_here"
export DATABASE_PATH="/secure/path/stock_data.db"

# 在配置中使用环境变量
{
  "tushare": {
    "token": "${TUSHARE_TOKEN}"
  },
  "database": {
    "path": "${DATABASE_PATH}"
  }
}
```

#### 2. 密钥管理

```python
# secrets_manager.py
import os
from cryptography.fernet import Fernet

class SecretsManager:
    def __init__(self, key_file="secrets.key"):
        if os.path.exists(key_file):
            with open(key_file, 'rb') as f:
                self.key = f.read()
        else:
            self.key = Fernet.generate_key()
            with open(key_file, 'wb') as f:
                f.write(self.key)
        
        self.cipher = Fernet(self.key)
    
    def encrypt(self, data):
        return self.cipher.encrypt(data.encode())
    
    def decrypt(self, encrypted_data):
        return self.cipher.decrypt(encrypted_data).decode()
```

---

## 性能优化

### 数据库优化

#### 1. 索引优化

```sql
-- 创建索引
CREATE INDEX idx_daily_data_ts_code ON daily_data(ts_code);
CREATE INDEX idx_daily_data_trade_date ON daily_data(trade_date);
CREATE INDEX idx_daily_data_created_at ON daily_data(created_at);

-- 复合索引
CREATE INDEX idx_daily_data_composite ON daily_data(ts_code, trade_date);
```

#### 2. 数据库配置

```sql
-- SQLite优化配置
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA cache_size = 10000;
PRAGMA temp_store = MEMORY;
PRAGMA mmap_size = 268435456;
```

### 应用优化

#### 1. 批量处理

```python
# 批量插入优化
def bulk_insert_optimized(data_list, batch_size=1000):
    for i in range(0, len(data_list), batch_size):
        batch = data_list[i:i + batch_size]
        db_manager.execute_batch_insert(
            "INSERT INTO daily_data VALUES (?, ?, ?, ?, ?, ?, ?)",
            batch
        )
```

#### 2. 内存管理

```python
# 内存优化
import gc

def process_large_dataset():
    # 分批处理大数据集
    for chunk in pd.read_csv('large_file.csv', chunksize=10000):
        process_chunk(chunk)
        del chunk
        gc.collect()
```

---

## 故障恢复

### 常见故障场景

#### 1. 数据库损坏

```bash
# 检查数据库完整性
sqlite3 data/stock_data.db "PRAGMA integrity_check;"

# 修复数据库
sqlite3 data/stock_data.db ".recover" | sqlite3 data/stock_data_recovered.db

# 恢复备份
cp backups/latest/stock_data.db data/stock_data.db
```

#### 2. 调度器异常

```bash
# 检查调度器状态
python -m src.command_line_interface scheduler status

# 重启调度器
python -m src.command_line_interface scheduler restart

# 清理锁文件
rm -f /tmp/scheduler.lock
```

#### 3. 磁盘空间不足

```bash
# 清理日志
find logs/ -name "*.log" -mtime +30 -delete

# 清理临时文件
rm -rf /tmp/stock_downloader_*

# 数据库真空
python -m src.command_line_interface database vacuum
```

### 恢复流程

#### 1. 系统恢复

```bash
#!/bin/bash
# recovery.sh

echo "Starting system recovery..."

# 停止服务
systemctl stop stock-downloader

# 恢复数据库
if [ -f "backups/latest/stock_data.db" ]; then
    cp backups/latest/stock_data.db data/stock_data.db
    echo "Database restored"
fi

# 恢复配置
if [ -f "backups/latest/config.json" ]; then
    cp backups/latest/config.json config/config.json
    echo "Configuration restored"
fi

# 重新初始化
python -m src.command_line_interface init

# 启动服务
systemctl start stock-downloader

echo "System recovery completed"
```

---

## 维护操作

### 定期维护

#### 1. 日常维护脚本

```bash
#!/bin/bash
# daily_maintenance.sh

echo "Starting daily maintenance..."

# 检查磁盘空间
df -h

# 检查系统状态
python -m src.command_line_interface status

# 清理过期日志
find logs/ -name "*.log" -mtime +7 -delete

# 数据库统计
python -m src.command_line_interface database stats

# 生成日报
python -m src.command_line_interface report --type daily --output "reports/daily_$(date +%Y%m%d).json"

echo "Daily maintenance completed"
```

#### 2. 周维护脚本

```bash
#!/bin/bash
# weekly_maintenance.sh

echo "Starting weekly maintenance..."

# 数据库优化
python -m src.command_line_interface database vacuum

# 完整性检查
python -m src.command_line_interface data integrity --repair

# 生成周报
python -m src.command_line_interface report --type weekly --output "reports/weekly_$(date +%Y%m%d).json"

# 清理旧备份
find backups/ -name "*.gz" -mtime +30 -delete

echo "Weekly maintenance completed"
```

### 监控维护

#### 1. 性能监控

```python
# performance_monitor.py
import psutil
import time
import json

def monitor_performance():
    metrics = {
        'timestamp': time.time(),
        'cpu_usage': psutil.cpu_percent(interval=1),
        'memory_usage': psutil.virtual_memory().percent,
        'disk_usage': psutil.disk_usage('/').percent,
        'network_io': psutil.net_io_counters()._asdict()
    }
    
    with open('performance_metrics.json', 'a') as f:
        f.write(json.dumps(metrics) + '\n')
    
    return metrics
```

#### 2. 日志分析

```python
# log_analyzer.py
import re
from collections import Counter

def analyze_logs(log_file):
    with open(log_file, 'r') as f:
        logs = f.readlines()
    
    # 统计错误类型
    error_pattern = r'ERROR - (.+)'
    errors = []
    for line in logs:
        match = re.search(error_pattern, line)
        if match:
            errors.append(match.group(1))
    
    error_counts = Counter(errors)
    
    return {
        'total_logs': len(logs),
        'error_count': len(errors),
        'top_errors': error_counts.most_common(10)
    }
```

---

## 附录

### A. 系统目录结构

```
/opt/stock-downloader/
├── src/                    # 源代码
├── config/                 # 配置文件
├── data/                   # 数据文件
├── logs/                   # 日志文件
├── backups/                # 备份文件
├── scripts/                # 维护脚本
├── docs/                   # 文档
├── requirements.txt        # 依赖列表
└── README.md              # 说明文件
```

### B. 端口使用

| 端口 | 用途 | 协议 |
|------|------|------|
| 8000 | Web界面 | HTTP |
| 8001 | API接口 | HTTP |
| 8002 | 监控端口 | HTTP |

### C. 日志文件说明

| 日志文件 | 内容 | 轮转周期 |
|----------|------|----------|
| system.log | 系统日志 | 日 |
| api.log | API调用日志 | 日 |
| download.log | 下载日志 | 日 |
| error.log | 错误日志 | 日 |
| performance.log | 性能日志 | 小时 |

### D. 命令参考

| 命令 | 说明 |
|------|------|
| `systemctl start stock-downloader` | 启动服务 |
| `systemctl stop stock-downloader` | 停止服务 |
| `systemctl restart stock-downloader` | 重启服务 |
| `systemctl status stock-downloader` | 查看状态 |
| `journalctl -u stock-downloader` | 查看日志 |

---

**版本信息**：v1.0.0  
**最后更新**：2025-01-14  
**文档版本**：1.0 