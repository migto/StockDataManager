# 配置文件说明

## 概述

本项目使用JSON格式的配置文件来管理系统的各种配置选项。配置文件位于 `config/config.json`，包含了Tushare API配置、数据库配置、下载策略、日志设置等所有系统配置。

## 配置文件结构

### 1. Tushare API配置 (`tushare`)

```json
{
    "tushare": {
        "token": "",                    // Tushare Pro API Token (必须配置)
        "api_url": "http://api.tushare.pro",  // API服务器地址
        "timeout": 30,                  // API请求超时时间（秒）
        "retry_count": 3,               // API请求失败重试次数
        "retry_delay": 1                // 重试间隔（秒）
    }
}
```

**重要**: 必须配置有效的Tushare Pro API Token才能使用本系统。

### 2. 数据库配置 (`database`)

```json
{
    "database": {
        "path": "data/stock_data.db",       // SQLite数据库文件路径
        "backup_path": "data/backups/",     // 数据库备份目录
        "connection_timeout": 30,           // 数据库连接超时时间（秒）
        "enable_foreign_keys": true,        // 启用外键约束
        "enable_wal_mode": true            // 启用WAL模式（提高性能）
    }
}
```

### 3. API限制配置 (`api_limits`)

```json
{
    "api_limits": {
        "free_account": {
            "total_points": 120,            // 免费账户每日总点数
            "calls_per_minute": 2,          // 每分钟最大调用次数
            "calls_per_hour": 60,           // 每小时最大调用次数
            "calls_per_day": 120            // 每日最大调用次数
        },
        "rate_limit_buffer": 0.1,           // 速率限制缓冲比例
        "monitor_window_hours": 24          // 监控窗口时间（小时）
    }
}
```

### 4. 下载配置 (`download`)

```json
{
    "download": {
        "batch_size": 100,                  // 批量下载大小
        "max_workers": 1,                   // 最大并发数（免费账户建议1）
        "enable_incremental": true,         // 启用增量下载
        "auto_retry": true,                 // 自动重试失败的下载
        "max_retry_attempts": 3             // 最大重试次数
    }
}
```

### 5. 日志配置 (`logging`)

```json
{
    "logging": {
        "level": "INFO",                    // 日志级别 (DEBUG, INFO, WARNING, ERROR)
        "file_path": "logs/stock_downloader.log",  // 日志文件路径
        "max_file_size": "10MB",            // 单个日志文件最大大小
        "backup_count": 5,                  // 保留的日志文件数量
        "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"  // 日志格式
    }
}
```

### 6. 调度器配置 (`scheduler`)

```json
{
    "scheduler": {
        "enabled": true,                    // 启用自动调度
        "run_time": "09:00",               // 每日运行时间
        "timezone": "Asia/Shanghai",        // 时区
        "weekends_enabled": false,          // 周末是否运行
        "holidays_enabled": false          // 节假日是否运行
    }
}
```

### 7. 通知配置 (`notifications`)

```json
{
    "notifications": {
        "enabled": false,                   // 启用通知
        "email": {
            "smtp_server": "",              // SMTP服务器地址
            "smtp_port": 587,               // SMTP端口
            "username": "",                 // 邮箱用户名
            "password": "",                 // 邮箱密码
            "recipients": []                // 接收者邮箱列表
        }
    }
}
```

### 8. 系统配置 (`system`)

```json
{
    "system": {
        "version": "1.0.0",                 // 系统版本
        "debug_mode": false,                // 调试模式
        "performance_monitoring": true,     // 性能监控
        "data_validation": true            // 数据验证
    }
}
```

## 配置管理器使用

### 命令行工具

配置管理器提供了丰富的命令行工具：

```bash
# 初始化配置文件
python src/config_manager.py init

# 设置配置值
python src/config_manager.py set tushare.token your_token_here

# 获取配置值
python src/config_manager.py get tushare.token

# 验证配置文件
python src/config_manager.py validate

# 查看配置信息
python src/config_manager.py info

# 备份配置文件
python src/config_manager.py backup

# 恢复配置文件
python src/config_manager.py restore backup_file.json

# 列出备份文件
python src/config_manager.py list-backups

# 导出配置（不包含敏感信息）
python src/config_manager.py export config_export.json

# 导入配置
python src/config_manager.py import config_import.json
```

### 编程接口

```python
from config_manager import ConfigManager

# 创建配置管理器实例
config = ConfigManager()

# 获取配置值
token = config.get('tushare.token')
db_path = config.get('database.path')

# 设置配置值
config.set('tushare.token', 'your_token_here')

# 支持字典式访问
config['tushare.token'] = 'your_token_here'
token = config['tushare.token']

# 验证配置
is_valid, errors = config.validate()

# 备份配置
backup_file = config.backup()

# 获取配置信息
info = config.get_config_info()
```

## 环境变量覆盖

支持以下环境变量覆盖配置：

- `TUSHARE_TOKEN`: Tushare Pro API Token
- `DB_PATH`: 数据库文件路径
- `DEBUG_MODE`: 调试模式 (true/false)

示例：
```bash
export TUSHARE_TOKEN="your_token_here"
export DEBUG_MODE="true"
python main.py
```

## 配置文件管理

### 备份和恢复

系统会自动在以下情况创建配置备份：
- 保存配置文件时
- 导入配置文件时
- 手动执行备份命令时

备份文件存储在 `config/backups/` 目录中，文件名格式为 `config_backup_YYYYMMDD_HHMMSS.json`。

### 配置验证

配置管理器会验证以下内容：
- Tushare Token是否配置
- 数据库路径是否有效
- 日志目录是否可创建
- API限制配置是否完整

### 默认配置

如果配置文件不存在，系统会自动创建包含默认值的配置文件。默认配置适用于大多数使用场景，用户只需要配置Tushare Token即可开始使用。

## 安全注意事项

1. **Token保护**: Tushare API Token是敏感信息，不要将其提交到版本控制系统
2. **配置备份**: 定期备份配置文件，特别是在重要配置变更之前
3. **权限管理**: 确保配置文件具有适当的文件权限，防止未授权访问
4. **导出安全**: 导出配置时，默认会隐藏敏感信息，除非明确指定包含

## 故障排除

### 常见问题

1. **配置文件损坏**: 删除配置文件，系统会自动创建新的默认配置
2. **Token无效**: 检查Tushare Token是否正确配置且有效
3. **数据库路径错误**: 确保数据库目录存在且有写权限
4. **API限制错误**: 检查API限制配置是否与您的Tushare账户类型匹配

### 日志调试

启用调试模式可以获得更详细的日志信息：

```bash
python src/config_manager.py set system.debug_mode true
```

或设置环境变量：

```bash
export DEBUG_MODE=true
```

## 配置文件版本管理

配置文件包含版本信息，便于后续升级和兼容性处理。当前版本为 `1.0.0`。

升级系统时，配置管理器会自动处理配置文件的版本兼容性问题。 