# 数据目录

此目录用于存储SQLite数据库文件和相关数据。

## 文件说明

- `stock_data.db` - 主数据库文件（自动创建）
- `stock_data.db-wal` - SQLite WAL文件（自动生成）
- `stock_data.db-shm` - SQLite共享内存文件（自动生成）
- `backup/` - 数据库备份目录

## 数据库表结构

- `stocks` - 股票基本信息表
- `daily_data` - 日线数据表
- `download_status` - 下载状态跟踪表
- `system_config` - 系统配置表
- `api_call_log` - API调用记录表

## 注意事项

- 数据库文件会自动创建，无需手动创建
- 定期备份数据库文件，避免数据丢失
- 数据库文件较大时，考虑进行压缩存储 