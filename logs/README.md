# 日志目录

此目录用于存储系统运行日志文件。

## 文件说明

- `stock_downloader.log` - 主程序日志
- `api_calls.log` - API调用日志
- `errors.log` - 错误日志
- `download_progress.log` - 下载进度日志
- `archive/` - 历史日志归档目录

## 日志级别

- DEBUG: 详细的调试信息
- INFO: 一般信息记录
- WARNING: 警告信息
- ERROR: 错误信息
- CRITICAL: 严重错误信息

## 日志配置

- 日志文件会自动按日期轮转
- 保留最近30天的日志文件
- 错误日志会单独记录到errors.log

## 注意事项

- 定期清理旧日志文件，避免占用过多磁盘空间
- 重要错误信息会同时记录到控制台和日志文件 