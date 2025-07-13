#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
日志记录管理器 - 负责全面的日志记录系统
"""

import os
import logging
import logging.handlers
from typing import Dict, List, Tuple, Optional, Any, Union
from datetime import datetime, timedelta
import json
import gzip
import shutil
from pathlib import Path
from enum import Enum
import threading
import time

from .config_manager import ConfigManager
from .database_manager import DatabaseManager


class LogLevel(Enum):
    """日志级别枚举"""
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL


class LogType(Enum):
    """日志类型枚举"""
    SYSTEM = 'system'          # 系统日志
    API = 'api'               # API调用日志
    DOWNLOAD = 'download'      # 下载日志
    DATABASE = 'database'      # 数据库日志
    ERROR = 'error'           # 错误日志
    PERFORMANCE = 'performance'  # 性能日志


class LoggingManager:
    """日志记录管理器
    
    负责实现全面的日志记录系统，包括结构化日志、日志轮转、日志分析等
    """
    
    def __init__(self, config_manager: ConfigManager = None):
        """初始化日志管理器
        
        Args:
            config_manager: 配置管理器
        """
        if config_manager is None:
            config_manager = ConfigManager()
        
        self.config = config_manager
        # 从配置管理器获取数据库路径
        db_path = self.config.get('database.path', 'data/stock_data.db')
        self.db_manager = DatabaseManager(db_path)
        
        # 日志配置
        self.log_config = {
            'log_dir': Path(self.config.get('logging.file_path', 'logs/stock_downloader.log')).parent,
            'log_level': self.config.get('logging.level', 'INFO'),
            'max_file_size': self._parse_size(self.config.get('logging.max_file_size', '10MB')),
            'backup_count': self.config.get('logging.backup_count', 5),
            'log_format': self.config.get('logging.format', 
                                        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
            'enable_console': True,
            'enable_file': True,
            'enable_database': True
        }
        
        # 创建日志目录
        self.log_config['log_dir'].mkdir(parents=True, exist_ok=True)
        
        # 初始化日志器
        self.loggers = {}
        self._setup_loggers()
        
        # 主日志器
        self.logger = self.get_logger('logging_manager')
        
        # 日志统计
        self.log_stats = {
            'total_logs': 0,
            'logs_by_level': {},
            'logs_by_type': {},
            'start_time': datetime.now().isoformat()
        }
        
        # 线程锁
        self._lock = threading.Lock()
    
    def _parse_size(self, size_str: str) -> int:
        """解析大小字符串为字节数
        
        Args:
            size_str: 大小字符串，如 '10MB'
            
        Returns:
            字节数
        """
        size_str = size_str.upper().strip()
        
        if size_str.endswith('KB'):
            return int(size_str[:-2]) * 1024
        elif size_str.endswith('MB'):
            return int(size_str[:-2]) * 1024 * 1024
        elif size_str.endswith('GB'):
            return int(size_str[:-2]) * 1024 * 1024 * 1024
        else:
            # 假设是字节
            return int(size_str)
    
    def _setup_loggers(self):
        """设置各类型日志器"""
        for log_type in LogType:
            logger_name = f"stock_downloader.{log_type.value}"
            logger = logging.getLogger(logger_name)
            logger.setLevel(getattr(logging, self.log_config['log_level']))
            
            # 清除现有处理器
            logger.handlers.clear()
            
            # 文件处理器 - 带轮转
            if self.log_config['enable_file']:
                log_file = self.log_config['log_dir'] / f"{log_type.value}.log"
                file_handler = logging.handlers.RotatingFileHandler(
                    log_file,
                    maxBytes=self.log_config['max_file_size'],
                    backupCount=self.log_config['backup_count'],
                    encoding='utf-8'
                )
                file_formatter = logging.Formatter(self.log_config['log_format'])
                file_handler.setFormatter(file_formatter)
                logger.addHandler(file_handler)
            
            # 控制台处理器
            if self.log_config['enable_console'] and log_type in [LogType.SYSTEM, LogType.ERROR]:
                console_handler = logging.StreamHandler()
                console_formatter = logging.Formatter(
                    '%(asctime)s - %(levelname)s - %(message)s'
                )
                console_handler.setFormatter(console_formatter)
                logger.addHandler(console_handler)
            
            # 数据库处理器
            if self.log_config['enable_database']:
                db_handler = DatabaseLogHandler(self.db_manager, log_type.value)
                logger.addHandler(db_handler)
            
            self.loggers[log_type.value] = logger
            
    def get_logger(self, log_type: Union[str, LogType]) -> logging.Logger:
        """获取指定类型的日志器
        
        Args:
            log_type: 日志类型
            
        Returns:
            日志器对象
        """
        if isinstance(log_type, LogType):
            log_type = log_type.value
        
        return self.loggers.get(log_type, logging.getLogger(f"stock_downloader.{log_type}"))
    
    def log_api_call(self, 
                    api_name: str,
                    params: Dict[str, Any] = None,
                    response_time: float = None,
                    success: bool = True,
                    error_message: str = None,
                    records_count: int = None):
        """记录API调用日志
        
        Args:
            api_name: API名称
            params: 请求参数
            response_time: 响应时间（毫秒）
            success: 是否成功
            error_message: 错误消息
            records_count: 返回记录数
        """
        api_logger = self.get_logger(LogType.API)
        
        log_data = {
            'api_name': api_name,
            'params': params,
            'response_time': response_time,
            'success': success,
            'records_count': records_count,
            'timestamp': datetime.now().isoformat()
        }
        
        if success:
            message = f"API调用成功: {api_name}"
            if response_time:
                message += f", 耗时: {response_time:.2f}ms"
            if records_count:
                message += f", 记录数: {records_count}"
            
            api_logger.info(message, extra={'log_data': log_data})
        else:
            message = f"API调用失败: {api_name}"
            if error_message:
                message += f", 错误: {error_message}"
            
            log_data['error_message'] = error_message
            api_logger.error(message, extra={'log_data': log_data})
        
        self._update_stats('api', 'info' if success else 'error')
    
    def log_download_progress(self,
                            task_id: str,
                            stock_code: str = None,
                            trading_date: str = None,
                            progress: float = None,
                            status: str = None,
                            records_downloaded: int = None,
                            error_message: str = None):
        """记录下载进度日志
        
        Args:
            task_id: 任务ID
            stock_code: 股票代码
            trading_date: 交易日期
            progress: 进度百分比
            status: 状态
            records_downloaded: 已下载记录数
            error_message: 错误消息
        """
        download_logger = self.get_logger(LogType.DOWNLOAD)
        
        log_data = {
            'task_id': task_id,
            'stock_code': stock_code,
            'trading_date': trading_date,
            'progress': progress,
            'status': status,
            'records_downloaded': records_downloaded,
            'timestamp': datetime.now().isoformat()
        }
        
        message = f"下载进度: 任务{task_id}"
        if stock_code:
            message += f", 股票{stock_code}"
        if trading_date:
            message += f", 日期{trading_date}"
        if progress is not None:
            message += f", 进度{progress:.1f}%"
        if status:
            message += f", 状态{status}"
        
        if error_message:
            log_data['error_message'] = error_message
            message += f", 错误: {error_message}"
            download_logger.error(message, extra={'log_data': log_data})
            level = 'error'
        else:
            download_logger.info(message, extra={'log_data': log_data})
            level = 'info'
        
        self._update_stats('download', level)
    
    def log_database_operation(self,
                             operation: str,
                             table_name: str = None,
                             affected_rows: int = None,
                             execution_time: float = None,
                             success: bool = True,
                             error_message: str = None):
        """记录数据库操作日志
        
        Args:
            operation: 操作类型
            table_name: 表名
            affected_rows: 影响行数
            execution_time: 执行时间（毫秒）
            success: 是否成功
            error_message: 错误消息
        """
        db_logger = self.get_logger(LogType.DATABASE)
        
        log_data = {
            'operation': operation,
            'table_name': table_name,
            'affected_rows': affected_rows,
            'execution_time': execution_time,
            'success': success,
            'timestamp': datetime.now().isoformat()
        }
        
        message = f"数据库操作: {operation}"
        if table_name:
            message += f", 表{table_name}"
        if affected_rows is not None:
            message += f", 影响行数{affected_rows}"
        if execution_time:
            message += f", 耗时{execution_time:.2f}ms"
        
        if success:
            db_logger.info(message, extra={'log_data': log_data})
            level = 'info'
        else:
            if error_message:
                log_data['error_message'] = error_message
                message += f", 错误: {error_message}"
            db_logger.error(message, extra={'log_data': log_data})
            level = 'error'
        
        self._update_stats('database', level)
    
    def log_performance_metric(self,
                             metric_name: str,
                             value: float,
                             unit: str = None,
                             context: Dict[str, Any] = None):
        """记录性能指标日志
        
        Args:
            metric_name: 指标名称
            value: 指标值
            unit: 单位
            context: 上下文信息
        """
        perf_logger = self.get_logger(LogType.PERFORMANCE)
        
        log_data = {
            'metric_name': metric_name,
            'value': value,
            'unit': unit,
            'context': context,
            'timestamp': datetime.now().isoformat()
        }
        
        message = f"性能指标: {metric_name}={value}"
        if unit:
            message += unit
        
        perf_logger.info(message, extra={'log_data': log_data})
        self._update_stats('performance', 'info')
    
    def log_system_event(self,
                        event_type: str,
                        message: str,
                        level: LogLevel = LogLevel.INFO,
                        context: Dict[str, Any] = None):
        """记录系统事件日志
        
        Args:
            event_type: 事件类型
            message: 日志消息
            level: 日志级别
            context: 上下文信息
        """
        system_logger = self.get_logger(LogType.SYSTEM)
        
        log_data = {
            'event_type': event_type,
            'context': context,
            'timestamp': datetime.now().isoformat()
        }
        
        full_message = f"[{event_type}] {message}"
        
        if level == LogLevel.DEBUG:
            system_logger.debug(full_message, extra={'log_data': log_data})
        elif level == LogLevel.INFO:
            system_logger.info(full_message, extra={'log_data': log_data})
        elif level == LogLevel.WARNING:
            system_logger.warning(full_message, extra={'log_data': log_data})
        elif level == LogLevel.ERROR:
            system_logger.error(full_message, extra={'log_data': log_data})
        elif level == LogLevel.CRITICAL:
            system_logger.critical(full_message, extra={'log_data': log_data})
        
        self._update_stats('system', level.name.lower())
    
    def _update_stats(self, log_type: str, level: str):
        """更新日志统计
        
        Args:
            log_type: 日志类型
            level: 日志级别
        """
        with self._lock:
            self.log_stats['total_logs'] += 1
            
            if log_type not in self.log_stats['logs_by_type']:
                self.log_stats['logs_by_type'][log_type] = 0
            self.log_stats['logs_by_type'][log_type] += 1
            
            if level not in self.log_stats['logs_by_level']:
                self.log_stats['logs_by_level'][level] = 0
            self.log_stats['logs_by_level'][level] += 1
    
    def get_log_statistics(self) -> Dict[str, Any]:
        """获取日志统计信息
        
        Returns:
            日志统计信息
        """
        with self._lock:
            return {
                'success': True,
                'statistics': self.log_stats.copy(),
                'config': self.log_config.copy(),
                'timestamp': datetime.now().isoformat()
            }
    
    def query_logs(self,
                  log_type: str = None,
                  start_time: str = None,
                  end_time: str = None,
                  level: str = None,
                  limit: int = 100) -> Dict[str, Any]:
        """查询日志记录
        
        Args:
            log_type: 日志类型
            start_time: 开始时间
            end_time: 结束时间
            level: 日志级别
            limit: 返回记录数限制
            
        Returns:
            日志查询结果
        """
        try:
            # 构建查询条件
            where_conditions = []
            query_params = []
            
            if log_type:
                where_conditions.append("log_type = ?")
                query_params.append(log_type)
            
            if start_time:
                where_conditions.append("timestamp >= ?")
                query_params.append(start_time)
            
            if end_time:
                where_conditions.append("timestamp <= ?")
                query_params.append(end_time)
            
            if level:
                where_conditions.append("level = ?")
                query_params.append(level.upper())
            
            where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""
            
            query = f"""
            SELECT log_type, level, message, timestamp, log_data
            FROM log_records
            {where_clause}
            ORDER BY timestamp DESC
            LIMIT ?
            """
            
            query_params.append(limit)
            
            results = self.db_manager.execute_query(query, tuple(query_params))
            
            logs = []
            for record in results:
                log_data = None
                try:
                    if record[4]:  # log_data字段
                        log_data = json.loads(record[4])
                except json.JSONDecodeError:
                    pass
                
                logs.append({
                    'log_type': record[0],
                    'level': record[1],
                    'message': record[2],
                    'timestamp': record[3],
                    'log_data': log_data
                })
            
            return {
                'success': True,
                'total_logs': len(logs),
                'logs': logs,
                'query_params': {
                    'log_type': log_type,
                    'start_time': start_time,
                    'end_time': end_time,
                    'level': level,
                    'limit': limit
                }
            }
            
        except Exception as e:
            self.logger.error(f"查询日志失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def cleanup_old_logs(self, days_old: int = 30) -> Dict[str, Any]:
        """清理旧日志
        
        Args:
            days_old: 清理多少天前的日志
            
        Returns:
            清理结果
        """
        try:
            # 清理数据库中的日志记录
            db_cleanup_query = """
            DELETE FROM log_records
            WHERE timestamp < datetime('now', '-{} days')
            """.format(days_old)
            
            deleted_db_records = self.db_manager.execute_update(db_cleanup_query)
            
            # 清理旧的日志文件
            deleted_files = 0
            cutoff_time = datetime.now() - timedelta(days=days_old)
            
            for log_file in self.log_config['log_dir'].glob("*.log.*"):
                if log_file.stat().st_mtime < cutoff_time.timestamp():
                    try:
                        log_file.unlink()
                        deleted_files += 1
                    except Exception as e:
                        self.logger.warning(f"删除日志文件失败 {log_file}: {e}")
            
            self.logger.info(f"清理日志完成: 数据库记录 {deleted_db_records}, 文件 {deleted_files}")
            
            return {
                'success': True,
                'deleted_db_records': deleted_db_records,
                'deleted_files': deleted_files,
                'days_old': days_old
            }
            
        except Exception as e:
            self.logger.error(f"清理日志失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def export_logs(self,
                   output_file: str,
                   log_type: str = None,
                   start_time: str = None,
                   end_time: str = None,
                   format: str = 'json') -> Dict[str, Any]:
        """导出日志
        
        Args:
            output_file: 输出文件路径
            log_type: 日志类型
            start_time: 开始时间
            end_time: 结束时间
            format: 输出格式 ('json', 'csv')
            
        Returns:
            导出结果
        """
        try:
            # 查询日志
            logs_result = self.query_logs(log_type, start_time, end_time, limit=10000)
            
            if not logs_result['success']:
                return logs_result
            
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            if format.lower() == 'json':
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(logs_result, f, ensure_ascii=False, indent=2)
            
            elif format.lower() == 'csv':
                import csv
                with open(output_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(['log_type', 'level', 'message', 'timestamp'])
                    
                    for log in logs_result['logs']:
                        writer.writerow([
                            log['log_type'],
                            log['level'],
                            log['message'],
                            log['timestamp']
                        ])
            
            else:
                raise ValueError(f"不支持的导出格式: {format}")
            
            self.logger.info(f"日志导出完成: {output_file}")
            
            return {
                'success': True,
                'output_file': output_file,
                'format': format,
                'exported_logs': logs_result['total_logs']
            }
            
        except Exception as e:
            self.logger.error(f"导出日志失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }


class DatabaseLogHandler(logging.Handler):
    """数据库日志处理器"""
    
    def __init__(self, db_manager: DatabaseManager, log_type: str):
        super().__init__()
        self.db_manager = db_manager
        self.log_type = log_type
        
        # 确保日志表存在
        self._ensure_log_table()
    
    def _ensure_log_table(self):
        """确保日志表存在"""
        try:
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS log_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                log_type TEXT NOT NULL,
                level TEXT NOT NULL,
                message TEXT NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                log_data TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            
            self.db_manager.execute_update(create_table_sql)
            
            # 创建索引
            index_sqls = [
                "CREATE INDEX IF NOT EXISTS idx_log_records_type ON log_records(log_type)",
                "CREATE INDEX IF NOT EXISTS idx_log_records_level ON log_records(level)",
                "CREATE INDEX IF NOT EXISTS idx_log_records_timestamp ON log_records(timestamp)"
            ]
            
            for index_sql in index_sqls:
                self.db_manager.execute_update(index_sql)
                
        except Exception as e:
            # 如果创建表失败，不要影响日志记录
            pass
    
    def emit(self, record):
        """发出日志记录"""
        try:
            # 获取附加数据
            log_data = getattr(record, 'log_data', None)
            log_data_json = json.dumps(log_data, ensure_ascii=False) if log_data else None
            
            # 插入日志记录
            insert_sql = """
            INSERT INTO log_records (log_type, level, message, log_data)
            VALUES (?, ?, ?, ?)
            """
            
            self.db_manager.execute_update(insert_sql, (
                self.log_type,
                record.levelname,
                record.getMessage(),
                log_data_json
            ))
            
        except Exception:
            # 数据库日志失败时不要影响程序运行
            self.handleError(record)


def main():
    """命令行主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='日志记录管理器')
    parser.add_argument('--stats', action='store_true', 
                       help='查看日志统计信息')
    parser.add_argument('--query', help='查询日志类型')
    parser.add_argument('--start-time', help='开始时间')
    parser.add_argument('--end-time', help='结束时间')
    parser.add_argument('--level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                       help='日志级别')
    parser.add_argument('--limit', type=int, default=20,
                       help='返回记录数限制')
    parser.add_argument('--cleanup', type=int, default=30,
                       help='清理指定天数前的日志')
    parser.add_argument('--export', help='导出日志到文件')
    parser.add_argument('--format', choices=['json', 'csv'], default='json',
                       help='导出格式')
    parser.add_argument('--test', action='store_true',
                       help='测试日志功能')
    
    args = parser.parse_args()
    
    # 创建管理器
    manager = LoggingManager()
    
    if args.stats:
        result = manager.get_log_statistics()
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    elif args.query:
        result = manager.query_logs(
            args.query, args.start_time, args.end_time, 
            args.level, args.limit
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    elif args.cleanup:
        result = manager.cleanup_old_logs(args.cleanup)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    elif args.export:
        result = manager.export_logs(
            args.export, args.query, args.start_time, 
            args.end_time, args.format
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    elif args.test:
        # 测试各种日志功能
        print("=== 测试日志功能 ===")
        
        # 测试API日志
        manager.log_api_call('test_api', {'param1': 'value1'}, 150.5, True, records_count=100)
        
        # 测试下载日志
        manager.log_download_progress('task_001', '000001.SZ', '20250110', 50.0, 'processing', 50)
        
        # 测试数据库日志
        manager.log_database_operation('INSERT', 'daily_data', 100, 25.3, True)
        
        # 测试性能日志
        manager.log_performance_metric('api_response_time', 150.5, 'ms', {'api': 'daily'})
        
        # 测试系统日志
        manager.log_system_event('startup', '系统启动完成', LogLevel.INFO, {'version': '1.0'})
        
        # 显示统计信息
        stats = manager.get_log_statistics()
        print("日志统计:")
        print(json.dumps(stats, ensure_ascii=False, indent=2))
        
        print("✅ 日志功能测试完成！")
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main() 