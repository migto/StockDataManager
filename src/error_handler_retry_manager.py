#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
错误处理和重试管理器 - 负责错误处理和重试机制
"""

import time
import random
import functools
import traceback
from typing import Dict, List, Tuple, Optional, Any, Callable, Union
from datetime import datetime, timedelta
import json
import logging
from pathlib import Path
from enum import Enum
import inspect

from .config_manager import ConfigManager
from .database_manager import DatabaseManager


class ErrorType(Enum):
    """错误类型枚举"""
    NETWORK_ERROR = 'network_error'          # 网络错误
    API_LIMIT_ERROR = 'api_limit_error'       # API限制错误
    AUTHENTICATION_ERROR = 'auth_error'       # 认证错误
    DATA_ERROR = 'data_error'                # 数据错误
    DATABASE_ERROR = 'database_error'         # 数据库错误
    TIMEOUT_ERROR = 'timeout_error'          # 超时错误
    UNKNOWN_ERROR = 'unknown_error'          # 未知错误


class RetryStrategy(Enum):
    """重试策略枚举"""
    FIXED_DELAY = 'fixed_delay'              # 固定延时
    EXPONENTIAL_BACKOFF = 'exponential_backoff'  # 指数退避
    LINEAR_BACKOFF = 'linear_backoff'        # 线性退避
    RANDOM_JITTER = 'random_jitter'          # 随机抖动


class ErrorHandlerRetryManager:
    """错误处理和重试管理器
    
    负责实现智能错误处理和重试机制
    """
    
    def __init__(self, config_manager: ConfigManager = None):
        """初始化错误处理和重试管理器
        
        Args:
            config_manager: 配置管理器
        """
        if config_manager is None:
            config_manager = ConfigManager()
        
        self.config = config_manager
        # 从配置管理器获取数据库路径
        db_path = self.config.get('database.path', 'data/stock_data.db')
        self.db_manager = DatabaseManager(db_path)
        self.logger = logging.getLogger(__name__)
        
        # 配置日志
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # 重试配置
        self.retry_config = {
            'max_retries': self.config.get('download.max_retry_attempts', 3),
            'base_delay': 1.0,  # 基础延时（秒）
            'max_delay': 300.0,  # 最大延时（秒）
            'exponential_base': 2.0,  # 指数退避基数
            'jitter_range': 0.1,  # 随机抖动范围
            'timeout': 30.0  # 操作超时时间
        }
        
        # 错误分类规则
        self.error_patterns = {
            ErrorType.NETWORK_ERROR: [
                'ConnectionError', 'TimeoutError', 'requests.exceptions.ConnectionError',
                'requests.exceptions.Timeout', 'URLError', 'socket.error'
            ],
            ErrorType.API_LIMIT_ERROR: [
                'too many requests', 'rate limit', 'quota exceeded', 
                'API limit', 'frequency limit'
            ],
            ErrorType.AUTHENTICATION_ERROR: [
                'authentication failed', 'invalid token', 'unauthorized',
                'permission denied', 'access denied'
            ],
            ErrorType.DATA_ERROR: [
                'invalid data', 'data format error', 'parsing error',
                'json decode error', 'validation error'
            ],
            ErrorType.DATABASE_ERROR: [
                'database error', 'sqlite3.Error', 'constraint failed',
                'database locked', 'no such table'
            ],
            ErrorType.TIMEOUT_ERROR: [
                'timeout', 'time out', 'operation timed out'
            ]
        }
        
        # 可重试的错误类型
        self.retryable_errors = {
            ErrorType.NETWORK_ERROR,
            ErrorType.API_LIMIT_ERROR,
            ErrorType.TIMEOUT_ERROR,
            ErrorType.DATABASE_ERROR
        }
        
        # 不可重试的错误类型
        self.non_retryable_errors = {
            ErrorType.AUTHENTICATION_ERROR,
            ErrorType.DATA_ERROR
        }
        
        # 错误统计
        self.error_stats = {
            'total_errors': 0,
            'error_by_type': {},
            'retries_attempted': 0,
            'successful_retries': 0
        }
    
    def classify_error(self, error: Exception) -> ErrorType:
        """分类错误类型
        
        Args:
            error: 异常对象
            
        Returns:
            错误类型
        """
        error_str = str(error).lower()
        error_type_name = type(error).__name__
        
        for error_type, patterns in self.error_patterns.items():
            for pattern in patterns:
                if pattern.lower() in error_str or pattern.lower() in error_type_name.lower():
                    return error_type
        
        return ErrorType.UNKNOWN_ERROR
    
    def is_retryable(self, error: Exception) -> bool:
        """判断错误是否可重试
        
        Args:
            error: 异常对象
            
        Returns:
            是否可重试
        """
        error_type = self.classify_error(error)
        return error_type in self.retryable_errors
    
    def calculate_delay(self, 
                       attempt: int, 
                       strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF) -> float:
        """计算重试延时
        
        Args:
            attempt: 重试次数（从1开始）
            strategy: 重试策略
            
        Returns:
            延时时间（秒）
        """
        base_delay = self.retry_config['base_delay']
        max_delay = self.retry_config['max_delay']
        
        if strategy == RetryStrategy.FIXED_DELAY:
            delay = base_delay
        elif strategy == RetryStrategy.EXPONENTIAL_BACKOFF:
            delay = base_delay * (self.retry_config['exponential_base'] ** (attempt - 1))
        elif strategy == RetryStrategy.LINEAR_BACKOFF:
            delay = base_delay * attempt
        elif strategy == RetryStrategy.RANDOM_JITTER:
            jitter = random.uniform(-self.retry_config['jitter_range'], 
                                  self.retry_config['jitter_range'])
            delay = base_delay * (1 + jitter)
        else:
            delay = base_delay
        
        # 添加随机抖动以避免雷群效应
        if strategy != RetryStrategy.RANDOM_JITTER:
            jitter = random.uniform(0.8, 1.2)
            delay *= jitter
        
        return min(delay, max_delay)
    
    def retry_decorator(self, 
                       max_retries: int = None,
                       strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF,
                       custom_exceptions: List[type] = None):
        """重试装饰器
        
        Args:
            max_retries: 最大重试次数
            strategy: 重试策略
            custom_exceptions: 自定义可重试异常类型
            
        Returns:
            装饰器函数
        """
        if max_retries is None:
            max_retries = self.retry_config['max_retries']
        
        def decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                last_exception = None
                
                for attempt in range(max_retries + 1):
                    try:
                        result = func(*args, **kwargs)
                        
                        # 成功执行，记录重试成功
                        if attempt > 0:
                            self.error_stats['successful_retries'] += 1
                            self.logger.info(f"函数 {func.__name__} 重试成功，尝试次数: {attempt + 1}")
                        
                        return result
                        
                    except Exception as e:
                        last_exception = e
                        self.error_stats['total_errors'] += 1
                        
                        # 记录错误统计
                        error_type = self.classify_error(e)
                        self.error_stats['error_by_type'][error_type.value] = (
                            self.error_stats['error_by_type'].get(error_type.value, 0) + 1
                        )
                        
                        # 检查是否可重试
                        is_retryable = (
                            self.is_retryable(e) or 
                            (custom_exceptions and any(isinstance(e, exc_type) for exc_type in custom_exceptions))
                        )
                        
                        if not is_retryable or attempt >= max_retries:
                            self.logger.error(f"函数 {func.__name__} 执行失败: {e}")
                            if attempt > 0:
                                self.logger.error(f"重试 {attempt} 次后仍然失败")
                            
                            # 记录错误详情
                            self._log_error_details(func.__name__, e, attempt)
                            raise e
                        
                        # 记录重试
                        self.error_stats['retries_attempted'] += 1
                        delay = self.calculate_delay(attempt + 1, strategy)
                        
                        self.logger.warning(
                            f"函数 {func.__name__} 执行失败 (尝试 {attempt + 1}/{max_retries + 1}): {e}"
                        )
                        self.logger.info(f"将在 {delay:.2f} 秒后重试")
                        
                        time.sleep(delay)
                
                # 如果执行到这里，说明所有重试都失败了
                if last_exception:
                    raise last_exception
                    
            return wrapper
        return decorator
    
    def _log_error_details(self, func_name: str, error: Exception, attempt: int):
        """记录错误详情到数据库
        
        Args:
            func_name: 函数名
            error: 异常对象
            attempt: 尝试次数
        """
        try:
            error_record = {
                'function_name': func_name,
                'error_type': self.classify_error(error).value,
                'error_message': str(error),
                'attempt_count': attempt + 1,
                'timestamp': datetime.now().isoformat(),
                'traceback': traceback.format_exc()
            }
            
            # 保存到系统配置表作为错误日志
            record_id = f"error_log_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
            self.db_manager.execute_update(
                "INSERT OR REPLACE INTO system_config (key, value, description) VALUES (?, ?, ?)",
                (record_id, json.dumps(error_record, ensure_ascii=False), "错误日志记录")
            )
            
        except Exception as log_error:
            self.logger.error(f"记录错误详情失败: {log_error}")
    
    def execute_with_retry(self, 
                          func: Callable,
                          max_retries: int = None,
                          strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF,
                          *args, **kwargs) -> Any:
        """执行函数并处理重试
        
        Args:
            func: 要执行的函数
            max_retries: 最大重试次数
            strategy: 重试策略
            *args, **kwargs: 函数参数
            
        Returns:
            函数执行结果
        """
        if max_retries is None:
            max_retries = self.retry_config['max_retries']
        
        last_exception = None
        
        for attempt in range(max_retries + 1):
            try:
                result = func(*args, **kwargs)
                
                if attempt > 0:
                    self.error_stats['successful_retries'] += 1
                    self.logger.info(f"函数 {func.__name__} 重试成功，尝试次数: {attempt + 1}")
                
                return result
                
            except Exception as e:
                last_exception = e
                self.error_stats['total_errors'] += 1
                
                error_type = self.classify_error(e)
                self.error_stats['error_by_type'][error_type.value] = (
                    self.error_stats['error_by_type'].get(error_type.value, 0) + 1
                )
                
                if not self.is_retryable(e) or attempt >= max_retries:
                    self.logger.error(f"函数 {func.__name__} 执行失败: {e}")
                    self._log_error_details(func.__name__, e, attempt)
                    raise e
                
                self.error_stats['retries_attempted'] += 1
                delay = self.calculate_delay(attempt + 1, strategy)
                
                self.logger.warning(
                    f"函数 {func.__name__} 执行失败 (尝试 {attempt + 1}/{max_retries + 1}): {e}"
                )
                self.logger.info(f"将在 {delay:.2f} 秒后重试")
                
                time.sleep(delay)
        
        if last_exception:
            raise last_exception
    
    def circuit_breaker(self, 
                       failure_threshold: int = 5,
                       timeout: int = 60):
        """断路器装饰器
        
        Args:
            failure_threshold: 失败阈值
            timeout: 断路器打开时间（秒）
            
        Returns:
            装饰器函数
        """
        def decorator(func: Callable) -> Callable:
            func._circuit_breaker_state = 'closed'  # closed, open, half_open
            func._failure_count = 0
            func._last_failure_time = None
            
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                current_time = time.time()
                
                # 检查断路器状态
                if func._circuit_breaker_state == 'open':
                    if current_time - func._last_failure_time > timeout:
                        func._circuit_breaker_state = 'half_open'
                        self.logger.info(f"断路器半开状态: {func.__name__}")
                    else:
                        raise Exception(f"断路器打开，函数 {func.__name__} 暂时不可用")
                
                try:
                    result = func(*args, **kwargs)
                    
                    # 执行成功，重置断路器
                    if func._circuit_breaker_state == 'half_open':
                        func._circuit_breaker_state = 'closed'
                        func._failure_count = 0
                        self.logger.info(f"断路器关闭: {func.__name__}")
                    
                    return result
                    
                except Exception as e:
                    func._failure_count += 1
                    func._last_failure_time = current_time
                    
                    # 检查是否需要打开断路器
                    if func._failure_count >= failure_threshold:
                        func._circuit_breaker_state = 'open'
                        self.logger.warning(f"断路器打开: {func.__name__}, 失败次数: {func._failure_count}")
                    
                    raise e
                    
            return wrapper
        return decorator
    
    def get_error_statistics(self) -> Dict[str, Any]:
        """获取错误统计信息
        
        Returns:
            错误统计信息
        """
        return {
            'success': True,
            'statistics': self.error_stats.copy(),
            'config': self.retry_config.copy(),
            'timestamp': datetime.now().isoformat()
        }
    
    def get_recent_errors(self, limit: int = 10) -> Dict[str, Any]:
        """获取最近的错误记录
        
        Args:
            limit: 返回记录数限制
            
        Returns:
            最近错误记录
        """
        try:
            query = """
            SELECT key, value, updated_at
            FROM system_config
            WHERE key LIKE 'error_log_%'
            ORDER BY updated_at DESC
            LIMIT ?
            """
            
            records = self.db_manager.execute_query(query, (limit,))
            error_logs = []
            
            for record in records:
                key, value, updated_at = record
                try:
                    error_data = json.loads(value)
                    error_logs.append({
                        'log_id': key,
                        'timestamp': updated_at,
                        'error_data': error_data
                    })
                except json.JSONDecodeError:
                    continue
            
            return {
                'success': True,
                'total_errors': len(error_logs),
                'error_logs': error_logs
            }
            
        except Exception as e:
            self.logger.error(f"获取错误记录失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def cleanup_old_error_logs(self, days_old: int = 7) -> Dict[str, Any]:
        """清理旧的错误日志
        
        Args:
            days_old: 清理多少天前的日志
            
        Returns:
            清理结果
        """
        try:
            cleanup_query = """
            DELETE FROM system_config
            WHERE key LIKE 'error_log_%'
            AND updated_at < datetime('now', '-{} days')
            """.format(days_old)
            
            deleted_count = self.db_manager.execute_update(cleanup_query)
            
            self.logger.info(f"清理了 {deleted_count} 条错误日志")
            
            return {
                'success': True,
                'deleted_count': deleted_count,
                'days_old': days_old
            }
            
        except Exception as e:
            self.logger.error(f"清理错误日志失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def reset_error_statistics(self) -> Dict[str, Any]:
        """重置错误统计
        
        Returns:
            重置结果
        """
        try:
            old_stats = self.error_stats.copy()
            
            self.error_stats = {
                'total_errors': 0,
                'error_by_type': {},
                'retries_attempted': 0,
                'successful_retries': 0
            }
            
            self.logger.info("错误统计已重置")
            
            return {
                'success': True,
                'old_statistics': old_stats,
                'reset_time': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"重置错误统计失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }


def main():
    """命令行主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='错误处理和重试管理器')
    parser.add_argument('--stats', action='store_true', 
                       help='查看错误统计信息')
    parser.add_argument('--recent-errors', type=int, default=10,
                       help='查看最近的错误记录')
    parser.add_argument('--cleanup', type=int, default=7,
                       help='清理指定天数前的错误日志')
    parser.add_argument('--reset-stats', action='store_true',
                       help='重置错误统计')
    parser.add_argument('--test-retry', action='store_true',
                       help='测试重试机制')
    
    args = parser.parse_args()
    
    # 创建管理器
    manager = ErrorHandlerRetryManager()
    
    if args.stats:
        result = manager.get_error_statistics()
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    elif args.recent_errors:
        result = manager.get_recent_errors(args.recent_errors)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    elif args.cleanup:
        result = manager.cleanup_old_error_logs(args.cleanup)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    elif args.reset_stats:
        result = manager.reset_error_statistics()
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    elif args.test_retry:
        # 测试重试机制
        @manager.retry_decorator(max_retries=3)
        def test_function():
            import random
            if random.random() < 0.7:  # 70%概率失败
                raise ConnectionError("模拟网络错误")
            return "成功执行"
        
        try:
            result = test_function()
            print(f"测试结果: {result}")
        except Exception as e:
            print(f"测试失败: {e}")
        
        # 显示统计信息
        stats = manager.get_error_statistics()
        print("\n错误统计:")
        print(json.dumps(stats, ensure_ascii=False, indent=2))
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main() 