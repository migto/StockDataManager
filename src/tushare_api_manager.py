#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Tushare API管理器
实现Tushare Pro API的访问管理，包含频率控制、错误处理、积分监控等功能
"""

import tushare as ts
import pandas as pd
import time
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Union
import sqlite3
from pathlib import Path
import json
import traceback

from config_manager import ConfigManager
from database_manager import DatabaseManager


class RateLimiter:
    """API调用频率限制器"""
    
    def __init__(self, max_calls_per_minute: int = 2):
        """初始化频率限制器
        
        Args:
            max_calls_per_minute: 每分钟最大调用次数
        """
        self.max_calls_per_minute = max_calls_per_minute
        self.call_times = []
        self.logger = logging.getLogger(__name__)
    
    def wait_if_needed(self):
        """如果需要则等待到可以进行下次调用"""
        now = datetime.now()
        
        # 移除一分钟前的记录
        cutoff_time = now - timedelta(minutes=1)
        self.call_times = [t for t in self.call_times if t > cutoff_time]
        
        # 如果达到限制，等待
        if len(self.call_times) >= self.max_calls_per_minute:
            # 计算需要等待的时间
            oldest_call = self.call_times[0]
            wait_time = 60 - (now - oldest_call).total_seconds()
            
            if wait_time > 0:
                self.logger.info(f"API调用频率限制，等待 {wait_time:.1f} 秒")
                time.sleep(wait_time)
        
        # 记录这次调用
        self.call_times.append(datetime.now())
    
    def get_remaining_calls(self) -> int:
        """获取本分钟内剩余可调用次数"""
        now = datetime.now()
        cutoff_time = now - timedelta(minutes=1)
        recent_calls = len([t for t in self.call_times if t > cutoff_time])
        return max(0, self.max_calls_per_minute - recent_calls)


class APICallTracker:
    """API调用跟踪器"""
    
    def __init__(self, db_manager: DatabaseManager):
        """初始化API调用跟踪器
        
        Args:
            db_manager: 数据库管理器实例
        """
        self.db_manager = db_manager
        self.logger = logging.getLogger(__name__)
    
    def log_api_call(self, api_name: str, success: bool, response_time: int = None, 
                     records_count: int = 0, error_message: str = None, 
                     request_params: Dict = None):
        """记录API调用日志
        
        Args:
            api_name: API接口名称
            success: 调用是否成功
            response_time: 响应时间（毫秒）
            records_count: 返回记录数
            error_message: 错误信息
            request_params: 请求参数
        """
        try:
            params_str = json.dumps(request_params, ensure_ascii=False) if request_params else None
            
            if not self.db_manager.connection:
                self.db_manager.connect()
            
            self.db_manager.connection.execute("""
                INSERT INTO api_call_log 
                (api_name, success, response_time, records_count, error_message, request_params)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (api_name, success, response_time, records_count, error_message, params_str))
            self.db_manager.connection.commit()
                
        except Exception as e:
            self.logger.error(f"记录API调用日志失败: {e}")
    
    def get_daily_stats(self, date: str = None) -> Dict[str, Any]:
        """获取每日API调用统计
        
        Args:
            date: 日期字符串，格式YYYY-MM-DD，默认今日
            
        Returns:
            统计信息字典
        """
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')
        
        try:
            if not self.db_manager.connection:
                self.db_manager.connect()
            
            cursor = self.db_manager.connection.execute("""
                SELECT 
                    api_name,
                    COUNT(*) as total_calls,
                    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as success_calls,
                    SUM(records_count) as total_records,
                    AVG(response_time) as avg_response_time
                FROM api_call_log 
                WHERE DATE(call_time) = ?
                GROUP BY api_name
            """, (date,))
                
                stats = {}
                for row in cursor.fetchall():
                    api_name, total_calls, success_calls, total_records, avg_response_time = row
                    stats[api_name] = {
                        'total_calls': total_calls,
                        'success_calls': success_calls,
                        'total_records': total_records or 0,
                        'avg_response_time': round(avg_response_time or 0, 2),
                        'success_rate': round(success_calls / total_calls * 100, 2) if total_calls > 0 else 0
                    }
                
                return stats
                
        except Exception as e:
            self.logger.error(f"获取API调用统计失败: {e}")
            return {}
    
    def get_points_consumed_today(self) -> int:
        """获取今日消耗的积分数
        
        Returns:
            今日消耗的积分总数
        """
        today = datetime.now().strftime('%Y-%m-%d')
        
        try:
            if not self.db_manager.connection:
                self.db_manager.connect()
            
            cursor = self.db_manager.connection.execute("""
                SELECT COUNT(*) FROM api_call_log 
                WHERE DATE(call_time) = ? AND success = 1
            """, (today,))
                
                result = cursor.fetchone()
                return result[0] if result else 0
                
        except Exception as e:
            self.logger.error(f"获取今日积分消耗失败: {e}")
            return 0


class TushareAPIManager:
    """Tushare API管理器
    
    负责管理Tushare Pro API的访问，包括：
    - API初始化和连接管理
    - 频率控制和积分监控
    - 错误处理和重试机制
    - API调用记录和日志
    - 数据验证和清洗
    """
    
    def __init__(self, config_manager: ConfigManager = None, db_manager: DatabaseManager = None):
        """初始化TushareAPIManager
        
        Args:
            config_manager: 配置管理器实例
            db_manager: 数据库管理器实例
        """
        self.config = config_manager or ConfigManager()
        self.db_manager = db_manager or DatabaseManager()
        self.logger = logging.getLogger(__name__)
        
        # 初始化API
        self.pro = None
        self._init_api()
        
        # 初始化频率限制器
        calls_per_minute = self.config.get('api_limits.free_account.calls_per_minute', 2)
        self.rate_limiter = RateLimiter(calls_per_minute)
        
        # 初始化调用跟踪器
        self.call_tracker = APICallTracker(self.db_manager)
        
        # API配置
        self.max_retries = self.config.get('tushare.retry_count', 3)
        self.retry_delay = self.config.get('tushare.retry_delay', 1)
        self.timeout = self.config.get('tushare.timeout', 30)
        
        # 积分限制
        self.daily_point_limit = self.config.get('api_limits.free_account.total_points', 120)
        
        self.logger.info("TushareAPIManager初始化完成")
    
    def _init_api(self):
        """初始化Tushare API"""
        token = self.config.get('tushare.token')
        if not token:
            raise ValueError("Tushare Token未配置，请先配置API Token")
        
        try:
            # 设置token
            ts.set_token(token)
            
            # 初始化API
            self.pro = ts.pro_api(token)
            
            self.logger.info("Tushare API初始化成功")
            
        except Exception as e:
            self.logger.error(f"Tushare API初始化失败: {e}")
            raise
    
    def _check_points_limit(self) -> bool:
        """检查今日积分是否超限
        
        Returns:
            True表示可以继续调用，False表示已超限
        """
        points_used = self.call_tracker.get_points_consumed_today()
        
        if points_used >= self.daily_point_limit:
            self.logger.warning(f"今日积分已用完: {points_used}/{self.daily_point_limit}")
            return False
        
        remaining_points = self.daily_point_limit - points_used
        self.logger.debug(f"今日剩余积分: {remaining_points}")
        
        return True
    
    def _api_call_with_retry(self, api_func, api_name: str, **kwargs) -> Optional[pd.DataFrame]:
        """带重试机制的API调用
        
        Args:
            api_func: API函数
            api_name: API名称
            **kwargs: API参数
            
        Returns:
            API返回的DataFrame或None
        """
        # 检查积分限制
        if not self._check_points_limit():
            raise Exception("今日API积分已用完，请明日再试")
        
        last_error = None
        start_time = datetime.now()
        
        for attempt in range(self.max_retries + 1):
            try:
                # 频率控制
                self.rate_limiter.wait_if_needed()
                
                # 调用API
                call_start = datetime.now()
                result = api_func(**kwargs)
                call_end = datetime.now()
                
                # 计算响应时间
                response_time = int((call_end - call_start).total_seconds() * 1000)
                
                # 验证结果
                if result is None or (isinstance(result, pd.DataFrame) and result.empty):
                    self.logger.warning(f"API {api_name} 返回空结果")
                    records_count = 0
                else:
                    records_count = len(result) if isinstance(result, pd.DataFrame) else 1
                    self.logger.info(f"API {api_name} 调用成功，返回 {records_count} 条记录")
                
                # 记录成功的调用
                self.call_tracker.log_api_call(
                    api_name=api_name,
                    success=True,
                    response_time=response_time,
                    records_count=records_count,
                    request_params=kwargs
                )
                
                return result
                
            except Exception as e:
                last_error = e
                error_msg = str(e)
                
                # 记录失败的调用
                response_time = int((datetime.now() - start_time).total_seconds() * 1000)
                self.call_tracker.log_api_call(
                    api_name=api_name,
                    success=False,
                    response_time=response_time,
                    error_message=error_msg,
                    request_params=kwargs
                )
                
                if attempt < self.max_retries:
                    wait_time = self.retry_delay * (2 ** attempt)  # 指数退避
                    self.logger.warning(f"API {api_name} 调用失败 (尝试 {attempt + 1}/{self.max_retries + 1}): {error_msg}")
                    self.logger.info(f"等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                else:
                    self.logger.error(f"API {api_name} 调用最终失败: {error_msg}")
        
        raise last_error
    
    def _validate_and_clean_data(self, data: pd.DataFrame, expected_columns: List[str] = None) -> pd.DataFrame:
        """验证和清洗数据
        
        Args:
            data: 原始数据
            expected_columns: 期望的列名列表
            
        Returns:
            清洗后的数据
        """
        if data is None or data.empty:
            return data
        
        # 检查必需的列
        if expected_columns:
            missing_columns = [col for col in expected_columns if col not in data.columns]
            if missing_columns:
                self.logger.warning(f"数据缺少列: {missing_columns}")
        
        # 数据类型转换
        numeric_columns = ['open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount']
        for col in numeric_columns:
            if col in data.columns:
                data[col] = pd.to_numeric(data[col], errors='coerce')
        
        # 日期格式处理
        date_columns = ['trade_date', 'list_date', 'delist_date', 'cal_date']
        for col in date_columns:
            if col in data.columns:
                # 确保日期格式为YYYY-MM-DD
                data[col] = pd.to_datetime(data[col], format='%Y%m%d', errors='coerce').dt.strftime('%Y-%m-%d')
        
        # 移除完全重复的行
        original_count = len(data)
        data = data.drop_duplicates()
        if len(data) < original_count:
            self.logger.info(f"移除了 {original_count - len(data)} 个重复行")
        
        return data
    
    def get_daily_data(self, trade_date: str = None, ts_code: str = None, 
                      start_date: str = None, end_date: str = None) -> Optional[pd.DataFrame]:
        """获取A股日线行情数据
        
        Args:
            trade_date: 交易日期（YYYYMMDD格式）
            ts_code: 股票代码，支持多个股票（逗号分隔）
            start_date: 开始日期（YYYYMMDD格式）
            end_date: 结束日期（YYYYMMDD格式）
            
        Returns:
            日线行情数据DataFrame
        """
        # 构建API参数
        params = {}
        if trade_date:
            params['trade_date'] = trade_date
        if ts_code:
            params['ts_code'] = ts_code
        if start_date:
            params['start_date'] = start_date
        if end_date:
            params['end_date'] = end_date
        
        # 调用API
        result = self._api_call_with_retry(self.pro.daily, 'daily', **params)
        
        # 验证和清洗数据
        if result is not None:
            expected_columns = ['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'vol']
            result = self._validate_and_clean_data(result, expected_columns)
        
        return result
    
    def get_stock_basic(self, exchange: str = '', list_status: str = 'L', 
                       is_hs: str = None) -> Optional[pd.DataFrame]:
        """获取A股基础信息数据
        
        Args:
            exchange: 交易所（SSE上交所 SZSE深交所）
            list_status: 上市状态（L上市 D退市 P暂停上市）
            is_hs: 是否沪深港通标的（N否 H沪股通 S深股通）
            
        Returns:
            股票基础信息DataFrame
        """
        # 构建API参数
        params = {
            'exchange': exchange,
            'list_status': list_status
        }
        if is_hs:
            params['is_hs'] = is_hs
        
        # 调用API
        result = self._api_call_with_retry(self.pro.stock_basic, 'stock_basic', **params)
        
        # 验证和清洗数据
        if result is not None:
            expected_columns = ['ts_code', 'symbol', 'name', 'area', 'industry', 'list_date']
            result = self._validate_and_clean_data(result, expected_columns)
        
        return result
    
    def get_trade_calendar(self, exchange: str = '', start_date: str = None, 
                          end_date: str = None, is_open: str = None) -> Optional[pd.DataFrame]:
        """获取交易日历数据
        
        Args:
            exchange: 交易所（SSE上交所 SZSE深交所）
            start_date: 开始日期（YYYYMMDD格式）
            end_date: 结束日期（YYYYMMDD格式）
            is_open: 是否交易（0休市 1交易）
            
        Returns:
            交易日历DataFrame
        """
        # 构建API参数
        params = {'exchange': exchange}
        if start_date:
            params['start_date'] = start_date
        if end_date:
            params['end_date'] = end_date
        if is_open:
            params['is_open'] = is_open
        
        # 调用API
        result = self._api_call_with_retry(self.pro.trade_cal, 'trade_cal', **params)
        
        # 验证和清洗数据
        if result is not None:
            expected_columns = ['exchange', 'cal_date', 'is_open']
            result = self._validate_and_clean_data(result, expected_columns)
        
        return result
    
    def get_api_status(self) -> Dict[str, Any]:
        """获取API状态信息
        
        Returns:
            API状态信息字典
        """
        today_stats = self.call_tracker.get_daily_stats()
        points_used = self.call_tracker.get_points_consumed_today()
        remaining_calls = self.rate_limiter.get_remaining_calls()
        
        return {
            'api_initialized': self.pro is not None,
            'points_used_today': points_used,
            'points_remaining': self.daily_point_limit - points_used,
            'calls_remaining_this_minute': remaining_calls,
            'daily_stats': today_stats,
            'daily_point_limit': self.daily_point_limit,
            'calls_per_minute_limit': self.rate_limiter.max_calls_per_minute
        }
    
    def test_connection(self) -> Dict[str, Any]:
        """测试API连接
        
        Returns:
            测试结果字典
        """
        try:
            # 尝试获取交易日历数据（数据量小，适合测试）
            test_date = datetime.now().strftime('%Y%m%d')
            result = self.get_trade_calendar(start_date=test_date, end_date=test_date)
            
            return {
                'success': True,
                'message': 'API连接测试成功',
                'test_data_count': len(result) if result is not None else 0,
                'api_status': self.get_api_status()
            }
            
        except Exception as e:
            return {
                'success': False,
                'message': f'API连接测试失败: {str(e)}',
                'error': str(e),
                'api_status': self.get_api_status()
            }


def main():
    """命令行接口"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Tushare API管理器")
    parser.add_argument('--test', action='store_true', help='测试API连接')
    parser.add_argument('--status', action='store_true', help='显示API状态')
    parser.add_argument('--stats', action='store_true', help='显示今日统计')
    
    # 数据获取命令
    parser.add_argument('--daily', help='获取日线数据（指定交易日期YYYYMMDD）')
    parser.add_argument('--stocks', action='store_true', help='获取股票基础信息')
    parser.add_argument('--calendar', help='获取交易日历（指定日期范围YYYYMMDD-YYYYMMDD）')
    
    args = parser.parse_args()
    
    if not any([args.test, args.status, args.stats, args.daily, args.stocks, args.calendar]):
        parser.print_help()
        return
    
    try:
        # 初始化API管理器
        api_manager = TushareAPIManager()
        
        if args.test:
            result = api_manager.test_connection()
            print("API连接测试结果:")
            print(f"  成功: {result['success']}")
            print(f"  消息: {result['message']}")
            if 'test_data_count' in result:
                print(f"  测试数据条数: {result['test_data_count']}")
        
        elif args.status:
            status = api_manager.get_api_status()
            print("API状态信息:")
            print(f"  API已初始化: {status['api_initialized']}")
            print(f"  今日已用积分: {status['points_used_today']}")
            print(f"  今日剩余积分: {status['points_remaining']}")
            print(f"  本分钟剩余调用次数: {status['calls_remaining_this_minute']}")
            print(f"  每日积分限制: {status['daily_point_limit']}")
            print(f"  每分钟调用限制: {status['calls_per_minute_limit']}")
        
        elif args.stats:
            stats = api_manager.call_tracker.get_daily_stats()
            print("今日API调用统计:")
            if stats:
                for api_name, stat in stats.items():
                    print(f"  {api_name}:")
                    print(f"    总调用次数: {stat['total_calls']}")
                    print(f"    成功次数: {stat['success_calls']}")
                    print(f"    成功率: {stat['success_rate']}%")
                    print(f"    返回记录数: {stat['total_records']}")
                    print(f"    平均响应时间: {stat['avg_response_time']}ms")
            else:
                print("  今日暂无API调用记录")
        
        elif args.daily:
            print(f"获取 {args.daily} 的日线数据...")
            df = api_manager.get_daily_data(trade_date=args.daily)
            if df is not None and not df.empty:
                print(f"成功获取 {len(df)} 条日线数据")
                print(df.head())
            else:
                print("未获取到数据")
        
        elif args.stocks:
            print("获取股票基础信息...")
            df = api_manager.get_stock_basic()
            if df is not None and not df.empty:
                print(f"成功获取 {len(df)} 只股票基础信息")
                print(df.head())
            else:
                print("未获取到数据")
        
        elif args.calendar:
            if '-' in args.calendar:
                start_date, end_date = args.calendar.split('-')
                print(f"获取 {start_date} 到 {end_date} 的交易日历...")
                df = api_manager.get_trade_calendar(start_date=start_date, end_date=end_date)
            else:
                print(f"获取 {args.calendar} 的交易日历...")
                df = api_manager.get_trade_calendar(start_date=args.calendar, end_date=args.calendar)
            
            if df is not None and not df.empty:
                print(f"成功获取 {len(df)} 条交易日历数据")
                print(df.head())
            else:
                print("未获取到数据")
    
    except Exception as e:
        print(f"错误: {e}")
        return 1
    
    return 0


if __name__ == '__main__':
    exit(main()) 