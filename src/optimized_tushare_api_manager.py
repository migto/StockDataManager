#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
优化版Tushare API管理器
专为免费账户设计，专注于daily接口，实现本地缓存策略
"""

import tushare as ts
import pandas as pd
import time
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
import json
from pathlib import Path

from .config_manager import ConfigManager


class OptimizedTushareAPIManager:
    """优化版Tushare API管理器
    
    专为免费账户设计的API管理器，特点：
    - 专注于daily接口（免费账户主要可用接口）
    - 本地缓存策略，减少API调用
    - 内置交易日历数据
    - 智能频率控制
    """
    
    def __init__(self, config_manager: ConfigManager = None):
        """初始化API管理器
        
        Args:
            config_manager: 配置管理器实例
        """
        self.config = config_manager or ConfigManager()
        self.logger = logging.getLogger(__name__)
        
        # API相关
        self.pro = None
        self._init_api()
        
        # 调用计数器（简单的内存计数）
        self.call_count_today = 0
        self.last_call_time = None
        self.calls_this_minute = []
        
        # 限制配置
        self.max_calls_per_minute = 2
        self.max_calls_per_day = 100  # 保守估计
        self.min_call_interval = 30  # 30秒间隔
        
        # 缓存目录
        self.cache_dir = Path("data/cache")
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # 内置交易日历（2024-2025年）
        self._init_trade_calendar()
        
        self.logger.info("优化版TushareAPIManager初始化完成")
    
    def _init_api(self):
        """初始化Tushare API"""
        token = self.config.get('tushare.token')
        if not token:
            raise ValueError("Tushare Token未配置，请先配置API Token")
        
        try:
            ts.set_token(token)
            self.pro = ts.pro_api(token)
            self.logger.info("Tushare API初始化成功")
            
        except Exception as e:
            self.logger.error(f"Tushare API初始化失败: {e}")
            raise
    
    def _init_trade_calendar(self):
        """初始化内置交易日历"""
        # 这里可以预置一些基本的交易日历数据
        # 实际项目中可以从文件加载或手动维护
        self.trade_calendar = {
            # 2024年部分节假日（示例）
            "2024-01-01": False,  # 元旦
            "2024-02-10": False,  # 春节开始
            "2024-02-11": False,
            "2024-02-12": False,
            "2024-02-13": False,
            "2024-02-14": False,
            "2024-02-15": False,
            "2024-02-16": False,
            "2024-02-17": False,  # 春节结束
            "2024-04-04": False,  # 清明节
            "2024-04-05": False,
            "2024-04-06": False,
            "2024-05-01": False,  # 劳动节
            "2024-05-02": False,
            "2024-05-03": False,
            "2024-06-10": False,  # 端午节
            "2024-09-15": False,  # 中秋节
            "2024-09-16": False,
            "2024-09-17": False,
            "2024-10-01": False,  # 国庆节开始
            "2024-10-02": False,
            "2024-10-03": False,
            "2024-10-04": False,
            "2024-10-05": False,
            "2024-10-06": False,
            "2024-10-07": False,  # 国庆节结束
        }
        
        self.logger.info(f"内置交易日历初始化完成，包含 {len(self.trade_calendar)} 个节假日")
    
    def is_trade_date(self, date_str: str) -> bool:
        """判断是否为交易日
        
        Args:
            date_str: 日期字符串，格式YYYY-MM-DD
            
        Returns:
            True表示交易日，False表示非交易日
        """
        # 检查是否在节假日列表中
        if date_str in self.trade_calendar:
            return self.trade_calendar[date_str]
        
        # 检查是否为周末
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        weekday = date_obj.weekday()
        
        # 0-4为周一到周五（交易日），5-6为周六周日（非交易日）
        return weekday < 5
    
    def get_recent_trade_dates(self, days: int = 10) -> List[str]:
        """获取最近的交易日列表
        
        Args:
            days: 查找天数
            
        Returns:
            交易日期列表，格式YYYY-MM-DD
        """
        trade_dates = []
        current_date = datetime.now()
        
        for i in range(days * 2):  # 查找范围扩大，确保找到足够的交易日
            check_date = current_date - timedelta(days=i)
            date_str = check_date.strftime('%Y-%m-%d')
            
            if self.is_trade_date(date_str):
                trade_dates.append(date_str)
                
                if len(trade_dates) >= days:
                    break
        
        return sorted(trade_dates, reverse=True)
    
    def _rate_limit_check(self) -> bool:
        """检查频率限制
        
        Returns:
            True表示可以调用，False表示需要等待
        """
        now = datetime.now()
        
        # 清理一分钟前的调用记录
        self.calls_this_minute = [
            call_time for call_time in self.calls_this_minute 
            if (now - call_time).total_seconds() < 60
        ]
        
        # 检查每分钟限制
        if len(self.calls_this_minute) >= self.max_calls_per_minute:
            self.logger.warning("已达到每分钟调用限制，需要等待")
            return False
        
        # 检查最小调用间隔
        if self.last_call_time:
            time_since_last = (now - self.last_call_time).total_seconds()
            if time_since_last < self.min_call_interval:
                self.logger.warning(f"调用间隔过短，需要等待 {self.min_call_interval - time_since_last:.1f} 秒")
                return False
        
        # 检查每日限制
        if self.call_count_today >= self.max_calls_per_day:
            self.logger.warning("已达到每日调用限制")
            return False
        
        return True
    
    def _wait_for_rate_limit(self):
        """等待直到可以进行API调用"""
        while not self._rate_limit_check():
            time.sleep(5)  # 等待5秒后重新检查
    
    def _record_api_call(self):
        """记录API调用"""
        now = datetime.now()
        self.calls_this_minute.append(now)
        self.last_call_time = now
        self.call_count_today += 1
        
        self.logger.debug(f"记录API调用，今日第 {self.call_count_today} 次")
    
    def get_daily_data(self, trade_date: str = None, ts_codes: List[str] = None, 
                      use_cache: bool = True) -> Optional[pd.DataFrame]:
        """获取A股日线行情数据
        
        Args:
            trade_date: 交易日期，格式YYYY-MM-DD
            ts_codes: 股票代码列表，如果为None则获取所有股票
            use_cache: 是否使用缓存
            
        Returns:
            日线行情数据DataFrame
        """
        if trade_date is None:
            # 获取最近的交易日
            recent_dates = self.get_recent_trade_dates(1)
            if not recent_dates:
                self.logger.error("无法确定最近的交易日")
                return None
            trade_date = recent_dates[0]
        
        # 转换日期格式为API需要的格式
        api_date = trade_date.replace('-', '')
        
        # 检查缓存
        cache_file = self.cache_dir / f"daily_{api_date}.csv"
        if use_cache and cache_file.exists():
            try:
                df = pd.read_csv(cache_file)
                self.logger.info(f"从缓存加载日线数据: {trade_date}, {len(df)} 条记录")
                return df
            except Exception as e:
                self.logger.warning(f"缓存文件读取失败: {e}")
        
        # 检查是否为交易日
        if not self.is_trade_date(trade_date):
            self.logger.warning(f"{trade_date} 不是交易日，跳过")
            return None
        
        # API调用
        try:
            self._wait_for_rate_limit()
            
            self.logger.info(f"开始获取 {trade_date} 的日线数据...")
            start_time = datetime.now()
            
            # 调用API
            if ts_codes:
                # 获取指定股票的数据
                ts_code_str = ','.join(ts_codes)
                df = self.pro.daily(ts_code=ts_code_str, trade_date=api_date)
            else:
                # 获取所有股票的数据
                df = self.pro.daily(trade_date=api_date)
            
            self._record_api_call()
            
            end_time = datetime.now()
            response_time = (end_time - start_time).total_seconds()
            
            if df is None or df.empty:
                self.logger.warning(f"{trade_date} 无数据返回，可能不是交易日")
                return None
            
            # 数据处理
            df = self._process_daily_data(df)
            
            # 保存到缓存
            if use_cache:
                try:
                    df.to_csv(cache_file, index=False)
                    self.logger.info(f"数据已缓存到: {cache_file}")
                except Exception as e:
                    self.logger.warning(f"缓存保存失败: {e}")
            
            self.logger.info(f"成功获取 {trade_date} 的日线数据: {len(df)} 条记录，耗时 {response_time:.2f} 秒")
            return df
            
        except Exception as e:
            self.logger.error(f"获取日线数据失败: {e}")
            return None
    
    def _process_daily_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理日线数据"""
        if df is None or df.empty:
            return df
        
        # 数据类型转换
        numeric_columns = ['open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 日期格式处理
        if 'trade_date' in df.columns:
            df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
        
        # 移除重复数据
        original_count = len(df)
        df = df.drop_duplicates(subset=['ts_code', 'trade_date'] if 'ts_code' in df.columns else None)
        if len(df) < original_count:
            self.logger.info(f"移除了 {original_count - len(df)} 个重复行")
        
        return df
    
    def batch_download_daily_data(self, start_date: str, end_date: str = None, 
                                 max_days: int = 10) -> Dict[str, Any]:
        """批量下载日线数据
        
        Args:
            start_date: 开始日期，格式YYYY-MM-DD
            end_date: 结束日期，格式YYYY-MM-DD，如果为None则使用start_date
            max_days: 最大下载天数（保护措施）
            
        Returns:
            下载结果统计
        """
        if end_date is None:
            end_date = start_date
        
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        # 生成日期范围
        date_range = []
        current_dt = start_dt
        while current_dt <= end_dt and len(date_range) < max_days:
            date_str = current_dt.strftime('%Y-%m-%d')
            if self.is_trade_date(date_str):
                date_range.append(date_str)
            current_dt += timedelta(days=1)
        
        if not date_range:
            return {
                'success': False,
                'message': '指定日期范围内没有交易日',
                'downloaded_dates': [],
                'total_records': 0
            }
        
        self.logger.info(f"开始批量下载，共 {len(date_range)} 个交易日")
        
        results = {
            'success': True,
            'message': f'批量下载完成',
            'downloaded_dates': [],
            'failed_dates': [],
            'total_records': 0,
            'total_api_calls': 0
        }
        
        for date_str in date_range:
            try:
                df = self.get_daily_data(trade_date=date_str, use_cache=True)
                if df is not None and not df.empty:
                    results['downloaded_dates'].append(date_str)
                    results['total_records'] += len(df)
                    results['total_api_calls'] += 1
                    self.logger.info(f"✓ {date_str}: {len(df)} 条记录")
                else:
                    results['failed_dates'].append(date_str)
                    self.logger.warning(f"✗ {date_str}: 无数据")
                
                # 避免频率限制，在调用间隔中等待
                if len(results['downloaded_dates']) < len(date_range):
                    self.logger.info("等待避免频率限制...")
                    time.sleep(self.min_call_interval)
                
            except Exception as e:
                results['failed_dates'].append(date_str)
                self.logger.error(f"✗ {date_str}: 下载失败 - {e}")
        
        success_rate = len(results['downloaded_dates']) / len(date_range) * 100
        results['success_rate'] = round(success_rate, 2)
        
        self.logger.info(f"批量下载完成: {len(results['downloaded_dates'])}/{len(date_range)} 成功，"
                        f"共 {results['total_records']} 条记录")
        
        return results
    
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
        if self.pro is None:
            print("❌ API未初始化")
            return None
        
        # 检查频率限制
        if not self._rate_limit_check():
            print("❌ API调用频率受限")
            return None
        
        try:
            print("🔄 正在获取股票基本信息...")
            
            # 构建API参数
            params = {
                'exchange': exchange,
                'list_status': list_status
            }
            if is_hs:
                params['is_hs'] = is_hs
            
            # 等待频率限制
            self._wait_for_rate_limit()
            
            # 调用API
            start_time = time.time()
            result = self.pro.stock_basic(**params)
            response_time = int((time.time() - start_time) * 1000)
            
            # 记录API调用
            self._record_api_call()
            
            if result is not None and not result.empty:
                print(f"✅ 成功获取 {len(result)} 条股票基本信息")
                
                # 验证必要字段
                expected_columns = ['ts_code', 'symbol', 'name', 'area', 'industry', 'list_date']
                missing_columns = [col for col in expected_columns if col not in result.columns]
                
                if missing_columns:
                    print(f"⚠️  缺失字段: {missing_columns}")
                
                return result
            else:
                print("❌ 获取股票基本信息为空")
                return None
                
        except Exception as e:
            print(f"❌ 获取股票基本信息失败: {e}")
            return None
    
    def get_api_status(self) -> Dict[str, Any]:
        """获取API状态信息"""
        recent_trade_dates = self.get_recent_trade_dates(5)
        
        return {
            'api_initialized': self.pro is not None,
            'calls_today': self.call_count_today,
            'calls_remaining_today': max(0, self.max_calls_per_day - self.call_count_today),
            'calls_this_minute': len(self.calls_this_minute),
            'last_call_time': self.last_call_time.strftime('%H:%M:%S') if self.last_call_time else None,
            'cache_directory': str(self.cache_dir),
            'recent_trade_dates': recent_trade_dates,
            'trade_calendar_entries': len(self.trade_calendar)
        }
    
    def clear_cache(self, days_old: int = 30) -> int:
        """清理缓存文件
        
        Args:
            days_old: 清理多少天前的缓存文件
            
        Returns:
            清理的文件数量
        """
        if not self.cache_dir.exists():
            return 0
        
        cutoff_date = datetime.now() - timedelta(days=days_old)
        cleared_count = 0
        
        for cache_file in self.cache_dir.glob("daily_*.csv"):
            try:
                file_stat = cache_file.stat()
                file_date = datetime.fromtimestamp(file_stat.st_mtime)
                
                if file_date < cutoff_date:
                    cache_file.unlink()
                    cleared_count += 1
                    self.logger.info(f"清理缓存文件: {cache_file.name}")
                    
            except Exception as e:
                self.logger.warning(f"清理缓存文件失败: {cache_file.name} - {e}")
        
        self.logger.info(f"缓存清理完成，共清理 {cleared_count} 个文件")
        return cleared_count


def main():
    """命令行接口"""
    import argparse
    
    parser = argparse.ArgumentParser(description="优化版Tushare API管理器")
    parser.add_argument('--status', action='store_true', help='显示API状态')
    parser.add_argument('--daily', help='获取指定日期的日线数据（YYYY-MM-DD）')
    parser.add_argument('--batch', help='批量下载日线数据（YYYY-MM-DD或YYYY-MM-DD,YYYY-MM-DD）')
    parser.add_argument('--recent', type=int, default=1, help='获取最近N个交易日的数据')
    parser.add_argument('--clear-cache', type=int, help='清理N天前的缓存文件')
    parser.add_argument('--trade-dates', type=int, default=10, help='显示最近N个交易日')
    
    args = parser.parse_args()
    
    if not any([args.status, args.daily, args.batch, args.recent > 1, args.clear_cache is not None, args.trade_dates != 10]):
        parser.print_help()
        return
    
    try:
        # 初始化API管理器
        api_manager = OptimizedTushareAPIManager()
        
        if args.status:
            status = api_manager.get_api_status()
            print("API状态信息:")
            for key, value in status.items():
                print(f"  {key}: {value}")
        
        elif args.daily:
            print(f"获取 {args.daily} 的日线数据...")
            df = api_manager.get_daily_data(trade_date=args.daily)
            if df is not None and not df.empty:
                print(f"成功获取 {len(df)} 条记录")
                print(df.head())
            else:
                print("未获取到数据")
        
        elif args.batch:
            if ',' in args.batch:
                start_date, end_date = args.batch.split(',')
            else:
                start_date = end_date = args.batch
            
            print(f"批量下载 {start_date} 到 {end_date} 的日线数据...")
            result = api_manager.batch_download_daily_data(start_date, end_date)
            
            print("批量下载结果:")
            print(f"  成功率: {result['success_rate']}%")
            print(f"  下载记录数: {result['total_records']}")
            print(f"  API调用次数: {result['total_api_calls']}")
            print(f"  成功日期: {len(result['downloaded_dates'])} 个")
            print(f"  失败日期: {len(result['failed_dates'])} 个")
        
        elif args.recent > 1:
            print(f"获取最近 {args.recent} 个交易日的数据...")
            trade_dates = api_manager.get_recent_trade_dates(args.recent)
            
            for date_str in trade_dates:
                df = api_manager.get_daily_data(trade_date=date_str)
                if df is not None and not df.empty:
                    print(f"✓ {date_str}: {len(df)} 条记录")
                else:
                    print(f"✗ {date_str}: 无数据")
        
        elif args.clear_cache is not None:
            cleared_count = api_manager.clear_cache(args.clear_cache)
            print(f"清理完成，共清理 {cleared_count} 个缓存文件")
        
        elif args.trade_dates != 10:
            trade_dates = api_manager.get_recent_trade_dates(args.trade_dates)
            print(f"最近 {len(trade_dates)} 个交易日:")
            for i, date_str in enumerate(trade_dates, 1):
                print(f"  {i}. {date_str}")
    
    except Exception as e:
        print(f"错误: {e}")
        return 1
    
    return 0


if __name__ == '__main__':
    exit(main()) 