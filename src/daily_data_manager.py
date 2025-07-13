#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
日线数据获取管理器
功能：
1. 按交易日期批量获取日线数据
2. 避免按股票代码循环，提高效率
3. 智能去重，只获取缺失的数据
4. 支持批量下载和断点续传
"""

import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set
import time
import sqlite3

from .database_manager import DatabaseManager
from .optimized_tushare_api_manager import OptimizedTushareAPIManager
from .stock_basic_manager import StockBasicManager


class DailyDataManager:
    """日线数据获取管理器"""
    
    def __init__(self, config_manager):
        """
        初始化日线数据管理器
        
        Args:
            config_manager: 配置管理器实例
        """
        self.config = config_manager
        db_path = config_manager.get('database_path', 'data/stock_data.db')
        self.db_manager = DatabaseManager(db_path)
        self.api_manager = OptimizedTushareAPIManager(config_manager)
        self.stock_manager = StockBasicManager(config_manager)
        
        # 初始化统计信息
        self.stats = {
            'total_dates_processed': 0,
            'total_records_downloaded': 0,
            'successful_dates': 0,
            'failed_dates': 0,
            'skipped_dates': 0,
            'api_calls_made': 0,
            'cache_hits': 0,
            'start_time': None,
            'end_time': None
        }
    
    def get_daily_data_by_date(self, trade_date: str, force_update: bool = False) -> pd.DataFrame:
        """
        按交易日期获取所有股票的日线数据
        
        Args:
            trade_date: 交易日期 (YYYYMMDD格式)
            force_update: 是否强制更新，忽略本地数据
        
        Returns:
            pd.DataFrame: 指定日期的所有股票日线数据
        """
        print(f"🔍 获取 {trade_date} 的日线数据...")
        
        # 转换日期格式以供is_trade_date使用
        formatted_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}"
        
        # 检查是否为交易日
        if not self.api_manager.is_trade_date(formatted_date):
            print(f"⚠️  {trade_date} 不是交易日，跳过")
            self.stats['skipped_dates'] += 1
            return pd.DataFrame()
        
        # 如果不强制更新，检查本地是否已有数据
        if not force_update and self._has_local_data(trade_date):
            print(f"📋 本地已存在 {trade_date} 的数据，使用本地数据")
            return self._load_local_data(trade_date)
        
        # 从API获取数据
        daily_data = self.api_manager.get_daily_data(trade_date=trade_date)
        
        if daily_data is not None and not daily_data.empty:
            # 保存到数据库
            self._save_to_database(daily_data)
            
            # 更新统计信息
            self.stats['total_records_downloaded'] += len(daily_data)
            self.stats['successful_dates'] += 1
            self.stats['api_calls_made'] += 1
            
            print(f"✅ 成功获取 {trade_date} 的 {len(daily_data)} 条数据")
            return daily_data
        else:
            print(f"❌ 获取 {trade_date} 的数据失败")
            self.stats['failed_dates'] += 1
            return pd.DataFrame()
    
    def batch_download_daily_data(self, start_date: str, end_date: str = None, 
                                 max_days: int = 30, force_update: bool = False) -> Dict:
        """
        批量下载日线数据
        
        Args:
            start_date: 开始日期 (YYYYMMDD格式)
            end_date: 结束日期 (YYYYMMDD格式)，为None时使用今天
            max_days: 最大下载天数限制
            force_update: 是否强制更新，忽略本地数据
        
        Returns:
            Dict: 下载结果统计
        """
        print("=" * 60)
        print("🚀 开始批量下载日线数据")
        print("=" * 60)
        
        # 重置统计信息
        self.stats['start_time'] = datetime.now()
        self.stats['total_dates_processed'] = 0
        self.stats['total_records_downloaded'] = 0
        self.stats['successful_dates'] = 0
        self.stats['failed_dates'] = 0
        self.stats['skipped_dates'] = 0
        self.stats['api_calls_made'] = 0
        self.stats['cache_hits'] = 0
        
        # 设置结束日期
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
        
        # 获取交易日期列表
        trade_dates = self._get_trade_dates_between(start_date, end_date, max_days)
        
        if not trade_dates:
            print("❌ 没有找到交易日期")
            return self._get_result_summary()
        
        print(f"📅 计划下载 {len(trade_dates)} 个交易日的数据")
        print(f"📅 日期范围: {trade_dates[0]} 到 {trade_dates[-1]}")
        
        # 检查已存在的数据
        existing_dates = self._get_existing_dates(trade_dates) if not force_update else set()
        remaining_dates = [d for d in trade_dates if d not in existing_dates]
        
        if existing_dates:
            print(f"📋 本地已存在 {len(existing_dates)} 个交易日的数据")
            self.stats['cache_hits'] = len(existing_dates)
        
        if not remaining_dates:
            print("✅ 所有数据已存在，无需下载")
            return self._get_result_summary()
        
        print(f"🔄 需要下载 {len(remaining_dates)} 个交易日的数据")
        
        # 逐个下载数据
        for i, trade_date in enumerate(remaining_dates, 1):
            print(f"\n进度 [{i}/{len(remaining_dates)}] 正在处理 {trade_date}")
            
            try:
                daily_data = self.get_daily_data_by_date(trade_date, force_update=True)
                self.stats['total_dates_processed'] += 1
                
                # 添加延迟，避免API频率限制
                if i < len(remaining_dates):
                    print("⏱️  等待API频率限制...")
                    time.sleep(2)  # 2秒延迟
                
            except Exception as e:
                print(f"❌ 处理 {trade_date} 时发生错误: {e}")
                self.stats['failed_dates'] += 1
        
        # 完成统计
        self.stats['end_time'] = datetime.now()
        
        print("\n" + "=" * 60)
        print("📊 批量下载完成")
        print("=" * 60)
        
        return self._get_result_summary()
    
    def _has_local_data(self, trade_date: str) -> bool:
        """检查本地是否已有指定日期的数据"""
        try:
            conn = self.db_manager.connect()
            cursor = conn.cursor()
            
            # 转换日期格式
            formatted_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}"
            
            query = """
            SELECT COUNT(*) as count
            FROM daily_data 
            WHERE trade_date = ?
            """
            
            cursor.execute(query, (formatted_date,))
            result = cursor.fetchone()
            count = result[0] if result else 0
            
            conn.close()
            
            return count > 0
            
        except Exception as e:
            print(f"❌ 检查本地数据失败: {e}")
            return False
    
    def _load_local_data(self, trade_date: str) -> pd.DataFrame:
        """从本地数据库加载指定日期的数据"""
        try:
            conn = self.db_manager.connect()
            
            # 转换日期格式
            formatted_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}"
            
            query = """
            SELECT * FROM daily_data 
            WHERE trade_date = ?
            ORDER BY ts_code
            """
            
            data = pd.read_sql_query(query, conn, params=(formatted_date,))
            conn.close()
            
            self.stats['cache_hits'] += 1
            return data
            
        except Exception as e:
            print(f"❌ 加载本地数据失败: {e}")
            return pd.DataFrame()
    
    def _save_to_database(self, daily_data: pd.DataFrame):
        """保存日线数据到数据库"""
        try:
            conn = self.db_manager.connect()
            
            # 使用INSERT OR REPLACE避免重复数据
            for _, row in daily_data.iterrows():
                query = """
                INSERT OR REPLACE INTO daily_data 
                (ts_code, trade_date, open, high, low, close, pre_close, 
                 change, pct_chg, vol, amount)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                cursor = conn.cursor()
                cursor.execute(query, (
                    row['ts_code'],
                    row['trade_date'],
                    row.get('open'),
                    row.get('high'),
                    row.get('low'),
                    row.get('close'),
                    row.get('pre_close'),
                    row.get('change'),
                    row.get('pct_chg'),
                    row.get('vol'),
                    row.get('amount')
                ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            print(f"❌ 保存数据到数据库失败: {e}")
    
    def _get_trade_dates_between(self, start_date: str, end_date: str, max_days: int) -> List[str]:
        """获取指定日期范围内的交易日列表"""
        try:
            # 使用API管理器获取交易日期
            all_dates = []
            
            # 转换日期格式
            start_dt = datetime.strptime(start_date, '%Y%m%d')
            end_dt = datetime.strptime(end_date, '%Y%m%d')
            
            # 限制最大天数
            if (end_dt - start_dt).days > max_days:
                end_dt = start_dt + timedelta(days=max_days)
                print(f"⚠️  限制最大天数为 {max_days} 天")
            
            # 生成日期列表
            current_date = start_dt
            while current_date <= end_dt:
                date_str = current_date.strftime('%Y%m%d')
                formatted_date = current_date.strftime('%Y-%m-%d')
                
                if self.api_manager.is_trade_date(formatted_date):
                    all_dates.append(date_str)
                current_date += timedelta(days=1)
            
            return all_dates
            
        except Exception as e:
            print(f"❌ 获取交易日期失败: {e}")
            return []
    
    def _get_existing_dates(self, trade_dates: List[str]) -> Set[str]:
        """获取已存在数据的交易日期"""
        try:
            conn = self.db_manager.connect()
            cursor = conn.cursor()
            
            # 转换日期格式
            formatted_dates = [f"{d[:4]}-{d[4:6]}-{d[6:8]}" for d in trade_dates]
            
            # 构建查询语句
            placeholders = ','.join(['?' for _ in formatted_dates])
            query = f"""
            SELECT DISTINCT trade_date
            FROM daily_data 
            WHERE trade_date IN ({placeholders})
            """
            
            cursor.execute(query, formatted_dates)
            results = cursor.fetchall()
            
            conn.close()
            
            # 转换回原格式
            existing_dates = set()
            for result in results:
                date_str = result[0].replace('-', '')
                existing_dates.add(date_str)
            
            return existing_dates
            
        except Exception as e:
            print(f"❌ 检查已存在数据失败: {e}")
            return set()
    
    def _get_result_summary(self) -> Dict:
        """获取结果摘要"""
        duration = None
        if self.stats['start_time'] and self.stats['end_time']:
            duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        
        summary = {
            'total_dates_processed': self.stats['total_dates_processed'],
            'total_records_downloaded': self.stats['total_records_downloaded'],
            'successful_dates': self.stats['successful_dates'],
            'failed_dates': self.stats['failed_dates'],
            'skipped_dates': self.stats['skipped_dates'],
            'api_calls_made': self.stats['api_calls_made'],
            'cache_hits': self.stats['cache_hits'],
            'duration_seconds': duration,
            'start_time': self.stats['start_time'],
            'end_time': self.stats['end_time']
        }
        
        # 打印摘要
        print(f"✅ 成功处理: {summary['successful_dates']} 个交易日")
        print(f"❌ 失败处理: {summary['failed_dates']} 个交易日")
        print(f"⏭️  跳过处理: {summary['skipped_dates']} 个交易日")
        print(f"📊 总计下载: {summary['total_records_downloaded']} 条记录")
        print(f"🌐 API调用: {summary['api_calls_made']} 次")
        print(f"💾 缓存命中: {summary['cache_hits']} 次")
        
        if duration:
            print(f"⏱️  总耗时: {duration:.2f} 秒")
        
        return summary
    
    def get_stock_daily_data(self, ts_code: str, start_date: str, end_date: str = None) -> pd.DataFrame:
        """
        获取指定股票的日线数据
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期 (YYYYMMDD格式)
            end_date: 结束日期 (YYYYMMDD格式)
        
        Returns:
            pd.DataFrame: 股票日线数据
        """
        try:
            conn = self.db_manager.connect()
            
            # 转换日期格式
            start_formatted = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"
            
            if end_date:
                end_formatted = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"
                query = """
                SELECT * FROM daily_data 
                WHERE ts_code = ? AND trade_date >= ? AND trade_date <= ?
                ORDER BY trade_date
                """
                params = (ts_code, start_formatted, end_formatted)
            else:
                query = """
                SELECT * FROM daily_data 
                WHERE ts_code = ? AND trade_date >= ?
                ORDER BY trade_date
                """
                params = (ts_code, start_formatted)
            
            data = pd.read_sql_query(query, conn, params=params)
            conn.close()
            
            return data
            
        except Exception as e:
            print(f"❌ 获取股票日线数据失败: {e}")
            return pd.DataFrame()
    
    def get_statistics(self) -> Dict:
        """获取统计信息"""
        return self.stats.copy()
    
    def print_status(self):
        """打印当前状态"""
        print("\n" + "=" * 60)
        print("📊 日线数据管理器状态")
        print("=" * 60)
        
        # 数据库统计
        try:
            conn = self.db_manager.connect()
            cursor = conn.cursor()
            
            # 总记录数
            cursor.execute("SELECT COUNT(*) FROM daily_data")
            total_records = cursor.fetchone()[0]
            
            # 股票数量
            cursor.execute("SELECT COUNT(DISTINCT ts_code) FROM daily_data")
            total_stocks = cursor.fetchone()[0]
            
            # 交易日数量
            cursor.execute("SELECT COUNT(DISTINCT trade_date) FROM daily_data")
            total_dates = cursor.fetchone()[0]
            
            # 最新日期
            cursor.execute("SELECT MAX(trade_date) FROM daily_data")
            latest_date = cursor.fetchone()[0]
            
            # 最旧日期
            cursor.execute("SELECT MIN(trade_date) FROM daily_data")
            earliest_date = cursor.fetchone()[0]
            
            conn.close()
            
            print(f"📊 总记录数: {total_records:,}")
            print(f"📈 股票数量: {total_stocks}")
            print(f"📅 交易日数: {total_dates}")
            print(f"📅 数据范围: {earliest_date} 到 {latest_date}")
            
        except Exception as e:
            print(f"❌ 获取数据库统计失败: {e}")
        
        # 当前统计
        print(f"🌐 API调用: {self.stats['api_calls_made']}")
        print(f"💾 缓存命中: {self.stats['cache_hits']}")
        print(f"📊 已处理日期: {self.stats['total_dates_processed']}")
        print(f"📥 已下载记录: {self.stats['total_records_downloaded']}")
        
        print("=" * 60)


def main():
    """命令行测试函数"""
    import argparse
    
    # 添加项目根目录到路径
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    from src.config_manager import ConfigManager
    
    parser = argparse.ArgumentParser(description='日线数据管理器')
    parser.add_argument('--status', action='store_true', help='显示当前状态')
    parser.add_argument('--date', type=str, help='获取指定日期的数据 (YYYYMMDD)')
    parser.add_argument('--batch', type=str, help='批量下载数据，格式: start_date,end_date')
    parser.add_argument('--stock', type=str, help='获取指定股票的数据，格式: ts_code,start_date,end_date')
    parser.add_argument('--force', action='store_true', help='强制更新，忽略本地数据')
    parser.add_argument('--max-days', type=int, default=30, help='最大下载天数')
    
    args = parser.parse_args()
    
    try:
        # 初始化配置和管理器
        config = ConfigManager()
        manager = DailyDataManager(config)
        
        if args.status:
            manager.print_status()
        
        elif args.date:
            data = manager.get_daily_data_by_date(args.date, args.force)
            if not data.empty:
                print(f"\n📊 {args.date} 的数据 ({len(data)} 条记录)")
                print(data.head())
            else:
                print(f"❌ 没有找到 {args.date} 的数据")
        
        elif args.batch:
            dates = args.batch.split(',')
            if len(dates) == 2:
                start_date, end_date = dates
                result = manager.batch_download_daily_data(
                    start_date.strip(), end_date.strip(), 
                    args.max_days, args.force
                )
            else:
                print("❌ 批量下载格式错误，请使用: start_date,end_date")
        
        elif args.stock:
            params = args.stock.split(',')
            if len(params) >= 2:
                ts_code = params[0].strip()
                start_date = params[1].strip()
                end_date = params[2].strip() if len(params) > 2 else None
                
                data = manager.get_stock_daily_data(ts_code, start_date, end_date)
                if not data.empty:
                    print(f"\n📊 {ts_code} 的数据 ({len(data)} 条记录)")
                    print(data.head())
                else:
                    print(f"❌ 没有找到 {ts_code} 的数据")
            else:
                print("❌ 股票查询格式错误，请使用: ts_code,start_date[,end_date]")
        
        else:
            # 默认显示状态
            manager.print_status()
    
    except Exception as e:
        print(f"❌ 执行失败: {e}")
        return 1
    
    return 0


if __name__ == '__main__':
    sys.exit(main()) 