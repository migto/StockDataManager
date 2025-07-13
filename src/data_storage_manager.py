#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据存储和查询管理器
功能：
1. 日线数据批量插入，自动去重
2. 查询已存在数据，避免重复下载
3. 数据完整性检查和验证
4. 高性能的数据查询和统计
"""

import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set, Any
import sqlite3
from collections import defaultdict

from .database_manager import DatabaseManager
from .stock_basic_manager import StockBasicManager


class DataStorageManager:
    """数据存储和查询管理器"""
    
    def __init__(self, config_manager):
        """
        初始化数据存储管理器
        
        Args:
            config_manager: 配置管理器实例
        """
        self.config = config_manager
        db_path = config_manager.get('database_path', 'data/stock_data.db')
        self.db_manager = DatabaseManager(db_path)
        self.stock_manager = StockBasicManager(config_manager)
        
        # 统计信息
        self.stats = {
            'total_inserted': 0,
            'total_updated': 0,
            'total_duplicates': 0,
            'total_errors': 0,
            'batch_operations': 0,
            'last_operation_time': None
        }
    
    def bulk_insert_daily_data(self, daily_data: pd.DataFrame, 
                              check_duplicates: bool = True) -> Dict[str, int]:
        """
        批量插入日线数据
        
        Args:
            daily_data: 日线数据DataFrame
            check_duplicates: 是否检查重复数据
        
        Returns:
            Dict: 插入结果统计
        """
        print(f"📊 开始批量插入日线数据，共 {len(daily_data)} 条记录")
        
        if daily_data.empty:
            print("⚠️  数据为空，跳过插入")
            return {'inserted': 0, 'updated': 0, 'duplicates': 0, 'errors': 0}
        
        # 数据预处理
        processed_data = self._preprocess_daily_data(daily_data)
        
        # 检查重复数据
        if check_duplicates:
            processed_data = self._filter_duplicates(processed_data)
        
        # 验证数据完整性
        valid_data = self._validate_daily_data(processed_data)
        
        # 批量插入
        result = self._execute_bulk_insert(valid_data)
        
        # 更新统计
        self.stats['total_inserted'] += result['inserted']
        self.stats['total_updated'] += result['updated']
        self.stats['total_duplicates'] += result['duplicates']
        self.stats['total_errors'] += result['errors']
        self.stats['batch_operations'] += 1
        self.stats['last_operation_time'] = datetime.now()
        
        print(f"✅ 批量插入完成")
        print(f"   插入: {result['inserted']} 条")
        print(f"   更新: {result['updated']} 条")
        print(f"   重复: {result['duplicates']} 条")
        print(f"   错误: {result['errors']} 条")
        
        return result
    
    def _preprocess_daily_data(self, daily_data: pd.DataFrame) -> pd.DataFrame:
        """预处理日线数据"""
        try:
            # 复制数据避免修改原始数据
            data = daily_data.copy()
            
            # 确保必要列存在
            required_columns = ['ts_code', 'trade_date', 'open', 'high', 'low', 'close']
            missing_columns = [col for col in required_columns if col not in data.columns]
            
            if missing_columns:
                raise ValueError(f"缺失必要列: {missing_columns}")
            
            # 数据类型转换
            numeric_columns = ['open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount']
            for col in numeric_columns:
                if col in data.columns:
                    data[col] = pd.to_numeric(data[col], errors='coerce')
            
            # 日期格式标准化
            if data['trade_date'].dtype == 'object':
                # 直接使用pandas的自动推断，支持多种格式
                data['trade_date'] = pd.to_datetime(data['trade_date'], errors='coerce')
                # 转换为标准格式
                data['trade_date'] = data['trade_date'].dt.strftime('%Y-%m-%d')
            
            # 移除无效行
            data = data.dropna(subset=['ts_code', 'trade_date'])
            
            return data
            
        except Exception as e:
            print(f"❌ 数据预处理失败: {e}")
            raise
    
    def _filter_duplicates(self, daily_data: pd.DataFrame) -> pd.DataFrame:
        """过滤重复数据"""
        try:
            if daily_data.empty:
                return daily_data
            
            # 获取已存在的数据
            existing_data = self._get_existing_data_keys(daily_data)
            
            if not existing_data:
                return daily_data
            
            # 过滤重复数据
            def is_duplicate(row):
                key = (row['ts_code'], row['trade_date'])
                return key in existing_data
            
            # 标记重复数据
            duplicates = daily_data[daily_data.apply(is_duplicate, axis=1)]
            filtered_data = daily_data[~daily_data.apply(is_duplicate, axis=1)]
            
            print(f"📋 过滤重复数据: {len(duplicates)} 条重复，{len(filtered_data)} 条新数据")
            
            return filtered_data
            
        except Exception as e:
            print(f"❌ 过滤重复数据失败: {e}")
            return daily_data
    
    def _get_existing_data_keys(self, daily_data: pd.DataFrame) -> Set[Tuple[str, str]]:
        """获取已存在数据的键值集合"""
        try:
            # 获取所有唯一的ts_code和trade_date组合
            unique_combinations = daily_data[['ts_code', 'trade_date']].drop_duplicates()
            
            # 构建查询条件
            conditions = []
            params = []
            
            for _, row in unique_combinations.iterrows():
                conditions.append("(ts_code = ? AND trade_date = ?)")
                params.extend([row['ts_code'], row['trade_date']])
            
            if not conditions:
                return set()
            
            # 执行查询
            query = f"""
            SELECT ts_code, trade_date 
            FROM daily_data 
            WHERE {' OR '.join(conditions)}
            """
            
            results = self.db_manager.execute_query(query, tuple(params))
            
            # 转换为集合
            existing_keys = set()
            for result in results:
                existing_keys.add((result['ts_code'], result['trade_date']))
            
            return existing_keys
            
        except Exception as e:
            print(f"❌ 获取已存在数据失败: {e}")
            return set()
    
    def _validate_daily_data(self, daily_data: pd.DataFrame) -> pd.DataFrame:
        """验证日线数据完整性"""
        try:
            if daily_data.empty:
                return daily_data
            
            # 数据有效性检查
            valid_data = daily_data.copy()
            
            # 检查价格数据合理性
            price_columns = ['open', 'high', 'low', 'close']
            for col in price_columns:
                if col in valid_data.columns:
                    # 移除负价格和零价格
                    valid_data = valid_data[valid_data[col] > 0]
            
            # 检查高低价格关系
            if all(col in valid_data.columns for col in ['high', 'low']):
                valid_data = valid_data[valid_data['high'] >= valid_data['low']]
            
            # 检查开盘价、收盘价在高低价范围内
            for price_col in ['open', 'close']:
                if price_col in valid_data.columns and all(col in valid_data.columns for col in ['high', 'low']):
                    valid_data = valid_data[
                        (valid_data[price_col] >= valid_data['low']) & 
                        (valid_data[price_col] <= valid_data['high'])
                    ]
            
            # 检查成交量
            if 'vol' in valid_data.columns:
                valid_data = valid_data[valid_data['vol'] >= 0]
            
            # 检查成交额
            if 'amount' in valid_data.columns:
                valid_data = valid_data[valid_data['amount'] >= 0]
            
            invalid_count = len(daily_data) - len(valid_data)
            if invalid_count > 0:
                print(f"⚠️  移除无效数据: {invalid_count} 条")
            
            return valid_data
            
        except Exception as e:
            print(f"❌ 数据验证失败: {e}")
            return daily_data
    
    def _execute_bulk_insert(self, daily_data: pd.DataFrame) -> Dict[str, int]:
        """执行批量插入"""
        try:
            if daily_data.empty:
                return {'inserted': 0, 'updated': 0, 'duplicates': 0, 'errors': 0}
            
            # 转换为字典列表
            data_list = []
            for _, row in daily_data.iterrows():
                data_dict = {
                    'ts_code': row['ts_code'],
                    'trade_date': row['trade_date'],
                    'open': row.get('open'),
                    'high': row.get('high'),
                    'low': row.get('low'),
                    'close': row.get('close'),
                    'pre_close': row.get('pre_close'),
                    'change': row.get('change'),
                    'pct_chg': row.get('pct_chg'),
                    'vol': row.get('vol'),
                    'amount': row.get('amount')
                }
                data_list.append(data_dict)
            
            # 批量插入或更新
            affected_rows = self.db_manager.bulk_insert_or_update(
                'daily_data', 
                data_list, 
                ['ts_code', 'trade_date']
            )
            
            return {
                'inserted': affected_rows,
                'updated': 0,
                'duplicates': 0,
                'errors': 0
            }
            
        except Exception as e:
            print(f"❌ 批量插入失败: {e}")
            return {'inserted': 0, 'updated': 0, 'duplicates': 0, 'errors': 1}
    
    def query_daily_data(self, ts_code: str = None, start_date: str = None, 
                        end_date: str = None, limit: int = None) -> pd.DataFrame:
        """
        查询日线数据
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期 (YYYY-MM-DD格式)
            end_date: 结束日期 (YYYY-MM-DD格式)
            limit: 限制返回条数
        
        Returns:
            pd.DataFrame: 查询结果
        """
        try:
            # 构建查询条件
            conditions = []
            params = []
            
            if ts_code:
                conditions.append("ts_code = ?")
                params.append(ts_code)
            
            if start_date:
                conditions.append("trade_date >= ?")
                params.append(start_date)
            
            if end_date:
                conditions.append("trade_date <= ?")
                params.append(end_date)
            
            # 构建查询语句
            where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
            limit_clause = f" LIMIT {limit}" if limit else ""
            
            query = f"""
            SELECT * FROM daily_data
            {where_clause}
            ORDER BY trade_date DESC, ts_code
            {limit_clause}
            """
            
            # 执行查询
            results = self.db_manager.execute_query(query, tuple(params))
            
            # 转换为DataFrame
            if results:
                df = pd.DataFrame([dict(row) for row in results])
                print(f"📊 查询完成，返回 {len(df)} 条记录")
                return df
            else:
                print("📊 查询完成，无数据")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"❌ 查询失败: {e}")
            return pd.DataFrame()
    
    def get_missing_data_dates(self, ts_code: str, start_date: str, end_date: str) -> List[str]:
        """
        获取缺失数据的交易日期
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期 (YYYY-MM-DD格式)
            end_date: 结束日期 (YYYY-MM-DD格式)
        
        Returns:
            List[str]: 缺失数据的日期列表
        """
        try:
            # 获取指定期间的所有交易日
            from .optimized_tushare_api_manager import OptimizedTushareAPIManager
            api_manager = OptimizedTushareAPIManager(self.config)
            
            # 生成交易日期列表
            trade_dates = []
            current_date = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            
            while current_date <= end_dt:
                date_str = current_date.strftime('%Y-%m-%d')
                if api_manager.is_trade_date(date_str):
                    trade_dates.append(date_str)
                current_date += timedelta(days=1)
            
            # 获取已存在的数据日期
            existing_dates = set()
            query = """
            SELECT DISTINCT trade_date 
            FROM daily_data 
            WHERE ts_code = ? AND trade_date >= ? AND trade_date <= ?
            """
            
            results = self.db_manager.execute_query(query, (ts_code, start_date, end_date))
            for result in results:
                existing_dates.add(result['trade_date'])
            
            # 计算缺失日期
            missing_dates = [date for date in trade_dates if date not in existing_dates]
            
            print(f"📊 {ts_code} 在 {start_date} 到 {end_date} 期间")
            print(f"   总交易日: {len(trade_dates)}")
            print(f"   已有数据: {len(existing_dates)}")
            print(f"   缺失数据: {len(missing_dates)}")
            
            return missing_dates
            
        except Exception as e:
            print(f"❌ 获取缺失数据失败: {e}")
            return []
    
    def get_data_coverage_report(self, ts_code: str = None) -> Dict[str, Any]:
        """
        获取数据覆盖度报告
        
        Args:
            ts_code: 股票代码，为None时获取所有股票的报告
        
        Returns:
            Dict: 数据覆盖度报告
        """
        try:
            report = {}
            
            if ts_code:
                # 单个股票报告
                report = self._get_single_stock_coverage(ts_code)
            else:
                # 所有股票报告
                report = self._get_all_stocks_coverage()
            
            return report
            
        except Exception as e:
            print(f"❌ 获取数据覆盖度报告失败: {e}")
            return {}
    
    def _get_single_stock_coverage(self, ts_code: str) -> Dict[str, Any]:
        """获取单个股票的数据覆盖度"""
        try:
            # 获取股票基本信息
            stock_info = self.stock_manager.get_stock_by_code(ts_code)
            if not stock_info:
                return {'error': '股票不存在'}
            
            # 获取数据统计
            query = """
            SELECT 
                COUNT(*) as total_records,
                MIN(trade_date) as earliest_date,
                MAX(trade_date) as latest_date
            FROM daily_data 
            WHERE ts_code = ?
            """
            
            result = self.db_manager.execute_query(query, (ts_code,))
            if not result:
                return {'error': '无数据'}
            
            stats = result[0]
            
            # 计算覆盖度
            list_date = stock_info.get('list_date', '19900101')
            if isinstance(list_date, str) and len(list_date) == 8:
                list_date = f"{list_date[:4]}-{list_date[4:6]}-{list_date[6:8]}"
            
            return {
                'ts_code': ts_code,
                'name': stock_info.get('name', ''),
                'list_date': list_date,
                'total_records': stats['total_records'],
                'earliest_date': stats['earliest_date'],
                'latest_date': stats['latest_date'],
                'has_data': stats['total_records'] > 0
            }
            
        except Exception as e:
            print(f"❌ 获取单个股票覆盖度失败: {e}")
            return {'error': str(e)}
    
    def _get_all_stocks_coverage(self) -> Dict[str, Any]:
        """获取所有股票的数据覆盖度"""
        try:
            # 获取数据库统计
            daily_stats = self.db_manager.get_table_statistics('daily_data')
            stock_stats = self.db_manager.get_table_statistics('stocks')
            
            # 获取有数据的股票数量
            query = """
            SELECT 
                COUNT(DISTINCT ts_code) as stocks_with_data,
                COUNT(*) as total_records,
                MIN(trade_date) as earliest_date,
                MAX(trade_date) as latest_date
            FROM daily_data
            """
            
            result = self.db_manager.execute_query(query)
            if not result:
                return {'error': '无数据'}
            
            data_stats = result[0]
            
            # 获取覆盖度最好的股票
            query = """
            SELECT 
                ts_code,
                COUNT(*) as record_count,
                MIN(trade_date) as earliest_date,
                MAX(trade_date) as latest_date
            FROM daily_data
            GROUP BY ts_code
            ORDER BY record_count DESC
            LIMIT 10
            """
            
            top_stocks = self.db_manager.execute_query(query)
            
            return {
                'total_stocks': stock_stats.get('total_records', 0),
                'stocks_with_data': data_stats['stocks_with_data'],
                'total_records': data_stats['total_records'],
                'earliest_date': data_stats['earliest_date'],
                'latest_date': data_stats['latest_date'],
                'coverage_rate': round(data_stats['stocks_with_data'] / max(stock_stats.get('total_records', 1), 1) * 100, 2),
                'top_stocks': [dict(stock) for stock in top_stocks]
            }
            
        except Exception as e:
            print(f"❌ 获取所有股票覆盖度失败: {e}")
            return {'error': str(e)}
    
    def clean_duplicate_data(self) -> int:
        """
        清理重复数据
        
        Returns:
            int: 清理的重复数据条数
        """
        try:
            print("🧹 开始清理重复数据...")
            
            # 查找重复数据
            query = """
            SELECT ts_code, trade_date, COUNT(*) as count
            FROM daily_data
            GROUP BY ts_code, trade_date
            HAVING COUNT(*) > 1
            """
            
            duplicates = self.db_manager.execute_query(query)
            
            if not duplicates:
                print("✅ 没有发现重复数据")
                return 0
            
            print(f"🔍 发现 {len(duplicates)} 组重复数据")
            
            # 删除重复数据，保留最新的
            deleted_count = 0
            for duplicate in duplicates:
                ts_code = duplicate['ts_code']
                trade_date = duplicate['trade_date']
                
                # 删除除了最后一条之外的所有重复记录
                delete_query = """
                DELETE FROM daily_data 
                WHERE id NOT IN (
                    SELECT MAX(id) 
                    FROM daily_data 
                    WHERE ts_code = ? AND trade_date = ?
                ) AND ts_code = ? AND trade_date = ?
                """
                
                count = self.db_manager.execute_delete(delete_query, (ts_code, trade_date, ts_code, trade_date))
                deleted_count += count
            
            print(f"✅ 清理完成，删除了 {deleted_count} 条重复数据")
            return deleted_count
            
        except Exception as e:
            print(f"❌ 清理重复数据失败: {e}")
            return 0
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            'operation_stats': self.stats.copy(),
            'database_stats': {
                'daily_data': self.db_manager.get_table_statistics('daily_data'),
                'stocks': self.db_manager.get_table_statistics('stocks')
            }
        }
    
    def print_status(self):
        """打印当前状态"""
        print("\n" + "=" * 60)
        print("📊 数据存储管理器状态")
        print("=" * 60)
        
        # 操作统计
        print("📈 操作统计:")
        print(f"   总插入: {self.stats['total_inserted']}")
        print(f"   总更新: {self.stats['total_updated']}")
        print(f"   重复数据: {self.stats['total_duplicates']}")
        print(f"   错误数: {self.stats['total_errors']}")
        print(f"   批量操作: {self.stats['batch_operations']}")
        
        if self.stats['last_operation_time']:
            print(f"   最后操作: {self.stats['last_operation_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 数据库统计
        try:
            daily_stats = self.db_manager.get_table_statistics('daily_data')
            stock_stats = self.db_manager.get_table_statistics('stocks')
            
            print("\n📊 数据库统计:")
            print(f"   股票总数: {stock_stats.get('total_records', 0)}")
            print(f"   日线记录: {daily_stats.get('total_records', 0)}")
            print(f"   覆盖股票: {daily_stats.get('unique_stocks', 0)}")
            print(f"   交易日数: {daily_stats.get('unique_dates', 0)}")
            
            if daily_stats.get('earliest_date') and daily_stats.get('latest_date'):
                print(f"   数据范围: {daily_stats['earliest_date']} 到 {daily_stats['latest_date']}")
            
        except Exception as e:
            print(f"❌ 获取数据库统计失败: {e}")
        
        print("=" * 60)


def main():
    """命令行测试函数"""
    import argparse
    
    # 添加项目根目录到路径
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    from src.config_manager import ConfigManager
    
    parser = argparse.ArgumentParser(description='数据存储管理器')
    parser.add_argument('--status', action='store_true', help='显示当前状态')
    parser.add_argument('--query', type=str, help='查询数据，格式: ts_code,start_date,end_date')
    parser.add_argument('--missing', type=str, help='查询缺失数据，格式: ts_code,start_date,end_date')
    parser.add_argument('--coverage', type=str, nargs='?', const='', help='数据覆盖度报告，可选股票代码')
    parser.add_argument('--clean', action='store_true', help='清理重复数据')
    parser.add_argument('--limit', type=int, default=100, help='查询结果限制')
    
    args = parser.parse_args()
    
    try:
        # 初始化配置和管理器
        config = ConfigManager()
        manager = DataStorageManager(config)
        
        if args.status:
            manager.print_status()
        
        elif args.query:
            params = args.query.split(',')
            if len(params) >= 1:
                ts_code = params[0].strip() if params[0].strip() else None
                start_date = params[1].strip() if len(params) > 1 and params[1].strip() else None
                end_date = params[2].strip() if len(params) > 2 and params[2].strip() else None
                
                data = manager.query_daily_data(ts_code, start_date, end_date, args.limit)
                if not data.empty:
                    print(f"\n📊 查询结果 ({len(data)} 条记录)")
                    print(data.head())
                else:
                    print("\n📊 查询结果为空")
            else:
                print("❌ 查询格式错误，请使用: ts_code,start_date,end_date")
        
        elif args.missing:
            params = args.missing.split(',')
            if len(params) == 3:
                ts_code = params[0].strip()
                start_date = params[1].strip()
                end_date = params[2].strip()
                
                missing_dates = manager.get_missing_data_dates(ts_code, start_date, end_date)
                if missing_dates:
                    print(f"\n📊 缺失数据日期 ({len(missing_dates)} 个)")
                    for date in missing_dates[:10]:  # 显示前10个
                        print(f"   {date}")
                    if len(missing_dates) > 10:
                        print(f"   ... 还有 {len(missing_dates) - 10} 个日期")
                else:
                    print("\n📊 没有缺失数据")
            else:
                print("❌ 缺失数据查询格式错误，请使用: ts_code,start_date,end_date")
        
        elif args.coverage is not None:
            ts_code = args.coverage if args.coverage else None
            report = manager.get_data_coverage_report(ts_code)
            
            if 'error' in report:
                print(f"❌ 获取覆盖度报告失败: {report['error']}")
            else:
                print(f"\n📊 数据覆盖度报告")
                print("-" * 40)
                if ts_code:
                    print(f"股票代码: {report['ts_code']}")
                    print(f"股票名称: {report['name']}")
                    print(f"上市日期: {report['list_date']}")
                    print(f"数据记录: {report['total_records']}")
                    print(f"数据范围: {report['earliest_date']} 到 {report['latest_date']}")
                else:
                    print(f"总股票数: {report['total_stocks']}")
                    print(f"有数据股票: {report['stocks_with_data']}")
                    print(f"覆盖率: {report['coverage_rate']}%")
                    print(f"总记录数: {report['total_records']}")
                    print(f"数据范围: {report['earliest_date']} 到 {report['latest_date']}")
        
        elif args.clean:
            deleted_count = manager.clean_duplicate_data()
            print(f"\n✅ 清理完成，删除了 {deleted_count} 条重复数据")
        
        else:
            # 默认显示状态
            manager.print_status()
    
    except Exception as e:
        print(f"❌ 执行失败: {e}")
        return 1
    
    return 0


if __name__ == '__main__':
    sys.exit(main()) 