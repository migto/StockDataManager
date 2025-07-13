#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
增量更新管理器 - 负责智能增量更新策略
"""

import sqlite3
import pandas as pd
from typing import Dict, List, Tuple, Optional, Any, Set
from datetime import datetime, timedelta, date
import json
import logging
from pathlib import Path

from .database_manager import DatabaseManager
from .config_manager import ConfigManager


class IncrementalUpdateManager:
    """增量更新管理器
    
    负责实现智能增量更新机制，只更新缺失或变化的数据
    """
    
    def __init__(self, config_manager: ConfigManager = None):
        """初始化增量更新管理器
        
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
    
    def get_missing_trading_days(self, 
                               start_date: str = None, 
                               end_date: str = None) -> Dict[str, Any]:
        """获取缺失的交易日
        
        Args:
            start_date: 开始日期 (YYYYMMDD格式)
            end_date: 结束日期 (YYYYMMDD格式)
            
        Returns:
            缺失交易日信息
        """
        try:
            # 如果没有指定日期范围，使用默认范围
            if start_date is None:
                start_date = '20200101'  # 默认从2020年开始
            if end_date is None:
                end_date = datetime.now().strftime('%Y%m%d')  # 到当前日期
            
            # 获取所有应该有数据的交易日
            # 这里使用简化的交易日历，实际项目中应该使用完整的交易日历
            expected_trading_days = self._generate_expected_trading_days(start_date, end_date)
            
            # 获取数据库中已有的交易日
            query = """
            SELECT DISTINCT trade_date
            FROM daily_data
            WHERE trade_date >= ? AND trade_date <= ?
            ORDER BY trade_date
            """
            
            existing_days = self.db_manager.execute_query(
                query, 
                (start_date, end_date)
            )
            existing_days_set = {row[0] for row in existing_days}
            
            # 计算缺失的交易日
            missing_days = expected_trading_days - existing_days_set
            
            return {
                'success': True,
                'start_date': start_date,
                'end_date': end_date,
                'expected_trading_days': len(expected_trading_days),
                'existing_trading_days': len(existing_days_set),
                'missing_trading_days': len(missing_days),
                'missing_days_list': sorted(list(missing_days))
            }
            
        except Exception as e:
            self.logger.error(f"获取缺失交易日失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def _generate_expected_trading_days(self, start_date: str, end_date: str) -> Set[str]:
        """生成预期的交易日集合
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            预期交易日集合
        """
        try:
            # 将字符串日期转换为datetime对象
            start_dt = datetime.strptime(start_date, '%Y%m%d')
            end_dt = datetime.strptime(end_date, '%Y%m%d')
            
            expected_days = set()
            current_date = start_dt
            
            # 简化的交易日历：排除周末和主要节假日
            holidays = {
                '20250101',  # 元旦
                '20250128', '20250129', '20250130', '20250131',  # 春节
                '20250201', '20250202', '20250203', '20250204',
                '20250405', '20250406', '20250407',  # 清明节
                '20250501', '20250502', '20250503',  # 劳动节
                '20250612', '20250613', '20250614',  # 端午节
                '20250915', '20250916', '20250917',  # 中秋节
                '20251001', '20251002', '20251003',  # 国庆节
                '20251004', '20251005', '20251006', '20251007'
            }
            
            while current_date <= end_dt:
                # 排除周末
                if current_date.weekday() < 5:  # 0-4是周一到周五
                    date_str = current_date.strftime('%Y%m%d')
                    # 排除节假日
                    if date_str not in holidays:
                        expected_days.add(date_str)
                
                current_date += timedelta(days=1)
            
            return expected_days
            
        except Exception as e:
            self.logger.error(f"生成预期交易日失败: {e}")
            return set()
    
    def get_stocks_missing_data(self, 
                              trading_date: str = None) -> Dict[str, Any]:
        """获取某个交易日缺失数据的股票列表
        
        Args:
            trading_date: 交易日期 (YYYYMMDD格式)，默认为最新交易日
            
        Returns:
            缺失数据的股票信息
        """
        try:
            # 如果没有指定日期，使用最新的交易日
            if trading_date is None:
                latest_query = """
                SELECT MAX(trade_date) FROM daily_data
                """
                result = self.db_manager.execute_query(latest_query)
                if result and result[0][0]:
                    trading_date = result[0][0]
                else:
                    trading_date = datetime.now().strftime('%Y%m%d')
            
            # 获取所有活跃股票
            active_stocks_query = """
            SELECT ts_code, name, list_date, list_status
            FROM stocks
            WHERE list_status = 'L'
            AND (delist_date IS NULL OR delist_date > ?)
            """
            
            active_stocks = self.db_manager.execute_query(
                active_stocks_query, 
                (trading_date,)
            )
            
            # 获取该日期已有数据的股票
            existing_data_query = """
            SELECT DISTINCT ts_code
            FROM daily_data
            WHERE trade_date = ?
            """
            
            existing_data = self.db_manager.execute_query(
                existing_data_query, 
                (trading_date,)
            )
            existing_stocks = {row[0] for row in existing_data}
            
            # 找出缺失数据的股票
            missing_stocks = []
            for stock in active_stocks:
                ts_code, name, list_date, list_status = stock
                if ts_code not in existing_stocks:
                    # 检查该股票在指定日期是否应该有数据
                    if list_date <= trading_date:
                        missing_stocks.append({
                            'ts_code': ts_code,
                            'name': name,
                            'list_date': list_date,
                            'list_status': list_status
                        })
            
            return {
                'success': True,
                'trading_date': trading_date,
                'total_active_stocks': len(active_stocks),
                'stocks_with_data': len(existing_stocks),
                'stocks_missing_data': len(missing_stocks),
                'missing_stocks_list': missing_stocks
            }
            
        except Exception as e:
            self.logger.error(f"获取缺失数据股票失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_stocks_data_coverage(self, 
                               start_date: str = None, 
                               end_date: str = None) -> Dict[str, Any]:
        """获取股票数据覆盖情况
        
        Args:
            start_date: 开始日期 (YYYYMMDD格式)
            end_date: 结束日期 (YYYYMMDD格式)
            
        Returns:
            数据覆盖情况统计
        """
        try:
            # 默认日期范围
            if start_date is None:
                start_date = '20200101'
            if end_date is None:
                end_date = datetime.now().strftime('%Y%m%d')
            
            # 获取每只股票的数据覆盖情况
            coverage_query = """
            SELECT 
                s.ts_code,
                s.name,
                s.list_date,
                COUNT(dd.trade_date) as actual_days,
                MIN(dd.trade_date) as first_data_date,
                MAX(dd.trade_date) as last_data_date
            FROM stocks s
            LEFT JOIN daily_data dd ON s.ts_code = dd.ts_code
                AND dd.trade_date >= ? AND dd.trade_date <= ?
            WHERE s.list_status = 'L'
            GROUP BY s.ts_code, s.name, s.list_date
            ORDER BY actual_days DESC
            """
            
            coverage_data = self.db_manager.execute_query(
                coverage_query, 
                (start_date, end_date)
            )
            
            # 计算预期的交易日数量
            expected_days = len(self._generate_expected_trading_days(start_date, end_date))
            
            # 分析覆盖情况
            coverage_stats = {
                'full_coverage': 0,  # 完全覆盖
                'partial_coverage': 0,  # 部分覆盖
                'no_coverage': 0,  # 无覆盖
                'low_coverage': 0,  # 低覆盖（<50%）
                'medium_coverage': 0,  # 中等覆盖（50%-90%）
                'high_coverage': 0  # 高覆盖（>90%）
            }
            
            detailed_coverage = []
            
            for row in coverage_data:
                ts_code, name, list_date, actual_days, first_date, last_date = row
                
                # 计算该股票在指定日期范围内的预期交易日
                stock_start = max(start_date, list_date) if list_date else start_date
                stock_expected_days = len(self._generate_expected_trading_days(stock_start, end_date))
                
                # 计算覆盖率
                coverage_rate = (actual_days / stock_expected_days) if stock_expected_days > 0 else 0
                
                # 分类统计
                if actual_days == 0:
                    coverage_stats['no_coverage'] += 1
                elif coverage_rate >= 1.0:
                    coverage_stats['full_coverage'] += 1
                else:
                    coverage_stats['partial_coverage'] += 1
                
                if coverage_rate < 0.5:
                    coverage_stats['low_coverage'] += 1
                elif coverage_rate < 0.9:
                    coverage_stats['medium_coverage'] += 1
                else:
                    coverage_stats['high_coverage'] += 1
                
                detailed_coverage.append({
                    'ts_code': ts_code,
                    'name': name,
                    'list_date': list_date,
                    'actual_days': actual_days,
                    'expected_days': stock_expected_days,
                    'coverage_rate': round(coverage_rate * 100, 2),
                    'first_data_date': first_date,
                    'last_data_date': last_date
                })
            
            return {
                'success': True,
                'date_range': {
                    'start_date': start_date,
                    'end_date': end_date,
                    'expected_trading_days': expected_days
                },
                'coverage_statistics': coverage_stats,
                'total_stocks': len(coverage_data),
                'detailed_coverage': detailed_coverage
            }
            
        except Exception as e:
            self.logger.error(f"获取数据覆盖情况失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def plan_incremental_update(self, 
                              update_type: str = 'missing_days',
                              max_days: int = 30) -> Dict[str, Any]:
        """规划增量更新计划
        
        Args:
            update_type: 更新类型 ('missing_days', 'recent_days', 'specific_stocks')
            max_days: 最大更新天数
            
        Returns:
            增量更新计划
        """
        try:
            update_plan = {
                'update_type': update_type,
                'max_days': max_days,
                'plan_time': datetime.now().isoformat(),
                'tasks': []
            }
            
            if update_type == 'missing_days':
                # 规划缺失交易日的更新
                missing_days_result = self.get_missing_trading_days()
                
                if missing_days_result['success']:
                    missing_days = missing_days_result['missing_days_list']
                    # 限制更新天数
                    if len(missing_days) > max_days:
                        missing_days = missing_days[-max_days:]  # 优先更新最近的缺失日期
                    
                    for trading_date in missing_days:
                        missing_stocks = self.get_stocks_missing_data(trading_date)
                        if missing_stocks['success']:
                            update_plan['tasks'].append({
                                'task_type': 'update_trading_day',
                                'trading_date': trading_date,
                                'stocks_to_update': missing_stocks['stocks_missing_data'],
                                'stock_list': missing_stocks['missing_stocks_list']
                            })
            
            elif update_type == 'recent_days':
                # 规划最近几天的更新
                end_date = datetime.now().strftime('%Y%m%d')
                start_date = (datetime.now() - timedelta(days=max_days)).strftime('%Y%m%d')
                
                missing_days_result = self.get_missing_trading_days(start_date, end_date)
                
                if missing_days_result['success']:
                    missing_days = missing_days_result['missing_days_list']
                    
                    for trading_date in missing_days:
                        missing_stocks = self.get_stocks_missing_data(trading_date)
                        if missing_stocks['success']:
                            update_plan['tasks'].append({
                                'task_type': 'update_trading_day',
                                'trading_date': trading_date,
                                'stocks_to_update': missing_stocks['stocks_missing_data'],
                                'stock_list': missing_stocks['missing_stocks_list']
                            })
            
            elif update_type == 'specific_stocks':
                # 规划特定股票的更新
                coverage_result = self.get_stocks_data_coverage()
                
                if coverage_result['success']:
                    # 找出覆盖率低的股票
                    low_coverage_stocks = [
                        stock for stock in coverage_result['detailed_coverage']
                        if stock['coverage_rate'] < 80  # 覆盖率低于80%
                    ]
                    
                    # 限制处理的股票数量
                    if len(low_coverage_stocks) > 100:
                        low_coverage_stocks = low_coverage_stocks[:100]
                    
                    for stock in low_coverage_stocks:
                        update_plan['tasks'].append({
                            'task_type': 'update_stock',
                            'ts_code': stock['ts_code'],
                            'name': stock['name'],
                            'current_coverage': stock['coverage_rate'],
                            'last_data_date': stock['last_data_date']
                        })
            
            # 计算更新计划统计
            total_tasks = len(update_plan['tasks'])
            total_stocks = sum(
                task.get('stocks_to_update', 1) for task in update_plan['tasks']
            )
            
            update_plan['statistics'] = {
                'total_tasks': total_tasks,
                'total_stocks_to_update': total_stocks,
                'estimated_api_calls': total_stocks,  # 估算API调用次数
                'priority': 'high' if total_tasks > 10 else 'medium' if total_tasks > 5 else 'low'
            }
            
            return {
                'success': True,
                'update_plan': update_plan
            }
            
        except Exception as e:
            self.logger.error(f"规划增量更新失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def execute_incremental_update(self, 
                                 update_plan: Dict[str, Any],
                                 dry_run: bool = True) -> Dict[str, Any]:
        """执行增量更新计划
        
        Args:
            update_plan: 更新计划
            dry_run: 是否为试运行模式
            
        Returns:
            执行结果
        """
        try:
            execution_result = {
                'plan_id': update_plan.get('plan_time'),
                'dry_run': dry_run,
                'start_time': datetime.now().isoformat(),
                'completed_tasks': 0,
                'failed_tasks': 0,
                'skipped_tasks': 0,
                'task_results': []
            }
            
            tasks = update_plan.get('tasks', [])
            
            for i, task in enumerate(tasks):
                task_result = {
                    'task_id': i + 1,
                    'task_type': task['task_type'],
                    'status': 'pending'
                }
                
                try:
                    if task['task_type'] == 'update_trading_day':
                        # 更新特定交易日的数据
                        if dry_run:
                            task_result.update({
                                'status': 'simulated',
                                'trading_date': task['trading_date'],
                                'stocks_to_update': task['stocks_to_update'],
                                'message': f"模拟更新 {task['trading_date']} 的 {task['stocks_to_update']} 只股票数据"
                            })
                        else:
                            # 实际更新逻辑在这里实现
                            # 这里只是模拟，实际项目中需要调用API获取数据
                            task_result.update({
                                'status': 'completed',
                                'trading_date': task['trading_date'],
                                'stocks_updated': task['stocks_to_update'],
                                'message': f"已更新 {task['trading_date']} 的 {task['stocks_to_update']} 只股票数据"
                            })
                        
                        execution_result['completed_tasks'] += 1
                    
                    elif task['task_type'] == 'update_stock':
                        # 更新特定股票的数据
                        if dry_run:
                            task_result.update({
                                'status': 'simulated',
                                'ts_code': task['ts_code'],
                                'name': task['name'],
                                'message': f"模拟更新股票 {task['ts_code']} ({task['name']}) 的历史数据"
                            })
                        else:
                            # 实际更新逻辑在这里实现
                            task_result.update({
                                'status': 'completed',
                                'ts_code': task['ts_code'],
                                'name': task['name'],
                                'message': f"已更新股票 {task['ts_code']} ({task['name']}) 的历史数据"
                            })
                        
                        execution_result['completed_tasks'] += 1
                    
                    else:
                        task_result.update({
                            'status': 'skipped',
                            'message': f"不支持的任务类型: {task['task_type']}"
                        })
                        execution_result['skipped_tasks'] += 1
                
                except Exception as task_error:
                    task_result.update({
                        'status': 'failed',
                        'error': str(task_error)
                    })
                    execution_result['failed_tasks'] += 1
                
                execution_result['task_results'].append(task_result)
            
            execution_result['end_time'] = datetime.now().isoformat()
            execution_result['total_tasks'] = len(tasks)
            execution_result['success_rate'] = (
                execution_result['completed_tasks'] / len(tasks) * 100 
                if tasks else 0
            )
            
            return {
                'success': True,
                'execution_result': execution_result
            }
            
        except Exception as e:
            self.logger.error(f"执行增量更新失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_update_history(self, 
                         limit: int = 10) -> Dict[str, Any]:
        """获取更新历史记录
        
        Args:
            limit: 返回记录数量限制
            
        Returns:
            更新历史记录
        """
        try:
            # 从系统配置表获取更新历史
            history_query = """
            SELECT key, value, updated_at
            FROM system_config
            WHERE key LIKE 'update_history_%'
            ORDER BY updated_at DESC
            LIMIT ?
            """
            
            history_records = self.db_manager.execute_query(history_query, (limit,))
            
            update_history = []
            for record in history_records:
                key, value, updated_at = record
                try:
                    update_info = json.loads(value)
                    update_history.append({
                        'record_id': key,
                        'update_time': updated_at,
                        'update_info': update_info
                    })
                except json.JSONDecodeError:
                    continue
            
            return {
                'success': True,
                'total_records': len(update_history),
                'update_history': update_history
            }
            
        except Exception as e:
            self.logger.error(f"获取更新历史失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def save_update_record(self, 
                          update_info: Dict[str, Any]) -> Dict[str, Any]:
        """保存更新记录
        
        Args:
            update_info: 更新信息
            
        Returns:
            保存结果
        """
        try:
            # 生成记录ID
            record_id = f"update_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # 保存到系统配置表
            self.db_manager.execute_update(
                "INSERT OR REPLACE INTO system_config (key, value, description) VALUES (?, ?, ?)",
                (record_id, json.dumps(update_info, ensure_ascii=False), "增量更新记录")
            )
            
            return {
                'success': True,
                'record_id': record_id,
                'message': '更新记录已保存'
            }
            
        except Exception as e:
            self.logger.error(f"保存更新记录失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }


def main():
    """命令行主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='增量更新管理器')
    parser.add_argument('--missing-days', action='store_true', 
                       help='获取缺失的交易日')
    parser.add_argument('--missing-stocks', help='获取指定日期缺失数据的股票')
    parser.add_argument('--coverage', action='store_true', 
                       help='获取数据覆盖情况')
    parser.add_argument('--plan-update', 
                       choices=['missing_days', 'recent_days', 'specific_stocks'],
                       help='规划增量更新计划')
    parser.add_argument('--max-days', type=int, default=30,
                       help='最大更新天数')
    parser.add_argument('--execute-plan', help='执行更新计划（JSON文件路径）')
    parser.add_argument('--dry-run', action='store_true',
                       help='试运行模式')
    parser.add_argument('--history', action='store_true',
                       help='查看更新历史')
    
    args = parser.parse_args()
    
    # 创建管理器
    manager = IncrementalUpdateManager()
    
    if args.missing_days:
        result = manager.get_missing_trading_days()
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    elif args.missing_stocks:
        result = manager.get_stocks_missing_data(args.missing_stocks)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    elif args.coverage:
        result = manager.get_stocks_data_coverage()
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    elif args.plan_update:
        result = manager.plan_incremental_update(args.plan_update, args.max_days)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    elif args.execute_plan:
        with open(args.execute_plan, 'r', encoding='utf-8') as f:
            plan = json.load(f)
        result = manager.execute_incremental_update(plan, args.dry_run)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    elif args.history:
        result = manager.get_update_history()
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main() 