#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
智能下载管理器 - 负责智能下载策略
"""

import sqlite3
import pandas as pd
from typing import Dict, List, Tuple, Optional, Any, Set
from datetime import datetime, timedelta, date
import json
import logging
from pathlib import Path
import time

from .config_manager import ConfigManager
from .database_manager import DatabaseManager
from .optimized_tushare_api_manager import OptimizedTushareAPIManager
from .stock_basic_manager import StockBasicManager
from .daily_data_manager import DailyDataManager
from .data_storage_manager import DataStorageManager
from .incremental_update_manager import IncrementalUpdateManager
from .data_integrity_manager import DataIntegrityManager


class SmartDownloadManager:
    """智能下载管理器
    
    负责实现智能下载策略，检查本地已存在数据，只下载缺失的交易日数据
    """
    
    def __init__(self, config_manager: ConfigManager = None):
        """初始化智能下载管理器
        
        Args:
            config_manager: 配置管理器
        """
        if config_manager is None:
            config_manager = ConfigManager()
        
        self.config = config_manager
        self.logger = logging.getLogger(__name__)
        
        # 初始化各个管理器
        self.db_manager = DatabaseManager(self.config.get('database.path', 'data/stock_data.db'))
        self.api_manager = OptimizedTushareAPIManager(config_manager)
        self.stock_manager = StockBasicManager(config_manager)
        self.daily_manager = DailyDataManager(config_manager)
        self.storage_manager = DataStorageManager(config_manager)
        self.incremental_manager = IncrementalUpdateManager(config_manager)
        self.integrity_manager = DataIntegrityManager(config_manager)
        
        # 配置日志
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # 下载策略配置
        self.download_config = {
            'batch_size': self.config.get('download.batch_size', 100),
            'max_workers': self.config.get('download.max_workers', 1),
            'enable_incremental': self.config.get('download.enable_incremental', True),
            'auto_retry': self.config.get('download.auto_retry', True),
            'max_retry_attempts': self.config.get('download.max_retry_attempts', 3)
        }
    
    def analyze_download_requirements(self, 
                                   start_date: str = None, 
                                   end_date: str = None) -> Dict[str, Any]:
        """分析下载需求
        
        Args:
            start_date: 开始日期 (YYYYMMDD格式)
            end_date: 结束日期 (YYYYMMDD格式)
            
        Returns:
            下载需求分析结果
        """
        try:
            # 默认日期范围
            if start_date is None:
                start_date = '20200101'  # 默认从2020年开始
            if end_date is None:
                end_date = datetime.now().strftime('%Y%m%d')
            
            self.logger.info(f"开始分析下载需求，日期范围: {start_date} - {end_date}")
            
            # 1. 获取股票基本信息
            stock_data = self.stock_manager.get_stock_basic_info()
            if stock_data is None or stock_data.empty:
                return {
                    'success': False,
                    'error': '获取股票基本信息失败'
                }
            
            active_stocks = stock_data[stock_data['list_status'] == 'L']
            
            # 2. 获取缺失交易日
            missing_days_result = self.incremental_manager.get_missing_trading_days(start_date, end_date)
            if not missing_days_result['success']:
                return {
                    'success': False,
                    'error': '获取缺失交易日失败',
                    'details': missing_days_result
                }
            
            # 3. 分析数据覆盖情况
            coverage_result = self.incremental_manager.get_stocks_data_coverage(start_date, end_date)
            if not coverage_result['success']:
                return {
                    'success': False,
                    'error': '分析数据覆盖情况失败',
                    'details': coverage_result
                }
            
            # 4. 计算下载需求
            missing_days = missing_days_result.get('missing_days_list', [])
            coverage_stats = coverage_result.get('coverage_statistics', {})
            
            # 计算需要下载的数据量
            total_download_tasks = 0
            for missing_day in missing_days:
                # 获取该日期缺失数据的股票
                missing_stocks = self.incremental_manager.get_stocks_missing_data(missing_day)
                if missing_stocks['success']:
                    total_download_tasks += missing_stocks['stocks_missing_data']
            
            # 估算API调用次数和时间
            estimated_api_calls = len(missing_days)  # 按交易日期批量获取
            estimated_time_minutes = estimated_api_calls * 0.5  # 假设每次调用耗时30秒
            
            return {
                'success': True,
                'analysis_time': datetime.now().isoformat(),
                'date_range': {
                    'start_date': start_date,
                    'end_date': end_date
                },
                'stock_info': {
                    'total_stocks': len(stock_data),
                    'active_stocks': len(active_stocks),
                    'delisted_stocks': len(stock_data) - len(active_stocks)
                },
                'missing_data': {
                    'missing_trading_days': len(missing_days),
                    'missing_days_list': missing_days[:10],  # 只显示前10个
                    'total_missing_records': total_download_tasks
                },
                'coverage_statistics': coverage_stats,
                'download_estimation': {
                    'estimated_api_calls': estimated_api_calls,
                    'estimated_time_minutes': estimated_time_minutes,
                    'priority': 'high' if total_download_tasks > 10000 else 'medium' if total_download_tasks > 1000 else 'low'
                }
            }
            
        except Exception as e:
            self.logger.error(f"分析下载需求失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def create_download_plan(self, 
                           download_type: str = 'missing_days',
                           max_days: int = 30,
                           priority_stocks: List[str] = None) -> Dict[str, Any]:
        """创建下载计划
        
        Args:
            download_type: 下载类型 ('missing_days', 'recent_days', 'priority_stocks')
            max_days: 最大下载天数
            priority_stocks: 优先股票列表
            
        Returns:
            下载计划
        """
        try:
            self.logger.info(f"创建下载计划，类型: {download_type}, 最大天数: {max_days}")
            
            # 使用增量更新管理器创建基础计划
            plan_result = self.incremental_manager.plan_incremental_update(download_type, max_days)
            
            if not plan_result['success']:
                return plan_result
            
            base_plan = plan_result['update_plan']
            
            # 增强下载计划
            enhanced_plan = {
                'plan_id': f"download_plan_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                'plan_type': download_type,
                'creation_time': datetime.now().isoformat(),
                'config': {
                    'max_days': max_days,
                    'batch_size': self.download_config['batch_size'],
                    'enable_retry': self.download_config['auto_retry'],
                    'max_retry_attempts': self.download_config['max_retry_attempts']
                },
                'tasks': [],
                'statistics': {
                    'total_tasks': 0,
                    'total_api_calls': 0,
                    'estimated_duration_minutes': 0,
                    'priority': 'medium'
                }
            }
            
            # 处理基础计划中的任务
            for task in base_plan.get('tasks', []):
                if task['task_type'] == 'update_trading_day':
                    enhanced_task = {
                        'task_id': len(enhanced_plan['tasks']) + 1,
                        'task_type': 'download_daily_data',
                        'trading_date': task['trading_date'],
                        'method': 'batch_by_date',
                        'priority': 'normal',
                        'status': 'pending',
                        'retry_count': 0,
                        'estimated_stocks': task.get('stocks_to_update', 0)
                    }
                    
                    # 如果有优先股票列表，调整任务优先级
                    if priority_stocks and task.get('stock_list'):
                        priority_count = sum(
                            1 for stock in task['stock_list'] 
                            if stock.get('ts_code') in priority_stocks
                        )
                        if priority_count > 0:
                            enhanced_task['priority'] = 'high'
                    
                    enhanced_plan['tasks'].append(enhanced_task)
                
                elif task['task_type'] == 'update_stock':
                    enhanced_task = {
                        'task_id': len(enhanced_plan['tasks']) + 1,
                        'task_type': 'download_stock_history',
                        'ts_code': task['ts_code'],
                        'stock_name': task['name'],
                        'method': 'by_stock',
                        'priority': 'high' if priority_stocks and task['ts_code'] in priority_stocks else 'normal',
                        'status': 'pending',
                        'retry_count': 0,
                        'last_data_date': task.get('last_data_date')
                    }
                    enhanced_plan['tasks'].append(enhanced_task)
            
            # 按优先级排序任务
            enhanced_plan['tasks'].sort(key=lambda x: (
                0 if x['priority'] == 'high' else 1 if x['priority'] == 'normal' else 2,
                x['task_id']
            ))
            
            # 重新分配任务ID
            for i, task in enumerate(enhanced_plan['tasks']):
                task['task_id'] = i + 1
            
            # 计算统计信息
            enhanced_plan['statistics']['total_tasks'] = len(enhanced_plan['tasks'])
            enhanced_plan['statistics']['total_api_calls'] = len([
                task for task in enhanced_plan['tasks'] 
                if task['task_type'] == 'download_daily_data'
            ])
            enhanced_plan['statistics']['estimated_duration_minutes'] = (
                enhanced_plan['statistics']['total_api_calls'] * 0.5
            )
            
            if enhanced_plan['statistics']['total_tasks'] > 20:
                enhanced_plan['statistics']['priority'] = 'high'
            elif enhanced_plan['statistics']['total_tasks'] > 10:
                enhanced_plan['statistics']['priority'] = 'medium'
            else:
                enhanced_plan['statistics']['priority'] = 'low'
            
            return {
                'success': True,
                'download_plan': enhanced_plan
            }
            
        except Exception as e:
            self.logger.error(f"创建下载计划失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def execute_download_plan(self, 
                            download_plan: Dict[str, Any],
                            dry_run: bool = False) -> Dict[str, Any]:
        """执行下载计划
        
        Args:
            download_plan: 下载计划
            dry_run: 是否为试运行模式
            
        Returns:
            执行结果
        """
        try:
            plan_id = download_plan.get('plan_id', 'unknown')
            self.logger.info(f"开始执行下载计划: {plan_id}, 试运行: {dry_run}")
            
            execution_result = {
                'plan_id': plan_id,
                'execution_id': f"exec_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                'dry_run': dry_run,
                'start_time': datetime.now().isoformat(),
                'end_time': None,
                'status': 'running',
                'progress': {
                    'completed_tasks': 0,
                    'failed_tasks': 0,
                    'skipped_tasks': 0,
                    'total_tasks': len(download_plan.get('tasks', []))
                },
                'task_results': [],
                'statistics': {
                    'total_records_downloaded': 0,
                    'total_api_calls': 0,
                    'average_task_time': 0,
                    'success_rate': 0
                }
            }
            
            tasks = download_plan.get('tasks', [])
            total_start_time = time.time()
            
            for i, task in enumerate(tasks):
                task_start_time = time.time()
                task_result = {
                    'task_id': task['task_id'],
                    'task_type': task['task_type'],
                    'status': 'pending',
                    'start_time': datetime.now().isoformat(),
                    'end_time': None,
                    'records_downloaded': 0,
                    'api_calls': 0,
                    'error_message': None
                }
                
                try:
                    if task['task_type'] == 'download_daily_data':
                        # 下载特定日期的日线数据
                        result = self._execute_daily_data_task(task, dry_run)
                        task_result.update(result)
                        
                    elif task['task_type'] == 'download_stock_history':
                        # 下载特定股票的历史数据
                        result = self._execute_stock_history_task(task, dry_run)
                        task_result.update(result)
                    
                    else:
                        task_result['status'] = 'skipped'
                        task_result['error_message'] = f"不支持的任务类型: {task['task_type']}"
                        execution_result['progress']['skipped_tasks'] += 1
                
                except Exception as task_error:
                    self.logger.error(f"任务 {task['task_id']} 执行失败: {task_error}")
                    task_result['status'] = 'failed'
                    task_result['error_message'] = str(task_error)
                    execution_result['progress']['failed_tasks'] += 1
                
                task_result['end_time'] = datetime.now().isoformat()
                task_result['duration_seconds'] = time.time() - task_start_time
                
                if task_result['status'] == 'completed':
                    execution_result['progress']['completed_tasks'] += 1
                    execution_result['statistics']['total_records_downloaded'] += task_result.get('records_downloaded', 0)
                    execution_result['statistics']['total_api_calls'] += task_result.get('api_calls', 0)
                
                execution_result['task_results'].append(task_result)
                
                # 进度日志
                if (i + 1) % 5 == 0 or i == len(tasks) - 1:
                    progress = (i + 1) / len(tasks) * 100
                    self.logger.info(f"执行进度: {progress:.1f}% ({i + 1}/{len(tasks)})")
                
                # 如果不是试运行，添加延时以避免API限制
                if not dry_run and i < len(tasks) - 1:
                    time.sleep(1)  # 1秒延时
            
            # 计算总体统计
            execution_result['end_time'] = datetime.now().isoformat()
            execution_result['status'] = 'completed'
            
            total_duration = time.time() - total_start_time
            if execution_result['progress']['completed_tasks'] > 0:
                execution_result['statistics']['average_task_time'] = (
                    total_duration / execution_result['progress']['completed_tasks']
                )
            
            total_tasks = execution_result['progress']['total_tasks']
            if total_tasks > 0:
                execution_result['statistics']['success_rate'] = (
                    execution_result['progress']['completed_tasks'] / total_tasks * 100
                )
            
            self.logger.info(f"下载计划执行完成: {plan_id}")
            self.logger.info(f"成功: {execution_result['progress']['completed_tasks']}/{total_tasks}")
            self.logger.info(f"下载记录: {execution_result['statistics']['total_records_downloaded']}")
            
            return {
                'success': True,
                'execution_result': execution_result
            }
            
        except Exception as e:
            self.logger.error(f"执行下载计划失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def _execute_daily_data_task(self, task: Dict[str, Any], dry_run: bool) -> Dict[str, Any]:
        """执行日线数据下载任务
        
        Args:
            task: 任务信息
            dry_run: 是否为试运行
            
        Returns:
            任务执行结果
        """
        trading_date = task['trading_date']
        
        if dry_run:
            # 模拟执行
            estimated_stocks = task.get('estimated_stocks', 0)
            return {
                'status': 'simulated',
                'trading_date': trading_date,
                'records_downloaded': estimated_stocks,
                'api_calls': 1,
                'message': f"模拟下载 {trading_date} 的 {estimated_stocks} 条记录"
            }
        
        try:
            # 实际下载
            download_result = self.daily_manager.download_daily_data([trading_date])
            
            if download_result['success']:
                records_count = 0
                for date_result in download_result['results']:
                    if date_result['success']:
                        records_count += date_result.get('records_count', 0)
                
                # 存储数据
                if records_count > 0:
                    data_to_store = []
                    for date_result in download_result['results']:
                        if date_result['success'] and 'data' in date_result:
                            data_to_store.extend(date_result['data'])
                    
                    if data_to_store:
                        storage_result = self.storage_manager.batch_insert_daily_data(data_to_store)
                        if storage_result['success']:
                            return {
                                'status': 'completed',
                                'trading_date': trading_date,
                                'records_downloaded': records_count,
                                'records_stored': storage_result.get('inserted_count', 0),
                                'api_calls': 1,
                                'message': f"成功下载并存储 {trading_date} 的 {records_count} 条记录"
                            }
                        else:
                            return {
                                'status': 'failed',
                                'trading_date': trading_date,
                                'records_downloaded': records_count,
                                'api_calls': 1,
                                'error_message': f"数据存储失败: {storage_result.get('error', '')}"
                            }
                
                return {
                    'status': 'completed',
                    'trading_date': trading_date,
                    'records_downloaded': 0,
                    'api_calls': 1,
                    'message': f"{trading_date} 无新数据"
                }
            
            else:
                return {
                    'status': 'failed',
                    'trading_date': trading_date,
                    'api_calls': 1,
                    'error_message': f"API调用失败: {download_result.get('error', '')}"
                }
        
        except Exception as e:
            return {
                'status': 'failed',
                'trading_date': trading_date,
                'api_calls': 0,
                'error_message': str(e)
            }
    
    def _execute_stock_history_task(self, task: Dict[str, Any], dry_run: bool) -> Dict[str, Any]:
        """执行股票历史数据下载任务
        
        Args:
            task: 任务信息
            dry_run: 是否为试运行
            
        Returns:
            任务执行结果
        """
        ts_code = task['ts_code']
        stock_name = task.get('stock_name', ts_code)
        
        if dry_run:
            return {
                'status': 'simulated',
                'ts_code': ts_code,
                'stock_name': stock_name,
                'records_downloaded': 100,  # 估算值
                'api_calls': 1,
                'message': f"模拟下载股票 {ts_code} ({stock_name}) 的历史数据"
            }
        
        try:
            # 这里可以实现具体的股票历史数据下载逻辑
            # 由于当前主要是演示，返回模拟结果
            return {
                'status': 'completed',
                'ts_code': ts_code,
                'stock_name': stock_name,
                'records_downloaded': 0,
                'api_calls': 1,
                'message': f"股票历史数据下载功能待实现: {ts_code}"
            }
            
        except Exception as e:
            return {
                'status': 'failed',
                'ts_code': ts_code,
                'stock_name': stock_name,
                'api_calls': 0,
                'error_message': str(e)
            }
    
    def get_download_status(self) -> Dict[str, Any]:
        """获取下载状态概览
        
        Returns:
            下载状态信息
        """
        try:
            # 获取数据库统计信息
            db_stats = self.storage_manager.get_statistics()
            
            # 获取最近的下载记录
            recent_downloads = self._get_recent_download_records(limit=5)
            
            # 获取数据覆盖情况
            coverage_result = self.incremental_manager.get_stocks_data_coverage()
            
            # 获取缺失数据统计
            missing_days_result = self.incremental_manager.get_missing_trading_days()
            
            return {
                'success': True,
                'status_time': datetime.now().isoformat(),
                'database_statistics': db_stats.get('statistics', {}),
                'recent_downloads': recent_downloads,
                'data_coverage': coverage_result.get('coverage_statistics', {}),
                'missing_data': {
                    'missing_trading_days': missing_days_result.get('missing_trading_days', 0),
                    'latest_missing_days': missing_days_result.get('missing_days_list', [])[:5]
                }
            }
            
        except Exception as e:
            self.logger.error(f"获取下载状态失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def _get_recent_download_records(self, limit: int = 5) -> List[Dict[str, Any]]:
        """获取最近的下载记录
        
        Args:
            limit: 返回记录数量
            
        Returns:
            下载记录列表
        """
        try:
            # 从系统配置表获取下载记录
            query = """
            SELECT key, value, updated_at
            FROM system_config
            WHERE key LIKE 'download_record_%'
            ORDER BY updated_at DESC
            LIMIT ?
            """
            
            records = self.db_manager.execute_query(query, (limit,))
            download_records = []
            
            for record in records:
                key, value, updated_at = record
                try:
                    record_data = json.loads(value)
                    download_records.append({
                        'record_id': key,
                        'download_time': updated_at,
                        'download_info': record_data
                    })
                except json.JSONDecodeError:
                    continue
            
            return download_records
            
        except Exception as e:
            self.logger.error(f"获取下载记录失败: {e}")
            return []
    
    def save_download_record(self, execution_result: Dict[str, Any]) -> Dict[str, Any]:
        """保存下载记录
        
        Args:
            execution_result: 执行结果
            
        Returns:
            保存结果
        """
        try:
            # 生成记录ID
            record_id = f"download_record_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # 准备记录数据
            record_data = {
                'execution_id': execution_result.get('execution_id'),
                'plan_id': execution_result.get('plan_id'),
                'execution_time': execution_result.get('start_time'),
                'status': execution_result.get('status'),
                'progress': execution_result.get('progress', {}),
                'statistics': execution_result.get('statistics', {}),
                'dry_run': execution_result.get('dry_run', False)
            }
            
            # 保存到系统配置表
            self.db_manager.execute_update(
                "INSERT OR REPLACE INTO system_config (key, value, description) VALUES (?, ?, ?)",
                (record_id, json.dumps(record_data, ensure_ascii=False), "下载执行记录")
            )
            
            return {
                'success': True,
                'record_id': record_id,
                'message': '下载记录已保存'
            }
            
        except Exception as e:
            self.logger.error(f"保存下载记录失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }


def main():
    """命令行主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='智能下载管理器')
    parser.add_argument('--analyze', action='store_true', 
                       help='分析下载需求')
    parser.add_argument('--start-date', help='开始日期 (YYYYMMDD)')
    parser.add_argument('--end-date', help='结束日期 (YYYYMMDD)')
    parser.add_argument('--create-plan', 
                       choices=['missing_days', 'recent_days', 'priority_stocks'],
                       help='创建下载计划')
    parser.add_argument('--max-days', type=int, default=30,
                       help='最大下载天数')
    parser.add_argument('--execute-plan', help='执行下载计划（JSON文件路径）')
    parser.add_argument('--dry-run', action='store_true',
                       help='试运行模式')
    parser.add_argument('--status', action='store_true',
                       help='查看下载状态')
    
    args = parser.parse_args()
    
    # 创建管理器
    manager = SmartDownloadManager()
    
    if args.analyze:
        result = manager.analyze_download_requirements(args.start_date, args.end_date)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    elif args.create_plan:
        result = manager.create_download_plan(args.create_plan, args.max_days)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    elif args.execute_plan:
        with open(args.execute_plan, 'r', encoding='utf-8') as f:
            plan = json.load(f)
        result = manager.execute_download_plan(plan, args.dry_run)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    elif args.status:
        result = manager.get_download_status()
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main() 