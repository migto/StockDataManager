#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
下载状态管理器 - 负责进度追踪和状态管理
"""

import sqlite3
import pandas as pd
from typing import Dict, List, Tuple, Optional, Any, Union
from datetime import datetime, timedelta, date
import json
import logging
from pathlib import Path
from enum import Enum

from .config_manager import ConfigManager
from .database_manager import DatabaseManager


class DownloadStatus(Enum):
    """下载状态枚举"""
    PENDING = 'pending'          # 等待下载
    IN_PROGRESS = 'in_progress'  # 下载中
    COMPLETED = 'completed'      # 已完成
    FAILED = 'failed'           # 失败
    PARTIAL = 'partial'         # 部分完成
    SKIPPED = 'skipped'         # 跳过


class DownloadStatusManager:
    """下载状态管理器
    
    负责管理下载进度追踪和状态管理，使用download_status表记录每只股票的下载状态
    """
    
    def __init__(self, config_manager: ConfigManager = None):
        """初始化下载状态管理器
        
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
    
    def initialize_stock_status(self, 
                              stock_codes: List[str] = None,
                              reset_existing: bool = False) -> Dict[str, Any]:
        """初始化股票下载状态
        
        Args:
            stock_codes: 股票代码列表，为None时初始化所有股票
            reset_existing: 是否重置已存在的状态
            
        Returns:
            初始化结果
        """
        try:
            self.logger.info("开始初始化股票下载状态")
            
            # 如果未指定股票代码，从stocks表获取所有活跃股票
            if stock_codes is None:
                query = """
                SELECT ts_code, name, list_date, list_status
                FROM stocks
                WHERE list_status = 'L'
                ORDER BY ts_code
                """
                stock_records = self.db_manager.execute_query(query)
                stock_codes = [record[0] for record in stock_records]
                self.logger.info(f"从stocks表获取到 {len(stock_codes)} 只活跃股票")
            
            initialized_count = 0
            updated_count = 0
            
            for ts_code in stock_codes:
                # 检查是否已存在状态记录
                existing_query = """
                SELECT ts_code, status, last_download_date, total_records
                FROM download_status
                WHERE ts_code = ?
                """
                existing = self.db_manager.execute_query(existing_query, (ts_code,))
                
                if existing and not reset_existing:
                    # 已存在且不重置，跳过
                    continue
                elif existing and reset_existing:
                    # 已存在且需要重置，更新记录
                    update_query = """
                    UPDATE download_status
                    SET status = ?, last_download_date = NULL, total_records = 0,
                        error_message = NULL, retry_count = 0, updated_at = CURRENT_TIMESTAMP
                    WHERE ts_code = ?
                    """
                    self.db_manager.execute_update(update_query, (DownloadStatus.PENDING.value, ts_code))
                    updated_count += 1
                else:
                    # 不存在，插入新记录
                    insert_query = """
                    INSERT INTO download_status 
                    (ts_code, last_download_date, total_records, status, error_message, retry_count, updated_at)
                    VALUES (?, NULL, 0, ?, NULL, 0, CURRENT_TIMESTAMP)
                    """
                    self.db_manager.execute_update(insert_query, (ts_code, DownloadStatus.PENDING.value))
                    initialized_count += 1
            
            total_processed = initialized_count + updated_count
            self.logger.info(f"状态初始化完成: 新增 {initialized_count}, 更新 {updated_count}")
            
            return {
                'success': True,
                'initialized_count': initialized_count,
                'updated_count': updated_count,
                'total_processed': total_processed,
                'total_stocks': len(stock_codes)
            }
            
        except Exception as e:
            self.logger.error(f"初始化股票下载状态失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def update_stock_status(self,
                          ts_code: str,
                          status: Union[DownloadStatus, str],
                          last_download_date: str = None,
                          total_records: int = None,
                          error_message: str = None,
                          increment_retry: bool = False) -> Dict[str, Any]:
        """更新股票下载状态
        
        Args:
            ts_code: 股票代码
            status: 下载状态
            last_download_date: 最后下载日期
            total_records: 总记录数
            error_message: 错误信息
            increment_retry: 是否增加重试次数
            
        Returns:
            更新结果
        """
        try:
            # 处理状态参数
            if isinstance(status, DownloadStatus):
                status_value = status.value
            else:
                status_value = status
            
            # 构建更新SQL
            update_fields = ['status = ?', 'updated_at = CURRENT_TIMESTAMP']
            update_values = [status_value]
            
            if last_download_date is not None:
                update_fields.append('last_download_date = ?')
                update_values.append(last_download_date)
            
            if total_records is not None:
                update_fields.append('total_records = ?')
                update_values.append(total_records)
            
            if error_message is not None:
                update_fields.append('error_message = ?')
                update_values.append(error_message)
            
            if increment_retry:
                update_fields.append('retry_count = retry_count + 1')
            
            update_values.append(ts_code)
            
            update_query = f"""
            UPDATE download_status
            SET {', '.join(update_fields)}
            WHERE ts_code = ?
            """
            
            affected_rows = self.db_manager.execute_update(update_query, tuple(update_values))
            
            if affected_rows > 0:
                self.logger.debug(f"更新股票 {ts_code} 状态为 {status_value}")
                return {
                    'success': True,
                    'ts_code': ts_code,
                    'status': status_value,
                    'affected_rows': affected_rows
                }
            else:
                # 记录不存在，创建新记录
                return self._create_status_record(ts_code, status_value, last_download_date, 
                                                total_records, error_message)
            
        except Exception as e:
            self.logger.error(f"更新股票状态失败 {ts_code}: {e}")
            return {
                'success': False,
                'error': str(e),
                'ts_code': ts_code
            }
    
    def _create_status_record(self,
                            ts_code: str,
                            status: str,
                            last_download_date: str = None,
                            total_records: int = None,
                            error_message: str = None) -> Dict[str, Any]:
        """创建新的状态记录"""
        try:
            insert_query = """
            INSERT INTO download_status 
            (ts_code, last_download_date, total_records, status, error_message, retry_count, updated_at)
            VALUES (?, ?, ?, ?, ?, 0, CURRENT_TIMESTAMP)
            """
            
            self.db_manager.execute_update(insert_query, (
                ts_code, 
                last_download_date, 
                total_records or 0, 
                status, 
                error_message
            ))
            
            self.logger.info(f"创建股票 {ts_code} 状态记录")
            return {
                'success': True,
                'ts_code': ts_code,
                'status': status,
                'created': True
            }
            
        except Exception as e:
            self.logger.error(f"创建状态记录失败 {ts_code}: {e}")
            return {
                'success': False,
                'error': str(e),
                'ts_code': ts_code
            }
    
    def get_stock_status(self, ts_code: str) -> Dict[str, Any]:
        """获取股票下载状态
        
        Args:
            ts_code: 股票代码
            
        Returns:
            股票状态信息
        """
        try:
            query = """
            SELECT ds.*, s.name, s.list_date, s.list_status
            FROM download_status ds
            LEFT JOIN stocks s ON ds.ts_code = s.ts_code
            WHERE ds.ts_code = ?
            """
            
            result = self.db_manager.execute_query(query, (ts_code,))
            
            if result:
                record = result[0]
                return {
                    'success': True,
                    'ts_code': record[0],
                    'last_download_date': record[1],
                    'total_records': record[2],
                    'status': record[3],
                    'error_message': record[4],
                    'retry_count': record[5],
                    'updated_at': record[6],
                    'stock_name': record[7],
                    'list_date': record[8],
                    'list_status': record[9]
                }
            else:
                return {
                    'success': False,
                    'error': f'股票 {ts_code} 状态记录不存在'
                }
                
        except Exception as e:
            self.logger.error(f"获取股票状态失败 {ts_code}: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_stocks_by_status(self, 
                           status: Union[DownloadStatus, str, List[str]] = None,
                           limit: int = None) -> Dict[str, Any]:
        """根据状态获取股票列表
        
        Args:
            status: 状态过滤条件，可以是单个状态或状态列表
            limit: 返回记录数限制
            
        Returns:
            股票列表
        """
        try:
            # 构建查询条件
            where_conditions = []
            query_params = []
            
            if status is not None:
                if isinstance(status, DownloadStatus):
                    status_list = [status.value]
                elif isinstance(status, str):
                    status_list = [status]
                elif isinstance(status, list):
                    status_list = status
                else:
                    raise ValueError(f"不支持的状态类型: {type(status)}")
                
                placeholders = ', '.join('?' * len(status_list))
                where_conditions.append(f"ds.status IN ({placeholders})")
                query_params.extend(status_list)
            
            where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""
            limit_clause = f"LIMIT {limit}" if limit else ""
            
            query = f"""
            SELECT ds.*, s.name, s.list_date, s.list_status
            FROM download_status ds
            LEFT JOIN stocks s ON ds.ts_code = s.ts_code
            {where_clause}
            ORDER BY ds.updated_at DESC
            {limit_clause}
            """
            
            results = self.db_manager.execute_query(query, tuple(query_params))
            
            stocks = []
            for record in results:
                stocks.append({
                    'ts_code': record[0],
                    'last_download_date': record[1],
                    'total_records': record[2],
                    'status': record[3],
                    'error_message': record[4],
                    'retry_count': record[5],
                    'updated_at': record[6],
                    'stock_name': record[7],
                    'list_date': record[8],
                    'list_status': record[9]
                })
            
            return {
                'success': True,
                'total_count': len(stocks),
                'stocks': stocks,
                'filter_status': status
            }
            
        except Exception as e:
            self.logger.error(f"根据状态获取股票列表失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_download_progress(self) -> Dict[str, Any]:
        """获取下载进度统计
        
        Returns:
            进度统计信息
        """
        try:
            # 获取各状态的统计
            status_query = """
            SELECT status, COUNT(*) as count
            FROM download_status
            GROUP BY status
            """
            
            status_results = self.db_manager.execute_query(status_query)
            status_stats = {result[0]: result[1] for result in status_results}
            
            # 获取总体统计
            total_query = """
            SELECT 
                COUNT(*) as total_stocks,
                SUM(total_records) as total_records,
                AVG(CASE WHEN total_records > 0 THEN total_records ELSE NULL END) as avg_records_per_stock,
                MAX(last_download_date) as latest_download_date,
                MIN(last_download_date) as earliest_download_date
            FROM download_status
            """
            
            total_result = self.db_manager.execute_query(total_query)
            if total_result:
                total_stats = {
                    'total_stocks': total_result[0][0],
                    'total_records': total_result[0][1] or 0,
                    'avg_records_per_stock': round(total_result[0][2] or 0, 2),
                    'latest_download_date': total_result[0][3],
                    'earliest_download_date': total_result[0][4]
                }
            else:
                total_stats = {
                    'total_stocks': 0,
                    'total_records': 0,
                    'avg_records_per_stock': 0,
                    'latest_download_date': None,
                    'earliest_download_date': None
                }
            
            # 计算完成率
            total_stocks = total_stats['total_stocks']
            completed_stocks = status_stats.get(DownloadStatus.COMPLETED.value, 0)
            completion_rate = (completed_stocks / total_stocks * 100) if total_stocks > 0 else 0
            
            # 获取失败重试统计
            retry_query = """
            SELECT 
                COUNT(*) as stocks_with_retries,
                SUM(retry_count) as total_retries,
                MAX(retry_count) as max_retries
            FROM download_status
            WHERE retry_count > 0
            """
            
            retry_result = self.db_manager.execute_query(retry_query)
            if retry_result:
                retry_stats = {
                    'stocks_with_retries': retry_result[0][0],
                    'total_retries': retry_result[0][1] or 0,
                    'max_retries': retry_result[0][2] or 0
                }
            else:
                retry_stats = {
                    'stocks_with_retries': 0,
                    'total_retries': 0,
                    'max_retries': 0
                }
            
            return {
                'success': True,
                'progress_time': datetime.now().isoformat(),
                'status_distribution': status_stats,
                'total_statistics': total_stats,
                'completion_rate': round(completion_rate, 2),
                'retry_statistics': retry_stats
            }
            
        except Exception as e:
            self.logger.error(f"获取下载进度失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_failed_stocks(self, 
                         min_retry_count: int = 0,
                         limit: int = None) -> Dict[str, Any]:
        """获取下载失败的股票
        
        Args:
            min_retry_count: 最小重试次数
            limit: 返回记录数限制
            
        Returns:
            失败股票列表
        """
        try:
            query = """
            SELECT ds.*, s.name, s.list_date
            FROM download_status ds
            LEFT JOIN stocks s ON ds.ts_code = s.ts_code
            WHERE ds.status = ? AND ds.retry_count >= ?
            ORDER BY ds.retry_count DESC, ds.updated_at DESC
            """
            
            if limit:
                query += f" LIMIT {limit}"
            
            results = self.db_manager.execute_query(
                query, 
                (DownloadStatus.FAILED.value, min_retry_count)
            )
            
            failed_stocks = []
            for record in results:
                failed_stocks.append({
                    'ts_code': record[0],
                    'last_download_date': record[1],
                    'total_records': record[2],
                    'status': record[3],
                    'error_message': record[4],
                    'retry_count': record[5],
                    'updated_at': record[6],
                    'stock_name': record[7],
                    'list_date': record[8]
                })
            
            return {
                'success': True,
                'total_failed': len(failed_stocks),
                'failed_stocks': failed_stocks,
                'min_retry_count': min_retry_count
            }
            
        except Exception as e:
            self.logger.error(f"获取失败股票列表失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def reset_failed_stocks(self, 
                          max_retry_count: int = None,
                          reset_to_status: str = None) -> Dict[str, Any]:
        """重置失败股票状态
        
        Args:
            max_retry_count: 最大重试次数过滤
            reset_to_status: 重置到的状态
            
        Returns:
            重置结果
        """
        try:
            if reset_to_status is None:
                reset_to_status = DownloadStatus.PENDING.value
            
            # 构建更新条件
            where_conditions = ["status = ?"]
            query_params = [DownloadStatus.FAILED.value]
            
            if max_retry_count is not None:
                where_conditions.append("retry_count <= ?")
                query_params.append(max_retry_count)
            
            query_params.extend([reset_to_status])
            
            update_query = f"""
            UPDATE download_status
            SET status = ?, error_message = NULL, retry_count = 0, updated_at = CURRENT_TIMESTAMP
            WHERE {' AND '.join(where_conditions)}
            """
            
            affected_rows = self.db_manager.execute_update(update_query, tuple(query_params))
            
            self.logger.info(f"重置了 {affected_rows} 只失败股票的状态")
            
            return {
                'success': True,
                'reset_count': affected_rows,
                'reset_to_status': reset_to_status,
                'max_retry_count': max_retry_count
            }
            
        except Exception as e:
            self.logger.error(f"重置失败股票状态失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def batch_update_status(self, 
                          updates: List[Dict[str, Any]]) -> Dict[str, Any]:
        """批量更新股票状态
        
        Args:
            updates: 更新列表，每个元素包含ts_code和其他更新字段
            
        Returns:
            批量更新结果
        """
        try:
            success_count = 0
            failed_count = 0
            failed_updates = []
            
            for update in updates:
                ts_code = update.get('ts_code')
                if not ts_code:
                    failed_count += 1
                    failed_updates.append({'update': update, 'error': 'missing ts_code'})
                    continue
                
                # 提取更新字段
                status = update.get('status')
                last_download_date = update.get('last_download_date')
                total_records = update.get('total_records')
                error_message = update.get('error_message')
                increment_retry = update.get('increment_retry', False)
                
                # 执行单个更新
                result = self.update_stock_status(
                    ts_code, status, last_download_date, 
                    total_records, error_message, increment_retry
                )
                
                if result['success']:
                    success_count += 1
                else:
                    failed_count += 1
                    failed_updates.append({
                        'update': update, 
                        'error': result.get('error', 'unknown error')
                    })
            
            self.logger.info(f"批量更新完成: 成功 {success_count}, 失败 {failed_count}")
            
            return {
                'success': failed_count == 0,
                'total_updates': len(updates),
                'success_count': success_count,
                'failed_count': failed_count,
                'failed_updates': failed_updates
            }
            
        except Exception as e:
            self.logger.error(f"批量更新股票状态失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def cleanup_old_status(self, days_old: int = 30) -> Dict[str, Any]:
        """清理旧的状态记录
        
        Args:
            days_old: 清理多少天前的记录
            
        Returns:
            清理结果
        """
        try:
            # 只清理已退市股票的状态记录
            cleanup_query = """
            DELETE FROM download_status
            WHERE ts_code IN (
                SELECT ts_code FROM stocks WHERE list_status != 'L'
            )
            AND updated_at < datetime('now', '-{} days')
            """.format(days_old)
            
            deleted_count = self.db_manager.execute_update(cleanup_query)
            
            self.logger.info(f"清理了 {deleted_count} 条旧状态记录")
            
            return {
                'success': True,
                'deleted_count': deleted_count,
                'days_old': days_old
            }
            
        except Exception as e:
            self.logger.error(f"清理旧状态记录失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }


def main():
    """命令行主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='下载状态管理器')
    parser.add_argument('--init', action='store_true', 
                       help='初始化所有股票下载状态')
    parser.add_argument('--reset', action='store_true',
                       help='重置所有状态')
    parser.add_argument('--progress', action='store_true',
                       help='查看下载进度')
    parser.add_argument('--status', help='查看指定股票状态')
    parser.add_argument('--failed', action='store_true',
                       help='查看失败的股票')
    parser.add_argument('--reset-failed', action='store_true',
                       help='重置失败的股票')
    parser.add_argument('--by-status', 
                       choices=['pending', 'in_progress', 'completed', 'failed', 'partial', 'skipped'],
                       help='按状态查看股票')
    parser.add_argument('--limit', type=int, default=20,
                       help='返回记录数限制')
    
    args = parser.parse_args()
    
    # 创建管理器
    manager = DownloadStatusManager()
    
    if args.init:
        result = manager.initialize_stock_status(reset_existing=args.reset)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    elif args.progress:
        result = manager.get_download_progress()
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    elif args.status:
        result = manager.get_stock_status(args.status)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    elif args.failed:
        result = manager.get_failed_stocks(limit=args.limit)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    elif args.reset_failed:
        result = manager.reset_failed_stocks()
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    elif args.by_status:
        result = manager.get_stocks_by_status(args.by_status, limit=args.limit)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main() 