#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据完整性管理器 - 负责数据去重和完整性检查
"""

import sqlite3
import pandas as pd
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime, timedelta
import json
import logging
from pathlib import Path

from .database_manager import DatabaseManager
from .config_manager import ConfigManager


class DataIntegrityManager:
    """数据完整性管理器
    
    负责数据去重、完整性检查和修复功能
    """
    
    def __init__(self, config_manager: ConfigManager = None):
        """初始化数据完整性管理器
        
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
    
    def check_duplicate_records(self, table_name: str) -> Dict[str, Any]:
        """检查重复记录
        
        Args:
            table_name: 表名
            
        Returns:
            检查结果字典
        """
        try:
            if table_name == 'daily_data':
                # 检查日线数据表的重复记录
                return self._check_daily_data_duplicates()
            elif table_name == 'stocks':
                # 检查股票基本信息表的重复记录
                return self._check_stocks_duplicates()
            else:
                raise ValueError(f"不支持的表名: {table_name}")
                
        except Exception as e:
            self.logger.error(f"检查重复记录失败: {e}")
            return {
                'success': False,
                'error': str(e),
                'duplicates': []
            }
    
    def _check_daily_data_duplicates(self) -> Dict[str, Any]:
        """检查日线数据表的重复记录"""
        query = """
        SELECT ts_code, trade_date, COUNT(*) as count
        FROM daily_data
        GROUP BY ts_code, trade_date
        HAVING COUNT(*) > 1
        ORDER BY ts_code, trade_date
        """
        
        try:
            duplicates = self.db_manager.execute_query(query)
            
            # 获取具体的重复记录详情
            duplicate_details = []
            for row in duplicates:
                ts_code, trade_date, count = row
                detail_query = """
                SELECT id, ts_code, trade_date, open, high, low, close, 
                       vol, amount, created_at
                FROM daily_data
                WHERE ts_code = ? AND trade_date = ?
                ORDER BY id
                """
                records = self.db_manager.execute_query(detail_query, (ts_code, trade_date))
                duplicate_details.append({
                    'ts_code': ts_code,
                    'trade_date': trade_date,
                    'count': count,
                    'records': records
                })
            
            return {
                'success': True,
                'table': 'daily_data',
                'total_duplicates': len(duplicates),
                'duplicates': duplicate_details
            }
            
        except Exception as e:
            self.logger.error(f"检查日线数据重复记录失败: {e}")
            return {
                'success': False,
                'error': str(e),
                'duplicates': []
            }
    
    def _check_stocks_duplicates(self) -> Dict[str, Any]:
        """检查股票基本信息表的重复记录"""
        # 检查ts_code重复
        query = """
        SELECT ts_code, COUNT(*) as count
        FROM stocks
        GROUP BY ts_code
        HAVING COUNT(*) > 1
        """
        
        try:
            duplicates = self.db_manager.execute_query(query)
            
            duplicate_details = []
            for row in duplicates:
                ts_code, count = row
                detail_query = """
                SELECT ts_code, symbol, name, area, industry, list_date, 
                       market, created_at, updated_at
                FROM stocks
                WHERE ts_code = ?
                ORDER BY created_at
                """
                records = self.db_manager.execute_query(detail_query, (ts_code,))
                duplicate_details.append({
                    'ts_code': ts_code,
                    'count': count,
                    'records': records
                })
            
            return {
                'success': True,
                'table': 'stocks',
                'total_duplicates': len(duplicates),
                'duplicates': duplicate_details
            }
            
        except Exception as e:
            self.logger.error(f"检查股票信息重复记录失败: {e}")
            return {
                'success': False,
                'error': str(e),
                'duplicates': []
            }
    
    def remove_duplicate_records(self, table_name: str, 
                               strategy: str = 'keep_latest') -> Dict[str, Any]:
        """移除重复记录
        
        Args:
            table_name: 表名
            strategy: 去重策略 ('keep_latest', 'keep_first', 'manual')
            
        Returns:
            操作结果
        """
        try:
            if table_name == 'daily_data':
                return self._remove_daily_data_duplicates(strategy)
            elif table_name == 'stocks':
                return self._remove_stocks_duplicates(strategy)
            else:
                raise ValueError(f"不支持的表名: {table_name}")
                
        except Exception as e:
            self.logger.error(f"移除重复记录失败: {e}")
            return {
                'success': False,
                'error': str(e),
                'removed_count': 0
            }
    
    def _remove_daily_data_duplicates(self, strategy: str) -> Dict[str, Any]:
        """移除日线数据重复记录"""
        if strategy == 'keep_latest':
            # 保留最新插入的记录（ID最大）
            query = """
            DELETE FROM daily_data
            WHERE id NOT IN (
                SELECT MAX(id)
                FROM daily_data
                GROUP BY ts_code, trade_date
            )
            """
        elif strategy == 'keep_first':
            # 保留最早插入的记录（ID最小）
            query = """
            DELETE FROM daily_data
            WHERE id NOT IN (
                SELECT MIN(id)
                FROM daily_data
                GROUP BY ts_code, trade_date
            )
            """
        else:
            raise ValueError(f"不支持的去重策略: {strategy}")
        
        try:
            # 先检查将要删除的记录数
            count_query = """
            SELECT COUNT(*)
            FROM daily_data
            WHERE id NOT IN (
                SELECT MAX(id)
                FROM daily_data
                GROUP BY ts_code, trade_date
            )
            """ if strategy == 'keep_latest' else """
            SELECT COUNT(*)
            FROM daily_data
            WHERE id NOT IN (
                SELECT MIN(id)
                FROM daily_data
                GROUP BY ts_code, trade_date
            )
            """
            
            result = self.db_manager.execute_query(count_query)
            duplicate_count = result[0][0] if result else 0
            
            if duplicate_count > 0:
                # 执行删除
                self.db_manager.execute_update(query)
                
                self.logger.info(f"移除了 {duplicate_count} 条重复的日线数据记录")
                
                return {
                    'success': True,
                    'table': 'daily_data',
                    'strategy': strategy,
                    'removed_count': duplicate_count
                }
            else:
                return {
                    'success': True,
                    'table': 'daily_data',
                    'strategy': strategy,
                    'removed_count': 0,
                    'message': '没有发现重复记录'
                }
                
        except Exception as e:
            self.logger.error(f"移除日线数据重复记录失败: {e}")
            return {
                'success': False,
                'error': str(e),
                'removed_count': 0
            }
    
    def _remove_stocks_duplicates(self, strategy: str) -> Dict[str, Any]:
        """移除股票信息重复记录"""
        if strategy == 'keep_latest':
            # 保留最新更新的记录
            query = """
            DELETE FROM stocks
            WHERE rowid NOT IN (
                SELECT MAX(rowid)
                FROM stocks
                GROUP BY ts_code
            )
            """
        elif strategy == 'keep_first':
            # 保留最早创建的记录
            query = """
            DELETE FROM stocks
            WHERE rowid NOT IN (
                SELECT MIN(rowid)
                FROM stocks
                GROUP BY ts_code
            )
            """
        else:
            raise ValueError(f"不支持的去重策略: {strategy}")
        
        try:
            # 先检查将要删除的记录数
            count_query = """
            SELECT COUNT(*)
            FROM stocks
            WHERE rowid NOT IN (
                SELECT MAX(rowid)
                FROM stocks
                GROUP BY ts_code
            )
            """ if strategy == 'keep_latest' else """
            SELECT COUNT(*)
            FROM stocks
            WHERE rowid NOT IN (
                SELECT MIN(rowid)
                FROM stocks
                GROUP BY ts_code
            )
            """
            
            result = self.db_manager.execute_query(count_query)
            duplicate_count = result[0][0] if result else 0
            
            if duplicate_count > 0:
                # 执行删除
                self.db_manager.execute_update(query)
                
                self.logger.info(f"移除了 {duplicate_count} 条重复的股票信息记录")
                
                return {
                    'success': True,
                    'table': 'stocks',
                    'strategy': strategy,
                    'removed_count': duplicate_count
                }
            else:
                return {
                    'success': True,
                    'table': 'stocks',
                    'strategy': strategy,
                    'removed_count': 0,
                    'message': '没有发现重复记录'
                }
                
        except Exception as e:
            self.logger.error(f"移除股票信息重复记录失败: {e}")
            return {
                'success': False,
                'error': str(e),
                'removed_count': 0
            }
    
    def check_data_integrity(self) -> Dict[str, Any]:
        """检查数据完整性
        
        Returns:
            完整性检查结果
        """
        try:
            integrity_results = {}
            
            # 1. 检查股票基本信息表完整性
            integrity_results['stocks'] = self._check_stocks_integrity()
            
            # 2. 检查日线数据表完整性
            integrity_results['daily_data'] = self._check_daily_data_integrity()
            
            # 3. 检查表之间的关联完整性
            integrity_results['relationships'] = self._check_relationship_integrity()
            
            # 4. 检查数据逻辑一致性
            integrity_results['logical_consistency'] = self._check_logical_consistency()
            
            return {
                'success': True,
                'check_time': datetime.now().isoformat(),
                'results': integrity_results
            }
            
        except Exception as e:
            self.logger.error(f"数据完整性检查失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def _check_stocks_integrity(self) -> Dict[str, Any]:
        """检查股票基本信息表完整性"""
        results = {}
        
        try:
            # 检查必填字段是否为空
            null_checks = [
                ('ts_code', 'SELECT COUNT(*) FROM stocks WHERE ts_code IS NULL OR ts_code = ""'),
                ('symbol', 'SELECT COUNT(*) FROM stocks WHERE symbol IS NULL OR symbol = ""'),
                ('name', 'SELECT COUNT(*) FROM stocks WHERE name IS NULL OR name = ""'),
                ('list_date', 'SELECT COUNT(*) FROM stocks WHERE list_date IS NULL OR list_date = ""')
            ]
            
            results['null_fields'] = {}
            for field, query in null_checks:
                result = self.db_manager.execute_query(query)
                count = result[0][0] if result else 0
                results['null_fields'][field] = count
            
            # 检查日期格式
            date_format_query = """
            SELECT COUNT(*) FROM stocks 
            WHERE list_date IS NOT NULL 
            AND list_date != ""
            AND NOT (
                list_date GLOB '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'
                OR list_date GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
            )
            """
            result = self.db_manager.execute_query(date_format_query)
            results['invalid_date_format'] = result[0][0] if result else 0
            
            # 检查股票代码格式
            code_format_query = """
            SELECT COUNT(*) FROM stocks 
            WHERE ts_code IS NOT NULL 
            AND ts_code NOT GLOB '[0-9][0-9][0-9][0-9][0-9][0-9].[SZ|SH]'
            """
            result = self.db_manager.execute_query(code_format_query)
            results['invalid_code_format'] = result[0][0] if result else 0
            
            # 总记录数
            total_query = "SELECT COUNT(*) FROM stocks"
            result = self.db_manager.execute_query(total_query)
            results['total_records'] = result[0][0] if result else 0
            
            return results
            
        except Exception as e:
            self.logger.error(f"检查股票信息完整性失败: {e}")
            return {'error': str(e)}
    
    def _check_daily_data_integrity(self) -> Dict[str, Any]:
        """检查日线数据表完整性"""
        results = {}
        
        try:
            # 检查必填字段是否为空
            null_checks = [
                ('ts_code', 'SELECT COUNT(*) FROM daily_data WHERE ts_code IS NULL OR ts_code = ""'),
                ('trade_date', 'SELECT COUNT(*) FROM daily_data WHERE trade_date IS NULL OR trade_date = ""'),
                ('open', 'SELECT COUNT(*) FROM daily_data WHERE open IS NULL'),
                ('high', 'SELECT COUNT(*) FROM daily_data WHERE high IS NULL'),
                ('low', 'SELECT COUNT(*) FROM daily_data WHERE low IS NULL'),
                ('close', 'SELECT COUNT(*) FROM daily_data WHERE close IS NULL')
            ]
            
            results['null_fields'] = {}
            for field, query in null_checks:
                result = self.db_manager.execute_query(query)
                count = result[0][0] if result else 0
                results['null_fields'][field] = count
            
            # 检查价格数据逻辑性
            price_logic_checks = [
                ('negative_prices', 'SELECT COUNT(*) FROM daily_data WHERE open < 0 OR high < 0 OR low < 0 OR close < 0'),
                ('invalid_high_low', 'SELECT COUNT(*) FROM daily_data WHERE high < low'),
                ('invalid_open_range', 'SELECT COUNT(*) FROM daily_data WHERE open < low OR open > high'),
                ('invalid_close_range', 'SELECT COUNT(*) FROM daily_data WHERE close < low OR close > high')
            ]
            
            results['price_logic_errors'] = {}
            for check_name, query in price_logic_checks:
                result = self.db_manager.execute_query(query)
                count = result[0][0] if result else 0
                results['price_logic_errors'][check_name] = count
            
            # 检查交易量数据
            volume_checks = [
                ('negative_volume', 'SELECT COUNT(*) FROM daily_data WHERE vol < 0'),
                ('negative_amount', 'SELECT COUNT(*) FROM daily_data WHERE amount < 0'),
                ('zero_volume_with_price', 'SELECT COUNT(*) FROM daily_data WHERE vol = 0 AND (open != close OR high != low)')
            ]
            
            results['volume_errors'] = {}
            for check_name, query in volume_checks:
                result = self.db_manager.execute_query(query)
                count = result[0][0] if result else 0
                results['volume_errors'][check_name] = count
            
            # 总记录数
            total_query = "SELECT COUNT(*) FROM daily_data"
            result = self.db_manager.execute_query(total_query)
            results['total_records'] = result[0][0] if result else 0
            
            return results
            
        except Exception as e:
            self.logger.error(f"检查日线数据完整性失败: {e}")
            return {'error': str(e)}
    
    def _check_relationship_integrity(self) -> Dict[str, Any]:
        """检查表之间的关联完整性"""
        results = {}
        
        try:
            # 检查日线数据表中的股票代码是否都存在于股票基本信息表中
            orphan_query = """
            SELECT COUNT(DISTINCT ts_code)
            FROM daily_data
            WHERE ts_code NOT IN (SELECT ts_code FROM stocks)
            """
            result = self.db_manager.execute_query(orphan_query)
            results['orphan_daily_data'] = result[0][0] if result else 0
            
            # 检查股票基本信息表中哪些股票没有日线数据
            missing_data_query = """
            SELECT COUNT(*)
            FROM stocks
            WHERE ts_code NOT IN (SELECT DISTINCT ts_code FROM daily_data)
            """
            result = self.db_manager.execute_query(missing_data_query)
            results['stocks_without_data'] = result[0][0] if result else 0
            
            # 检查下载状态表与股票基本信息表的关联
            status_orphan_query = """
            SELECT COUNT(*)
            FROM download_status
            WHERE ts_code NOT IN (SELECT ts_code FROM stocks)
            """
            result = self.db_manager.execute_query(status_orphan_query)
            results['orphan_download_status'] = result[0][0] if result else 0
            
            return results
            
        except Exception as e:
            self.logger.error(f"检查关联完整性失败: {e}")
            return {'error': str(e)}
    
    def _check_logical_consistency(self) -> Dict[str, Any]:
        """检查数据逻辑一致性"""
        results = {}
        
        try:
            # 检查日线数据的日期是否早于股票上市日期
            early_data_query = """
            SELECT COUNT(*)
            FROM daily_data dd
            JOIN stocks s ON dd.ts_code = s.ts_code
            WHERE dd.trade_date < s.list_date
            """
            result = self.db_manager.execute_query(early_data_query)
            results['data_before_listing'] = result[0][0] if result else 0
            
            # 检查未来日期的数据
            future_data_query = """
            SELECT COUNT(*)
            FROM daily_data
            WHERE trade_date > date('now')
            """
            result = self.db_manager.execute_query(future_data_query)
            results['future_data'] = result[0][0] if result else 0
            
            # 检查非工作日的数据（简单检查周六周日）
            weekend_query = """
            SELECT COUNT(*)
            FROM daily_data
            WHERE strftime('%w', trade_date) IN ('0', '6')
            """
            result = self.db_manager.execute_query(weekend_query)
            results['weekend_data'] = result[0][0] if result else 0
            
            return results
            
        except Exception as e:
            self.logger.error(f"检查逻辑一致性失败: {e}")
            return {'error': str(e)}
    
    def repair_data_integrity(self, repair_types: List[str] = None) -> Dict[str, Any]:
        """修复数据完整性问题
        
        Args:
            repair_types: 要修复的问题类型列表
            
        Returns:
            修复结果
        """
        if repair_types is None:
            repair_types = ['null_values', 'invalid_prices', 'orphan_records']
        
        repair_results = {}
        
        try:
            for repair_type in repair_types:
                if repair_type == 'null_values':
                    repair_results['null_values'] = self._repair_null_values()
                elif repair_type == 'invalid_prices':
                    repair_results['invalid_prices'] = self._repair_invalid_prices()
                elif repair_type == 'orphan_records':
                    repair_results['orphan_records'] = self._repair_orphan_records()
                else:
                    repair_results[repair_type] = {
                        'success': False,
                        'error': f'不支持的修复类型: {repair_type}'
                    }
            
            return {
                'success': True,
                'repair_time': datetime.now().isoformat(),
                'results': repair_results
            }
            
        except Exception as e:
            self.logger.error(f"数据完整性修复失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def _repair_null_values(self) -> Dict[str, Any]:
        """修复空值问题"""
        try:
            # 删除关键字段为空的记录
            queries = [
                "DELETE FROM daily_data WHERE ts_code IS NULL OR ts_code = ''",
                "DELETE FROM daily_data WHERE trade_date IS NULL OR trade_date = ''",
                "DELETE FROM stocks WHERE ts_code IS NULL OR ts_code = ''"
            ]
            
            total_deleted = 0
            for query in queries:
                deleted = self.db_manager.execute_update(query)
                total_deleted += deleted
            
            return {
                'success': True,
                'deleted_records': total_deleted
            }
            
        except Exception as e:
            self.logger.error(f"修复空值失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def _repair_invalid_prices(self) -> Dict[str, Any]:
        """修复无效价格数据"""
        try:
            # 删除负价格记录
            query = """
            DELETE FROM daily_data 
            WHERE open < 0 OR high < 0 OR low < 0 OR close < 0
            """
            
            deleted_negative = self.db_manager.execute_update(query)
            
            # 删除逻辑错误的价格记录
            query = """
            DELETE FROM daily_data 
            WHERE high < low OR open < low OR open > high OR close < low OR close > high
            """
            
            deleted_logic = self.db_manager.execute_update(query)
            
            return {
                'success': True,
                'deleted_negative_prices': deleted_negative,
                'deleted_logic_errors': deleted_logic,
                'total_deleted': deleted_negative + deleted_logic
            }
            
        except Exception as e:
            self.logger.error(f"修复无效价格失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def _repair_orphan_records(self) -> Dict[str, Any]:
        """修复孤立记录"""
        try:
            # 删除日线数据表中不存在对应股票基本信息的记录
            query = """
            DELETE FROM daily_data
            WHERE ts_code NOT IN (SELECT ts_code FROM stocks)
            """
            
            deleted_orphan_daily = self.db_manager.execute_update(query)
            
            # 删除下载状态表中不存在对应股票基本信息的记录
            query = """
            DELETE FROM download_status
            WHERE ts_code NOT IN (SELECT ts_code FROM stocks)
            """
            
            deleted_orphan_status = self.db_manager.execute_update(query)
            
            return {
                'success': True,
                'deleted_orphan_daily': deleted_orphan_daily,
                'deleted_orphan_status': deleted_orphan_status,
                'total_deleted': deleted_orphan_daily + deleted_orphan_status
            }
            
        except Exception as e:
            self.logger.error(f"修复孤立记录失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def generate_integrity_report(self, output_file: str = None) -> Dict[str, Any]:
        """生成完整性检查报告
        
        Args:
            output_file: 输出文件路径
            
        Returns:
            报告生成结果
        """
        try:
            # 执行完整性检查
            integrity_results = self.check_data_integrity()
            
            if not integrity_results['success']:
                return integrity_results
            
            # 检查重复记录
            duplicate_results = {}
            for table in ['daily_data', 'stocks']:
                duplicate_results[table] = self.check_duplicate_records(table)
            
            # 生成报告
            report = {
                'report_time': datetime.now().isoformat(),
                'integrity_check': integrity_results['results'],
                'duplicate_check': duplicate_results,
                'summary': self._generate_summary(integrity_results['results'], duplicate_results)
            }
            
            # 保存报告到文件
            if output_file:
                output_path = Path(output_file)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(report, f, ensure_ascii=False, indent=2)
                
                self.logger.info(f"完整性检查报告已保存到: {output_path}")
            
            return {
                'success': True,
                'report': report,
                'output_file': output_file
            }
            
        except Exception as e:
            self.logger.error(f"生成完整性报告失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def _generate_summary(self, integrity_results: Dict, duplicate_results: Dict) -> Dict[str, Any]:
        """生成检查结果摘要"""
        summary = {
            'total_issues': 0,
            'critical_issues': 0,
            'warnings': 0,
            'recommendations': []
        }
        
        # 统计股票信息问题
        stocks_issues = integrity_results.get('stocks', {})
        if 'null_fields' in stocks_issues:
            for field, count in stocks_issues['null_fields'].items():
                if count > 0:
                    summary['total_issues'] += count
                    if field in ['ts_code', 'symbol']:
                        summary['critical_issues'] += count
                    else:
                        summary['warnings'] += count
        
        # 统计日线数据问题
        daily_issues = integrity_results.get('daily_data', {})
        if 'null_fields' in daily_issues:
            for field, count in daily_issues['null_fields'].items():
                if count > 0:
                    summary['total_issues'] += count
                    if field in ['ts_code', 'trade_date']:
                        summary['critical_issues'] += count
                    else:
                        summary['warnings'] += count
        
        # 统计价格逻辑错误
        if 'price_logic_errors' in daily_issues:
            for error_type, count in daily_issues['price_logic_errors'].items():
                if count > 0:
                    summary['total_issues'] += count
                    summary['critical_issues'] += count
        
        # 统计关联完整性问题
        relationship_issues = integrity_results.get('relationships', {})
        for issue_type, count in relationship_issues.items():
            if count > 0:
                summary['total_issues'] += count
                if issue_type == 'orphan_daily_data':
                    summary['critical_issues'] += count
                else:
                    summary['warnings'] += count
        
        # 统计重复记录
        for table, result in duplicate_results.items():
            if result.get('success') and result.get('total_duplicates', 0) > 0:
                summary['total_issues'] += result['total_duplicates']
                summary['warnings'] += result['total_duplicates']
        
        # 生成建议
        if summary['critical_issues'] > 0:
            summary['recommendations'].append('立即修复关键问题，如空值和无效价格数据')
        
        if summary['warnings'] > 0:
            summary['recommendations'].append('考虑修复警告级别的问题以提高数据质量')
        
        if any(result.get('total_duplicates', 0) > 0 for result in duplicate_results.values()):
            summary['recommendations'].append('使用去重功能清理重复记录')
        
        if summary['total_issues'] == 0:
            summary['recommendations'].append('数据完整性良好，无需修复')
        
        return summary


def main():
    """命令行主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='数据完整性管理器')
    parser.add_argument('--check-duplicates', choices=['daily_data', 'stocks'], 
                       help='检查指定表的重复记录')
    parser.add_argument('--remove-duplicates', choices=['daily_data', 'stocks'], 
                       help='移除指定表的重复记录')
    parser.add_argument('--strategy', choices=['keep_latest', 'keep_first'], 
                       default='keep_latest', help='去重策略')
    parser.add_argument('--check-integrity', action='store_true', 
                       help='检查数据完整性')
    parser.add_argument('--repair-data', nargs='*', 
                       choices=['null_values', 'invalid_prices', 'orphan_records'],
                       help='修复数据完整性问题')
    parser.add_argument('--generate-report', help='生成完整性报告文件路径')
    
    args = parser.parse_args()
    
    # 创建管理器
    manager = DataIntegrityManager()
    
    if args.check_duplicates:
        result = manager.check_duplicate_records(args.check_duplicates)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    elif args.remove_duplicates:
        result = manager.remove_duplicate_records(args.remove_duplicates, args.strategy)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    elif args.check_integrity:
        result = manager.check_data_integrity()
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    elif args.repair_data is not None:
        result = manager.repair_data_integrity(args.repair_data)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    elif args.generate_report:
        result = manager.generate_integrity_report(args.generate_report)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main() 