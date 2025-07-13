#!/usr/bin/env python
"""
数据库模式验证器 - DatabaseSchemaValidator
验证数据库表结构是否符合预期
"""

import sqlite3
import logging
from typing import Dict, List, Any
from database_manager import DatabaseManager


class DatabaseSchemaValidator:
    """数据库模式验证器"""
    
    def __init__(self, db_path: str = "data/stock_data.db"):
        """
        初始化验证器
        
        Args:
            db_path: 数据库文件路径
        """
        self.db_manager = DatabaseManager(db_path)
        self.logger = logging.getLogger(__name__)
        
        # 预期的表结构
        self.expected_tables = {
            'stocks': {
                'columns': [
                    ('ts_code', 'TEXT', 'PRIMARY KEY'),
                    ('symbol', 'TEXT', 'NOT NULL'),
                    ('name', 'TEXT', 'NOT NULL'),
                    ('area', 'TEXT', ''),
                    ('industry', 'TEXT', ''),
                    ('list_date', 'DATE', ''),
                    ('market', 'TEXT', ''),
                    ('exchange', 'TEXT', ''),
                    ('curr_type', 'TEXT', ''),
                    ('list_status', 'TEXT', ''),
                    ('delist_date', 'DATE', ''),
                    ('created_at', 'TIMESTAMP', 'DEFAULT CURRENT_TIMESTAMP'),
                    ('updated_at', 'TIMESTAMP', 'DEFAULT CURRENT_TIMESTAMP'),
                ]
            },
            'daily_data': {
                'columns': [
                    ('id', 'INTEGER', 'PRIMARY KEY AUTOINCREMENT'),
                    ('ts_code', 'TEXT', 'NOT NULL'),
                    ('trade_date', 'DATE', 'NOT NULL'),
                    ('open', 'REAL', ''),
                    ('high', 'REAL', ''),
                    ('low', 'REAL', ''),
                    ('close', 'REAL', ''),
                    ('pre_close', 'REAL', ''),
                    ('change', 'REAL', ''),
                    ('pct_chg', 'REAL', ''),
                    ('vol', 'REAL', ''),
                    ('amount', 'REAL', ''),
                    ('created_at', 'TIMESTAMP', 'DEFAULT CURRENT_TIMESTAMP'),
                ]
            },
            'download_status': {
                'columns': [
                    ('ts_code', 'TEXT', 'PRIMARY KEY'),
                    ('last_download_date', 'DATE', ''),
                    ('total_records', 'INTEGER', 'DEFAULT 0'),
                    ('status', 'TEXT', 'DEFAULT \'pending\''),
                    ('error_message', 'TEXT', ''),
                    ('retry_count', 'INTEGER', 'DEFAULT 0'),
                    ('updated_at', 'TIMESTAMP', 'DEFAULT CURRENT_TIMESTAMP'),
                ]
            },
            'system_config': {
                'columns': [
                    ('key', 'TEXT', 'PRIMARY KEY'),
                    ('value', 'TEXT', ''),
                    ('description', 'TEXT', ''),
                    ('data_type', 'TEXT', 'DEFAULT \'string\''),
                    ('updated_at', 'TIMESTAMP', 'DEFAULT CURRENT_TIMESTAMP'),
                ]
            },
            'api_call_log': {
                'columns': [
                    ('id', 'INTEGER', 'PRIMARY KEY AUTOINCREMENT'),
                    ('api_name', 'TEXT', 'NOT NULL'),
                    ('call_time', 'TIMESTAMP', 'DEFAULT CURRENT_TIMESTAMP'),
                    ('success', 'BOOLEAN', 'DEFAULT 0'),
                    ('response_time', 'INTEGER', ''),
                    ('records_count', 'INTEGER', 'DEFAULT 0'),
                    ('error_message', 'TEXT', ''),
                    ('request_params', 'TEXT', ''),
                ]
            },
            'trade_calendar': {
                'columns': [
                    ('cal_date', 'DATE', 'PRIMARY KEY'),
                    ('is_open', 'BOOLEAN', 'DEFAULT 0'),
                    ('pretrade_date', 'DATE', ''),
                    ('created_at', 'TIMESTAMP', 'DEFAULT CURRENT_TIMESTAMP'),
                ]
            }
        }
        
        # 预期的索引
        self.expected_indexes = [
            'idx_daily_data_ts_code',
            'idx_daily_data_trade_date',
            'idx_daily_data_ts_code_date',
            'idx_api_call_log_api_name',
            'idx_api_call_log_call_time',
            'idx_api_call_log_success',
            'idx_stocks_symbol',
            'idx_stocks_name',
            'idx_stocks_industry',
            'idx_stocks_list_date',
            'idx_download_status_status',
            'idx_download_status_last_download_date',
            'idx_trade_calendar_is_open',
        ]
        
        # 预期的视图
        self.expected_views = [
            'v_active_stocks',
            'v_latest_daily_data',
            'v_download_progress',
        ]
        
        # 预期的触发器
        self.expected_triggers = [
            'update_stocks_timestamp',
            'update_download_status_timestamp',
            'update_system_config_timestamp',
        ]
    
    def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """
        获取表的结构信息
        
        Args:
            table_name: 表名
            
        Returns:
            List[Dict[str, Any]]: 表结构信息
        """
        try:
            query = f"PRAGMA table_info({table_name})"
            result = self.db_manager.execute_query(query)
            
            columns = []
            for row in result:
                columns.append({
                    'name': row['name'],
                    'type': row['type'],
                    'notnull': row['notnull'],
                    'default_value': row['dflt_value'],
                    'pk': row['pk']
                })
            
            return columns
            
        except sqlite3.Error as e:
            self.logger.error(f"获取表结构失败: {e}")
            return []
    
    def get_indexes(self) -> List[str]:
        """
        获取所有索引名称
        
        Returns:
            List[str]: 索引名称列表
        """
        try:
            query = "SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'"
            result = self.db_manager.execute_query(query)
            return [row['name'] for row in result]
            
        except sqlite3.Error as e:
            self.logger.error(f"获取索引失败: {e}")
            return []
    
    def get_views(self) -> List[str]:
        """
        获取所有视图名称
        
        Returns:
            List[str]: 视图名称列表
        """
        try:
            query = "SELECT name FROM sqlite_master WHERE type='view'"
            result = self.db_manager.execute_query(query)
            return [row['name'] for row in result]
            
        except sqlite3.Error as e:
            self.logger.error(f"获取视图失败: {e}")
            return []
    
    def get_triggers(self) -> List[str]:
        """
        获取所有触发器名称
        
        Returns:
            List[str]: 触发器名称列表
        """
        try:
            query = "SELECT name FROM sqlite_master WHERE type='trigger'"
            result = self.db_manager.execute_query(query)
            return [row['name'] for row in result]
            
        except sqlite3.Error as e:
            self.logger.error(f"获取触发器失败: {e}")
            return []
    
    def validate_table_structure(self, table_name: str) -> Dict[str, Any]:
        """
        验证表结构
        
        Args:
            table_name: 表名
            
        Returns:
            Dict[str, Any]: 验证结果
        """
        result = {
            'table_name': table_name,
            'exists': False,
            'columns_correct': True,
            'missing_columns': [],
            'extra_columns': [],
            'incorrect_types': [],
            'issues': []
        }
        
        try:
            # 检查表是否存在
            tables_query = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
            tables = self.db_manager.execute_query(tables_query, (table_name,))
            
            if not tables:
                result['exists'] = False
                result['issues'].append(f"表 {table_name} 不存在")
                return result
            
            result['exists'] = True
            
            # 获取实际表结构
            actual_columns = self.get_table_schema(table_name)
            actual_column_names = [col['name'] for col in actual_columns]
            
            # 获取预期表结构
            expected_columns = self.expected_tables[table_name]['columns']
            expected_column_names = [col[0] for col in expected_columns]
            
            # 检查缺失的列
            missing_columns = set(expected_column_names) - set(actual_column_names)
            if missing_columns:
                result['missing_columns'] = list(missing_columns)
                result['columns_correct'] = False
                result['issues'].append(f"缺失列: {', '.join(missing_columns)}")
            
            # 检查多余的列
            extra_columns = set(actual_column_names) - set(expected_column_names)
            if extra_columns:
                result['extra_columns'] = list(extra_columns)
                result['issues'].append(f"多余列: {', '.join(extra_columns)}")
            
            # 检查列类型
            actual_types = {col['name']: col['type'] for col in actual_columns}
            expected_types = {col[0]: col[1] for col in expected_columns}
            
            for col_name in expected_column_names:
                if col_name in actual_types:
                    if actual_types[col_name] != expected_types[col_name]:
                        result['incorrect_types'].append({
                            'column': col_name,
                            'expected': expected_types[col_name],
                            'actual': actual_types[col_name]
                        })
                        result['columns_correct'] = False
                        result['issues'].append(
                            f"列 {col_name} 类型不匹配: 期望 {expected_types[col_name]}, 实际 {actual_types[col_name]}"
                        )
            
            return result
            
        except Exception as e:
            result['issues'].append(f"验证异常: {e}")
            return result
    
    def validate_all_tables(self) -> Dict[str, Any]:
        """
        验证所有表的结构
        
        Returns:
            Dict[str, Any]: 验证结果
        """
        results = {
            'overall_success': True,
            'tables': {},
            'summary': {
                'total_tables': len(self.expected_tables),
                'passed_tables': 0,
                'failed_tables': 0,
                'total_issues': 0
            }
        }
        
        for table_name in self.expected_tables:
            table_result = self.validate_table_structure(table_name)
            results['tables'][table_name] = table_result
            
            if table_result['exists'] and table_result['columns_correct'] and not table_result['issues']:
                results['summary']['passed_tables'] += 1
            else:
                results['summary']['failed_tables'] += 1
                results['overall_success'] = False
                results['summary']['total_issues'] += len(table_result['issues'])
        
        return results
    
    def validate_indexes(self) -> Dict[str, Any]:
        """
        验证索引
        
        Returns:
            Dict[str, Any]: 验证结果
        """
        actual_indexes = self.get_indexes()
        missing_indexes = set(self.expected_indexes) - set(actual_indexes)
        extra_indexes = set(actual_indexes) - set(self.expected_indexes)
        
        return {
            'success': len(missing_indexes) == 0,
            'total_expected': len(self.expected_indexes),
            'total_actual': len(actual_indexes),
            'missing_indexes': list(missing_indexes),
            'extra_indexes': list(extra_indexes),
            'issues': len(missing_indexes) + len(extra_indexes)
        }
    
    def validate_views(self) -> Dict[str, Any]:
        """
        验证视图
        
        Returns:
            Dict[str, Any]: 验证结果
        """
        actual_views = self.get_views()
        missing_views = set(self.expected_views) - set(actual_views)
        extra_views = set(actual_views) - set(self.expected_views)
        
        return {
            'success': len(missing_views) == 0,
            'total_expected': len(self.expected_views),
            'total_actual': len(actual_views),
            'missing_views': list(missing_views),
            'extra_views': list(extra_views),
            'issues': len(missing_views) + len(extra_views)
        }
    
    def validate_triggers(self) -> Dict[str, Any]:
        """
        验证触发器
        
        Returns:
            Dict[str, Any]: 验证结果
        """
        actual_triggers = self.get_triggers()
        missing_triggers = set(self.expected_triggers) - set(actual_triggers)
        extra_triggers = set(actual_triggers) - set(self.expected_triggers)
        
        return {
            'success': len(missing_triggers) == 0,
            'total_expected': len(self.expected_triggers),
            'total_actual': len(actual_triggers),
            'missing_triggers': list(missing_triggers),
            'extra_triggers': list(extra_triggers),
            'issues': len(missing_triggers) + len(extra_triggers)
        }
    
    def validate_all(self) -> Dict[str, Any]:
        """
        执行完整的数据库验证
        
        Returns:
            Dict[str, Any]: 完整验证结果
        """
        results = {
            'overall_success': True,
            'tables': self.validate_all_tables(),
            'indexes': self.validate_indexes(),
            'views': self.validate_views(),
            'triggers': self.validate_triggers(),
            'summary': {
                'total_components': 0,
                'passed_components': 0,
                'failed_components': 0,
                'total_issues': 0
            }
        }
        
        # 统计汇总
        components = [
            ('tables', results['tables']['overall_success']),
            ('indexes', results['indexes']['success']),
            ('views', results['views']['success']),
            ('triggers', results['triggers']['success'])
        ]
        
        for component_name, success in components:
            results['summary']['total_components'] += 1
            if success:
                results['summary']['passed_components'] += 1
            else:
                results['summary']['failed_components'] += 1
                results['overall_success'] = False
        
        # 计算总问题数
        results['summary']['total_issues'] = (
            results['tables']['summary']['total_issues'] +
            results['indexes']['issues'] +
            results['views']['issues'] +
            results['triggers']['issues']
        )
        
        return results
    
    def print_validation_report(self, results: Dict[str, Any]):
        """
        打印验证报告
        
        Args:
            results: 验证结果
        """
        print("=" * 60)
        print("数据库模式验证报告")
        print("=" * 60)
        
        if results['overall_success']:
            print("✅ 数据库模式验证通过")
        else:
            print("❌ 数据库模式验证失败")
        
        print(f"\n📊 总体统计:")
        print(f"  组件总数: {results['summary']['total_components']}")
        print(f"  通过组件: {results['summary']['passed_components']}")
        print(f"  失败组件: {results['summary']['failed_components']}")
        print(f"  问题总数: {results['summary']['total_issues']}")
        
        # 表结构验证结果
        print(f"\n🗂️  表结构验证:")
        tables_result = results['tables']
        print(f"  表总数: {tables_result['summary']['total_tables']}")
        print(f"  通过表: {tables_result['summary']['passed_tables']}")
        print(f"  失败表: {tables_result['summary']['failed_tables']}")
        
        for table_name, table_result in tables_result['tables'].items():
            if table_result['exists'] and table_result['columns_correct'] and not table_result['issues']:
                print(f"    {table_name}: ✅")
            else:
                print(f"    {table_name}: ❌")
                for issue in table_result['issues']:
                    print(f"      - {issue}")
        
        # 索引验证结果
        print(f"\n📇 索引验证:")
        indexes_result = results['indexes']
        if indexes_result['success']:
            print(f"  ✅ 所有索引正常 ({indexes_result['total_expected']} 个)")
        else:
            print(f"  ❌ 索引验证失败")
            if indexes_result['missing_indexes']:
                print(f"    缺失索引: {', '.join(indexes_result['missing_indexes'])}")
            if indexes_result['extra_indexes']:
                print(f"    多余索引: {', '.join(indexes_result['extra_indexes'])}")
        
        # 视图验证结果
        print(f"\n👁️  视图验证:")
        views_result = results['views']
        if views_result['success']:
            print(f"  ✅ 所有视图正常 ({views_result['total_expected']} 个)")
        else:
            print(f"  ❌ 视图验证失败")
            if views_result['missing_views']:
                print(f"    缺失视图: {', '.join(views_result['missing_views'])}")
            if views_result['extra_views']:
                print(f"    多余视图: {', '.join(views_result['extra_views'])}")
        
        # 触发器验证结果
        print(f"\n⚡ 触发器验证:")
        triggers_result = results['triggers']
        if triggers_result['success']:
            print(f"  ✅ 所有触发器正常 ({triggers_result['total_expected']} 个)")
        else:
            print(f"  ❌ 触发器验证失败")
            if triggers_result['missing_triggers']:
                print(f"    缺失触发器: {', '.join(triggers_result['missing_triggers'])}")
            if triggers_result['extra_triggers']:
                print(f"    多余触发器: {', '.join(triggers_result['extra_triggers'])}")
        
        print("=" * 60)


def main():
    """命令行主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='数据库模式验证器')
    parser.add_argument('--db-path', default='data/stock_data.db', help='数据库文件路径')
    
    args = parser.parse_args()
    
    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    validator = DatabaseSchemaValidator(args.db_path)
    
    try:
        validator.db_manager.connect()
        results = validator.validate_all()
        validator.print_validation_report(results)
        
        if not results['overall_success']:
            exit(1)
            
    finally:
        validator.db_manager.disconnect()


if __name__ == "__main__":
    main() 