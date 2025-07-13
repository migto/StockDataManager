#!/usr/bin/env python
"""
数据库管理器 - DatabaseManager
负责SQLite数据库的连接、初始化和基础操作
"""

import sqlite3
import logging
import os
import sys
from pathlib import Path
from typing import Optional, List, Dict, Any, Union
from datetime import datetime
import json


class DatabaseManager:
    """数据库管理器类"""
    
    def __init__(self, db_path: str = "data/stock_data.db"):
        """
        初始化数据库管理器
        
        Args:
            db_path: 数据库文件路径
        """
        self.db_path = db_path
        self.connection: Optional[sqlite3.Connection] = None
        self.logger = logging.getLogger(__name__)
        
        # 确保数据目录存在
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        # 设置日志
        self._setup_logging()
    
    def _setup_logging(self):
        """设置日志配置"""
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
    
    def connect(self) -> sqlite3.Connection:
        """
        创建数据库连接
        
        Returns:
            sqlite3.Connection: 数据库连接对象
        """
        try:
            self.connection = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                timeout=30.0
            )
            
            # 设置行工厂，使查询结果返回字典
            self.connection.row_factory = sqlite3.Row
            
            # 启用外键约束
            self.connection.execute("PRAGMA foreign_keys = ON")
            
            # 设置WAL模式以提高并发性能
            self.connection.execute("PRAGMA journal_mode = WAL")
            
            self.logger.info(f"数据库连接成功: {self.db_path}")
            return self.connection
            
        except sqlite3.Error as e:
            self.logger.error(f"数据库连接失败: {e}")
            raise
    
    def disconnect(self):
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()
            self.connection = None
            self.logger.info("数据库连接已关闭")
    
    def __enter__(self):
        """上下文管理器入口"""
        return self.connect()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.disconnect()
    
    def execute_script(self, script_path: str) -> bool:
        """
        执行SQL脚本文件
        
        Args:
            script_path: SQL脚本文件路径
            
        Returns:
            bool: 执行是否成功
        """
        try:
            if not os.path.exists(script_path):
                self.logger.error(f"SQL脚本文件不存在: {script_path}")
                return False
            
            with open(script_path, 'r', encoding='utf-8') as f:
                sql_script = f.read()
            
            if not self.connection:
                self.connect()
            
            self.connection.executescript(sql_script)
            self.connection.commit()
            
            self.logger.info(f"SQL脚本执行成功: {script_path}")
            return True
            
        except sqlite3.Error as e:
            self.logger.error(f"SQL脚本执行失败: {e}")
            if self.connection:
                self.connection.rollback()
            return False
    
    def initialize_database(self) -> bool:
        """
        初始化数据库
        
        Returns:
            bool: 初始化是否成功
        """
        try:
            # 获取SQL脚本路径
            script_path = os.path.join(
                os.path.dirname(__file__), 
                'database_init.sql'
            )
            
            self.logger.info("开始初始化数据库...")
            
            # 执行初始化脚本
            if self.execute_script(script_path):
                # 设置Tushare token（如果提供）
                token = "a3c869c34d4f150270b80d307a57a4e20fa9d665c99742aa39edf41f"
                self.set_config('tushare_token', token)
                
                # 更新数据库版本
                self.set_config('db_version', '1.0.0')
                self.set_config('db_init_time', datetime.now().isoformat())
                
                self.logger.info("数据库初始化完成")
                return True
            else:
                self.logger.error("数据库初始化失败")
                return False
                
        except Exception as e:
            self.logger.error(f"数据库初始化异常: {e}")
            return False
    
    def execute_query(self, query: str, params: tuple = None) -> List[sqlite3.Row]:
        """
        执行查询语句
        
        Args:
            query: SQL查询语句
            params: 查询参数
            
        Returns:
            List[sqlite3.Row]: 查询结果
        """
        try:
            if not self.connection:
                self.connect()
            
            cursor = self.connection.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            results = cursor.fetchall()
            return results
            
        except sqlite3.Error as e:
            self.logger.error(f"查询执行失败: {e}")
            raise
    
    def execute_insert(self, query: str, params: tuple = None) -> int:
        """
        执行插入语句
        
        Args:
            query: SQL插入语句
            params: 插入参数
            
        Returns:
            int: 插入的行ID
        """
        try:
            if not self.connection:
                self.connect()
            
            cursor = self.connection.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            self.connection.commit()
            return cursor.lastrowid
            
        except sqlite3.Error as e:
            self.logger.error(f"插入执行失败: {e}")
            if self.connection:
                self.connection.rollback()
            raise
    
    def execute_batch_insert(self, query: str, data: List[tuple]) -> int:
        """
        批量插入数据
        
        Args:
            query: SQL插入语句
            data: 插入数据列表
            
        Returns:
            int: 插入的行数
        """
        try:
            if not self.connection:
                self.connect()
            
            cursor = self.connection.cursor()
            cursor.executemany(query, data)
            
            self.connection.commit()
            return cursor.rowcount
            
        except sqlite3.Error as e:
            self.logger.error(f"批量插入失败: {e}")
            if self.connection:
                self.connection.rollback()
            raise
    
    def execute_update(self, query: str, params: tuple = None) -> int:
        """
        执行更新语句
        
        Args:
            query: SQL更新语句
            params: 更新参数
            
        Returns:
            int: 更新的行数
        """
        try:
            if not self.connection:
                self.connect()
            
            cursor = self.connection.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            self.connection.commit()
            return cursor.rowcount
            
        except sqlite3.Error as e:
            self.logger.error(f"更新执行失败: {e}")
            if self.connection:
                self.connection.rollback()
            raise
    
    def execute_delete(self, query: str, params: tuple = None) -> int:
        """
        执行删除语句
        
        Args:
            query: SQL删除语句
            params: 删除参数
            
        Returns:
            int: 删除的行数
        """
        try:
            if not self.connection:
                self.connect()
            
            cursor = self.connection.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            self.connection.commit()
            return cursor.rowcount
            
        except sqlite3.Error as e:
            self.logger.error(f"删除执行失败: {e}")
            if self.connection:
                self.connection.rollback()
            raise
    
    def execute_transaction(self, operations: List[Dict[str, Any]]) -> bool:
        """
        执行事务操作
        
        Args:
            operations: 操作列表，每个操作包含{'type': 'query'/'insert'/'update'/'delete', 'sql': '', 'params': ()}
            
        Returns:
            bool: 事务是否成功
        """
        try:
            if not self.connection:
                self.connect()
            
            # 开始事务
            self.connection.execute("BEGIN")
            
            cursor = self.connection.cursor()
            for operation in operations:
                op_type = operation.get('type', 'query')
                sql = operation.get('sql', '')
                params = operation.get('params', ())
                
                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)
            
            # 提交事务
            self.connection.commit()
            self.logger.info(f"事务成功执行，包含 {len(operations)} 个操作")
            return True
            
        except sqlite3.Error as e:
            self.logger.error(f"事务执行失败: {e}")
            if self.connection:
                self.connection.rollback()
            raise
    
    def insert_or_update(self, table: str, data: Dict[str, Any], conflict_columns: List[str]) -> int:
        """
        插入或更新数据（使用ON CONFLICT处理）
        
        Args:
            table: 表名
            data: 数据字典
            conflict_columns: 冲突检测列
            
        Returns:
            int: 受影响的行数
        """
        try:
            if not self.connection:
                self.connect()
            
            columns = list(data.keys())
            placeholders = ['?' for _ in columns]
            values = list(data.values())
            
            # 构建UPDATE部分
            update_parts = [f"{col} = excluded.{col}" for col in columns if col not in conflict_columns]
            
            sql = f"""
            INSERT INTO {table} ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
            ON CONFLICT ({', '.join(conflict_columns)})
            DO UPDATE SET {', '.join(update_parts)}
            """
            
            cursor = self.connection.cursor()
            cursor.execute(sql, values)
            self.connection.commit()
            
            return cursor.rowcount
            
        except sqlite3.Error as e:
            self.logger.error(f"插入或更新失败: {e}")
            if self.connection:
                self.connection.rollback()
            raise
    
    def bulk_insert_or_update(self, table: str, data_list: List[Dict[str, Any]], conflict_columns: List[str]) -> int:
        """
        批量插入或更新数据
        
        Args:
            table: 表名
            data_list: 数据字典列表
            conflict_columns: 冲突检测列
            
        Returns:
            int: 受影响的总行数
        """
        try:
            if not data_list:
                return 0
            
            if not self.connection:
                self.connect()
            
            columns = list(data_list[0].keys())
            placeholders = ['?' for _ in columns]
            
            # 构建UPDATE部分
            update_parts = [f"{col} = excluded.{col}" for col in columns if col not in conflict_columns]
            
            sql = f"""
            INSERT INTO {table} ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
            ON CONFLICT ({', '.join(conflict_columns)})
            DO UPDATE SET {', '.join(update_parts)}
            """
            
            # 准备数据
            values_list = []
            for data in data_list:
                values_list.append([data[col] for col in columns])
            
            cursor = self.connection.cursor()
            cursor.executemany(sql, values_list)
            self.connection.commit()
            
            return cursor.rowcount
            
        except sqlite3.Error as e:
            self.logger.error(f"批量插入或更新失败: {e}")
            if self.connection:
                self.connection.rollback()
            raise
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        获取表信息
        
        Args:
            table_name: 表名
            
        Returns:
            Dict: 表信息
        """
        try:
            if not self.connection:
                self.connect()
            
            cursor = self.connection.cursor()
            
            # 获取表结构
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            # 获取索引信息
            cursor.execute(f"PRAGMA index_list({table_name})")
            indexes = cursor.fetchall()
            
            # 获取外键信息
            cursor.execute(f"PRAGMA foreign_key_list({table_name})")
            foreign_keys = cursor.fetchall()
            
            # 获取记录数
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            record_count = cursor.fetchone()[0]
            
            return {
                'columns': [dict(col) for col in columns],
                'indexes': [dict(idx) for idx in indexes],
                'foreign_keys': [dict(fk) for fk in foreign_keys],
                'record_count': record_count
            }
            
        except sqlite3.Error as e:
            self.logger.error(f"获取表信息失败: {e}")
            raise
    
    def get_table_statistics(self, table_name: str) -> Dict[str, Any]:
        """
        获取表统计信息
        
        Args:
            table_name: 表名
            
        Returns:
            Dict: 表统计信息
        """
        try:
            if not self.connection:
                self.connect()
            
            cursor = self.connection.cursor()
            
            # 基础统计
            cursor.execute(f"SELECT COUNT(*) as total_records FROM {table_name}")
            total_records = cursor.fetchone()[0]
            
            # 特定表的统计
            stats = {'total_records': total_records}
            
            if table_name == 'daily_data':
                # 日线数据统计
                cursor.execute("SELECT COUNT(DISTINCT ts_code) as unique_stocks FROM daily_data")
                stats['unique_stocks'] = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(DISTINCT trade_date) as unique_dates FROM daily_data")
                stats['unique_dates'] = cursor.fetchone()[0]
                
                cursor.execute("SELECT MIN(trade_date) as earliest_date, MAX(trade_date) as latest_date FROM daily_data")
                date_range = cursor.fetchone()
                stats['earliest_date'] = date_range[0]
                stats['latest_date'] = date_range[1]
                
            elif table_name == 'stocks':
                # 股票基本信息统计
                cursor.execute("SELECT COUNT(*) as active_stocks FROM stocks WHERE list_status = 'L'")
                stats['active_stocks'] = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) as delisted_stocks FROM stocks WHERE list_status = 'D'")
                stats['delisted_stocks'] = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(DISTINCT market) as unique_markets FROM stocks")
                stats['unique_markets'] = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(DISTINCT industry) as unique_industries FROM stocks")
                stats['unique_industries'] = cursor.fetchone()[0]
            
            return stats
            
        except sqlite3.Error as e:
            self.logger.error(f"获取表统计失败: {e}")
            raise
    
    def vacuum_database(self) -> bool:
        """
        执行数据库清理和优化
        
        Returns:
            bool: 清理是否成功
        """
        try:
            if not self.connection:
                self.connect()
            
            # 执行VACUUM
            self.connection.execute("VACUUM")
            
            # 执行ANALYZE
            self.connection.execute("ANALYZE")
            
            self.logger.info("数据库清理和优化完成")
            return True
            
        except sqlite3.Error as e:
            self.logger.error(f"数据库清理失败: {e}")
            raise
    
    def get_database_size(self) -> Dict[str, Any]:
        """
        获取数据库大小信息
        
        Returns:
            Dict: 数据库大小信息
        """
        try:
            # 获取文件大小
            db_file_size = os.path.getsize(self.db_path)
            
            if not self.connection:
                self.connect()
            
            cursor = self.connection.cursor()
            
            # 获取页面信息
            cursor.execute("PRAGMA page_count")
            page_count = cursor.fetchone()[0]
            
            cursor.execute("PRAGMA page_size")
            page_size = cursor.fetchone()[0]
            
            # 获取表大小
            cursor.execute("""
                SELECT name
                FROM sqlite_master 
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
            """)
            
            tables = cursor.fetchall()
            table_sizes = {}
            
            for table in tables:
                table_name = table[0]
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                record_count = cursor.fetchone()[0]
                table_sizes[table_name] = record_count
            
            return {
                'file_size_bytes': db_file_size,
                'file_size_mb': round(db_file_size / 1024 / 1024, 2),
                'page_count': page_count,
                'page_size': page_size,
                'total_pages_size': page_count * page_size,
                'table_record_counts': table_sizes
            }
            
        except (sqlite3.Error, OSError) as e:
            self.logger.error(f"获取数据库大小失败: {e}")
            raise

    def get_config(self, key: str, default: Any = None) -> Any:
        """
        获取系统配置
        
        Args:
            key: 配置键
            default: 默认值
            
        Returns:
            Any: 配置值
        """
        try:
            query = "SELECT value, data_type FROM system_config WHERE key = ?"
            result = self.execute_query(query, (key,))
            
            if result:
                value, data_type = result[0]
                
                # 根据数据类型转换值
                if data_type == 'int':
                    return int(value) if value else default
                elif data_type == 'float':
                    return float(value) if value else default
                elif data_type == 'bool':
                    return value.lower() in ('1', 'true', 'yes') if value else default
                elif data_type == 'datetime':
                    return datetime.fromisoformat(value) if value else default
                else:
                    return value if value else default
            
            return default
            
        except (sqlite3.Error, ValueError) as e:
            self.logger.error(f"获取配置失败: {e}")
            return default
    
    def set_config(self, key: str, value: Any, description: str = None) -> bool:
        """
        设置系统配置
        
        Args:
            key: 配置键
            value: 配置值
            description: 配置说明
            
        Returns:
            bool: 设置是否成功
        """
        try:
            # 确定数据类型
            data_type = 'string'
            if isinstance(value, int):
                data_type = 'int'
            elif isinstance(value, float):
                data_type = 'float'
            elif isinstance(value, bool):
                data_type = 'bool'
                value = '1' if value else '0'
            elif isinstance(value, datetime):
                data_type = 'datetime'
                value = value.isoformat()
            
            query = """
                INSERT OR REPLACE INTO system_config (key, value, description, data_type)
                VALUES (?, ?, ?, ?)
            """
            
            self.execute_insert(query, (key, str(value), description, data_type))
            return True
            
        except sqlite3.Error as e:
            self.logger.error(f"设置配置失败: {e}")
            return False
    
    def get_database_info(self) -> Dict[str, Any]:
        """
        获取数据库信息
        
        Returns:
            Dict[str, Any]: 数据库信息
        """
        try:
            info = {
                'db_path': self.db_path,
                'db_size': os.path.getsize(self.db_path) if os.path.exists(self.db_path) else 0,
                'db_version': self.get_config('db_version', 'unknown'),
                'init_time': self.get_config('db_init_time', 'unknown'),
                'tables': [],
                'record_counts': {}
            }
            
            # 获取表信息
            tables_query = "SELECT name FROM sqlite_master WHERE type='table'"
            tables = self.execute_query(tables_query)
            
            for table in tables:
                table_name = table['name']
                info['tables'].append(table_name)
                
                # 获取记录数
                count_query = f"SELECT COUNT(*) as count FROM {table_name}"
                count_result = self.execute_query(count_query)
                info['record_counts'][table_name] = count_result[0]['count']
            
            return info
            
        except sqlite3.Error as e:
            self.logger.error(f"获取数据库信息失败: {e}")
            return {}
    
    def backup_database(self, backup_path: str = None) -> bool:
        """
        备份数据库
        
        Args:
            backup_path: 备份文件路径
            
        Returns:
            bool: 备份是否成功
        """
        try:
            if not backup_path:
                backup_path = f"data/backup/stock_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
            
            # 确保备份目录存在
            os.makedirs(os.path.dirname(backup_path), exist_ok=True)
            
            if not self.connection:
                self.connect()
            
            # 使用SQLite的备份API
            backup_conn = sqlite3.connect(backup_path)
            self.connection.backup(backup_conn)
            backup_conn.close()
            
            self.logger.info(f"数据库备份成功: {backup_path}")
            return True
            
        except sqlite3.Error as e:
            self.logger.error(f"数据库备份失败: {e}")
            return False


def main():
    """命令行主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='数据库管理器')
    parser.add_argument('--init', action='store_true', help='初始化数据库')
    parser.add_argument('--info', action='store_true', help='显示数据库信息')
    parser.add_argument('--backup', action='store_true', help='备份数据库')
    parser.add_argument('--db-path', default='data/stock_data.db', help='数据库文件路径')
    
    args = parser.parse_args()
    
    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    db_manager = DatabaseManager(args.db_path)
    
    try:
        if args.init:
            print("正在初始化数据库...")
            if db_manager.initialize_database():
                print("✅ 数据库初始化成功")
            else:
                print("❌ 数据库初始化失败")
                sys.exit(1)
        
        elif args.info:
            print("正在获取数据库信息...")
            info = db_manager.get_database_info()
            if info:
                print(f"📊 数据库信息:")
                print(f"  路径: {info['db_path']}")
                print(f"  大小: {info['db_size'] / 1024 / 1024:.2f} MB")
                print(f"  版本: {info['db_version']}")
                print(f"  初始化时间: {info['init_time']}")
                print(f"  表数量: {len(info['tables'])}")
                print(f"  表信息:")
                for table in info['tables']:
                    count = info['record_counts'].get(table, 0)
                    print(f"    {table}: {count} 条记录")
            else:
                print("❌ 获取数据库信息失败")
        
        elif args.backup:
            print("正在备份数据库...")
            if db_manager.backup_database():
                print("✅ 数据库备份成功")
            else:
                print("❌ 数据库备份失败")
        
        else:
            parser.print_help()
    
    finally:
        db_manager.disconnect()


if __name__ == "__main__":
    main() 