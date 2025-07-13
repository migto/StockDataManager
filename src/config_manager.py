#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
配置管理器
提供配置文件的读取、写入、验证和管理功能
"""

import json
import os
import shutil
import logging
from datetime import datetime
from typing import Any, Dict, Optional, Union
from pathlib import Path
import argparse


class ConfigManager:
    """配置管理器类
    
    负责管理系统配置文件，包括：
    - 配置文件的读取、写入、验证
    - 配置项的获取和设置
    - 配置文件的备份和恢复
    - 环境变量覆盖支持
    """
    
    def __init__(self, config_file: str = "config/config.json"):
        """初始化配置管理器
        
        Args:
            config_file: 配置文件路径
        """
        self.config_file = Path(config_file)
        self.config_dir = self.config_file.parent
        self.backup_dir = self.config_dir / "backups"
        self._config = {}
        self._default_config = {}
        self.logger = logging.getLogger(__name__)
        
        # 确保配置目录存在
        self.config_dir.mkdir(parents=True, exist_ok=True)
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        
        # 加载默认配置
        self._load_default_config()
        
        # 加载用户配置
        self.reload()
    
    def _load_default_config(self):
        """加载默认配置"""
        self._default_config = {
            "tushare": {
                "token": "",
                "api_url": "http://api.tushare.pro",
                "timeout": 30,
                "retry_count": 3,
                "retry_delay": 1
            },
            "database": {
                "path": "data/stock_data.db",
                "backup_path": "data/backups/",
                "connection_timeout": 30,
                "enable_foreign_keys": True,
                "enable_wal_mode": True
            },
            "api_limits": {
                "free_account": {
                    "total_points": 120,
                    "calls_per_minute": 2,
                    "calls_per_hour": 60,
                    "calls_per_day": 120
                },
                "rate_limit_buffer": 0.1,
                "monitor_window_hours": 24
            },
            "download": {
                "batch_size": 100,
                "max_workers": 1,
                "enable_incremental": True,
                "auto_retry": True,
                "max_retry_attempts": 3
            },
            "logging": {
                "level": "INFO",
                "file_path": "logs/stock_downloader.log",
                "max_file_size": "10MB",
                "backup_count": 5,
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            },
            "scheduler": {
                "enabled": True,
                "run_time": "09:00",
                "timezone": "Asia/Shanghai",
                "weekends_enabled": False,
                "holidays_enabled": False
            },
            "notifications": {
                "enabled": False,
                "email": {
                    "smtp_server": "",
                    "smtp_port": 587,
                    "username": "",
                    "password": "",
                    "recipients": []
                }
            },
            "system": {
                "version": "1.0.0",
                "debug_mode": False,
                "performance_monitoring": True,
                "data_validation": True
            }
        }
    
    def reload(self):
        """重新加载配置文件"""
        if not self.config_file.exists():
            self.logger.info(f"配置文件不存在，创建默认配置文件: {self.config_file}")
            self._create_default_config()
        
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                self._config = json.load(f)
            
            # 合并默认配置，确保所有必要的配置项都存在
            self._merge_default_config()
            
            # 应用环境变量覆盖
            self._apply_env_overrides()
            
            self.logger.info(f"配置文件加载成功: {self.config_file}")
            
        except Exception as e:
            self.logger.error(f"加载配置文件失败: {e}")
            self.logger.info("使用默认配置")
            self._config = self._default_config.copy()
    
    def _create_default_config(self):
        """创建默认配置文件"""
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self._default_config, f, indent=4, ensure_ascii=False)
            self.logger.info(f"默认配置文件创建成功: {self.config_file}")
        except Exception as e:
            self.logger.error(f"创建默认配置文件失败: {e}")
            raise
    
    def _merge_default_config(self):
        """合并默认配置，确保所有必要的配置项都存在"""
        def merge_dict(default: dict, user: dict) -> dict:
            """递归合并字典"""
            result = default.copy()
            for key, value in user.items():
                if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                    result[key] = merge_dict(result[key], value)
                else:
                    result[key] = value
            return result
        
        self._config = merge_dict(self._default_config, self._config)
    
    def _apply_env_overrides(self):
        """应用环境变量覆盖"""
        # Tushare Token
        tushare_token = os.getenv('TUSHARE_TOKEN')
        if tushare_token:
            self._config['tushare']['token'] = tushare_token
            self.logger.info("使用环境变量 TUSHARE_TOKEN 覆盖配置")
        
        # 数据库路径
        db_path = os.getenv('DB_PATH')
        if db_path:
            self._config['database']['path'] = db_path
            self.logger.info("使用环境变量 DB_PATH 覆盖配置")
        
        # 调试模式
        debug_mode = os.getenv('DEBUG_MODE')
        if debug_mode:
            self._config['system']['debug_mode'] = debug_mode.lower() in ('true', '1', 'yes')
            self.logger.info("使用环境变量 DEBUG_MODE 覆盖配置")
    
    def get(self, key: str, default: Any = None) -> Any:
        """获取配置值
        
        Args:
            key: 配置键，支持点号分隔的嵌套键（如 'tushare.token'）
            default: 默认值
            
        Returns:
            配置值
        """
        keys = key.split('.')
        value = self._config
        
        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default
    
    def set(self, key: str, value: Any, save: bool = True):
        """设置配置值
        
        Args:
            key: 配置键，支持点号分隔的嵌套键（如 'tushare.token'）
            value: 配置值
            save: 是否立即保存到文件
        """
        keys = key.split('.')
        config = self._config
        
        # 导航到父级字典
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        
        # 设置值
        config[keys[-1]] = value
        
        if save:
            self.save()
    
    def save(self):
        """保存配置到文件"""
        try:
            # 创建备份
            if self.config_file.exists():
                self.backup()
            
            # 保存配置
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self._config, f, indent=4, ensure_ascii=False)
            
            self.logger.info(f"配置文件保存成功: {self.config_file}")
            
        except Exception as e:
            self.logger.error(f"保存配置文件失败: {e}")
            raise
    
    def backup(self) -> str:
        """备份配置文件
        
        Returns:
            备份文件路径
        """
        if not self.config_file.exists():
            raise FileNotFoundError(f"配置文件不存在: {self.config_file}")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = self.backup_dir / f"config_backup_{timestamp}.json"
        
        try:
            shutil.copy2(self.config_file, backup_file)
            self.logger.info(f"配置文件备份成功: {backup_file}")
            return str(backup_file)
        except Exception as e:
            self.logger.error(f"备份配置文件失败: {e}")
            raise
    
    def restore(self, backup_file: str):
        """恢复配置文件
        
        Args:
            backup_file: 备份文件路径
        """
        backup_path = Path(backup_file)
        if not backup_path.exists():
            raise FileNotFoundError(f"备份文件不存在: {backup_file}")
        
        try:
            shutil.copy2(backup_path, self.config_file)
            self.reload()
            self.logger.info(f"配置文件恢复成功: {backup_file}")
        except Exception as e:
            self.logger.error(f"恢复配置文件失败: {e}")
            raise
    
    def validate(self) -> tuple[bool, list[str]]:
        """验证配置文件
        
        Returns:
            (是否有效, 错误消息列表)
        """
        errors = []
        
        # 验证 Tushare Token
        if not self.get('tushare.token'):
            errors.append("Tushare Token 未配置")
        
        # 验证数据库路径
        db_path = self.get('database.path')
        if not db_path:
            errors.append("数据库路径未配置")
        else:
            db_dir = Path(db_path).parent
            if not db_dir.exists():
                try:
                    db_dir.mkdir(parents=True, exist_ok=True)
                except Exception as e:
                    errors.append(f"无法创建数据库目录: {e}")
        
        # 验证日志路径
        log_path = self.get('logging.file_path')
        if log_path:
            log_dir = Path(log_path).parent
            if not log_dir.exists():
                try:
                    log_dir.mkdir(parents=True, exist_ok=True)
                except Exception as e:
                    errors.append(f"无法创建日志目录: {e}")
        
        # 验证API限制配置
        api_limits = self.get('api_limits.free_account')
        if api_limits:
            required_fields = ['total_points', 'calls_per_minute', 'calls_per_hour', 'calls_per_day']
            for field in required_fields:
                if field not in api_limits:
                    errors.append(f"API限制配置缺少字段: {field}")
        
        return len(errors) == 0, errors
    
    def get_config_info(self) -> dict:
        """获取配置信息
        
        Returns:
            配置信息字典
        """
        return {
            "config_file": str(self.config_file),
            "config_exists": self.config_file.exists(),
            "config_size": self.config_file.stat().st_size if self.config_file.exists() else 0,
            "backup_count": len(list(self.backup_dir.glob("config_backup_*.json"))),
            "tushare_token_configured": bool(self.get('tushare.token')),
            "database_path": self.get('database.path'),
            "logging_level": self.get('logging.level'),
            "scheduler_enabled": self.get('scheduler.enabled')
        }
    
    def list_backups(self) -> list[dict]:
        """列出所有备份文件
        
        Returns:
            备份文件信息列表
        """
        backups = []
        for backup_file in sorted(self.backup_dir.glob("config_backup_*.json"), reverse=True):
            stat = backup_file.stat()
            backups.append({
                "file": str(backup_file),
                "name": backup_file.name,
                "size": stat.st_size,
                "created": datetime.fromtimestamp(stat.st_ctime).strftime("%Y-%m-%d %H:%M:%S")
            })
        return backups
    
    def cleanup_old_backups(self, keep_count: int = 10):
        """清理旧的备份文件
        
        Args:
            keep_count: 保留的备份数量
        """
        backups = sorted(self.backup_dir.glob("config_backup_*.json"))
        if len(backups) > keep_count:
            for backup_file in backups[:-keep_count]:
                try:
                    backup_file.unlink()
                    self.logger.info(f"删除旧备份文件: {backup_file}")
                except Exception as e:
                    self.logger.error(f"删除备份文件失败: {e}")
    
    def export_config(self, export_file: str, include_sensitive: bool = False):
        """导出配置到文件
        
        Args:
            export_file: 导出文件路径
            include_sensitive: 是否包含敏感信息（如Token）
        """
        config_to_export = self._config.copy()
        
        if not include_sensitive:
            # 移除敏感信息
            if 'tushare' in config_to_export:
                config_to_export['tushare']['token'] = "***"
            if 'notifications' in config_to_export and 'email' in config_to_export['notifications']:
                config_to_export['notifications']['email']['password'] = "***"
        
        try:
            with open(export_file, 'w', encoding='utf-8') as f:
                json.dump(config_to_export, f, indent=4, ensure_ascii=False)
            self.logger.info(f"配置导出成功: {export_file}")
        except Exception as e:
            self.logger.error(f"导出配置失败: {e}")
            raise
    
    def import_config(self, import_file: str):
        """从文件导入配置
        
        Args:
            import_file: 导入文件路径
        """
        import_path = Path(import_file)
        if not import_path.exists():
            raise FileNotFoundError(f"导入文件不存在: {import_file}")
        
        try:
            with open(import_path, 'r', encoding='utf-8') as f:
                imported_config = json.load(f)
            
            # 备份当前配置
            self.backup()
            
            # 合并配置
            self._config = imported_config
            self._merge_default_config()
            
            # 保存配置
            self.save()
            
            self.logger.info(f"配置导入成功: {import_file}")
            
        except Exception as e:
            self.logger.error(f"导入配置失败: {e}")
            raise
    
    def __getitem__(self, key: str) -> Any:
        """支持字典式访问"""
        return self.get(key)
    
    def __setitem__(self, key: str, value: Any):
        """支持字典式设置"""
        self.set(key, value)
    
    def __contains__(self, key: str) -> bool:
        """支持 in 操作符"""
        return self.get(key) is not None


def main():
    """命令行接口"""
    parser = argparse.ArgumentParser(description="配置管理器")
    parser.add_argument('--config', '-c', default='config/config.json', help='配置文件路径')
    
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # 初始化命令
    init_parser = subparsers.add_parser('init', help='初始化配置文件')
    init_parser.add_argument('--force', '-f', action='store_true', help='强制重新创建配置文件')
    
    # 设置命令
    set_parser = subparsers.add_parser('set', help='设置配置值')
    set_parser.add_argument('key', help='配置键')
    set_parser.add_argument('value', help='配置值')
    
    # 获取命令
    get_parser = subparsers.add_parser('get', help='获取配置值')
    get_parser.add_argument('key', help='配置键')
    
    # 验证命令
    validate_parser = subparsers.add_parser('validate', help='验证配置文件')
    
    # 信息命令
    info_parser = subparsers.add_parser('info', help='显示配置信息')
    
    # 备份命令
    backup_parser = subparsers.add_parser('backup', help='备份配置文件')
    
    # 恢复命令
    restore_parser = subparsers.add_parser('restore', help='恢复配置文件')
    restore_parser.add_argument('backup_file', help='备份文件路径')
    
    # 列出备份命令
    list_backups_parser = subparsers.add_parser('list-backups', help='列出备份文件')
    
    # 导出命令
    export_parser = subparsers.add_parser('export', help='导出配置')
    export_parser.add_argument('export_file', help='导出文件路径')
    export_parser.add_argument('--include-sensitive', action='store_true', help='包含敏感信息')
    
    # 导入命令
    import_parser = subparsers.add_parser('import', help='导入配置')
    import_parser.add_argument('import_file', help='导入文件路径')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        config_manager = ConfigManager(args.config)
        
        if args.command == 'init':
            if args.force or not config_manager.config_file.exists():
                config_manager._create_default_config()
                print(f"配置文件初始化成功: {config_manager.config_file}")
            else:
                print(f"配置文件已存在: {config_manager.config_file}")
        
        elif args.command == 'set':
            config_manager.set(args.key, args.value)
            print(f"配置设置成功: {args.key} = {args.value}")
        
        elif args.command == 'get':
            value = config_manager.get(args.key)
            if value is not None:
                print(f"{args.key} = {value}")
            else:
                print(f"配置项不存在: {args.key}")
        
        elif args.command == 'validate':
            is_valid, errors = config_manager.validate()
            if is_valid:
                print("配置验证通过")
            else:
                print("配置验证失败:")
                for error in errors:
                    print(f"  - {error}")
        
        elif args.command == 'info':
            info = config_manager.get_config_info()
            print("配置信息:")
            for key, value in info.items():
                print(f"  {key}: {value}")
        
        elif args.command == 'backup':
            backup_file = config_manager.backup()
            print(f"配置备份成功: {backup_file}")
        
        elif args.command == 'restore':
            config_manager.restore(args.backup_file)
            print(f"配置恢复成功: {args.backup_file}")
        
        elif args.command == 'list-backups':
            backups = config_manager.list_backups()
            if backups:
                print("备份文件列表:")
                for backup in backups:
                    print(f"  {backup['name']} ({backup['size']} bytes) - {backup['created']}")
            else:
                print("没有找到备份文件")
        
        elif args.command == 'export':
            config_manager.export_config(args.export_file, args.include_sensitive)
            print(f"配置导出成功: {args.export_file}")
        
        elif args.command == 'import':
            config_manager.import_config(args.import_file)
            print(f"配置导入成功: {args.import_file}")
    
    except Exception as e:
        print(f"错误: {e}")
        return 1
    
    return 0


if __name__ == '__main__':
    exit(main()) 