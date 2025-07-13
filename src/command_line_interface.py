"""
命令行界面模块
提供完整的命令行界面功能
"""

import argparse
import sys
import os
import time
import json
import signal
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pathlib import Path
import threading

# 导入所有需要的管理器
from .config_manager import ConfigManager
from .database_manager import DatabaseManager
from .logging_manager import LoggingManager, LogLevel, LogType
from .schedule_manager import ScheduleManager, TaskType, ScheduleStatus
from .smart_download_manager import SmartDownloadManager
from .download_status_manager import DownloadStatusManager
from .data_integrity_manager import DataIntegrityManager
from .incremental_update_manager import IncrementalUpdateManager
from .stock_basic_manager import StockBasicManager
from .daily_data_manager import DailyDataManager
from .optimized_tushare_api_manager import OptimizedTushareAPIManager


class CommandLineInterface:
    """命令行界面类"""
    
    def __init__(self):
        """初始化命令行界面"""
        self.config_manager = None
        self.schedule_manager = None
        self.db_manager = None
        self.logging_manager = None
        self.download_manager = None
        self.status_manager = None
        self.integrity_manager = None
        self.update_manager = None
        self.stock_manager = None
        self.daily_manager = None
        self.api_manager = None
        
        # 创建解析器
        self.parser = self._create_parser()
        
        # 设置信号处理器
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _create_parser(self) -> argparse.ArgumentParser:
        """创建命令行参数解析器"""
        parser = argparse.ArgumentParser(
            description='A股日线数据下载系统',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
使用示例:
  %(prog)s --config config.json init           # 初始化系统
  %(prog)s scheduler start                     # 启动调度器
  %(prog)s scheduler status                    # 查看调度状态
  %(prog)s task run daily_download             # 运行每日下载任务
  %(prog)s data download --stocks 000001      # 下载指定股票数据
  %(prog)s status --summary                    # 查看系统状态
  %(prog)s logs --type system --limit 50      # 查看日志
  %(prog)s config get database.path           # 获取配置
            """
        )
        
        # 全局选项
        parser.add_argument('--config', '-c', 
                          default='config/config.json',
                          help='配置文件路径 (默认: config/config.json)')
        parser.add_argument('--verbose', '-v', 
                          action='store_true',
                          help='详细输出')
        parser.add_argument('--quiet', '-q', 
                          action='store_true',
                          help='静默模式')
        parser.add_argument('--dry-run', 
                          action='store_true',
                          help='预演模式，不执行实际操作')
        
        # 子命令
        subparsers = parser.add_subparsers(dest='command', help='可用命令')
        
        # 初始化命令
        init_parser = subparsers.add_parser('init', help='初始化系统')
        init_parser.add_argument('--force', action='store_true', help='强制重新初始化')
        
        # 调度器命令
        scheduler_parser = subparsers.add_parser('scheduler', help='调度器管理')
        scheduler_subparsers = scheduler_parser.add_subparsers(dest='scheduler_action')
        
        # 调度器子命令
        scheduler_subparsers.add_parser('start', help='启动调度器')
        scheduler_subparsers.add_parser('stop', help='停止调度器')
        scheduler_subparsers.add_parser('pause', help='暂停调度器')
        scheduler_subparsers.add_parser('resume', help='恢复调度器')
        scheduler_subparsers.add_parser('restart', help='重启调度器')
        scheduler_subparsers.add_parser('status', help='查看调度器状态')
        
        # 任务命令
        task_parser = subparsers.add_parser('task', help='任务管理')
        task_subparsers = task_parser.add_subparsers(dest='task_action')
        
        # 任务子命令
        task_run_parser = task_subparsers.add_parser('run', help='运行任务')
        task_run_parser.add_argument('task_type', 
                                   choices=[t.value for t in TaskType],
                                   help='任务类型')
        
        task_history_parser = task_subparsers.add_parser('history', help='查看任务历史')
        task_history_parser.add_argument('--limit', '-l', type=int, default=20,
                                       help='显示条数 (默认: 20)')
        task_history_parser.add_argument('--type', '-t', 
                                       choices=[t.value for t in TaskType],
                                       help='过滤任务类型')
        
        task_config_parser = task_subparsers.add_parser('config', help='任务配置')
        task_config_parser.add_argument('task_type', 
                                       choices=[t.value for t in TaskType],
                                       help='任务类型')
        task_config_parser.add_argument('--time', help='运行时间')
        task_config_parser.add_argument('--enabled', type=bool, help='是否启用')
        
        # 数据命令
        data_parser = subparsers.add_parser('data', help='数据管理')
        data_subparsers = data_parser.add_subparsers(dest='data_action')
        
        # 数据子命令
        data_download_parser = data_subparsers.add_parser('download', help='下载数据')
        data_download_parser.add_argument('--stocks', nargs='+', help='股票代码列表')
        data_download_parser.add_argument('--start-date', help='开始日期 (YYYYMMDD)')
        data_download_parser.add_argument('--end-date', help='结束日期 (YYYYMMDD)')
        data_download_parser.add_argument('--type', 
                                        choices=['missing_days', 'recent_days', 'priority_stocks'],
                                        default='missing_days',
                                        help='下载类型')
        data_download_parser.add_argument('--max-days', type=int, default=30,
                                        help='最大天数')
        
        data_integrity_parser = data_subparsers.add_parser('integrity', help='数据完整性检查')
        data_integrity_parser.add_argument('--repair', action='store_true',
                                         help='修复发现的问题')
        data_integrity_parser.add_argument('--report', action='store_true',
                                         help='生成详细报告')
        
        data_update_parser = data_subparsers.add_parser('update', help='增量更新')
        data_update_parser.add_argument('--stocks', nargs='+', help='股票代码列表')
        data_update_parser.add_argument('--days', type=int, default=7,
                                      help='更新天数')
        
        # 状态命令
        status_parser = subparsers.add_parser('status', help='系统状态')
        status_parser.add_argument('--summary', action='store_true',
                                 help='显示摘要信息')
        status_parser.add_argument('--detailed', action='store_true',
                                 help='显示详细信息')
        status_parser.add_argument('--stocks', nargs='+', help='查看指定股票状态')
        
        # 日志命令
        logs_parser = subparsers.add_parser('logs', help='日志查看')
        logs_parser.add_argument('--type', '-t', 
                               choices=[t.value for t in LogType],
                               help='日志类型')
        logs_parser.add_argument('--level', '-l',
                               choices=['debug', 'info', 'warning', 'error', 'critical'],
                               help='日志级别')
        logs_parser.add_argument('--limit', type=int, default=50,
                               help='显示条数 (默认: 50)')
        logs_parser.add_argument('--start-time', help='开始时间')
        logs_parser.add_argument('--end-time', help='结束时间')
        logs_parser.add_argument('--export', help='导出到文件')
        
        # 配置命令
        config_parser = subparsers.add_parser('config', help='配置管理')
        config_subparsers = config_parser.add_subparsers(dest='config_action')
        
        # 配置子命令
        config_get_parser = config_subparsers.add_parser('get', help='获取配置')
        config_get_parser.add_argument('key', help='配置键')
        
        config_set_parser = config_subparsers.add_parser('set', help='设置配置')
        config_set_parser.add_argument('key', help='配置键')
        config_set_parser.add_argument('value', help='配置值')
        
        config_subparsers.add_parser('list', help='列出所有配置')
        config_subparsers.add_parser('backup', help='备份配置')
        config_subparsers.add_parser('validate', help='验证配置')
        
        # 报告命令
        report_parser = subparsers.add_parser('report', help='报告生成')
        report_parser.add_argument('--type', 
                                 choices=['daily', 'weekly', 'monthly', 'custom'],
                                 default='daily',
                                 help='报告类型')
        report_parser.add_argument('--start-date', help='开始日期')
        report_parser.add_argument('--end-date', help='结束日期')
        report_parser.add_argument('--output', help='输出文件')
        
        # 数据库命令
        db_parser = subparsers.add_parser('database', help='数据库管理')
        db_subparsers = db_parser.add_subparsers(dest='db_action')
        
        # 数据库子命令
        db_subparsers.add_parser('info', help='数据库信息')
        db_subparsers.add_parser('backup', help='备份数据库')
        db_subparsers.add_parser('vacuum', help='优化数据库')
        db_subparsers.add_parser('stats', help='数据库统计')
        
        db_query_parser = db_subparsers.add_parser('query', help='执行查询')
        db_query_parser.add_argument('sql', help='SQL查询语句')
        db_query_parser.add_argument('--limit', type=int, default=100,
                                   help='结果限制')
        
        return parser
    
    def _init_managers(self, config_file: str):
        """初始化所有管理器"""
        try:
            # 配置管理器
            self.config_manager = ConfigManager(config_file)
            
            # 数据库管理器
            self.db_manager = DatabaseManager(
                self.config_manager.get('database.path', 'data/stock_data.db')
            )
            
            # 日志管理器
            self.logging_manager = LoggingManager(self.config_manager)
            
            # 调度管理器
            self.schedule_manager = ScheduleManager(self.config_manager)
            
            # 下载管理器
            self.download_manager = SmartDownloadManager(self.config_manager)
            
            # 状态管理器
            self.status_manager = DownloadStatusManager(self.config_manager)
            
            # 完整性管理器
            self.integrity_manager = DataIntegrityManager(self.config_manager)
            
            # 增量更新管理器
            self.update_manager = IncrementalUpdateManager(self.config_manager)
            
            # 股票基本信息管理器
            self.stock_manager = StockBasicManager(self.config_manager)
            
            # 日线数据管理器
            self.daily_manager = DailyDataManager(self.config_manager)
            
            # API管理器
            self.api_manager = OptimizedTushareAPIManager(self.config_manager)
            
            if not self.quiet:
                print("[OK] 管理器初始化成功")
                
        except Exception as e:
            print(f"[ERROR] 管理器初始化失败: {str(e)}")
            sys.exit(1)
    
    def run(self):
        """运行命令行界面"""
        args = self.parser.parse_args()
        
        # 设置详细程度
        self.verbose = args.verbose
        self.quiet = args.quiet
        self.dry_run = args.dry_run
        
        # 如果没有指定命令，显示帮助
        if not args.command:
            self.parser.print_help()
            return
        
        # 初始化管理器
        self._init_managers(args.config)
        
        # 执行命令
        try:
            if args.command == 'init':
                self._handle_init(args)
            elif args.command == 'scheduler':
                self._handle_scheduler(args)
            elif args.command == 'task':
                self._handle_task(args)
            elif args.command == 'data':
                self._handle_data(args)
            elif args.command == 'status':
                self._handle_status(args)
            elif args.command == 'logs':
                self._handle_logs(args)
            elif args.command == 'config':
                self._handle_config(args)
            elif args.command == 'report':
                self._handle_report(args)
            elif args.command == 'database':
                self._handle_database(args)
            else:
                print(f"❌ 未知命令: {args.command}")
                self.parser.print_help()
                
        except KeyboardInterrupt:
            print("\n🛑 操作被中断")
            sys.exit(0)
        except Exception as e:
            print(f"❌ 执行失败: {str(e)}")
            if self.verbose:
                import traceback
                traceback.print_exc()
            sys.exit(1)
    
    def _handle_init(self, args):
        """处理初始化命令"""
        if not self.quiet:
            print("[INFO] 初始化系统...")
        
        # 创建必要的目录
        dirs = ['data', 'logs', 'config', 'data/backups']
        for dir_path in dirs:
            Path(dir_path).mkdir(parents=True, exist_ok=True)
            if self.verbose:
                print(f"[INFO] 创建目录: {dir_path}")
        
        # 初始化数据库
        if self.db_manager.initialize_database():
            if self.verbose:
                print("[OK] 数据库初始化成功")
        else:
            print("[ERROR] 数据库初始化失败")
            return
        
        # 初始化配置
        if args.force:
            self.config_manager.save()
            if self.verbose:
                print("[INFO] 配置文件重新生成")
        
        # 检查API连接
        try:
            if self.api_manager.check_connection():
                if self.verbose:
                    print("[OK] API连接测试成功")
            else:
                print("[WARN] API连接测试失败，请检查token配置")
        except Exception as e:
            print(f"[WARN] API连接测试失败: {str(e)}")
        
        if not self.quiet:
            print("[OK] 系统初始化完成")
    
    def _handle_scheduler(self, args):
        """处理调度器命令"""
        if not args.scheduler_action:
            print("请指定调度器操作")
            return
        
        try:
            if args.scheduler_action == 'start':
                if self.dry_run:
                    print("[DRY-RUN] 启动调度器")
                    return
                
                print("[INFO] 启动调度器...")
                self.schedule_manager.start_scheduler()
                print("[OK] 调度器启动成功")
                
                if not self.quiet:
                    print("按 Ctrl+C 停止调度器")
                    try:
                        while True:
                            time.sleep(1)
                    except KeyboardInterrupt:
                        print("\n[INFO] 停止调度器...")
                        self.schedule_manager.stop_scheduler()
                        print("[OK] 调度器已停止")
            
            elif args.scheduler_action == 'stop':
                if self.dry_run:
                    print("[DRY-RUN] 停止调度器")
                    return
                
                print("[INFO] 停止调度器...")
                self.schedule_manager.stop_scheduler()
                print("[OK] 调度器已停止")
            
            elif args.scheduler_action == 'pause':
                if self.dry_run:
                    print("[DRY-RUN] 暂停调度器")
                    return
                
                print("[INFO] 暂停调度器...")
                self.schedule_manager.pause_scheduler()
                print("[OK] 调度器已暂停")
            
            elif args.scheduler_action == 'resume':
                if self.dry_run:
                    print("[DRY-RUN] 恢复调度器")
                    return
                
                print("[INFO] 恢复调度器...")
                self.schedule_manager.resume_scheduler()
                print("[OK] 调度器已恢复")
            
            elif args.scheduler_action == 'restart':
                if self.dry_run:
                    print("[DRY-RUN] 重启调度器")
                    return
                
                print("[INFO] 重启调度器...")
                self.schedule_manager.stop_scheduler()
                time.sleep(2)
                self.schedule_manager.start_scheduler()
                print("[OK] 调度器重启成功")
            
            elif args.scheduler_action == 'status':
                status = self.schedule_manager.get_schedule_status()
                self._print_scheduler_status(status)
                
        except Exception as e:
            print(f"[ERROR] 调度器操作失败: {str(e)}")
            raise
    
    def _handle_task(self, args):
        """处理任务命令"""
        if not args.task_action:
            print("请指定任务操作")
            return
        
        try:
            if args.task_action == 'run':
                task_type = TaskType(args.task_type)
                
                if self.dry_run:
                    print(f"[DRY-RUN] 运行任务: {task_type.value}")
                    return
                
                print(f"[INFO] 运行任务: {task_type.value}")
                result = self.schedule_manager.run_task_immediately(task_type)
                print(f"[OK] 任务启动: {result['message']}")
                
                if not self.quiet:
                    # 等待任务完成
                    print("[INFO] 等待任务完成...")
                    time.sleep(2)
                    
                    # 检查任务状态
                    history = self.schedule_manager.get_task_history(1)
                    if history:
                        task = history[0]
                        print(f"[INFO] 任务状态: {task['status']}")
                        if task['status'] == 'completed':
                            print("[OK] 任务执行成功")
                        elif task['status'] == 'failed':
                            print(f"[ERROR] 任务执行失败: {task['error_message']}")
            
            elif args.task_action == 'history':
                history = self.schedule_manager.get_task_history(args.limit)
                self._print_task_history(history, args.type)
            
            elif args.task_action == 'config':
                task_type = TaskType(args.task_type)
                config = {}
                if args.time:
                    config['time'] = args.time
                if args.enabled is not None:
                    config['enabled'] = args.enabled
                
                if config:
                    if self.dry_run:
                        print(f"🔄 [预演] 更新任务配置: {task_type.value}")
                        return
                    
                    self.schedule_manager.update_task_config(task_type, config)
                    print(f"✅ 任务配置已更新: {task_type.value}")
                else:
                    # 显示当前配置
                    status = self.schedule_manager.get_schedule_status()
                    task_config = status['task_configs'].get(task_type.value)
                    if task_config:
                        self._print_task_config(task_type.value, task_config)
                    else:
                        print(f"❌ 任务配置不存在: {task_type.value}")
                        
        except Exception as e:
            print(f"❌ 任务操作失败: {str(e)}")
            raise
    
    def _handle_data(self, args):
        """处理数据命令"""
        if not args.data_action:
            print("请指定数据操作")
            return
        
        try:
            if args.data_action == 'download':
                if self.dry_run:
                    print("🔄 [预演] 下载数据")
                    return
                
                print("📥 开始下载数据...")
                
                # 创建下载计划
                download_plan = self.download_manager.create_download_plan(
                    download_type=args.type,
                    max_days=args.max_days,
                    priority_stocks=args.stocks
                )
                
                if self.verbose:
                    print(f"📋 下载计划: {len(download_plan.get('tasks', []))} 个任务")
                
                # 执行下载
                result = self.download_manager.execute_download_plan(download_plan)
                
                if result.get('success'):
                    print("✅ 数据下载完成")
                    if self.verbose:
                        print(f"📊 下载统计: {result.get('statistics', {})}")
                else:
                    print("❌ 数据下载失败")
                    if result.get('error'):
                        print(f"错误: {result['error']}")
            
            elif args.data_action == 'integrity':
                print("🔍 检查数据完整性...")
                
                # 检查完整性
                report = self.integrity_manager.check_data_integrity()
                self._print_integrity_report(report)
                
                # 修复问题
                if args.repair and report.get('issues_found', 0) > 0:
                    if self.dry_run:
                        print("🔄 [预演] 修复数据问题")
                        return
                    
                    print("🔧 修复数据问题...")
                    repair_result = self.integrity_manager.repair_data_issues()
                    if repair_result.get('success'):
                        print("✅ 数据修复完成")
                    else:
                        print("❌ 数据修复失败")
                
                # 生成报告
                if args.report:
                    report_file = f"integrity_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                    with open(report_file, 'w', encoding='utf-8') as f:
                        json.dump(report, f, ensure_ascii=False, indent=2)
                    print(f"📄 报告已保存: {report_file}")
            
            elif args.data_action == 'update':
                if self.dry_run:
                    print("🔄 [预演] 增量更新")
                    return
                
                print("🔄 增量更新数据...")
                
                # 创建更新计划
                plan = self.update_manager.create_update_plan(
                    plan_type='recent_days',
                    days=args.days,
                    stock_codes=args.stocks
                )
                
                if self.verbose:
                    print(f"📋 更新计划: {len(plan.get('tasks', []))} 个任务")
                
                # 执行更新
                result = self.update_manager.execute_update_plan(plan)
                
                if result.get('success'):
                    print("✅ 增量更新完成")
                    if self.verbose:
                        print(f"📊 更新统计: {result.get('statistics', {})}")
                else:
                    print("❌ 增量更新失败")
                    if result.get('error'):
                        print(f"错误: {result['error']}")
                        
        except Exception as e:
            print(f"❌ 数据操作失败: {str(e)}")
            raise
    
    def _handle_status(self, args):
        """处理状态命令"""
        try:
            if args.summary:
                self._print_system_summary()
            elif args.detailed:
                self._print_system_detailed()
            elif args.stocks:
                self._print_stocks_status(args.stocks)
            else:
                self._print_system_status()
                
        except Exception as e:
            print(f"❌ 状态查询失败: {str(e)}")
            raise
    
    def _handle_logs(self, args):
        """处理日志命令"""
        try:
            # 查询日志
            logs = self.logging_manager.query_logs(
                log_type=args.type,
                start_time=args.start_time,
                end_time=args.end_time,
                level=args.level,
                limit=args.limit
            )
            
            # 显示日志
            self._print_logs(logs)
            
            # 导出日志
            if args.export:
                if self.dry_run:
                    print(f"🔄 [预演] 导出日志到: {args.export}")
                    return
                
                result = self.logging_manager.export_logs(
                    output_file=args.export,
                    log_type=args.type,
                    start_time=args.start_time,
                    end_time=args.end_time
                )
                
                if result.get('success'):
                    print(f"✅ 日志已导出: {args.export}")
                else:
                    print(f"❌ 日志导出失败: {result.get('error')}")
                    
        except Exception as e:
            print(f"❌ 日志查询失败: {str(e)}")
            raise
    
    def _handle_config(self, args):
        """处理配置命令"""
        try:
            if args.config_action == 'get':
                value = self.config_manager.get(args.key)
                if value is not None:
                    print(f"{args.key}: {value}")
                else:
                    print(f"❌ 配置项不存在: {args.key}")
            
            elif args.config_action == 'set':
                if self.dry_run:
                    print(f"🔄 [预演] 设置配置: {args.key} = {args.value}")
                    return
                
                self.config_manager.set(args.key, args.value)
                print(f"✅ 配置已设置: {args.key} = {args.value}")
            
            elif args.config_action == 'list':
                config_info = self.config_manager.get_config_info()
                self._print_config_info(config_info)
            
            elif args.config_action == 'backup':
                if self.dry_run:
                    print("🔄 [预演] 备份配置")
                    return
                
                backup_file = self.config_manager.backup()
                print(f"✅ 配置已备份: {backup_file}")
            
            elif args.config_action == 'validate':
                is_valid, errors = self.config_manager.validate()
                if is_valid:
                    print("✅ 配置验证通过")
                else:
                    print("❌ 配置验证失败:")
                    for error in errors:
                        print(f"  - {error}")
                        
        except Exception as e:
            print(f"❌ 配置操作失败: {str(e)}")
            raise
    
    def _handle_report(self, args):
        """处理报告命令"""
        try:
            print(f"📊 生成{args.type}报告...")
            
            # 生成报告
            report = self._generate_report(args.type, args.start_date, args.end_date)
            
            # 显示报告
            self._print_report(report)
            
            # 保存报告
            if args.output:
                if self.dry_run:
                    print(f"🔄 [预演] 保存报告到: {args.output}")
                    return
                
                with open(args.output, 'w', encoding='utf-8') as f:
                    json.dump(report, f, ensure_ascii=False, indent=2)
                print(f"✅ 报告已保存: {args.output}")
                
        except Exception as e:
            print(f"❌ 报告生成失败: {str(e)}")
            raise
    
    def _handle_database(self, args):
        """处理数据库命令"""
        try:
            if args.db_action == 'info':
                info = self.db_manager.get_database_info()
                self._print_database_info(info)
            
            elif args.db_action == 'backup':
                if self.dry_run:
                    print("🔄 [预演] 备份数据库")
                    return
                
                backup_file = self.db_manager.backup_database()
                if backup_file:
                    print(f"✅ 数据库已备份: {backup_file}")
                else:
                    print("❌ 数据库备份失败")
            
            elif args.db_action == 'vacuum':
                if self.dry_run:
                    print("🔄 [预演] 优化数据库")
                    return
                
                if self.db_manager.vacuum_database():
                    print("✅ 数据库优化完成")
                else:
                    print("❌ 数据库优化失败")
            
            elif args.db_action == 'stats':
                stats = self._get_database_stats()
                self._print_database_stats(stats)
            
            elif args.db_action == 'query':
                if self.dry_run:
                    print(f"🔄 [预演] 执行查询: {args.sql}")
                    return
                
                results = self.db_manager.execute_query(args.sql)[:args.limit]
                self._print_query_results(results)
                
        except Exception as e:
            print(f"❌ 数据库操作失败: {str(e)}")
            raise
    
    def _print_scheduler_status(self, status: Dict[str, Any]):
        """打印调度器状态"""
        print(f"📋 调度器状态: {status['status']}")
        print(f"🔧 任务配置: {len(status['task_configs'])} 个任务")
        
        for task_type, config in status['task_configs'].items():
            enabled = "✅" if config.get('enabled', True) else "❌"
            print(f"  {enabled} {task_type}: {config.get('time', 'N/A')}")
        
        if status.get('current_task'):
            print(f"⏳ 当前任务: {status['current_task']}")
        
        if status.get('next_run'):
            print("⏰ 下次运行:")
            for task, next_time in status['next_run'].items():
                print(f"  {task}: {next_time}")
    
    def _print_task_history(self, history: List[Dict[str, Any]], task_type_filter: str = None):
        """打印任务历史"""
        if not history:
            print("📋 暂无任务历史")
            return
        
        print(f"📋 任务历史 (最近 {len(history)} 条):")
        
        for task in history:
            if task_type_filter and task['task_type'] != task_type_filter:
                continue
            
            status_icon = "✅" if task['status'] == 'completed' else "❌" if task['status'] == 'failed' else "⏳"
            duration = f"{task['execution_time']}s" if task['execution_time'] else "N/A"
            
            print(f"  {status_icon} {task['start_time']} - {task['task_type']} ({duration})")
            if task['error_message']:
                print(f"    错误: {task['error_message']}")
    
    def _print_task_config(self, task_type: str, config: Dict[str, Any]):
        """打印任务配置"""
        print(f"📋 任务配置: {task_type}")
        print(f"  时间: {config.get('time', 'N/A')}")
        print(f"  启用: {'是' if config.get('enabled', True) else '否'}")
        print(f"  描述: {config.get('description', 'N/A')}")
    
    def _print_integrity_report(self, report: Dict[str, Any]):
        """打印完整性报告"""
        print(f"🔍 数据完整性报告:")
        print(f"  检查项目: {len(report)}")
        
        for check_name, result in report.items():
            if isinstance(result, dict):
                issues = result.get('issues_found', 0)
                status_icon = "✅" if issues == 0 else "⚠️"
                print(f"  {status_icon} {check_name}: {issues} 个问题")
    
    def _print_system_summary(self):
        """打印系统摘要"""
        print("📊 系统摘要:")
        
        # 调度器状态
        scheduler_status = self.schedule_manager.get_schedule_status()
        print(f"  调度器: {scheduler_status['status']}")
        
        # 下载统计
        download_stats = self.status_manager.get_download_statistics()
        print(f"  下载统计: {download_stats}")
        
        # 数据库大小
        db_size = self.db_manager.get_database_size()
        print(f"  数据库大小: {db_size.get('size_mb', 0):.2f} MB")
    
    def _print_system_detailed(self):
        """打印系统详细信息"""
        print("📊 系统详细信息:")
        
        # 系统摘要
        self._print_system_summary()
        
        # 配置信息
        print("\n⚙️ 配置信息:")
        config_info = self.config_manager.get_config_info()
        print(f"  配置文件: {config_info.get('file_path')}")
        print(f"  修改时间: {config_info.get('last_modified')}")
        
        # 数据库信息
        print("\n📊 数据库信息:")
        db_info = self.db_manager.get_database_info()
        print(f"  数据库文件: {db_info.get('file_path')}")
        print(f"  文件大小: {db_info.get('file_size_mb', 0):.2f} MB")
        print(f"  表数量: {db_info.get('table_count', 0)}")
    
    def _print_stocks_status(self, stocks: List[str]):
        """打印股票状态"""
        print(f"📊 股票状态: {len(stocks)} 只股票")
        
        for stock_code in stocks:
            status = self.status_manager.get_stock_status(stock_code)
            if status:
                print(f"  {stock_code}: {status.get('status', 'unknown')}")
            else:
                print(f"  {stock_code}: 未找到")
    
    def _print_system_status(self):
        """打印系统状态"""
        print("📊 系统状态:")
        
        # 调度器状态
        scheduler_status = self.schedule_manager.get_schedule_status()
        print(f"  📋 调度器: {scheduler_status['status']}")
        
        # 数据库状态
        db_info = self.db_manager.get_database_info()
        print(f"  📊 数据库: {db_info.get('file_size_mb', 0):.2f} MB")
        
        # API状态
        try:
            api_status = self.api_manager.check_connection()
            print(f"  🔗 API: {'连接正常' if api_status else '连接异常'}")
        except Exception:
            print(f"  🔗 API: 连接异常")
    
    def _print_logs(self, logs: Dict[str, Any]):
        """打印日志"""
        if not logs.get('logs'):
            print("📋 暂无日志")
            return
        
        print(f"📋 日志 (共 {len(logs['logs'])} 条):")
        
        for log in logs['logs']:
            level_icon = {
                'info': 'ℹ️',
                'warning': '⚠️',
                'error': '❌',
                'critical': '🚨',
                'debug': '🐛'
            }.get(log.get('level', 'info'), 'ℹ️')
            
            print(f"  {level_icon} {log.get('timestamp')} - {log.get('message')}")
            if self.verbose and log.get('context'):
                print(f"    上下文: {log['context']}")
    
    def _print_config_info(self, config_info: Dict[str, Any]):
        """打印配置信息"""
        print("⚙️ 配置信息:")
        print(f"  配置文件: {config_info.get('file_path')}")
        print(f"  修改时间: {config_info.get('last_modified')}")
        print(f"  文件大小: {config_info.get('file_size', 0)} bytes")
        print(f"  备份数量: {config_info.get('backup_count', 0)}")
    
    def _print_report(self, report: Dict[str, Any]):
        """打印报告"""
        print("📊 报告:")
        print(f"  生成时间: {report.get('generated_at')}")
        print(f"  报告类型: {report.get('type')}")
        print(f"  时间范围: {report.get('period')}")
        
        if report.get('summary'):
            print("  摘要:")
            for key, value in report['summary'].items():
                print(f"    {key}: {value}")
    
    def _print_database_info(self, info: Dict[str, Any]):
        """打印数据库信息"""
        print("📊 数据库信息:")
        print(f"  文件路径: {info.get('file_path')}")
        print(f"  文件大小: {info.get('file_size_mb', 0):.2f} MB")
        print(f"  表数量: {info.get('table_count', 0)}")
        print(f"  连接数: {info.get('connection_count', 0)}")
        print(f"  WAL模式: {'是' if info.get('wal_mode') else '否'}")
    
    def _print_database_stats(self, stats: Dict[str, Any]):
        """打印数据库统计"""
        print("📊 数据库统计:")
        for table, table_stats in stats.items():
            print(f"  {table}:")
            print(f"    记录数: {table_stats.get('count', 0)}")
            print(f"    大小: {table_stats.get('size_mb', 0):.2f} MB")
    
    def _print_query_results(self, results: List[Any]):
        """打印查询结果"""
        if not results:
            print("📋 查询结果为空")
            return
        
        print(f"📋 查询结果 (共 {len(results)} 条):")
        
        for i, row in enumerate(results, 1):
            print(f"  {i}. {row}")
    
    def _generate_report(self, report_type: str, start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """生成报告"""
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        if not start_date:
            if report_type == 'daily':
                start_date = end_date
            elif report_type == 'weekly':
                start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
            elif report_type == 'monthly':
                start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            else:
                start_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        # 生成报告内容
        report = {
            'type': report_type,
            'period': f"{start_date} to {end_date}",
            'generated_at': datetime.now().isoformat(),
            'summary': {
                'total_stocks': 0,
                'total_records': 0,
                'download_success': 0,
                'download_failed': 0
            }
        }
        
        # 添加具体统计
        try:
            # 下载统计
            download_stats = self.status_manager.get_download_statistics()
            report['download_stats'] = download_stats
            
            # 任务统计
            task_history = self.schedule_manager.get_task_history(100)
            report['task_stats'] = {
                'total_tasks': len(task_history),
                'completed_tasks': len([t for t in task_history if t['status'] == 'completed']),
                'failed_tasks': len([t for t in task_history if t['status'] == 'failed'])
            }
            
            # 数据库统计
            db_stats = self._get_database_stats()
            report['database_stats'] = db_stats
            
        except Exception as e:
            report['error'] = str(e)
        
        return report
    
    def _get_database_stats(self) -> Dict[str, Any]:
        """获取数据库统计"""
        stats = {}
        
        try:
            tables = ['stocks', 'daily_data', 'download_status', 'api_call_log']
            
            for table in tables:
                try:
                    table_stats = self.db_manager.get_table_statistics(table)
                    stats[table] = table_stats
                except Exception:
                    stats[table] = {'count': 0, 'size_mb': 0}
        except Exception:
            pass
        
        return stats
    
    def _signal_handler(self, signum, frame):
        """信号处理器"""
        print(f"\n🛑 收到信号 {signum}，正在退出...")
        
        # 如果调度器正在运行，停止它
        if self.schedule_manager and self.schedule_manager.status == ScheduleStatus.RUNNING:
            self.schedule_manager.stop_scheduler()
        
        sys.exit(0)


def main():
    """主函数"""
    cli = CommandLineInterface()
    cli.run()


if __name__ == "__main__":
    main() 