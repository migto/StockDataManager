"""
定时任务调度管理器
实现每日自动下载任务调度和管理
"""

import schedule
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from enum import Enum
import json
import signal
import sys
import os

# 导入所有需要的管理器
from .config_manager import ConfigManager
from .database_manager import DatabaseManager
from .logging_manager import LoggingManager, LogLevel, LogType
from .smart_download_manager import SmartDownloadManager
from .error_handler_retry_manager import ErrorHandlerRetryManager, ErrorType
from .download_status_manager import DownloadStatusManager
from .data_integrity_manager import DataIntegrityManager
from .optimized_tushare_api_manager import OptimizedTushareAPIManager


class ScheduleStatus(Enum):
    """调度状态枚举"""
    STOPPED = "stopped"
    RUNNING = "running"
    PAUSED = "paused"
    ERROR = "error"


class TaskType(Enum):
    """任务类型枚举"""
    DAILY_DOWNLOAD = "daily_download"
    WEEKLY_CLEANUP = "weekly_cleanup"
    MONTHLY_REPORT = "monthly_report"
    INTEGRITY_CHECK = "integrity_check"
    STATUS_UPDATE = "status_update"


class ScheduleManager:
    """定时任务调度管理器"""
    
    def __init__(self, config_manager: ConfigManager):
        """
        初始化调度管理器
        
        Args:
            config_manager: 配置管理器
        """
        self.config_manager = config_manager
        self.db_manager = DatabaseManager(config_manager.get('database.path', 'data/stock_data.db'))
        self.logging_manager = LoggingManager(config_manager)
        self.api_manager = OptimizedTushareAPIManager(config_manager)
        self.download_manager = SmartDownloadManager(config_manager)
        self.error_handler = ErrorHandlerRetryManager(config_manager)
        self.status_manager = DownloadStatusManager(config_manager)
        self.integrity_manager = DataIntegrityManager(config_manager)
        
        # 调度状态
        self.status = ScheduleStatus.STOPPED
        self.schedule_thread: Optional[threading.Thread] = None
        self.stop_event = threading.Event()
        
        # 任务记录
        self.task_history: List[Dict[str, Any]] = []
        self.current_task: Optional[Dict[str, Any]] = None
        
        # 任务配置
        self.task_configs = {
            TaskType.DAILY_DOWNLOAD: {
                'time': '09:00',
                'enabled': True,
                'function': self._daily_download_task,
                'description': '每日股票数据下载任务'
            },
            TaskType.WEEKLY_CLEANUP: {
                'time': '02:00',
                'weekday': 1,  # 星期一
                'enabled': True,
                'function': self._weekly_cleanup_task,
                'description': '每周数据清理任务'
            },
            TaskType.MONTHLY_REPORT: {
                'time': '03:00',
                'day': 1,  # 每月1号
                'enabled': True,
                'function': self._monthly_report_task,
                'description': '每月数据报告任务'
            },
            TaskType.INTEGRITY_CHECK: {
                'time': '01:00',
                'enabled': True,
                'function': self._integrity_check_task,
                'description': '数据完整性检查任务'
            }
        }
        
        # 初始化数据库表
        self._init_schedule_tables()
        
        # 设置信号处理器
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _init_schedule_tables(self):
        """初始化调度相关数据库表"""
        try:
            # 创建任务记录表
            self.db_manager.execute_update("""
                CREATE TABLE IF NOT EXISTS schedule_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_type TEXT NOT NULL,
                    task_name TEXT NOT NULL,
                    start_time TIMESTAMP NOT NULL,
                    end_time TIMESTAMP,
                    status TEXT DEFAULT 'running',
                    result TEXT,
                    error_message TEXT,
                    execution_time_seconds INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 创建索引
            self.db_manager.execute_update("""
                CREATE INDEX IF NOT EXISTS idx_schedule_tasks_type 
                ON schedule_tasks(task_type)
            """)
            
            self.db_manager.execute_update("""
                CREATE INDEX IF NOT EXISTS idx_schedule_tasks_time 
                ON schedule_tasks(start_time)
            """)
            
            # 创建任务配置表
            self.db_manager.execute_update("""
                CREATE TABLE IF NOT EXISTS schedule_config (
                    task_type TEXT PRIMARY KEY,
                    config_json TEXT NOT NULL,
                    enabled BOOLEAN DEFAULT 1,
                    last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            self.logging_manager.log_system_event(
                "schedule_init",
                "调度数据库表初始化完成",
                LogLevel.INFO,
                {"tables": ["schedule_tasks", "schedule_config"]}
            )
            
        except Exception as e:
            self.logging_manager.log_system_event(
                "schedule_init",
                f"调度数据库表初始化失败: {str(e)}",
                LogLevel.ERROR,
                {"error": str(e)}
            )
    
    def start_scheduler(self):
        """启动调度器"""
        if self.status == ScheduleStatus.RUNNING:
            self.logging_manager.log_system_event(
                "scheduler_warning",
                "调度器已经在运行中",
                LogLevel.WARNING
            )
            return
        
        try:
            self.status = ScheduleStatus.RUNNING
            self.stop_event.clear()
            
            # 清除所有已有任务
            schedule.clear()
            
            # 添加调度任务
            self._setup_tasks()
            
            # 启动调度线程
            self.schedule_thread = threading.Thread(
                target=self._schedule_loop,
                daemon=True
            )
            self.schedule_thread.start()
            
            self.logging_manager.log_system_event(
                "scheduler_start",
                "调度器启动成功",
                LogLevel.INFO,
                {"status": self.status.value}
            )
            
        except Exception as e:
            self.status = ScheduleStatus.ERROR
            self.logging_manager.log_system_event(
                "scheduler_start",
                f"调度器启动失败: {str(e)}",
                LogLevel.ERROR,
                {"error": str(e)}
            )
            raise
    
    def stop_scheduler(self):
        """停止调度器"""
        if self.status == ScheduleStatus.STOPPED:
            self.logging_manager.log_system_event(
                "scheduler_warning",
                "调度器已经停止",
                LogLevel.WARNING
            )
            return
        
        try:
            self.status = ScheduleStatus.STOPPED
            self.stop_event.set()
            
            # 清除所有任务
            schedule.clear()
            
            # 等待线程结束
            if self.schedule_thread and self.schedule_thread.is_alive():
                self.schedule_thread.join(timeout=10)
            
            self.logging_manager.log_system_event(
                "scheduler_stop",
                "调度器停止成功",
                LogLevel.INFO,
                {"status": self.status.value}
            )
            
        except Exception as e:
            self.status = ScheduleStatus.ERROR
            self.logging_manager.log_system_event(
                "scheduler_stop",
                f"调度器停止失败: {str(e)}",
                LogLevel.ERROR,
                {"error": str(e)}
            )
            raise
    
    def pause_scheduler(self):
        """暂停调度器"""
        if self.status == ScheduleStatus.RUNNING:
            self.status = ScheduleStatus.PAUSED
            self.logging_manager.log_system_event(
                "scheduler_pause",
                "调度器已暂停",
                LogLevel.INFO
            )
    
    def resume_scheduler(self):
        """恢复调度器"""
        if self.status == ScheduleStatus.PAUSED:
            self.status = ScheduleStatus.RUNNING
            self.logging_manager.log_system_event(
                "scheduler_resume",
                "调度器已恢复",
                LogLevel.INFO
            )
    
    def _setup_tasks(self):
        """设置调度任务"""
        for task_type, config in self.task_configs.items():
            if not config.get('enabled', True):
                continue
            
            try:
                # 每日任务
                if task_type == TaskType.DAILY_DOWNLOAD:
                    schedule.every().day.at(config['time']).do(
                        self._run_task_with_error_handling,
                        task_type,
                        config['function']
                    )
                
                # 每周任务
                elif task_type == TaskType.WEEKLY_CLEANUP:
                    if config['weekday'] == 1:  # 星期一
                        schedule.every().monday.at(config['time']).do(
                            self._run_task_with_error_handling,
                            task_type,
                            config['function']
                        )
                
                # 每月任务
                elif task_type == TaskType.MONTHLY_REPORT:
                    # 每天检查是否是月初
                    schedule.every().day.at(config['time']).do(
                        self._check_monthly_task,
                        task_type,
                        config['function']
                    )
                
                # 每日任务
                elif task_type == TaskType.INTEGRITY_CHECK:
                    schedule.every().day.at(config['time']).do(
                        self._run_task_with_error_handling,
                        task_type,
                        config['function']
                    )
                
                self.logging_manager.log_system_event(
                    "task_setup",
                    f"任务 {task_type.value} 设置成功",
                    LogLevel.INFO,
                    {"task_type": task_type.value, "config": config}
                )
                
            except Exception as e:
                self.logging_manager.log_system_event(
                    "task_setup",
                    f"任务 {task_type.value} 设置失败: {str(e)}",
                    LogLevel.ERROR,
                    {"task_type": task_type.value, "error": str(e)}
                )
    
    def _schedule_loop(self):
        """调度循环"""
        while not self.stop_event.is_set():
            try:
                if self.status == ScheduleStatus.RUNNING:
                    schedule.run_pending()
                
                # 等待1秒，避免CPU占用过高
                time.sleep(1)
                
            except Exception as e:
                self.logging_manager.log_system_event(
                    "scheduler_loop",
                    f"调度循环出错: {str(e)}",
                    LogLevel.ERROR,
                    {"error": str(e)}
                )
                time.sleep(5)  # 出错后等待5秒
    
    def _run_task_with_error_handling(self, task_type: TaskType, task_function: Callable):
        """带错误处理的任务执行"""
        task_id = None
        start_time = datetime.now()
        
        try:
            # 记录任务开始
            task_id = self._record_task_start(task_type, start_time)
            
            self.logging_manager.log_system_event(
                "task_start",
                f"开始执行任务: {task_type.value}",
                LogLevel.INFO,
                {"task_type": task_type.value, "task_id": task_id}
            )
            
            # 执行任务
            result = task_function()
            
            # 记录任务完成
            end_time = datetime.now()
            execution_time = int((end_time - start_time).total_seconds())
            
            self._record_task_end(task_id, 'completed', result, execution_time)
            
            self.logging_manager.log_system_event(
                "task_complete",
                f"任务 {task_type.value} 执行完成",
                LogLevel.INFO,
                {
                    "task_type": task_type.value,
                    "task_id": task_id,
                    "execution_time": execution_time,
                    "result": result
                }
            )
            
        except Exception as e:
            # 记录任务错误
            end_time = datetime.now()
            execution_time = int((end_time - start_time).total_seconds())
            
            if task_id:
                self._record_task_end(task_id, 'failed', None, execution_time, str(e))
            
            self.logging_manager.log_system_event(
                "task_failed",
                f"任务 {task_type.value} 执行失败: {str(e)}",
                LogLevel.ERROR,
                {
                    "task_type": task_type.value,
                    "task_id": task_id,
                    "execution_time": execution_time,
                    "error": str(e)
                }
            )
            
            # 记录错误到错误处理器
            self.error_handler.record_error(
                ErrorType.SYSTEM,
                str(e),
                {"task_type": task_type.value, "task_id": task_id}
            )
    
    def _check_monthly_task(self, task_type: TaskType, task_function: Callable):
        """检查是否需要执行月度任务"""
        today = datetime.now().date()
        if today.day == 1:  # 每月1号
            self._run_task_with_error_handling(task_type, task_function)
    
    def _record_task_start(self, task_type: TaskType, start_time: datetime) -> int:
        """记录任务开始"""
        task_name = self.task_configs[task_type]['description']
        
        task_id = self.db_manager.execute_insert(
            """
            INSERT INTO schedule_tasks 
            (task_type, task_name, start_time, status) 
            VALUES (?, ?, ?, 'running')
            """,
            (task_type.value, task_name, start_time)
        )
        
        return task_id
    
    def _record_task_end(
        self, 
        task_id: int, 
        status: str, 
        result: Any, 
        execution_time: int, 
        error_message: str = None
    ):
        """记录任务结束"""
        result_json = json.dumps(result) if result else None
        
        self.db_manager.execute_update(
            """
            UPDATE schedule_tasks 
            SET end_time = ?, status = ?, result = ?, 
                error_message = ?, execution_time_seconds = ?
            WHERE id = ?
            """,
            (
                datetime.now(),
                status,
                result_json,
                error_message,
                execution_time,
                task_id
            )
        )
    
    def _daily_download_task(self) -> Dict[str, Any]:
        """每日下载任务"""
        try:
            # 分析下载需求
            download_analysis = self.download_manager.analyze_download_requirements()
            
            # 如果没有需要下载的数据，直接返回
            if not download_analysis.get('stocks_need_download'):
                return {
                    "status": "success",
                    "message": "没有需要下载的数据",
                    "analysis": download_analysis
                }
            
            # 创建下载计划
            download_plan = self.download_manager.create_download_plan(
                download_type='recent_days',
                max_days=1  # 只下载最近1天的数据
            )
            
            # 执行下载计划
            download_result = self.download_manager.execute_download_plan(download_plan)
            
            # 更新状态
            self.status_manager.update_download_progress()
            
            return {
                "status": "success",
                "message": "每日下载任务完成",
                "analysis": download_analysis,
                "plan": download_plan,
                "result": download_result
            }
            
        except Exception as e:
            self.logging_manager.log_system_event(
                "daily_download_error",
                f"每日下载任务执行失败: {str(e)}",
                LogLevel.ERROR,
                {"error": str(e)}
            )
            raise
    
    def _weekly_cleanup_task(self) -> Dict[str, Any]:
        """每周清理任务"""
        try:
            # 清理过期日志
            log_cleanup = self.logging_manager.cleanup_old_logs(days=30)
            
            # 清理错误记录
            error_cleanup = self.error_handler.cleanup_old_errors(days=30)
            
            # 清理任务记录
            task_cleanup = self._cleanup_old_tasks(days=90)
            
            # 数据库优化
            self.db_manager.vacuum_database()
            
            return {
                "status": "success",
                "message": "每周清理任务完成",
                "log_cleanup": log_cleanup,
                "error_cleanup": error_cleanup,
                "task_cleanup": task_cleanup
            }
            
        except Exception as e:
            self.logging_manager.log_system_event(
                "weekly_cleanup_error",
                f"每周清理任务执行失败: {str(e)}",
                LogLevel.ERROR,
                {"error": str(e)}
            )
            raise
    
    def _monthly_report_task(self) -> Dict[str, Any]:
        """每月报告任务"""
        try:
            # 生成月度统计报告
            end_date = datetime.now().date()
            start_date = end_date.replace(day=1)
            
            # 下载统计
            download_stats = self.status_manager.get_download_statistics()
            
            # 错误统计
            error_stats = self.error_handler.get_error_statistics()
            
            # 任务统计
            task_stats = self._get_task_statistics(start_date, end_date)
            
            # 数据完整性统计
            integrity_stats = self.integrity_manager.get_integrity_statistics()
            
            report = {
                "period": f"{start_date} to {end_date}",
                "download_stats": download_stats,
                "error_stats": error_stats,
                "task_stats": task_stats,
                "integrity_stats": integrity_stats
            }
            
            # 保存报告
            self._save_monthly_report(report, start_date)
            
            return {
                "status": "success",
                "message": "每月报告任务完成",
                "report": report
            }
            
        except Exception as e:
            self.logging_manager.log_system_event(
                "monthly_report_error",
                f"每月报告任务执行失败: {str(e)}",
                LogLevel.ERROR,
                {"error": str(e)}
            )
            raise
    
    def _integrity_check_task(self) -> Dict[str, Any]:
        """数据完整性检查任务"""
        try:
            # 检查数据完整性
            integrity_report = self.integrity_manager.check_data_integrity()
            
            # 如果有问题，尝试修复
            if integrity_report.get('issues_found', 0) > 0:
                repair_result = self.integrity_manager.repair_data_issues()
                return {
                    "status": "success",
                    "message": "数据完整性检查完成，发现问题并已修复",
                    "integrity_report": integrity_report,
                    "repair_result": repair_result
                }
            else:
                return {
                    "status": "success",
                    "message": "数据完整性检查完成，没有发现问题",
                    "integrity_report": integrity_report
                }
                
        except Exception as e:
            self.logging_manager.log_system_event(
                "integrity_check_error",
                f"数据完整性检查任务执行失败: {str(e)}",
                LogLevel.ERROR,
                {"error": str(e)}
            )
            raise
    
    def _cleanup_old_tasks(self, days: int) -> Dict[str, Any]:
        """清理旧任务记录"""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        # 统计要删除的记录数
        count = self.db_manager.execute_query(
            "SELECT COUNT(*) FROM schedule_tasks WHERE created_at < ?",
            (cutoff_date,)
        )[0][0]
        
        # 删除旧记录
        self.db_manager.execute_update(
            "DELETE FROM schedule_tasks WHERE created_at < ?",
            (cutoff_date,)
        )
        
        return {
            "deleted_count": count,
            "cutoff_date": cutoff_date.isoformat()
        }
    
    def _get_task_statistics(self, start_date, end_date) -> Dict[str, Any]:
        """获取任务统计信息"""
        stats = {}
        
        # 按任务类型统计
        results = self.db_manager.execute_query(
            """
            SELECT task_type, status, COUNT(*) as count,
                   AVG(execution_time_seconds) as avg_time
            FROM schedule_tasks 
            WHERE DATE(start_time) BETWEEN ? AND ?
            GROUP BY task_type, status
            """,
            (start_date, end_date)
        )
        
        for row in results:
            task_type = row[0]
            status = row[1]
            count = row[2]
            avg_time = row[3]
            
            if task_type not in stats:
                stats[task_type] = {}
            
            stats[task_type][status] = {
                "count": count,
                "avg_execution_time": avg_time
            }
        
        return stats
    
    def _save_monthly_report(self, report: Dict[str, Any], month_date):
        """保存月度报告"""
        # 创建报告表（如果不存在）
        self.db_manager.execute_update("""
            CREATE TABLE IF NOT EXISTS monthly_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                report_month DATE NOT NULL,
                report_data TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 保存报告
        self.db_manager.execute_insert(
            "INSERT INTO monthly_reports (report_month, report_data) VALUES (?, ?)",
            (month_date, json.dumps(report))
        )
    
    def get_schedule_status(self) -> Dict[str, Any]:
        """获取调度状态"""
        return {
            "status": self.status.value,
            "current_task": self.current_task,
            "next_run": self._get_next_run_times(),
            "task_configs": {
                task_type.value: config 
                for task_type, config in self.task_configs.items()
            }
        }
    
    def _get_next_run_times(self) -> Dict[str, str]:
        """获取下次运行时间"""
        next_runs = {}
        
        for job in schedule.jobs:
            if hasattr(job, 'start_day'):
                next_runs[job.job_func.__name__] = str(job.next_run)
        
        return next_runs
    
    def get_task_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        """获取任务历史"""
        results = self.db_manager.execute_query(
            """
            SELECT task_type, task_name, start_time, end_time, status, 
                   execution_time_seconds, error_message
            FROM schedule_tasks 
            ORDER BY start_time DESC 
            LIMIT ?
            """,
            (limit,)
        )
        
        history = []
        for row in results:
            history.append({
                "task_type": row[0],
                "task_name": row[1],
                "start_time": row[2],
                "end_time": row[3],
                "status": row[4],
                "execution_time": row[5],
                "error_message": row[6]
            })
        
        return history
    
    def update_task_config(self, task_type: TaskType, config: Dict[str, Any]):
        """更新任务配置"""
        if task_type in self.task_configs:
            self.task_configs[task_type].update(config)
            
            # 保存到数据库
            self.db_manager.execute_update(
                """
                INSERT OR REPLACE INTO schedule_config 
                (task_type, config_json, enabled) 
                VALUES (?, ?, ?)
                """,
                (
                    task_type.value,
                    json.dumps(config),
                    config.get('enabled', True)
                )
            )
            
            # 如果调度器正在运行，重新设置任务
            if self.status == ScheduleStatus.RUNNING:
                schedule.clear()
                self._setup_tasks()
    
    def run_task_immediately(self, task_type: TaskType) -> Dict[str, Any]:
        """立即运行指定任务"""
        if task_type not in self.task_configs:
            raise ValueError(f"未知的任务类型: {task_type}")
        
        task_function = self.task_configs[task_type]['function']
        
        # 在新线程中运行任务
        task_thread = threading.Thread(
            target=self._run_task_with_error_handling,
            args=(task_type, task_function)
        )
        task_thread.start()
        
        return {
            "status": "started",
            "message": f"任务 {task_type.value} 已开始执行",
            "task_type": task_type.value
        }
    
    def _signal_handler(self, signum, frame):
        """信号处理器"""
        self.logging_manager.log_system_event(
            "signal_handler",
            f"收到信号 {signum}，正在优雅关闭...",
            LogLevel.INFO
        )
        
        self.stop_scheduler()
        sys.exit(0)
    
    def __del__(self):
        """析构函数"""
        if hasattr(self, 'status') and self.status == ScheduleStatus.RUNNING:
            self.stop_scheduler()


def main():
    """主函数 - 命令行界面"""
    import argparse
    
    parser = argparse.ArgumentParser(description='股票数据下载调度器')
    parser.add_argument('--config', default='config.json', help='配置文件路径')
    parser.add_argument('--start', action='store_true', help='启动调度器')
    parser.add_argument('--stop', action='store_true', help='停止调度器')
    parser.add_argument('--status', action='store_true', help='查看调度状态')
    parser.add_argument('--history', type=int, default=10, help='查看任务历史')
    parser.add_argument('--run-task', choices=[t.value for t in TaskType], help='立即运行指定任务')
    
    args = parser.parse_args()
    
    # 初始化配置
    config_manager = ConfigManager(args.config)
    schedule_manager = ScheduleManager(config_manager)
    
    try:
        if args.start:
            print("启动调度器...")
            schedule_manager.start_scheduler()
            print("调度器启动成功，按 Ctrl+C 停止")
            
            # 保持运行
            while True:
                time.sleep(1)
        
        elif args.stop:
            print("停止调度器...")
            schedule_manager.stop_scheduler()
            print("调度器已停止")
        
        elif args.status:
            status = schedule_manager.get_schedule_status()
            print(f"调度状态: {status}")
        
        elif args.history:
            history = schedule_manager.get_task_history(args.history)
            print("任务历史:")
            for task in history:
                print(f"  {task['start_time']} - {task['task_type']}: {task['status']}")
        
        elif args.run_task:
            task_type = TaskType(args.run_task)
            result = schedule_manager.run_task_immediately(task_type)
            print(f"任务执行结果: {result}")
        
        else:
            parser.print_help()
            
    except KeyboardInterrupt:
        print("\n收到中断信号，正在停止...")
        schedule_manager.stop_scheduler()
    except Exception as e:
        print(f"执行出错: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main() 