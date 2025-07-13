"""
A股日线数据下载系统测试包
Stock Daily Data Download System Tests

包含以下测试模块：
- test_config_manager: 配置管理器测试
- test_database_manager: 数据库管理器测试
- test_tushare_api_manager: Tushare API管理器测试
- test_download_scheduler: 下载调度器测试
- test_data_processor: 数据处理器测试
- test_stock_info_manager: 股票信息管理器测试
"""

import sys
import os

# 将src目录添加到Python路径中，以便测试能够导入源代码模块
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src')) 