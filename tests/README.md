# 测试目录

此目录用于存储单元测试和集成测试文件。

## 文件说明

- `test_config_manager.py` - 配置管理器测试
- `test_database_manager.py` - 数据库管理器测试
- `test_tushare_api_manager.py` - Tushare API管理器测试
- `test_download_scheduler.py` - 下载调度器测试
- `test_data_processor.py` - 数据处理器测试
- `test_stock_info_manager.py` - 股票信息管理器测试
- `__init__.py` - 测试包初始化文件

## 测试框架

使用 pytest 作为测试框架：

```bash
# 运行所有测试
pytest tests/

# 运行单个测试文件
pytest tests/test_database_manager.py

# 运行测试并显示覆盖率
pytest tests/ --cov=src/
```

## 测试原则

- 每个模块都应该有对应的测试文件
- 测试应该覆盖正常情况和异常情况
- 使用mock对象模拟外部依赖（API调用、数据库操作等）
- 测试数据不应该影响真实数据

## 注意事项

- 测试文件应该独立运行，不依赖测试执行顺序
- 使用临时数据库进行数据库相关测试
- API测试使用mock数据，避免消耗真实API调用次数 