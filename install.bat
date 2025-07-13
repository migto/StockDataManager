@echo off
echo ================================
echo A股日线数据下载系统安装脚本
echo ================================

echo 1. 检查Python版本...
python --version
if %errorlevel% neq 0 (
    echo 错误：未找到Python，请先安装Python 3.8或更高版本
    pause
    exit /b 1
)

echo 2. 升级pip...
python -m pip install --upgrade pip

echo 3. 安装项目依赖...
pip install -r requirements.txt

echo 4. 创建数据目录...
if not exist "data\backup" mkdir data\backup
if not exist "logs\archive" mkdir logs\archive

echo 5. 安装完成！
echo 请运行以下命令配置您的Tushare Pro API Token：
echo python src/config_manager.py --setup
echo.
echo 然后运行以下命令初始化数据库：
echo python src/database_manager.py --init
echo.
echo 最后运行以下命令开始下载数据：
echo python main.py
echo.
pause 