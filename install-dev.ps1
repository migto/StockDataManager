# A股日线数据下载系统开发环境安装脚本 (PowerShell版本)

Write-Host "================================" -ForegroundColor Green
Write-Host "开发环境安装脚本" -ForegroundColor Green
Write-Host "================================" -ForegroundColor Green

# 检查Python版本
Write-Host "1. 检查Python版本..." -ForegroundColor Yellow
try {
    $pythonVersion = python --version 2>&1
    Write-Host "找到Python: $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "错误：未找到Python，请先安装Python 3.8或更高版本" -ForegroundColor Red
    Read-Host "按任意键退出"
    exit 1
}

# 升级pip
Write-Host "2. 升级pip..." -ForegroundColor Yellow
python -m pip install --upgrade pip

# 安装开发环境依赖
Write-Host "3. 安装开发环境依赖..." -ForegroundColor Yellow
pip install -r requirements-dev.txt

# 创建必要的目录
Write-Host "4. 创建开发目录..." -ForegroundColor Yellow
if (-not (Test-Path "data\backup")) {
    New-Item -ItemType Directory -Path "data\backup" | Out-Null
}
if (-not (Test-Path "logs\archive")) {
    New-Item -ItemType Directory -Path "logs\archive" | Out-Null
}

# 配置pre-commit hooks
Write-Host "5. 配置pre-commit hooks..." -ForegroundColor Yellow
try {
    pre-commit install
    Write-Host "pre-commit hooks 配置成功" -ForegroundColor Green
} catch {
    Write-Host "警告：pre-commit hooks 配置失败，请手动运行 'pre-commit install'" -ForegroundColor Yellow
}

# 安装完成
Write-Host "6. 开发环境安装完成！" -ForegroundColor Green
Write-Host ""
Write-Host "开发环境功能：" -ForegroundColor Cyan
Write-Host "• 代码格式化：black ." -ForegroundColor Gray
Write-Host "• 代码检查：flake8 src/" -ForegroundColor Gray
Write-Host "• 导入排序：isort src/" -ForegroundColor Gray
Write-Host "• 类型检查：mypy src/" -ForegroundColor Gray
Write-Host "• 运行测试：pytest tests/" -ForegroundColor Gray
Write-Host "• 测试覆盖率：pytest tests/ --cov=src/" -ForegroundColor Gray
Write-Host "• 启动Jupyter：jupyter notebook" -ForegroundColor Gray
Write-Host ""
Write-Host "接下来的步骤：" -ForegroundColor Cyan
Write-Host "1. 配置Tushare Pro API Token：" -ForegroundColor White
Write-Host "   python src/config_manager.py --setup" -ForegroundColor Gray
Write-Host ""
Write-Host "2. 初始化数据库：" -ForegroundColor White
Write-Host "   python src/database_manager.py --init" -ForegroundColor Gray
Write-Host ""
Read-Host "按任意键退出" 