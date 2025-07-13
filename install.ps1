# A股日线数据下载系统安装脚本 (PowerShell版本)

Write-Host "================================" -ForegroundColor Green
Write-Host "A股日线数据下载系统安装脚本" -ForegroundColor Green
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

# 安装项目依赖
Write-Host "3. 安装项目依赖..." -ForegroundColor Yellow
pip install -r requirements.txt

# 创建必要的目录
Write-Host "4. 创建数据目录..." -ForegroundColor Yellow
if (-not (Test-Path "data\backup")) {
    New-Item -ItemType Directory -Path "data\backup" | Out-Null
}
if (-not (Test-Path "logs\archive")) {
    New-Item -ItemType Directory -Path "logs\archive" | Out-Null
}

# 初始化配置文件
Write-Host "5. 初始化配置文件..." -ForegroundColor Yellow
try {
    $configInit = python src/config_manager.py init 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "配置文件初始化成功" -ForegroundColor Green
    } else {
        Write-Host "配置文件初始化失败" -ForegroundColor Red
        Write-Host $configInit -ForegroundColor Red
    }
} catch {
    Write-Host "警告：配置文件初始化失败，请手动运行 'python src/config_manager.py init'" -ForegroundColor Yellow
}

# 初始化数据库
Write-Host "6. 初始化数据库..." -ForegroundColor Yellow
try {
    $dbInit = python src/database_manager.py --init 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "数据库初始化成功" -ForegroundColor Green
        
        # 验证数据库结构
        Write-Host "7. 验证数据库结构..." -ForegroundColor Yellow
        $dbValidate = python src/database_schema_validator.py 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Host "数据库验证完成" -ForegroundColor Green
        } else {
            Write-Host "数据库验证失败，但不影响使用" -ForegroundColor Yellow
        }
    } else {
        Write-Host "数据库初始化失败" -ForegroundColor Red
        Write-Host $dbInit -ForegroundColor Red
    }
} catch {
    Write-Host "警告：数据库初始化失败，请手动运行 'python src/database_manager.py --init'" -ForegroundColor Yellow
}

# 安装完成
Write-Host "8. 安装完成！" -ForegroundColor Green
Write-Host ""
Write-Host "接下来的步骤：" -ForegroundColor Cyan
Write-Host "1. 配置Tushare Pro API Token：" -ForegroundColor White
Write-Host "   python src/config_manager.py set tushare.token YOUR_TOKEN_HERE" -ForegroundColor Gray
Write-Host ""
Write-Host "2. 查看数据库信息：" -ForegroundColor White
Write-Host "   python src/database_manager.py --info" -ForegroundColor Gray
Write-Host ""
Write-Host "3. 开始下载数据：" -ForegroundColor White
Write-Host "   python main.py" -ForegroundColor Gray
Write-Host ""
Read-Host "按任意键退出" 