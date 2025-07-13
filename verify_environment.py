#!/usr/bin/env python
"""
Python环境验证脚本
验证A股日线数据下载系统的依赖是否正确安装
"""

import sys
import importlib
import platform
from typing import List, Tuple, Dict


def check_python_version() -> Tuple[bool, str]:
    """检查Python版本是否符合要求"""
    version = sys.version_info
    required_version = (3, 8)
    
    if version >= required_version:
        return True, f"Python {version.major}.{version.minor}.{version.micro} ✓"
    else:
        return False, f"Python {version.major}.{version.minor}.{version.micro} ✗ (需要 >= 3.8)"


def check_required_packages() -> List[Tuple[str, bool, str]]:
    """检查必需的包是否已安装"""
    required_packages = [
        ('tushare', '>=1.2.89'),
        ('pandas', '>=1.3.0'),
        ('numpy', '>=1.20.0'),
        ('schedule', '>=1.1.0'),
        ('python-dateutil', '>=2.8.0'),
        ('pytz', '>=2021.1'),
        ('requests', '>=2.25.0'),
    ]
    
    results = []
    for package, min_version in required_packages:
        try:
            # 尝试导入包
            if package == 'python-dateutil':
                import dateutil
                module_name = 'dateutil'
            else:
                module = importlib.import_module(package)
                module_name = package
            
            # 检查版本
            try:
                if hasattr(module, '__version__'):
                    version = module.__version__
                elif package == 'python-dateutil':
                    version = dateutil.__version__
                else:
                    version = "未知版本"
                    
                results.append((package, True, f"{version} ✓"))
            except:
                results.append((package, True, f"已安装 ✓"))
                
        except ImportError:
            results.append((package, False, f"未安装 ✗"))
    
    return results


def check_optional_packages() -> List[Tuple[str, bool, str]]:
    """检查可选包是否已安装"""
    optional_packages = [
        'openpyxl',
        'xlrd',
        'pytest',
        'pytest-cov',
        'black',
        'flake8',
    ]
    
    results = []
    for package in optional_packages:
        try:
            module = importlib.import_module(package)
            try:
                version = getattr(module, '__version__', "未知版本")
                results.append((package, True, f"{version} ✓"))
            except:
                results.append((package, True, f"已安装 ✓"))
        except ImportError:
            results.append((package, False, f"未安装 ○"))
    
    return results


def check_system_info() -> Dict[str, str]:
    """获取系统信息"""
    return {
        "操作系统": platform.system(),
        "系统版本": platform.platform(),
        "处理器": platform.processor(),
        "Python路径": sys.executable,
    }


def main():
    """主函数"""
    print("=" * 50)
    print("A股日线数据下载系统环境验证")
    print("=" * 50)
    
    # 检查系统信息
    print("\n📋 系统信息:")
    system_info = check_system_info()
    for key, value in system_info.items():
        print(f"  {key}: {value}")
    
    # 检查Python版本
    print("\n🐍 Python版本:")
    python_ok, python_msg = check_python_version()
    print(f"  {python_msg}")
    
    # 检查必需包
    print("\n📦 必需依赖包:")
    required_results = check_required_packages()
    required_ok = True
    for package, ok, msg in required_results:
        print(f"  {package:20} {msg}")
        if not ok:
            required_ok = False
    
    # 检查可选包
    print("\n🔧 可选依赖包:")
    optional_results = check_optional_packages()
    for package, ok, msg in optional_results:
        print(f"  {package:20} {msg}")
    
    # 总结
    print("\n" + "=" * 50)
    if python_ok and required_ok:
        print("✅ 环境验证通过！系统可以正常运行。")
        print("\n接下来的步骤:")
        print("1. 运行: python src/config_manager.py --setup")
        print("2. 运行: python src/database_manager.py --init")
        print("3. 运行: python main.py")
    else:
        print("❌ 环境验证失败！请安装缺失的依赖。")
        print("\n修复方法:")
        if not python_ok:
            print("- 请升级Python到3.8或更高版本")
        if not required_ok:
            print("- 运行: pip install -r requirements.txt")
    
    print("=" * 50)


if __name__ == "__main__":
    main() 