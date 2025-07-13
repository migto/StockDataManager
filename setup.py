"""
A股日线数据下载系统安装配置
"""

from setuptools import setup, find_packages

# 读取README文件作为长描述
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

# 读取requirements.txt文件
with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="stock-daily-downloader",
    version="1.0.0",
    author="Stock Data System",
    author_email="",
    description="A股日线数据下载系统 - 基于Tushare Pro API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Financial and Insurance Industry",
        "Topic :: Office/Business :: Financial :: Investment",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=6.2.0",
            "pytest-cov>=2.12.0",
            "pytest-mock>=3.6.0",
            "black>=21.0.0",
            "flake8>=3.9.0",
            "isort>=5.9.0",
            "mypy>=0.910",
        ],
    },
    entry_points={
        "console_scripts": [
            "stock-downloader=main:main",
        ],
    },
    include_package_data=True,
    zip_safe=False,
) 