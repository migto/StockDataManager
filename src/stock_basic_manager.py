#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
股票基本信息管理器
功能：
1. 一次性获取所有股票基本信息
2. 本地缓存机制，避免重复API调用
3. 每日最多更新一次
4. 支持强制更新和数据验证
"""

import os
import sys
import json
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import time
import sqlite3

from .database_manager import DatabaseManager
from .optimized_tushare_api_manager import OptimizedTushareAPIManager


class StockBasicManager:
    """股票基本信息管理器"""
    
    def __init__(self, config_manager):
        """
        初始化股票基本信息管理器
        
        Args:
            config_manager: 配置管理器实例
        """
        self.config = config_manager
        db_path = config_manager.get('database_path', 'data/stock_data.db')
        self.db_manager = DatabaseManager(db_path)
        self.api_manager = OptimizedTushareAPIManager(config_manager)
        
        # 缓存配置
        self.cache_dir = Path(self.config.get('cache_path', 'data/cache'))
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        self.cache_file = self.cache_dir / 'stock_basic_cache.json'
        self.csv_cache_file = self.cache_dir / 'stock_basic_data.csv'
        
        # 缓存有效期（24小时）
        self.cache_validity_hours = 24
        
        # 初始化统计信息
        self.stats = {
            'total_stocks': 0,
            'active_stocks': 0,
            'delisted_stocks': 0,
            'last_update': None,
            'cache_hits': 0,
            'api_calls': 0
        }
    
    def get_stock_basic_info(self, force_update: bool = False) -> pd.DataFrame:
        """
        获取股票基本信息
        
        Args:
            force_update: 是否强制更新，忽略缓存
        
        Returns:
            pd.DataFrame: 包含所有股票基本信息的DataFrame
        """
        print("=" * 60)
        print("🔍 股票基本信息获取开始")
        print("=" * 60)
        
        # 检查缓存
        if not force_update and self._is_cache_valid():
            print("📋 使用本地缓存数据")
            return self._load_from_cache()
        
        # 缓存无效或强制更新，从API获取
        print("🌐 从API获取股票基本信息...")
        stock_data = self._fetch_from_api()
        
        if stock_data is not None and not stock_data.empty:
            # 保存到缓存
            self._save_to_cache(stock_data)
            
            # 保存到数据库
            self._save_to_database(stock_data)
            
            # 更新统计信息
            self._update_stats(stock_data)
            
            print(f"✅ 成功获取 {len(stock_data)} 只股票基本信息")
            return stock_data
        else:
            print("❌ 获取股票基本信息失败")
            # 尝试从缓存加载
            if self._cache_exists():
                print("📋 回退到缓存数据")
                return self._load_from_cache()
            else:
                raise Exception("无法获取股票基本信息且无可用缓存")
    
    def _is_cache_valid(self) -> bool:
        """检查缓存是否有效"""
        if not self.cache_file.exists():
            return False
        
        # 读取缓存元数据
        try:
            with open(self.cache_file, 'r', encoding='utf-8') as f:
                cache_meta = json.load(f)
            
            cache_time = datetime.fromisoformat(cache_meta['timestamp'])
            now = datetime.now()
            
            # 检查是否在24小时内
            if now - cache_time < timedelta(hours=self.cache_validity_hours):
                print(f"📋 缓存有效，更新时间: {cache_time.strftime('%Y-%m-%d %H:%M:%S')}")
                return True
            else:
                print(f"⏰ 缓存已过期，上次更新: {cache_time.strftime('%Y-%m-%d %H:%M:%S')}")
                return False
        except Exception as e:
            print(f"❌ 检查缓存失败: {e}")
            return False
    
    def _cache_exists(self) -> bool:
        """检查缓存文件是否存在"""
        return self.cache_file.exists() and self.csv_cache_file.exists()
    
    def _load_from_cache(self) -> pd.DataFrame:
        """从缓存加载数据"""
        try:
            # 读取CSV数据
            stock_data = pd.read_csv(self.csv_cache_file)
            
            # 更新统计信息
            self.stats['cache_hits'] += 1
            self._update_stats(stock_data)
            
            print(f"📋 从缓存加载 {len(stock_data)} 只股票信息")
            return stock_data
            
        except Exception as e:
            print(f"❌ 从缓存加载失败: {e}")
            raise
    
    def _fetch_from_api(self) -> Optional[pd.DataFrame]:
        """从API获取股票基本信息"""
        try:
            # 检查API调用限制
            if not self._check_api_limit():
                print("❌ API调用次数已达到每日限制")
                return self._load_fallback_data()
            
            print("🔄 正在调用stock_basic接口...")
            
            # 调用API
            stock_data = self.api_manager.get_stock_basic()
            
            if stock_data is not None and not stock_data.empty:
                self.stats['api_calls'] += 1
                print(f"✅ API调用成功，获取 {len(stock_data)} 只股票信息")
                return stock_data
            else:
                print("❌ API调用失败或返回空数据")
                return self._load_fallback_data()
                
        except Exception as e:
            print(f"❌ API调用异常: {e}")
            if "每小时最多访问该接口1次" in str(e):
                print("⏰ 遇到API频率限制，使用fallback数据")
            return self._load_fallback_data()
    
    def _load_fallback_data(self) -> Optional[pd.DataFrame]:
        """加载fallback股票数据"""
        try:
            fallback_file = Path("data/stock_basic_fallback.csv")
            if fallback_file.exists():
                stock_data = pd.read_csv(fallback_file)
                print(f"📋 使用fallback数据，包含 {len(stock_data)} 只股票")
                return stock_data
            else:
                print("❌ fallback数据文件不存在")
                return None
        except Exception as e:
            print(f"❌ 加载fallback数据失败: {e}")
            return None
    
    def _check_api_limit(self) -> bool:
        """检查API调用限制"""
        # 从数据库检查今日API调用次数
        try:
            conn = self.db_manager.connect()
            cursor = conn.cursor()
            today = datetime.now().strftime('%Y-%m-%d')
            
            query = """
            SELECT COUNT(*) as call_count
            FROM api_call_log 
            WHERE api_name = 'stock_basic' 
            AND date(call_time) = ?
            AND success = 1
            """
            
            cursor.execute(query, (today,))
            result = cursor.fetchone()
            call_count = result[0] if result else 0
            
            # stock_basic接口每日限制5次
            if call_count >= 5:
                print(f"⚠️  今日stock_basic接口调用次数已达上限: {call_count}/5")
                return False
            else:
                print(f"✅ 今日stock_basic接口调用次数: {call_count}/5")
                return True
                    
        except Exception as e:
            print(f"❌ 检查API限制失败: {e}")
            return False
    
    def _save_to_cache(self, stock_data: pd.DataFrame):
        """保存数据到缓存"""
        try:
            # 保存元数据
            cache_meta = {
                'timestamp': datetime.now().isoformat(),
                'record_count': len(stock_data),
                'cache_version': '1.0'
            }
            
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_meta, f, ensure_ascii=False, indent=2)
            
            # 保存CSV数据
            stock_data.to_csv(self.csv_cache_file, index=False, encoding='utf-8')
            
            print(f"💾 已保存 {len(stock_data)} 条记录到缓存")
            
        except Exception as e:
            print(f"❌ 保存缓存失败: {e}")
    
    def _save_to_database(self, stock_data: pd.DataFrame):
        """保存数据到数据库"""
        try:
            conn = self.db_manager.connect()
            # 清空现有数据
            conn.execute("DELETE FROM stocks")
            
            # 插入新数据
            stock_data.to_sql('stocks', conn, if_exists='append', index=False)
            
            conn.commit()
            conn.close()
            
            print(f"💾 已保存 {len(stock_data)} 条记录到数据库")
            
        except Exception as e:
            print(f"❌ 保存数据库失败: {e}")
    
    def _update_stats(self, stock_data: pd.DataFrame):
        """更新统计信息"""
        try:
            self.stats['total_stocks'] = len(stock_data)
            self.stats['active_stocks'] = len(stock_data[stock_data['list_status'] == 'L'])
            self.stats['delisted_stocks'] = len(stock_data[stock_data['list_status'] == 'D'])
            self.stats['last_update'] = datetime.now().isoformat()
            
        except Exception as e:
            print(f"❌ 更新统计信息失败: {e}")
    
    def get_stock_by_code(self, ts_code: str) -> Optional[Dict]:
        """
        根据股票代码获取基本信息
        
        Args:
            ts_code: 股票代码 (如: 000001.SZ)
        
        Returns:
            Dict: 股票基本信息，不存在则返回None
        """
        try:
            stock_data = self.get_stock_basic_info()
            stock_info = stock_data[stock_data['ts_code'] == ts_code]
            
            if not stock_info.empty:
                return stock_info.iloc[0].to_dict()
            else:
                return None
                
        except Exception as e:
            print(f"❌ 获取股票信息失败: {e}")
            return None
    
    def get_active_stocks(self) -> pd.DataFrame:
        """获取所有正常交易的股票"""
        try:
            stock_data = self.get_stock_basic_info()
            return stock_data[stock_data['list_status'] == 'L']
            
        except Exception as e:
            print(f"❌ 获取活跃股票失败: {e}")
            return pd.DataFrame()
    
    def get_stocks_by_market(self, market: str) -> pd.DataFrame:
        """
        根据市场获取股票列表
        
        Args:
            market: 市场代码 (主板/创业板/科创板/CDR等)
        
        Returns:
            pd.DataFrame: 股票列表
        """
        try:
            stock_data = self.get_stock_basic_info()
            return stock_data[stock_data['market'] == market]
            
        except Exception as e:
            print(f"❌ 获取市场股票失败: {e}")
            return pd.DataFrame()
    
    def get_stocks_by_industry(self, industry: str) -> pd.DataFrame:
        """
        根据行业获取股票列表
        
        Args:
            industry: 行业名称
        
        Returns:
            pd.DataFrame: 股票列表
        """
        try:
            stock_data = self.get_stock_basic_info()
            return stock_data[stock_data['industry'] == industry]
            
        except Exception as e:
            print(f"❌ 获取行业股票失败: {e}")
            return pd.DataFrame()
    
    def get_statistics(self) -> Dict:
        """获取统计信息"""
        return self.stats.copy()
    
    def clear_cache(self):
        """清空缓存"""
        try:
            if self.cache_file.exists():
                self.cache_file.unlink()
            if self.csv_cache_file.exists():
                self.csv_cache_file.unlink()
            
            print("🗑️  缓存已清空")
            
        except Exception as e:
            print(f"❌ 清空缓存失败: {e}")
    
    def print_status(self):
        """打印当前状态"""
        print("\n" + "=" * 60)
        print("📊 股票基本信息管理器状态")
        print("=" * 60)
        
        # 缓存状态
        cache_status = "有效" if self._is_cache_valid() else "无效/不存在"
        print(f"📋 缓存状态: {cache_status}")
        
        if self._cache_exists():
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    cache_meta = json.load(f)
                print(f"📅 缓存时间: {cache_meta['timestamp']}")
                print(f"📊 缓存记录: {cache_meta['record_count']}")
            except:
                pass
        
        # 统计信息
        print(f"📈 总股票数: {self.stats['total_stocks']}")
        print(f"🟢 活跃股票: {self.stats['active_stocks']}")
        print(f"🔴 已退市股票: {self.stats['delisted_stocks']}")
        print(f"💾 缓存命中: {self.stats['cache_hits']}")
        print(f"🌐 API调用: {self.stats['api_calls']}")
        
        if self.stats['last_update']:
            print(f"🕒 最后更新: {self.stats['last_update']}")
        
        print("=" * 60)


def main():
    """命令行测试函数"""
    import sys
    import argparse
    
    # 添加项目根目录到路径
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    from src.config_manager import ConfigManager
    
    parser = argparse.ArgumentParser(description='股票基本信息管理器')
    parser.add_argument('--status', action='store_true', help='显示当前状态')
    parser.add_argument('--update', action='store_true', help='更新股票基本信息')
    parser.add_argument('--force', action='store_true', help='强制更新，忽略缓存')
    parser.add_argument('--clear-cache', action='store_true', help='清空缓存')
    parser.add_argument('--code', type=str, help='查询指定股票代码')
    parser.add_argument('--market', type=str, help='查询指定市场股票')
    parser.add_argument('--industry', type=str, help='查询指定行业股票')
    parser.add_argument('--active', action='store_true', help='显示活跃股票')
    
    args = parser.parse_args()
    
    try:
        # 初始化配置和管理器
        config = ConfigManager()
        manager = StockBasicManager(config)
        
        if args.status:
            manager.print_status()
        
        elif args.clear_cache:
            manager.clear_cache()
        
        elif args.update:
            stock_data = manager.get_stock_basic_info(force_update=args.force)
            print(f"\n✅ 更新完成，共获取 {len(stock_data)} 只股票信息")
            manager.print_status()
        
        elif args.code:
            stock_info = manager.get_stock_by_code(args.code)
            if stock_info:
                print(f"\n📊 股票信息: {args.code}")
                print("-" * 40)
                for key, value in stock_info.items():
                    print(f"{key}: {value}")
            else:
                print(f"❌ 未找到股票: {args.code}")
        
        elif args.market:
            stocks = manager.get_stocks_by_market(args.market)
            print(f"\n📊 {args.market} 市场股票 ({len(stocks)} 只)")
            print("-" * 40)
            for _, stock in stocks.iterrows():
                print(f"{stock['ts_code']}: {stock['name']}")
        
        elif args.industry:
            stocks = manager.get_stocks_by_industry(args.industry)
            print(f"\n📊 {args.industry} 行业股票 ({len(stocks)} 只)")
            print("-" * 40)
            for _, stock in stocks.iterrows():
                print(f"{stock['ts_code']}: {stock['name']}")
        
        elif args.active:
            stocks = manager.get_active_stocks()
            print(f"\n📊 活跃股票 ({len(stocks)} 只)")
            print("-" * 40)
            for _, stock in stocks.iterrows():
                print(f"{stock['ts_code']}: {stock['name']}")
        
        else:
            # 默认显示状态
            manager.print_status()
    
    except Exception as e:
        print(f"❌ 执行失败: {e}")
        return 1
    
    return 0


if __name__ == '__main__':
    sys.exit(main()) 