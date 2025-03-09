import os
import json
import hashlib
import time
from datetime import datetime, timedelta
import logging
from typing import Dict, Any, Optional, Union, Tuple
import sqlite3
import pickle

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PropertyCache:
    """
    房产数据缓存系统，用于缓存API调用结果和分析数据，
    减少重复API调用和提高性能。
    """

    def __init__(self, cache_dir="cache", db_path=None, ttl_days=7):
        """
        初始化缓存系统

        参数:
            cache_dir (str): 缓存目录
            db_path (str): SQLite数据库路径，默认为cache_dir/property_cache.db
            ttl_days (int): 缓存有效期(天)
        """
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)

        # SQLite缓存数据库
        self.db_path = db_path or os.path.join(cache_dir, "property_cache.db")
        self.ttl_days = ttl_days
        self._init_db()

        # 内存缓存，用于最频繁访问的数据
        self.memory_cache = {}
        self.memory_cache_ttl = {}  # 记录内存缓存项的过期时间

        logger.info(f"缓存系统初始化完成，缓存目录：{cache_dir}，数据库：{self.db_path}")

    def _init_db(self):
        """初始化SQLite数据库"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # 创建GPT分析结果缓存表
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS gpt_analysis_cache (
                    content_hash TEXT PRIMARY KEY,
                    request_data TEXT,
                    response_data BLOB,
                    created_at TIMESTAMP,
                    expires_at TIMESTAMP,
                    model TEXT,
                    token_count INTEGER
                )
                ''')

                # 创建房源数据缓存表
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS property_data_cache (
                    property_id TEXT PRIMARY KEY,
                    data BLOB,
                    source_url TEXT,
                    created_at TIMESTAMP,
                    expires_at TIMESTAMP,
                    last_accessed TIMESTAMP
                )
                ''')

                # 创建缓存统计表
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS cache_stats (
                    date TEXT PRIMARY KEY,
                    api_calls INTEGER DEFAULT 0,
                    cache_hits INTEGER DEFAULT 0,
                    tokens_saved INTEGER DEFAULT 0,
                    cost_saved REAL DEFAULT 0.0
                )
                ''')

                conn.commit()
                logger.info("数据库表初始化完成")
        except sqlite3.Error as e:
            logger.error(f"初始化数据库失败: {e}")
            raise

    def _compute_hash(self, content: str) -> str:
        """计算内容的哈希值作为缓存键"""
        return hashlib.md5(content.encode('utf-8')).hexdigest()

    def get_gpt_analysis(self, content: str, model: str) -> Optional[Dict[str, Any]]:
        """
        获取GPT分析结果缓存

        参数:
            content (str): 需要分析的内容
            model (str): 使用的模型名称

        返回:
            Optional[Dict[str, Any]]: 缓存的分析结果，如果没有则返回None
        """
        content_hash = self._compute_hash(content + model)  # 同样的内容但不同模型应有不同缓存

        # 先检查内存缓存
        if content_hash in self.memory_cache:
            if datetime.now() < self.memory_cache_ttl.get(content_hash, datetime.now()):
                logger.debug(f"内存缓存命中: {content_hash[:8]}")
                self._update_stats(cache_hit=True)
                return self.memory_cache[content_hash]
            else:
                # 过期了，从内存缓存中移除
                del self.memory_cache[content_hash]
                del self.memory_cache_ttl[content_hash]

        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT response_data, expires_at, token_count FROM gpt_analysis_cache WHERE content_hash = ? AND model = ?",
                    (content_hash, model)
                )
                result = cursor.fetchone()

                if result:
                    response_data, expires_at, token_count = result
                    expires_at = datetime.fromisoformat(expires_at)

                    if datetime.now() < expires_at:
                        # 缓存有效
                        analysis_result = pickle.loads(response_data)

                        # 添加到内存缓存
                        self.memory_cache[content_hash] = analysis_result
                        self.memory_cache_ttl[content_hash] = expires_at

                        # 更新统计信息
                        self._update_stats(cache_hit=True, tokens_saved=token_count)
                        logger.info(f"GPT分析缓存命中: {content_hash[:8]}")

                        return analysis_result
                    else:
                        # 缓存过期，删除
                        cursor.execute(
                            "DELETE FROM gpt_analysis_cache WHERE content_hash = ?",
                            (content_hash,)
                        )
                        conn.commit()
                        logger.debug(f"删除过期缓存: {content_hash[:8]}")

                return None
        except Exception as e:
            logger.error(f"获取GPT分析缓存失败: {e}")
            return None

    def cache_gpt_analysis(self, content: str, model: str, analysis_result: Any, token_count: int,
                           ttl_days: Optional[int] = None) -> bool:
        """
        缓存GPT分析结果

        参数:
            content (str): 分析的内容
            model (str): 使用的模型名称
            analysis_result (Any): 分析结果
            token_count (int): 使用的token数量
            ttl_days (Optional[int]): 可选的缓存有效期(天)

        返回:
            bool: 是否成功缓存
        """
        content_hash = self._compute_hash(content + model)
        ttl = ttl_days or self.ttl_days
        now = datetime.now()
        expires_at = now + timedelta(days=ttl)

        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # 将分析结果序列化
                response_data = pickle.dumps(analysis_result)

                cursor.execute(
                    "INSERT OR REPLACE INTO gpt_analysis_cache (content_hash, request_data, response_data, created_at, expires_at, model, token_count) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (content_hash, content[:200], response_data, now.isoformat(), expires_at.isoformat(), model,
                     token_count)
                )
                conn.commit()

                # 同时添加到内存缓存
                self.memory_cache[content_hash] = analysis_result
                self.memory_cache_ttl[content_hash] = expires_at

                logger.info(f"GPT分析结果已缓存: {content_hash[:8]}, 模型: {model}, Token数: {token_count}")
                self._update_stats(api_call=True)

                return True
        except Exception as e:
            logger.error(f"缓存GPT分析结果失败: {e}")
            return False

    def get_property_data(self, property_id: str) -> Optional[Dict[str, Any]]:
        """
        获取缓存的房源数据

        参数:
            property_id (str): 房源ID

        返回:
            Optional[Dict[str, Any]]: 缓存的房源数据，如果没有则返回None
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT data, expires_at FROM property_data_cache WHERE property_id = ?",
                    (property_id,)
                )
                result = cursor.fetchone()

                if result:
                    data_blob, expires_at = result
                    expires_at = datetime.fromisoformat(expires_at)

                    if datetime.now() < expires_at:
                        # 更新最后访问时间
                        cursor.execute(
                            "UPDATE property_data_cache SET last_accessed = ? WHERE property_id = ?",
                            (datetime.now().isoformat(), property_id)
                        )
                        conn.commit()

                        property_data = pickle.loads(data_blob)
                        logger.debug(f"房源数据缓存命中: {property_id}")
                        return property_data
                    else:
                        # 缓存过期，删除
                        cursor.execute(
                            "DELETE FROM property_data_cache WHERE property_id = ?",
                            (property_id,)
                        )
                        conn.commit()

                return None
        except Exception as e:
            logger.error(f"获取房源数据缓存失败: {e}")
            return None

    def cache_property_data(self, property_id: str, data: Dict[str, Any], source_url: str,
                            ttl_days: Optional[int] = None) -> bool:
        """
        缓存房源数据

        参数:
            property_id (str): 房源ID
            data (Dict[str, Any]): 房源数据
            source_url (str): 数据来源URL
            ttl_days (Optional[int]): 可选的缓存有效期(天)

        返回:
            bool: 是否成功缓存
        """
        ttl = ttl_days or self.ttl_days
        now = datetime.now()
        expires_at = now + timedelta(days=ttl)

        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # 序列化数据
                data_blob = pickle.dumps(data)

                cursor.execute(
                    "INSERT OR REPLACE INTO property_data_cache (property_id, data, source_url, created_at, expires_at, last_accessed) VALUES (?, ?, ?, ?, ?, ?)",
                    (property_id, data_blob, source_url, now.isoformat(), expires_at.isoformat(), now.isoformat())
                )
                conn.commit()

                logger.info(f"房源数据已缓存: {property_id}")
                return True
        except Exception as e:
            logger.error(f"缓存房源数据失败: {e}")
            return False

    def bulk_cache_properties(self, properties: Dict[str, Dict[str, Any]], source_url: str) -> Tuple[int, int]:
        """
        批量缓存多个房源数据

        参数:
            properties (Dict[str, Dict[str, Any]]): 房源数据字典，键为房源ID
            source_url (str): 数据来源URL

        返回:
            Tuple[int, int]: (成功缓存数量, 总数量)
        """
        success_count = 0
        total_count = len(properties)

        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.isolation_level = None  # 启用自动提交模式
                cursor = conn.cursor()
                cursor.execute("BEGIN TRANSACTION")

                now = datetime.now()
                expires_at = now + timedelta(days=self.ttl_days)

                for property_id, data in properties.items():
                    try:
                        # 序列化数据
                        data_blob = pickle.dumps(data)

                        cursor.execute(
                            "INSERT OR REPLACE INTO property_data_cache (property_id, data, source_url, created_at, expires_at, last_accessed) VALUES (?, ?, ?, ?, ?, ?)",
                            (property_id, data_blob, source_url, now.isoformat(), expires_at.isoformat(),
                             now.isoformat())
                        )
                        success_count += 1
                    except Exception as e:
                        logger.error(f"缓存房源 {property_id} 失败: {e}")

                cursor.execute("COMMIT")

                logger.info(f"批量缓存完成: {success_count}/{total_count} 个房源")
                return (success_count, total_count)
        except Exception as e:
            logger.error(f"批量缓存失败: {e}")
            return (success_count, total_count)

    def clean_expired_cache(self) -> int:
        """
        清理过期缓存

        返回:
            int: 清理的缓存项数量
        """
        cleaned_count = 0
        now = datetime.now().isoformat()

        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # 清理GPT分析缓存
                cursor.execute(
                    "DELETE FROM gpt_analysis_cache WHERE expires_at < ?",
                    (now,)
                )
                cleaned_count += cursor.rowcount

                # 清理房源数据缓存
                cursor.execute(
                    "DELETE FROM property_data_cache WHERE expires_at < ?",
                    (now,)
                )
                cleaned_count += cursor.rowcount

                conn.commit()

                logger.info(f"已清理 {cleaned_count} 个过期缓存项")
                return cleaned_count
        except Exception as e:
            logger.error(f"清理过期缓存失败: {e}")
            return 0

    def clear_memory_cache(self):
        """清空内存缓存"""
        cache_size = len(self.memory_cache)
        self.memory_cache.clear()
        self.memory_cache_ttl.clear()
        logger.info(f"已清空内存缓存，释放 {cache_size} 个缓存项")

    def _update_stats(self, api_call=False, cache_hit=False, tokens_saved=0):
        """更新缓存统计信息"""
        today = datetime.now().strftime('%Y-%m-%d')
        cost_per_token = 0.00001  # 估算每token成本，根据实际API定价调整
        cost_saved = tokens_saved * cost_per_token

        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # 检查今天的统计记录是否存在
                cursor.execute(
                    "SELECT * FROM cache_stats WHERE date = ?",
                    (today,)
                )
                result = cursor.fetchone()

                if result:
                    # 更新现有记录
                    if api_call:
                        cursor.execute(
                            "UPDATE cache_stats SET api_calls = api_calls + 1 WHERE date = ?",
                            (today,)
                        )
                    if cache_hit:
                        cursor.execute(
                            "UPDATE cache_stats SET cache_hits = cache_hits + 1, tokens_saved = tokens_saved + ?, cost_saved = cost_saved + ? WHERE date = ?",
                            (tokens_saved, cost_saved, today)
                        )
                else:
                    # 创建新记录
                    cursor.execute(
                        "INSERT INTO cache_stats (date, api_calls, cache_hits, tokens_saved, cost_saved) VALUES (?, ?, ?, ?, ?)",
                        (today, 1 if api_call else 0, 1 if cache_hit else 0, tokens_saved, cost_saved)
                    )

                conn.commit()
        except Exception as e:
            logger.error(f"更新缓存统计失败: {e}")

    def get_stats(self, days=30) -> Dict[str, Any]:
        """
        获取缓存使用统计

        参数:
            days (int): 要获取的天数

        返回:
            Dict[str, Any]: 统计信息
        """
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT SUM(api_calls), SUM(cache_hits), SUM(tokens_saved), SUM(cost_saved) FROM cache_stats WHERE date >= ?",
                    (start_date,)
                )
                result = cursor.fetchone()

                if result:
                    api_calls, cache_hits, tokens_saved, cost_saved = result

                    # 计算缓存命中率
                    total_requests = (api_calls or 0) + (cache_hits or 0)
                    hit_rate = (cache_hits or 0) / total_requests if total_requests > 0 else 0

                    return {
                        "period_days": days,
                        "api_calls": api_calls or 0,
                        "cache_hits": cache_hits or 0,
                        "total_requests": total_requests,
                        "hit_rate": hit_rate,
                        "tokens_saved": tokens_saved or 0,
                        "cost_saved": cost_saved or 0
                    }

                return {
                    "period_days": days,
                    "api_calls": 0,
                    "cache_hits": 0,
                    "total_requests": 0,
                    "hit_rate": 0,
                    "tokens_saved": 0,
                    "cost_saved": 0
                }
        except Exception as e:
            logger.error(f"获取缓存统计失败: {e}")
            return {
                "error": str(e),
                "period_days": days
            }