"""
下载器基类和工具函数

提供:
- 速率控制配置
- 断点续传支持
- 日志跟踪
- 通用工具函数
"""

import time
import json
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Set, Dict, Optional, List
from contextlib import contextmanager

# 缓存目录
CACHE_DIR = Path(__file__).parent.parent / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# 速率控制配置
RATE_LIMITS = {
    "baostock": {
        "delay": 0.05,       # 每次请求后等待 (秒)
        "batch_delay": 1.0,  # 每批次后额外等待
        "batch_size": 100,   # 批次大小
    },
    "akshare": {
        "delay": 0.3,
        "batch_delay": 2.0,
        "batch_size": 50,
    },
    "akshare_slow": {
        "delay": 0.5,
        "batch_delay": 3.0,
        "batch_size": 30,
    },
}


class RateLimiter:
    """速率限制器"""

    def __init__(self, source: str = "akshare"):
        config = RATE_LIMITS.get(source, RATE_LIMITS["akshare"])
        self.delay = config["delay"]
        self.batch_delay = config["batch_delay"]
        self.batch_size = config["batch_size"]
        self._count = 0

    def wait(self):
        """每次请求后调用"""
        self._count += 1
        time.sleep(self.delay)
        if self._count % self.batch_size == 0:
            time.sleep(self.batch_delay)

    def reset(self):
        """重置计数器"""
        self._count = 0


class CheckpointManager:
    """断点管理器"""

    def __init__(self, db_path: Path):
        self.checkpoint_file = db_path.with_suffix(".checkpoint")
        self.failed_file = db_path.with_suffix(".failed.json")

    def get_checkpoint(self) -> dict:
        """获取断点信息"""
        if self.checkpoint_file.exists():
            try:
                return json.loads(self.checkpoint_file.read_text())
            except Exception:
                return {}
        return {}

    def save_checkpoint(self, data: dict):
        """保存断点"""
        self.checkpoint_file.write_text(json.dumps(data, indent=2))

    def clear_checkpoint(self):
        """清除断点"""
        if self.checkpoint_file.exists():
            self.checkpoint_file.unlink()

    def get_failed_items(self) -> List[str]:
        """获取失败的项目列表"""
        if self.failed_file.exists():
            try:
                return json.loads(self.failed_file.read_text())
            except Exception:
                return []
        return []

    def save_failed_items(self, items: List[str]):
        """保存失败的项目"""
        self.failed_file.write_text(json.dumps(items, indent=2))

    def clear_failed_items(self):
        """清除失败记录"""
        if self.failed_file.exists():
            self.failed_file.unlink()


def normalize_code(code: str) -> str:
    """
    标准化股票代码为 sh.XXXXXX 或 sz.XXXXXX 格式

    支持输入格式:
    - 600000 (无前缀)
    - sh600000 / sz000001 (带前缀无点号)
    - sh.600000 / sz.000001 (带前缀有点号)
    """
    code = str(code).strip()

    # 移除可能存在的前缀
    if code.startswith(('sh', 'sz', 'bj')):
        prefix = code[:2]
        code_num = code[2:]
        if code_num.startswith('.'):
            code_num = code_num[1:]
        return f"{prefix}.{code_num}"

    # 根据代码判断市场
    if code.startswith('6'):
        return f"sh.{code}"
    elif code.startswith(('0', '3')):
        return f"sz.{code}"
    elif code.startswith(('4', '8')):
        return f"bj.{code}"
    else:
        return f"sz.{code}"


def code_to_akshare(code: str) -> str:
    """
    将 sh.XXXXXX / sz.XXXXXX 格式转换为 AKShare 格式 (纯数字)
    """
    if '.' in code:
        return code.split('.')[1]
    return code


@contextmanager
def get_db_connection(db_path: Path):
    """数据库连接上下文管理器"""
    conn = sqlite3.connect(str(db_path))
    try:
        yield conn
    finally:
        conn.close()


def get_downloaded_dates(conn: sqlite3.Connection, table: str = "download_log") -> Set[str]:
    """获取已下载的日期"""
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT download_date FROM {table}")
        return {row[0] for row in cursor.fetchall()}
    except sqlite3.OperationalError:
        return set()


def log_download(conn: sqlite3.Connection, date_str: str, count: int, table: str = "download_log"):
    """记录下载日志"""
    cursor = conn.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            download_date TEXT PRIMARY KEY,
            stock_count INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute(f"""
        INSERT OR REPLACE INTO {table} (download_date, stock_count)
        VALUES (?, ?)
    """, (date_str, count))
    conn.commit()
