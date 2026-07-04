"""幂等存储抽象 — 防止消息重复消费（at-least-once 投递下的必备兜底）

提供两个实现：
  - InMemoryStore：进程内 set，零依赖，重启即失（适合单进程教学演示）
  - SqliteStore：SQLite 持久化，无新依赖，重启不丢（演示生产级幂等）

生产环境多 worker / 多机共享幂等应换 Redis 或 Postgres（见 README 说明）。
"""

from __future__ import annotations

import asyncio
import sqlite3
from abc import ABC, abstractmethod


class IdempotencyStore(ABC):
    """幂等存储抽象。"""

    @abstractmethod
    async def check_and_mark(self, key: str) -> bool:
        """检查 key 是否已处理；未处理则标记并返回 True，已处理返回 False。

        实现须保证原子性：并发下同一 key 只有第一次返回 True。
        """

    @abstractmethod
    async def unmark(self, key: str) -> None:
        """取消标记（处理失败时调用，让重试消息能再次进入处理）。"""


class InMemoryStore(IdempotencyStore):
    """进程内幂等存储（set）。重启即失，仅适合单进程演示。"""

    def __init__(self) -> None:
        self._seen: set[str] = set()

    async def check_and_mark(self, key: str) -> bool:
        if key in self._seen:
            return False
        self._seen.add(key)
        return True

    async def unmark(self, key: str) -> None:
        self._seen.discard(key)


class SqliteStore(IdempotencyStore):
    """SQLite 持久化幂等存储。

    用 PRIMARY KEY + INSERT，借 sqlite3.IntegrityError 判重，天然原子。
    sqlite3 是同步阻塞库，通过 asyncio.to_thread 避免阻塞事件循环。

    已知限制：多个 worker 进程若指向各自的 db 文件，幂等不共享；
    真正多机部署应换 Redis/Postgres 共享存储。
    """

    def __init__(self, db_path: str = "idempotency.db") -> None:
        # check_same_thread=False：连接在主线程建，check_and_mark 通过 to_thread
        # 在工作线程执行。本框架 prefetch=1 串行消费，无并发写；多线程并发场景
        # 需自行加锁或改用专用单线程 executor。
        self._db = sqlite3.connect(db_path, check_same_thread=False)
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS processed ("
            "  key TEXT PRIMARY KEY,"
            "  ts TEXT DEFAULT CURRENT_TIMESTAMP"
            ")"
        )
        self._db.commit()

    async def check_and_mark(self, key: str) -> bool:
        def _op() -> bool:
            try:
                self._db.execute("INSERT INTO processed (key) VALUES (?)", (key,))
                self._db.commit()
                return True
            except sqlite3.IntegrityError:
                # 重复 key：rollback 释放事务锁，否则后续写会 database is locked
                self._db.rollback()
                return False

        return await asyncio.to_thread(_op)

    async def unmark(self, key: str) -> None:
        def _op() -> None:
            self._db.execute("DELETE FROM processed WHERE key = ?", (key,))
            self._db.commit()

        await asyncio.to_thread(_op)

    def close(self) -> None:
        self._db.close()
