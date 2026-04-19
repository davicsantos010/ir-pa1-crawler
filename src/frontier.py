from __future__ import annotations

import queue
import threading
from dataclasses import dataclass


@dataclass(frozen=True)
class FrontierItem:
    url: str
    depth: int


class Frontier:
    def __init__(self) -> None:
        self._queue: queue.Queue[FrontierItem] = queue.Queue()
        self._seen: set[str] = set()
        self._seen_lock = threading.Lock()

    def add_if_new(self, url: str, depth: int) -> bool:
        with self._seen_lock:
            if url in self._seen:
                return False
            self._seen.add(url)
        self._queue.put(FrontierItem(url=url, depth=depth))
        return True

    def get(self, timeout: float = 1.0) -> FrontierItem:
        return self._queue.get(timeout=timeout)

    def task_done(self) -> None:
        self._queue.task_done()

    def size(self) -> int:
        return self._queue.qsize()

    def seen_count(self) -> int:
        with self._seen_lock:
            return len(self._seen)

    def put_sentinel(self, count: int) -> None:
        for _ in range(count):
            self._queue.put(FrontierItem(url="", depth=-1))
