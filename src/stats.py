from __future__ import annotations

import json
import threading
import time
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse


class CrawlStats:
    def __init__(self) -> None:
        self.start_time = time.time()
        self.end_time: float | None = None
        self.pages_crawled = 0
        self.pages_discovered = 0
        self.domains: set[str] = set()
        self.pages_per_domain: Counter[str] = Counter()
        self.tokens_per_page: list[int] = []
        self.http_status_counter: Counter[str] = Counter()
        self.error_counter: Counter[str] = Counter()
        self.bytes_downloaded = 0
        self._lock = threading.Lock()

    def register_discovery(self, count: int = 1) -> None:
        with self._lock:
            self.pages_discovered += count

    def register_success(self, url: str, text: str, response_size: int) -> None:
        domain = urlparse(url).netloc.lower()
        token_count = len(text.split())
        with self._lock:
            self.pages_crawled += 1
            self.domains.add(domain)
            self.pages_per_domain[domain] += 1
            self.tokens_per_page.append(token_count)
            self.bytes_downloaded += response_size

    def register_http_status(self, status_code: int) -> None:
        with self._lock:
            self.http_status_counter[str(status_code)] += 1

    def register_error(self, error_name: str) -> None:
        with self._lock:
            self.error_counter[error_name] += 1

    def finish(self) -> None:
        self.end_time = time.time()

    def to_dict(self) -> dict:
        with self._lock:
            start_time = self.start_time
            end_time = self.end_time or time.time()
            pages_crawled = self.pages_crawled
            pages_discovered = self.pages_discovered
            domains = set(self.domains)
            pages_per_domain = Counter(self.pages_per_domain)
            tokens = list(self.tokens_per_page)
            http_status_counter = Counter(self.http_status_counter)
            error_counter = Counter(self.error_counter)
            bytes_downloaded = self.bytes_downloaded

        elapsed = end_time - start_time
        avg_tokens = (sum(tokens) / len(tokens)) if tokens else 0.0
        sorted_domains = pages_per_domain.most_common()

        return {
            "start_time_unix": int(start_time),
            "end_time_unix": int(end_time),
            "elapsed_seconds": elapsed,
            "pages_crawled": pages_crawled,
            "pages_discovered": pages_discovered,
            "unique_domains": len(domains),
            "pages_per_domain": dict(sorted_domains),
            "tokens_per_page": {
                "count": len(tokens),
                "min": min(tokens) if tokens else 0,
                "max": max(tokens) if tokens else 0,
                "avg": avg_tokens,
            },
            "http_status_counter": dict(http_status_counter),
            "error_counter": dict(error_counter),
            "bytes_downloaded": bytes_downloaded,
            "pages_per_second": (pages_crawled / elapsed) if elapsed > 0 else 0.0,
        }

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        snapshot = self.to_dict()
        with path.open("w", encoding="utf-8") as fh:
            json.dump(snapshot, fh, ensure_ascii=False, indent=2)