from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import requests

from src.frontier import Frontier, FrontierItem
from src.parser_utils import build_soup, extract_links, extract_title, extract_visible_text, first_n_words
from src.robots_manager import RobotsManager
from src.stats import CrawlStats
from src.storage import WARCStorage


@dataclass
class CrawlConfig:
    limit: int
    debug: bool
    timeout: float
    user_agent: str
    max_retries: int
    max_depth: int | None
    respect_nofollow: bool


class CrawlController:
    def __init__(
        self,
        frontier: Frontier,
        robots_manager: RobotsManager,
        storage: WARCStorage,
        stats: CrawlStats,
        config: CrawlConfig,
    ) -> None:
        self.frontier = frontier
        self.robots_manager = robots_manager
        self.storage = storage
        self.stats = stats
        self.config = config
        self.stop_event = threading.Event()
        self._success_lock = threading.Lock()

    def reached_limit(self) -> bool:
        return self.stats.pages_crawled >= self.config.limit

    def register_success_if_allowed(self) -> bool:
        with self._success_lock:
            if self.stats.pages_crawled >= self.config.limit:
                return False
            return True


class Worker(threading.Thread):
    def __init__(self, worker_id: int, controller: CrawlController) -> None:
        super().__init__(name=f"worker-{worker_id}", daemon=True)
        self.worker_id = worker_id
        self.controller = controller
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": controller.config.user_agent})

    def run(self) -> None:
        while not self.controller.stop_event.is_set():
            try:
                item = self.controller.frontier.get(timeout=1.0)
            except Exception:
                if self.controller.reached_limit():
                    self.controller.stop_event.set()
                continue

            try:
                if item.depth == -1:
                    return
                if self.controller.reached_limit():
                    self.controller.stop_event.set()
                    return
                self.process_item(item)
            finally:
                self.controller.frontier.task_done()

    def process_item(self, item: FrontierItem) -> None:
        url = item.url
        try:
            if not self.controller.robots_manager.allowed(self.session, url):
                return

            self.controller.robots_manager.wait_for_turn(self.session, url)
            response = self.fetch_with_retries(url)
            if response is None:
                return

            self.controller.stats.register_http_status(response.status_code)
            if not response.ok:
                return

            content_type = response.headers.get("Content-Type", "").lower()
            if "text/html" not in content_type:
                return

            if not self.controller.register_success_if_allowed():
                self.controller.stop_event.set()
                return

            html_text = response.text
            html_bytes = response.content
            soup = build_soup(html_text)
            title = extract_title(soup)
            visible_text = extract_visible_text(soup)
            first_20 = first_n_words(visible_text, 20)

            self.controller.storage.write_response(url, html_bytes, content_type=content_type)
            self.controller.stats.register_success(
                url=url,
                text=visible_text,
                status_code=response.status_code,
                response_size=len(html_bytes),
            )

            if self.controller.config.debug:
                debug_record = {
                    "URL": url,
                    "Title": title,
                    "Text": first_20,
                    "Timestamp": int(time.time()),
                }
                print(json.dumps(debug_record, ensure_ascii=False), flush=True)

            if self.controller.reached_limit():
                self.controller.stop_event.set()
                return

            if self.controller.config.max_depth is not None and item.depth >= self.controller.config.max_depth:
                return

            links = extract_links(
                soup,
                base_url=url,
                respect_nofollow=self.controller.config.respect_nofollow,
            )
            for link in links:
                added = self.controller.frontier.add_if_new(link, item.depth + 1)
                if added:
                    self.controller.stats.register_discovery()

        except Exception as exc:
            self.controller.stats.register_error(type(exc).__name__)

    def fetch_with_retries(self, url: str) -> requests.Response | None:
        for attempt in range(self.controller.config.max_retries + 1):
            try:
                return self.session.get(
                    url,
                    timeout=self.controller.config.timeout,
                    allow_redirects=True,
                )
            except requests.RequestException as exc:
                self.controller.stats.register_error(type(exc).__name__)
                if attempt < self.controller.config.max_retries:
                    time.sleep(min(2 ** attempt, 2))
                else:
                    return None
        return None
