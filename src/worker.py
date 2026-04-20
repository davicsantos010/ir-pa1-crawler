from __future__ import annotations

import json
import queue
import threading
import time
from dataclasses import dataclass

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
    progress_interval_pages: int


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
        self._reserved_successes = 0
        self._activity_lock = threading.Lock()
        self._active_workers = 0
        self._progress_lock = threading.Lock()
        self._next_progress_milestone = max(1, config.progress_interval_pages)

    def reached_limit(self) -> bool:
        with self._success_lock:
            return self._reserved_successes >= self.config.limit

    def reserve_success_slot(self) -> bool:
        with self._success_lock:
            if self._reserved_successes >= self.config.limit:
                return False
            self._reserved_successes += 1
            return True

    def release_success_slot(self) -> None:
        with self._success_lock:
            if self._reserved_successes > 0:
                self._reserved_successes -= 1

    def begin_processing(self) -> None:
        with self._activity_lock:
            self._active_workers += 1

    def finish_processing(self) -> None:
        with self._activity_lock:
            if self._active_workers > 0:
                self._active_workers -= 1

    def is_idle_and_frontier_empty(self) -> bool:
        with self._activity_lock:
            return self._active_workers == 0 and self.frontier.size() == 0

    def report_progress_if_needed(self) -> None:
        if self.config.progress_interval_pages <= 0:
            return

        snapshot = self.stats.to_dict()
        pages_crawled = snapshot["pages_crawled"]

        with self._progress_lock:
            if pages_crawled < self._next_progress_milestone and pages_crawled != self.config.limit:
                return

            print(
                "[PROGRESS] "
                f"pages_crawled={snapshot['pages_crawled']} "
                f"pages_discovered={snapshot['pages_discovered']} "
                f"unique_domains={snapshot['unique_domains']} "
                f"pages_per_second={snapshot['pages_per_second']:.4f} "
                f"frontier_seen={self.frontier.seen_count()}",
                flush=True,
            )

            while self._next_progress_milestone <= pages_crawled:
                self._next_progress_milestone += max(1, self.config.progress_interval_pages)


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
            except queue.Empty:
                if self.controller.reached_limit() or self.controller.is_idle_and_frontier_empty():
                    self.controller.stop_event.set()
                continue

            try:
                if item.depth == -1:
                    return
                if self.controller.reached_limit():
                    self.controller.stop_event.set()
                    return
                self.controller.begin_processing()
                self.process_item(item)
            finally:
                self.controller.finish_processing()
                self.controller.frontier.task_done()

    def process_item(self, item: FrontierItem) -> None:
        url = item.url
        reserved_slot = False

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

            if not self.controller.reserve_success_slot():
                self.controller.stop_event.set()
                return
            reserved_slot = True

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
                response_size=len(html_bytes),
            )
            reserved_slot = False

            if self.controller.config.debug:
                debug_record = {
                    "URL": url,
                    "Title": title,
                    "Text": first_20,
                    "Timestamp": int(time.time()),
                }
                print(json.dumps(debug_record, ensure_ascii=False), flush=True)

            self.controller.report_progress_if_needed()

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
            if reserved_slot:
                self.controller.release_success_slot()
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