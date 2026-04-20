from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from urllib.parse import urlparse

import requests
from protego import Protego


@dataclass
class HostPolicy:
    parser: Protego
    crawl_delay: float
    last_access: float
    lock: threading.Lock


class RobotsManager:
    def __init__(self, user_agent: str, timeout: float) -> None:
        self.user_agent = user_agent
        self.timeout = timeout
        self._policies: dict[str, HostPolicy] = {}
        self._global_lock = threading.Lock()

    def _robots_url(self, url: str) -> str:
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}/robots.txt"

    def _load_policy(self, session: requests.Session, url: str) -> HostPolicy:
        robots_url = self._robots_url(url)
        crawl_delay = 0.1
        parser = Protego.parse("")

        try:
            response = session.get(
                robots_url,
                timeout=self.timeout,
                headers={"User-Agent": self.user_agent},
                allow_redirects=True,
            )
            if response.ok:
                parser = Protego.parse(response.text)
                declared_delay = parser.crawl_delay(self.user_agent)
                if declared_delay is None:
                    declared_delay = parser.crawl_delay("*")
                if declared_delay is not None:
                    crawl_delay = max(float(declared_delay), 0.1)
        except Exception:
            pass

        return HostPolicy(
            parser=parser,
            crawl_delay=crawl_delay,
            last_access=0.0,
            lock=threading.Lock(),
        )

    def get_policy(self, session: requests.Session, url: str) -> HostPolicy:
        host = urlparse(url).netloc.lower()
        with self._global_lock:
            if host not in self._policies:
                self._policies[host] = self._load_policy(session, url)
            return self._policies[host]

    def allowed(self, session: requests.Session, url: str) -> bool:
        policy = self.get_policy(session, url)
        return policy.parser.can_fetch(self.user_agent, url)

    def wait_for_turn(self, session: requests.Session, url: str) -> None:
        policy = self.get_policy(session, url)
        with policy.lock:
            now = time.time()
            elapsed = now - policy.last_access
            wait_time = policy.crawl_delay - elapsed
            if wait_time > 0:
                time.sleep(wait_time)
            policy.last_access = time.time()