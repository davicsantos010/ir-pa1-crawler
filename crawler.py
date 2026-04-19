from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from src.frontier import Frontier
from src.normalization import normalize_url
from src.robots_manager import RobotsManager
from src.stats import CrawlStats
from src.storage import WARCStorage
from src.worker import CrawlConfig, CrawlController, Worker


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Multithreaded polite crawler for IR PA1")
    parser.add_argument("-s", "--seeds", required=True, help="Path to the seeds file")
    parser.add_argument("-n", "--limit", required=True, type=int, help="Target number of webpages to crawl")
    parser.add_argument("-d", "--debug", action="store_true", help="Enable debug mode")

    parser.add_argument("--threads", type=int, default=16, help="Number of worker threads")
    parser.add_argument("--output-dir", default="output", help="Output directory")
    parser.add_argument("--timeout", type=float, default=10.0, help="HTTP request timeout in seconds")
    parser.add_argument(
        "--user-agent",
        default="UFMG-IR-Crawler/1.0 (+academic project)",
        help="User-Agent header value",
    )
    parser.add_argument("--max-retries", type=int, default=2, help="Number of retries per request")
    parser.add_argument("--max-depth", type=int, default=None, help="Optional maximum crawl depth")
    parser.add_argument("--warc-prefix", default="corpus", help="Prefix for WARC files")
    parser.add_argument("--respect-nofollow", action="store_true", help="Respect rel=nofollow links")
    return parser.parse_args()


def load_seed_urls(seeds_path: Path) -> list[str]:
    if not seeds_path.exists():
        raise FileNotFoundError(f"Seeds file not found: {seeds_path}")

    urls: list[str] = []
    with seeds_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            urls.append(normalize_url(line))
    return urls


def main() -> int:
    args = parse_args()

    seeds_path = Path(args.seeds)
    output_dir = Path(args.output_dir)
    warc_dir = output_dir / "warc"
    stats_dir = output_dir / "stats"

    frontier = Frontier()
    stats = CrawlStats()
    storage = WARCStorage(output_dir=warc_dir, prefix=args.warc_prefix, pages_per_file=1000)
    robots_manager = RobotsManager(user_agent=args.user_agent, timeout=args.timeout)

    seeds = load_seed_urls(seeds_path)
    if not seeds:
        raise ValueError("The seeds file is empty.")

    for url in seeds:
        if frontier.add_if_new(url, depth=0):
            stats.register_discovery()

    config = CrawlConfig(
        limit=args.limit,
        debug=args.debug,
        timeout=args.timeout,
        user_agent=args.user_agent,
        max_retries=args.max_retries,
        max_depth=args.max_depth,
        respect_nofollow=args.respect_nofollow,
    )

    controller = CrawlController(
        frontier=frontier,
        robots_manager=robots_manager,
        storage=storage,
        stats=stats,
        config=config,
    )

    workers = [Worker(worker_id=i + 1, controller=controller) for i in range(args.threads)]

    try:
        for worker in workers:
            worker.start()

        while not controller.stop_event.is_set():
            if stats.pages_crawled >= args.limit:
                controller.stop_event.set()
                break
            time.sleep(0.5)

    except KeyboardInterrupt:
        controller.stop_event.set()
    finally:
        frontier.put_sentinel(len(workers))
        for worker in workers:
            worker.join()
        stats.finish()
        storage.close()
        stats.save(stats_dir / "crawl_stats.json")

    return 0


if __name__ == "__main__":
    sys.exit(main())
