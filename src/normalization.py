from __future__ import annotations

from urllib.parse import urljoin, urlparse, urlunparse

from url_normalize import url_normalize


DISALLOWED_SCHEMES = {"mailto", "javascript", "tel", "ftp", "file", "data"}
DISALLOWED_SUFFIXES = {
    ".pdf", ".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp", ".ico",
    ".zip", ".rar", ".7z", ".tar", ".gz", ".bz2", ".xz",
    ".mp3", ".wav", ".mp4", ".avi", ".mov", ".wmv", ".mkv",
    ".doc", ".docx", ".ppt", ".pptx", ".xls", ".xlsx",
    ".css", ".js", ".json", ".xml", ".rss", ".atom",
}


def normalize_url(url: str) -> str:
    normalized = url_normalize(url)
    parsed = urlparse(normalized)
    parsed = parsed._replace(fragment="")

    netloc = parsed.netloc.lower()
    if parsed.scheme == "http" and netloc.endswith(":80"):
        netloc = netloc[:-3]
    elif parsed.scheme == "https" and netloc.endswith(":443"):
        netloc = netloc[:-4]

    path = parsed.path or "/"
    rebuilt = parsed._replace(netloc=netloc, path=path)
    return urlunparse(rebuilt)


def resolve_and_normalize(base_url: str, link: str) -> str | None:
    if not link:
        return None

    absolute = urljoin(base_url, link.strip())
    parsed = urlparse(absolute)

    if parsed.scheme.lower() not in {"http", "https"}:
        return None

    if parsed.scheme.lower() in DISALLOWED_SCHEMES:
        return None

    lowered_path = parsed.path.lower()
    if any(lowered_path.endswith(suffix) for suffix in DISALLOWED_SUFFIXES):
        return None

    try:
        return normalize_url(absolute)
    except Exception:
        return None
