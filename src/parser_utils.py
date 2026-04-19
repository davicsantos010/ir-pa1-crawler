from __future__ import annotations

from bs4 import BeautifulSoup

from src.normalization import resolve_and_normalize


def build_soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


def extract_title(soup: BeautifulSoup) -> str:
    if soup.title:
        return " ".join(soup.title.get_text(" ", strip=True).split())
    return ""


def extract_visible_text(soup: BeautifulSoup) -> str:
    cloned = BeautifulSoup(str(soup), "html.parser")
    for tag in cloned(["script", "style", "noscript", "template"]):
        tag.decompose()
    text = cloned.get_text(" ", strip=True)
    return " ".join(text.split())


def first_n_words(text: str, n: int = 20) -> str:
    return " ".join(text.split()[:n])


def extract_links(soup: BeautifulSoup, base_url: str, respect_nofollow: bool = False) -> list[str]:
    links: list[str] = []
    for anchor in soup.find_all("a", href=True):
        rel_values = anchor.get("rel") or []
        if respect_nofollow and any(str(value).lower() == "nofollow" for value in rel_values):
            continue

        normalized = resolve_and_normalize(base_url, anchor.get("href"))
        if normalized:
            links.append(normalized)
    return links
