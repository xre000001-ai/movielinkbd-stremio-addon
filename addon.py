#!/usr/bin/env python3
"""MovieLinkBD Stremio addon.

The addon reads public MovieLinkBD catalog/detail pages and exposes:
- movie and series catalogs with source posters;
- metadata with episode lists where the source publishes them;
- source-accurate browser links for the source page and published download pages.

It deliberately does not proxy video bytes, bypass access controls, invent HLS
URLs, or turn a download page into a fake native stream. Native playback is
only safe when a real provider exposes a verified playable media URL; this site
currently publishes browser/download destinations, so responses use
``externalUrl`` only.
"""

from __future__ import annotations

import base64
import hashlib
import html as html_lib
import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, quote, unquote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter

VERSION = "0.1.0"
BRAND = "MovieLinkBD"
DEFAULT_BASES = (
    "https://movielinkbd.net",
    "https://movielinkbd.tv",
    "https://ecptfv.movielinkbd.li",
    "https://movielinkbd.one",
    "https://movielinkbd.work",
    "https://movielinkbd.shop",
)
SOURCE_BASES = tuple(
    x.rstrip("/") for x in os.environ.get("MOVIELINKBD_BASES", "").split(",")
    if x.strip()
) or DEFAULT_BASES
HTTP_TIMEOUT = float(os.environ.get("MOVIELINKBD_HTTP_TIMEOUT", "18"))
PAGE_TTL = float(os.environ.get("MOVIELINKBD_PAGE_TTL", "300"))
CATALOG_TTL = float(os.environ.get("MOVIELINKBD_CATALOG_TTL", "180"))
META_TTL = float(os.environ.get("MOVIELINKBD_META_TTL", "300"))
MAX_CATALOG_ITEMS = 30

CATALOGS = (
    {"id": "mlbd-latest-movies", "name": "MovieLinkBD · Latest Movies", "path": "/", "type": "movie"},
    {"id": "mlbd-latest-series", "name": "MovieLinkBD · Latest Series", "path": "/", "type": "series"},
    {"id": "mlbd-bangla-movies", "name": "MovieLinkBD · Bangla Movies", "path": "/category/bangla-movies/", "type": "movie"},
    {"id": "mlbd-web-series", "name": "MovieLinkBD · Web Series", "path": "/category/web-series/", "type": "series"},
    {"id": "mlbd-hindi-movies", "name": "MovieLinkBD · Hindi / Bollywood Movies", "path": "/category/hindi-movies/", "type": "movie"},
    {"id": "mlbd-hindi-dubbed-movies", "name": "MovieLinkBD · Hindi Dubbed Movies", "path": "/category/hindi-dubbed-movies/", "type": "movie"},
    {"id": "mlbd-english-movies", "name": "MovieLinkBD · English Movies", "path": "/category/english-movies/", "type": "movie"},
    {"id": "mlbd-dual-audio-movies", "name": "MovieLinkBD · Dual Audio Movies", "path": "/category/dual-audio/", "type": "movie"},
    {"id": "mlbd-dual-audio-series", "name": "MovieLinkBD · Dual Audio Series", "path": "/category/dual-audio/", "type": "series"},
    {"id": "mlbd-anime-movies", "name": "MovieLinkBD · Anime Movies", "path": "/category/anime/", "type": "movie"},
    {"id": "mlbd-anime-series", "name": "MovieLinkBD · Anime Series", "path": "/category/anime/", "type": "series"},
    {"id": "mlbd-drama-movies", "name": "MovieLinkBD · Drama Movies", "path": "/category/k-drama/", "type": "movie"},
    {"id": "mlbd-drama-series", "name": "MovieLinkBD · Drama Series", "path": "/category/k-drama/", "type": "series"},
    {"id": "mlbd-animation-movies", "name": "MovieLinkBD · Animation Movies", "path": "/category/animation/", "type": "movie"},
    {"id": "mlbd-animation-series", "name": "MovieLinkBD · Animation Series", "path": "/category/animation/", "type": "series"},
    {"id": "mlbd-horror-movies", "name": "MovieLinkBD · Horror Movies", "path": "/category/horror/", "type": "movie"},
    {"id": "mlbd-horror-series", "name": "MovieLinkBD · Horror Series", "path": "/category/horror/", "type": "series"},
)
CATALOG_BY_ID = {x["id"]: x for x in CATALOGS}

MANIFEST = {
    "id": "org.movielinkbd.stremio",
    "version": VERSION,
    "name": BRAND,
    "description": (
        "MovieLinkBD movie and series catalogs with source posters, metadata, "
        "episodes and source-accurate browser links. No video-byte relay."
    ),
    "logo": "https://pub-69b6234af505413bbeac2bbc8039195c.r2.dev/img/movielinkbd.webp",
    "types": ["movie", "series"],
    "idPrefixes": ["mlbd-", "mlbde-"],
    "resources": [
        {"name": "catalog", "types": ["movie", "series"]},
        {"name": "meta", "types": ["movie", "series"], "idPrefixes": ["mlbd-"]},
        {"name": "stream", "types": ["movie", "series"], "idPrefixes": ["mlbd-", "mlbde-"]},
    ],
    "catalogs": [
        {
            "type": item["type"],
            "id": item["id"],
            "name": item["name"],
            "extra": [
                {"name": "search", "isRequired": False},
                {"name": "skip", "isRequired": False},
            ],
        }
        for item in CATALOGS
    ],
    "behaviorHints": {"configurable": False, "configurationRequired": False},
}


class TTLCache:
    """Thread-safe positive cache; transient failures are never cached."""

    def __init__(self, name: str, budget: int = 8 * 1024 * 1024):
        self.name = name
        self.budget = budget
        self.data: dict[Any, tuple[float, Any]] = {}
        self.bytes = 0
        self.lock = threading.RLock()

    @staticmethod
    def _size(value: Any) -> int:
        try:
            return len(value) if isinstance(value, (str, bytes)) else len(repr(value))
        except Exception:
            return 256

    def get(self, key: Any) -> tuple[bool, Any]:
        with self.lock:
            entry = self.data.get(key)
            if entry is None:
                return False, None
            expiry, value = entry
            if expiry <= time.time():
                old = self.data.pop(key, None)
                if old is not None:
                    self.bytes -= self._size(old)
                return False, None
            return True, value

    def put(self, key: Any, value: Any, ttl: float) -> None:
        entry = (time.time() + ttl, value)
        with self.lock:
            old = self.data.get(key)
            if old is not None:
                self.bytes -= self._size(old)
            self.data[key] = entry
            self.bytes += self._size(entry)
            while self.bytes > self.budget and self.data:
                expired_key = min(self.data, key=lambda x: self.data[x][0])
                self.bytes -= self._size(self.data.pop(expired_key))

    def clear(self) -> None:
        with self.lock:
            self.data.clear()
            self.bytes = 0

    def stats(self) -> dict[str, Any]:
        with self.lock:
            return {"name": self.name, "entries": len(self.data), "bytes": self.bytes, "budget": self.budget}


C_HTML = TTLCache("html", 24 * 1024 * 1024)
C_CATALOG = TTLCache("catalog", 12 * 1024 * 1024)
C_META = TTLCache("meta", 16 * 1024 * 1024)
C_WP = TTLCache("wordpress-api", 12 * 1024 * 1024)
HTTP = requests.Session()
HTTP.headers.update({
    "User-Agent": os.environ.get(
        "MOVIELINKBD_USER_AGENT",
        "Mozilla/5.0 (compatible; MovieLinkBD-Stremio/0.1; +https://movielinkbd.net/)",
    ),
    "Accept-Language": "en-US,en;q=0.8",
})
HTTP.mount("https://", HTTPAdapter(pool_connections=16, pool_maxsize=32, max_retries=0))
HTTP.mount("http://", HTTPAdapter(pool_connections=16, pool_maxsize=32, max_retries=0))

STATS = {
    "started": time.time(),
    "catalog_requests": 0,
    "meta_requests": 0,
    "stream_requests": 0,
    "source_fetches": 0,
    "source_failures": 0,
    "browser_links": 0,
    "empty_results": 0,
}
STATS_LOCK = threading.Lock()
PARSE_POOL = ThreadPoolExecutor(max_workers=6, thread_name_prefix="mlbd-parse")


def stat(name: str, amount: int = 1) -> None:
    with STATS_LOCK:
        STATS[name] = STATS.get(name, 0) + amount


def clean_text(value: Any) -> str:
    text = html_lib.unescape(str(value or ""))
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def source_path(url_or_path: str) -> str:
    raw = str(url_or_path or "")
    parsed = urlparse(raw)
    path = parsed.path if parsed.scheme else raw.split("?", 1)[0]
    path = "/" + path.lstrip("/")
    return re.sub(r"/{2,}", "/", path)


def absolute_url(value: str, base: str) -> str:
    value = html_lib.unescape(str(value or "").strip())
    if not value or value.startswith("data:"):
        return ""
    return urljoin(base.rstrip("/") + "/", value)


def is_source_host(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return (
        host.endswith("movielinkbd.net")
        or host.endswith("movielinkbd.tv")
        or host.endswith("movielinkbd.li")
        or host.endswith("movielinkbd.one")
        or host.endswith("movielinkbd.work")
        or host.endswith("movielinkbd.shop")
    )


def make_source_id(path: str) -> str:
    raw = source_path(path).encode("utf-8")
    return "mlbd-" + base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def decode_source_id(identifier: str) -> str | None:
    value = str(identifier or "")
    if not value.startswith("mlbd-"):
        return None
    encoded = value[5:]
    if not encoded or len(encoded) > 900:
        return None
    try:
        decoded = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)).decode("utf-8")
    except Exception:
        return None
    if not decoded.startswith("/") or ".." in decoded or "\\" in decoded:
        return None
    return source_path(decoded)


def make_episode_id(path: str, episode_label: str) -> str:
    raw = (source_path(path) + "\x1f" + clean_text(episode_label)).encode("utf-8")
    return "mlbde-" + base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def decode_episode_id(identifier: str) -> tuple[str, str] | None:
    value = str(identifier or "")
    if not value.startswith("mlbde-"):
        return None
    encoded = value[6:]
    try:
        decoded = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)).decode("utf-8")
        path, label = decoded.split("\x1f", 1)
    except Exception:
        return None
    if not path.startswith("/") or ".." in path or "\\" in path or not clean_text(label):
        return None
    return source_path(path), clean_text(label)


def source_bases() -> tuple[str, ...]:
    raw = os.environ.get("MOVIELINKBD_BASES", "")
    if not raw.strip():
        return SOURCE_BASES
    values = tuple(x.rstrip("/") for x in raw.split(",") if x.strip())
    return values or SOURCE_BASES


def looks_like_challenge(text: str) -> bool:
    head = (text or "")[:12000].lower()
    return (
        "just a moment" in head
        or "performing security verification" in head
        or "cf-chl-" in head
        or "challenge-platform" in head
    )


def fetch_html(url: str, referer: str = "") -> str | None:
    key = (url, referer)
    hit, value = C_HTML.get(key)
    if hit:
        return value
    stat("source_fetches")
    try:
        response = HTTP.get(
            url,
            headers={"Referer": referer} if referer else {},
            timeout=HTTP_TIMEOUT,
            allow_redirects=True,
        )
    except requests.RequestException:
        stat("source_failures")
        return None
    if response.status_code != 200 or not response.text or looks_like_challenge(response.text):
        stat("source_failures")
        return None
    C_HTML.put(key, response.text, PAGE_TTL)
    return response.text


def fetch_source_path(path: str) -> tuple[str, str, str] | None:
    """Try the discovered active mirrors without caching a negative result."""
    path = source_path(path)
    for base in source_bases():
        url = base + path
        text = fetch_html(url)
        if text is not None:
            return base, url, text
    return None


def fetch_json(url: str, params: dict[str, Any] | None = None) -> Any:
    """Fetch a positive JSON response without negative-caching failures."""
    params = params or {}
    key = (url, tuple(sorted((str(k), str(v)) for k, v in params.items())))
    hit, value = C_WP.get(key)
    if hit:
        return value
    try:
        response = HTTP.get(url, params=params, timeout=HTTP_TIMEOUT, headers={"Accept": "application/json"})
        if response.status_code != 200:
            return None
        value = response.json()
    except (requests.RequestException, ValueError):
        return None
    if isinstance(value, (dict, list)) and value:
        C_WP.put(key, value, PAGE_TTL)
        return value
    return None


def wordpress_category_id(base: str, slug: str) -> int | None:
    if not slug:
        return None
    value = fetch_json(base.rstrip("/") + "/wp-json/wp/v2/categories", {"slug": slug, "per_page": 1})
    if isinstance(value, list) and value and isinstance(value[0], dict) and value[0].get("id"):
        return int(value[0]["id"])
    return None


def wordpress_kind(post: dict[str, Any]) -> str:
    content = post.get("content", {}) if isinstance(post, dict) else {}
    rendered = content.get("rendered", "") if isinstance(content, dict) else ""
    title = clean_text((post.get("title") or {}).get("rendered", "")) if isinstance(post, dict) else ""
    text = clean_text(rendered)
    if re.search(r"\b(?:series\s+info|series\s+name|total\s+seasons|total\s+episodes)\b", text, re.I):
        return "series"
    if re.search(r"\b(?:movie\s+details|full\s+movie\s+name|runtime)\b", text, re.I):
        return "movie"
    return infer_kind(str(post.get("link", "")) if isinstance(post, dict) else "", title, text)


def wordpress_posts(base: str, definition: dict[str, Any], page: int, search: str) -> list[dict[str, Any]] | None:
    """Use the public WordPress API when available for authoritative type data."""
    if "movielinkbd.net" not in (urlparse(base).hostname or "").lower():
        return None
    params: dict[str, Any] = {
        "per_page": MAX_CATALOG_ITEMS,
        "page": page,
        "_fields": "id,link,title,content,excerpt,categories,genre",
    }
    if search:
        params["search"] = search
    else:
        path_match = re.search(r"/category/([^/]+)/", definition.get("path", ""))
        if path_match:
            category_id = wordpress_category_id(base, path_match.group(1))
            if category_id is None:
                return None
            params["categories"] = category_id
    value = fetch_json(base.rstrip("/") + "/wp-json/wp/v2/posts", params)
    return value if isinstance(value, list) else None


def text_from_meta(soup: BeautifulSoup, selector: str) -> str:
    values = [clean_text(x.get("content")) for x in soup.select(selector) if x.get("content")]
    return max(values, key=len, default="")


def first_year(*values: str) -> int | None:
    for value in values:
        match = re.search(r"\b((?:19|20)\d{2})\b", value or "")
        if match:
            return int(match.group(1))
    return None


def infer_kind(path: str, title: str, detail_text: str = "") -> str:
    lowered = " ".join((path, title, detail_text)).lower()
    if re.search(r"/(?:series|drama)/", path.lower()):
        return "series"
    if re.search(r"\b(?:series\s+name|season|episode|ep\.?\s*\d+|s\d{1,2}\b|web\s*series|ongoing)\b", lowered):
        return "series"
    return "movie"


def is_adult_item(title: str, card: Any = None) -> bool:
    if card is not None and str(card.get("data-adult", "0")) == "1":
        return True
    return bool(re.search(r"(?:\b18\s*\+|\badult\b(?!\s+education\b)|download18plus)", title or "", re.I))


def parse_listing(text: str, base_url: str = "https://movielinkbd.net") -> list[dict[str, Any]]:
    """Parse MovieLinkBD cards and keep real poster URLs only."""
    soup = BeautifulSoup(text or "", "lxml")
    anchors = soup.select("a.yv-movie-card")
    if not anchors:
        anchors = [a for a in soup.find_all("a", href=True) if a.find("img")]
    output: list[dict[str, Any]] = []
    seen: set[str] = set()
    for anchor in anchors:
        href = absolute_url(anchor.get("href", ""), base_url)
        if not href or not is_source_host(href):
            continue
        path = source_path(href)
        if path in ("/", "/page/2/") or any(path.startswith(prefix) for prefix in ("/category/", "/tag/", "/author/", "/feed/", "/wp-")):
            continue
        title = clean_text(anchor.get("title"))
        if not title:
            title_node = anchor.select_one(".yv-movie-title")
            title = clean_text(title_node.get_text(" ", strip=True) if title_node else "")
        image = anchor.select_one("img")
        if not title and image:
            title = clean_text(image.get("alt"))
        if not title or path in seen:
            continue
        seen.add(path)
        poster = ""
        if image:
            attrs = image.attrs
            poster = absolute_url(attrs.get("data-lazy-src") or attrs.get("data-src") or attrs.get("src", ""), base_url)
        rating = clean_text(anchor.select_one(".yv-movie-rating").get_text(" ", strip=True) if anchor.select_one(".yv-movie-rating") else "")
        adult = is_adult_item(title, anchor)
        output.append({
            "id": make_source_id(path),
            "source_path": path,
            "source_url": href,
            "name": title,
            "poster": poster,
            "kind": infer_kind(path, title),
            "year": first_year(title),
            "rating": rating,
            "adult": adult,
        })
    return output


def parse_detail(text: str, path: str, base_url: str) -> dict[str, Any] | None:
    soup = BeautifulSoup(text or "", "lxml")
    h1 = soup.select_one("h1")
    title = clean_text(h1.get_text(" ", strip=True) if h1 else "")
    if not title:
        title = text_from_meta(soup, 'meta[property="og:title"]')
        title = re.sub(r"\s*[-|]\s*MovieLinkBD.*$", "", title, flags=re.I).strip()
    if not title:
        return None
    poster = ""
    hero = soup.select_one(".yv-poster img")
    if hero:
        poster = absolute_url(hero.get("data-src") or hero.get("src", ""), base_url)
    if not poster:
        for image in soup.find_all("img"):
            candidate = absolute_url(image.get("data-src") or image.get("src", ""), base_url)
            if candidate and "movielinkbd.webp" not in candidate and "simran" not in candidate and "gravatar" not in candidate:
                poster = candidate
                break
    description = text_from_meta(soup, 'meta[property="og:description"]')
    if not description:
        description = text_from_meta(soup, 'meta[name="description"]')
    kind = infer_kind(path, title, description)
    rows = soup.select(".yv-ep-row")
    if rows:
        kind = "series"
    display_name = title
    name_match = re.search(r"(?:Series Name|Full Movie Name)\s*:\s*(.*?)(?:\s+Genres:|\s+Release Date|\s+Release Dates|$)", description, re.I)
    if name_match:
        display_name = clean_text(name_match.group(1))
    season_match = re.search(r"(?:season|s)\s*0?(\d+)", title, re.I)
    season = int(season_match.group(1)) if season_match else 1
    videos: list[dict[str, Any]] = []
    seen_labels: set[str] = set()
    for row in rows:
        label_node = row.select_one(".yv-ep-title")
        label = clean_text(label_node.get_text(" ", strip=True) if label_node else "")
        if (not label
                or re.search(r"(?:episode|ep)\s*\d+\s*[-–]\s*\d+", label, re.I)
                or not re.search(r"(?:episode|ep)\s*\d+", label, re.I)):
            continue
        episode_match = re.search(r"(?:episode|ep)\s*0?(\d+)", label, re.I)
        if not episode_match or label in seen_labels:
            continue
        seen_labels.add(label)
        videos.append({
            "id": make_episode_id(path, label),
            "title": label,
            "season": season,
            "episode": int(episode_match.group(1)),
            "thumbnail": poster,
        })
    return {
        "id": make_source_id(path),
        "type": kind,
        "name": display_name,
        "poster": poster,
        "posterShape": "poster",
        "description": description[:4000] if description else "MovieLinkBD source metadata",
        "year": first_year(title, description),
        "videos": videos,
        "source_url": absolute_url(path, base_url),
        "source_path": source_path(path),
        "genres": parse_genres(description),
    }


def parse_genres(description: str) -> list[str]:
    match = re.search(r"Genres?\s*:\s*(.*?)(?:\s+Release|\s+Runtime|\s+Country|\s+Language|$)", description or "", re.I)
    if not match:
        return []
    return [clean_text(x) for x in re.split(r",|/|·", match.group(1)) if clean_text(x)][:8]


def parse_quality(label: str) -> str:
    match = re.search(r"\b(2160|1440|1080|720|480|360)\s*p\b", label or "", re.I)
    return match.group(1) + "p" if match else ""


def parse_download_links(text: str, path: str, episode_label: str = "") -> list[dict[str, str]]:
    """Extract only source-published browser destinations, never media bytes."""
    soup = BeautifulSoup(text or "", "lxml")
    selected_rows = soup.select(".yv-ep-row")
    if episode_label:
        wanted_number = re.search(r"(?:episode|ep)\s*0?(\d+)", episode_label, re.I)
        selected = []
        for row in selected_rows:
            row_title_node = row.select_one(".yv-ep-title")
            row_label = clean_text(row_title_node.get_text(" ", strip=True) if row_title_node else "")
            if re.search(r"(?:episode|ep)\s*\d+\s*[-–]\s*\d+", row_label, re.I):
                continue
            exact = row_label == clean_text(episode_label)
            number_match = wanted_number and re.search(
                r"(?:episode|ep)\s*0?" + re.escape(wanted_number.group(1)) + r"(?!\s*[-–]\s*\d+)",
                row_label,
                re.I,
            )
            if exact or number_match:
                selected.append(row)
        selected_rows = selected
    links: list[dict[str, str]] = []
    seen: set[str] = set()
    containers = selected_rows if episode_label else soup.select(".dl-box, .yv-ep-row")
    for container in containers:
        label_text = clean_text(container.get_text(" ", strip=True))
        for anchor in container.find_all("a", href=True):
            href = absolute_url(anchor.get("href", ""), "https://kitecloud.me/")
            host = (urlparse(href).hostname or "").lower()
            if host != "kitecloud.me" or href in seen:
                continue
            seen.add(href)
            label = clean_text(anchor.get_text(" ", strip=True)) or label_text
            quality = parse_quality(label + " " + label_text)
            size_match = re.search(r"\b(\d+(?:\.\d+)?\s*(?:MB|GB))\b", label_text, re.I)
            links.append({"url": href, "label": label, "quality": quality, "size": size_match.group(1) if size_match else ""})
    if not links and not episode_label:
        for anchor in soup.find_all("a", href=True):
            href = absolute_url(anchor.get("href", ""), "https://kitecloud.me/")
            if (urlparse(href).hostname or "").lower() == "kitecloud.me" and href not in seen:
                seen.add(href)
                label = clean_text(anchor.get_text(" ", strip=True))
                links.append({"url": href, "label": label, "quality": parse_quality(label), "size": ""})
    return links


def catalog_definition(catalog_id: str) -> dict[str, Any] | None:
    return CATALOG_BY_ID.get(str(catalog_id or ""))


def catalog_path(definition: dict[str, Any], page: int, search: str) -> str:
    if search:
        suffix = "?s=" + quote(search, safe="")
        if page > 1:
            suffix += "&page=" + str(page)
        return "/" + suffix
    path = definition["path"]
    if page <= 1 or path != "/":
        return path
    return "/page/%d/" % page


def catalog_page_items(definition: dict[str, Any], page: int, search: str) -> list[dict[str, Any]] | None:
    fetched = fetch_source_path(catalog_path(definition, page, search))
    if not fetched:
        return None
    base, _url, text = fetched
    cards = {item["source_path"]: item for item in parse_listing(text, base)}
    authoritative = wordpress_posts(base, definition, page, search)
    if authoritative is not None:
        parsed = []
        for post in authoritative:
            link = post.get("link", "") if isinstance(post, dict) else ""
            path = source_path(link)
            card = dict(cards.get(path, {}))
            title = clean_text((post.get("title") or {}).get("rendered", ""))
            if not title:
                title = card.get("name", "")
            card.update({
                "id": make_source_id(path),
                "source_path": path,
                "source_url": link,
                "name": title,
                "kind": wordpress_kind(post),
                "adult": 11 in (post.get("categories") or []) or is_adult_item(title),
            })
            parsed.append(card)
    else:
        parsed = list(cards.values())
    return [
        x for x in parsed
        if x.get("source_path") and not x.get("adult") and x.get("kind") == definition["type"]
    ]


def catalog_meta(item: dict[str, Any], definition: dict[str, Any]) -> dict[str, Any]:
    meta = {
        "id": item["id"],
        "type": definition["type"],
        "name": item["name"],
        "poster": item.get("poster", ""),
        "posterShape": "poster",
        "description": "MovieLinkBD · %s" % ("series" if definition["type"] == "series" else "movie"),
    }
    if item.get("year"):
        meta["year"] = item["year"]
    return meta


def catalog_items(catalog_id: str, search: str = "", skip: int = 0) -> list[dict[str, Any]]:
    definition = catalog_definition(catalog_id)
    if not definition:
        return []
    try:
        skip = max(0, int(skip or 0))
    except (TypeError, ValueError):
        skip = 0
    if skip and definition["path"] != "/" and not search:
        return []
    key = (catalog_id, clean_text(search).lower(), skip)
    hit, value = C_CATALOG.get(key)
    if hit:
        return value
    stat("catalog_requests")
    parsed: list[dict[str, Any]] = []
    if definition["path"] == "/" and not search and skip:
        raw_page = 1
        target = skip + MAX_CATALOG_ITEMS
        while len(parsed) < target and raw_page <= 12:
            page_items = catalog_page_items(definition, raw_page, "")
            if page_items is None:
                break
            parsed.extend(page_items)
            if not page_items:
                break
            raw_page += 1
        selected = parsed[skip:skip + MAX_CATALOG_ITEMS]
    else:
        page = skip // MAX_CATALOG_ITEMS + 1
        page_items = catalog_page_items(definition, page, search)
        if page_items is None:
            return []
        start = 0 if search else (skip % MAX_CATALOG_ITEMS if page > 1 else 0)
        selected = page_items[start:start + MAX_CATALOG_ITEMS]
    result = [catalog_meta(item, definition) for item in selected]
    if result:
        C_CATALOG.put(key, result, CATALOG_TTL)
    return result

def metadata_for(identifier: str) -> dict[str, Any] | None:
    path = decode_source_id(identifier)
    if not path:
        return None
    hit, value = C_META.get(path)
    if hit:
        return value
    fetched = fetch_source_path(path)
    if not fetched:
        return None
    base, _url, text = fetched
    value = parse_detail(text, path, base)
    if value:
        C_META.put(path, value, META_TTL)
    return value


def stream_cards_for(identifier: str, media_type: str) -> list[dict[str, Any]]:
    episode = decode_episode_id(identifier)
    if episode:
        path, episode_label = episode
    else:
        path, episode_label = decode_source_id(identifier), ""
    if not path:
        return []
    fetched = fetch_source_path(path)
    if not fetched:
        return []
    base, source_url, text = fetched
    title = (parse_detail(text, path, base) or {}).get("name", "MovieLinkBD title")
    links = parse_download_links(text, path, episode_label)
    cards: list[dict[str, Any]] = [{
        "name": "%s · Open source page · browser" % title,
        "description": "MovieLinkBD source page · browser only · no media relay",
        "externalUrl": source_url,
        "behaviorHints": {"bingeGroup": "mlbd|" + path},
    }]
    for link in links:
        quality = (" · " + link["quality"]) if link.get("quality") else ""
        size = (" · " + link["size"]) if link.get("size") else ""
        cards.append({
            "name": "%s%s%s · browser link" % (title, quality, size),
            "description": "Source-published browser/download page · %s · no native URL synthesized" % (link.get("label") or "MovieLinkBD link"),
            "externalUrl": link["url"],
            "behaviorHints": {"bingeGroup": "mlbd|" + path},
        })
        stat("browser_links")
    return cards


def split_config_path(path: str) -> tuple[str, str]:
    clean = "/" + str(path or "").lstrip("/")
    return "", clean


def json_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def public_base(handler: BaseHTTPRequestHandler) -> str:
    forwarded = handler.headers.get("X-Forwarded-Proto", "").split(",")[0].strip()
    scheme = forwarded or ("https" if handler.headers.get("X-Forwarded-Proto") else "http")
    host = handler.headers.get("Host", "127.0.0.1:7070")
    return scheme + "://" + host


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args: Any) -> None:
        return

    def send_body(self, code: int, body: Any, content_type: str = "application/json", cache: int = 0) -> None:
        raw = body if isinstance(body, bytes) else (body.encode("utf-8") if isinstance(body, str) else json_bytes(body))
        self.send_response(code)
        self.send_header("Content-Type", content_type + ("; charset=utf-8" if content_type.startswith(("text/", "application/json")) else ""))
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "public, max-age=%d" % cache if cache else "no-store")
        self.end_headers()
        try:
            self.wfile.write(raw)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)
        try:
            if path in ("/", "/health"):
                if path == "/":
                    return self.send_body(200, {"addon": BRAND, "version": VERSION, "manifest": "/manifest.json"})
                with STATS_LOCK:
                    stats = dict(STATS)
                return self.send_body(200, {
                    "ok": True,
                    "addon": BRAND,
                    "version": VERSION,
                    "sourceBases": list(source_bases()),
                    "uptime_s": int(time.time() - stats["started"]),
                    "stats": stats,
                    "caches": [C_HTML.stats(), C_CATALOG.stats(), C_META.stats()],
                    "egress": "catalog/meta JSON and external browser links only; no video-byte relay",
                })
            if path in ("/manifest", "/manifest.json"):
                return self.send_body(200, MANIFEST, cache=600)
            match = re.match(r"^/catalog/(movie|series)/([^/]+)\.json$", path)
            if match:
                catalog_id = unquote(match.group(2))
                definition = catalog_definition(catalog_id)
                if not definition or definition["type"] != match.group(1):
                    return self.send_body(404, {"error": "unknown catalog"})
                search = (query.get("search") or [""])[0]
                skip = (query.get("skip") or ["0"])[0]
                return self.send_body(200, {"metas": catalog_items(catalog_id, search, skip)}, cache=120)
            match = re.match(r"^/meta/(movie|series)/([^/]+)\.json$", path)
            if match:
                stat("meta_requests")
                meta = metadata_for(unquote(match.group(2)))
                if not meta:
                    return self.send_body(404, {"error": "metadata unavailable"})
                if meta["type"] != match.group(1):
                    return self.send_body(404, {"error": "source type mismatch"})
                return self.send_body(200, {"meta": meta}, cache=120)
            match = re.match(r"^/stream/(movie|series)/([^/]+)\.json$", path)
            if match:
                stat("stream_requests")
                cards = stream_cards_for(unquote(match.group(2)), match.group(1))
                if not cards:
                    stat("empty_results")
                    return self.send_body(200, {"streams": [], "message": "no source-published browser link found"})
                return self.send_body(200, {"streams": cards}, cache=60)
            return self.send_body(404, {"error": "not found"})
        except Exception:
            stat("empty_results")
            return self.send_body(500, {"error": "internal server error"})


def run() -> None:
    port = int(os.environ.get("PORT", "7070"))
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print("%s %s listening on :%d" % (BRAND, VERSION, port), flush=True)
    server.serve_forever()


if __name__ == "__main__":
    run()
