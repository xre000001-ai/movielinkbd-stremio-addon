"""
MovieLinkBD — a STREAM-ONLY Stremio addon for the movielinksbd / MovieLinkBD
mirror family.  Torrentio-style: no catalogs; the user opens any movie or
series from their own catalogs and this addon serves direct streams.

MIRROR FAMILY (probed live 2026-09-19):
    primary   movieslinkbd.com  — WordPress + wp-json live search
              movielink.ch      — WordPress + wp-json live search
              movielinkbd.net   — WordPress (HTML ?s= search fallback)
    last-ditch movielinkbd.com / .one / .shop (Cloudflare-challenged from
              datacenter IPs; they fail fast and the next mirror answers)
    excluded  movielinkbd.tv  (user directive: "tv ta bade")
              movielinksbd.com (domain-parked on intivesearch)
              movielinkbd.to / .io / mlsbd.site (dead)

FLOW (zero-bandwidth — only tiny JSON/HTML/probes are fetched):
    id (tt… / tmdb:) -> Cinemeta / TMDB title + year
    -> mirror search (wp-json mlmbd/v1/search, HTML fallback)
    -> best title/year match -> movie page
    -> /generate/?file=<b64> buttons -> base64 FILENAMES (dedupe; quality,
       container and episode-range parsed from the filename itself — the
       button labels are routinely mislabeled, e.g. a "1080p" button that
       points at the 720p file)
    -> dl.vircloud.site/api/sign/<b64> -> signed direct downloadUrl
       (Cloudflare worker, HMAC sig, ~6 h expiry, no referer required)
    -> Range probe bytes=0-1 (positive-only gate: the sign API happily
       signs nonexistent files, so a card is emitted ONLY after the CDN
       really serves the first bytes)
    -> direct card.  No proxyHeaders: the CDN is ungated.

Sections: 1 config · 2 utilities · 3 metadata · 4 search/match ·
5 page files · 6 sign+probe · 7 cards · 8 landing · 9 server.
"""

import base64
import gzip
import json
import os
import re
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs, quote, unquote

import requests

# ----------------------------------------------------------------- 1. config
VERSION = "1.2.0"
BRAND = "MovieLinkBD"
PORT = int(os.environ.get("PORT", "7000"))
PUBLIC_URL = os.environ.get("MLSBD_PUBLIC_URL", "").rstrip("/")
TMDB_API_KEY = os.environ.get("MLSBD_TMDB_KEY", "1af06616dcbb28ff03088d87d63211f5")
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")

# The movielinksbd family.  movielinkbd.tv is deliberately absent.
MIRRORS = [
    # User directive: the current movielinkbd.li front.  Cloudflare-gated
    # from many datacenter IPs; the family mirrors below auto-answer when
    # this one is challenged.
    "https://u7n8gg.movielinkbd.li",
    "https://movieslinkbd.com",
    "https://movielink.ch",
    "https://movielinkbd.net",
    # Cloudflare-challenged from datacenter IPs; kept as last resort.
    "https://movielinkbd.com",
    "https://movielinkbd.one",
    "https://movielinkbd.shop",
]
SEARCH_PATH = "/wp-json/mlmbd/v1/search"      # ?term=<q> -> [{link,title,image}]

# The current movielinkbd.li front (user directive 2026-09-19): a custom
# hash/slug app with tokenised getWatch/getLink endpoints.  Cloudflare
# challenges every server-side request right now (verified from the addon
# host too via /debug/li); the adapter stays armed and auto-activates the
# moment the challenge lifts.  The prefix (u7n8gg) rotates — update
# LI_FRONT when the site moves.
LI_FRONT = os.environ.get("MLSBD_LI_FRONT", "https://u7n8gg.movielinkbd.li")
SIGN_API = "https://dl.vircloud.site/api/sign/"
HTTP_TIMEOUT = 12.0
PROBE_TIMEOUT = 15.0
MIRROR_LOCK = threading.Lock()
STATS = {"requests": 0, "streams": 0, "cards": 0, "searches": 0,
         "signs": 0, "probes": 0, "probe_dead": 0}

MANIFEST = {
    "id": "movielinksbd.stremio",
    "version": VERSION,
    "name": "MovieLinkBD (movielinksbd)",
    "description": (
        "Stream-only addon for the movielinksbd / MovieLinkBD mirror family "
        "(movieslinkbd.com, movielink.ch, movielinkbd.net and fallback "
        "mirrors; movielinkbd.tv excluded). Open any movie or series and "
        "direct 480p-2160p MKV/MP4 links from the site's vircloud CDN "
        "appear. No catalogs."),
    "types": ["movie", "series"],
    "idPrefixes": ["tt", "tmdb:"],
    "logo": "/logo.png",
    "behaviorHints": {"configurable": False},
    "catalogs": [],
    "resources": ["stream"],
}

HTTP = requests.Session()
HTTP.headers.update({"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"})

# -------------------------------------------------------------- 2. utilities
class TTLCache:
    """Small thread-safe positive cache (never stores transient negatives)."""

    def __init__(self, name, budget=4 * 1024 * 1024):
        self.name = name
        self.data = {}
        self.lock = threading.Lock()
        self.budget = budget

    def get(self, key):
        ent = self.data.get(key)
        if ent is None:
            return False, None
        expiry, value = ent
        if expiry <= time.time():
            with self.lock:
                self.data.pop(key, None)
            return False, None
        return True, value

    def put(self, key, value, ttl):
        with self.lock:
            self.data[key] = (time.time() + ttl, value)


C_SEARCH = TTLCache("search", 1 << 20)      # (mirror, term) -> results
C_PAGE = TTLCache("pages", 4 << 20)         # page url -> file list
C_SIGN = TTLCache("sign", 2 << 20)          # b64 -> signed url (until exp)
C_PROBE = TTLCache("probe", 1 << 20)        # b64 -> True (short positive)
C_ID = TTLCache("ids", 1 << 20)             # id -> (title, year)


def clean_text(value):
    text = re.sub(r"<[^>]+>", " ", str(value or ""))
    text = text.replace("&amp;", "&").replace("&#038;", "&")
    text = text.replace("&quot;", '"').replace("&#8217;", "'")
    return re.sub(r"\s+", " ", text).strip()


def _gzip(body):
    return gzip.compress(body.encode("utf-8"))


# --------------------------------------------------------------- 3. metadata
def resolve_id(identifier):
    """tt… -> Cinemeta, tmdb:… -> TMDB.  Returns (title, year) or None."""
    # Normalise: keep "tmdb:<id>" whole, drop any trailing :season:episode.
    m = re.fullmatch(r"(tt\d+|tmdb:\d+)(?::\d+:\d+)?", (identifier or "").strip())
    base = m.group(1) if m else (identifier or "").strip()
    lowered = base.lower()
    hit, value = C_ID.get(lowered)
    if hit:
        return value
    title, year = "", None
    try:
        if re.fullmatch(r"tt\d+", lowered):
            r = HTTP.get("https://v3-cinemeta.strem.io/meta/movie/%s.json" % base,
                         timeout=HTTP_TIMEOUT)
            if r.status_code != 200:
                r = HTTP.get("https://v3-cinemeta.strem.io/meta/series/%s.json" % base,
                             timeout=HTTP_TIMEOUT)
            if r.status_code == 200:
                meta = (r.json() or {}).get("meta") or {}
                title = meta.get("name") or ""
                year = _year_of(meta.get("releaseInfo") or meta.get("year"))
        elif lowered.startswith("tmdb"):
            tid = re.sub(r"^tmdb:?", "", base)
            r = HTTP.get("https://api.themoviedb.org/3/tv/%s" % tid,
                         params={"api_key": TMDB_API_KEY}, timeout=HTTP_TIMEOUT)
            if r.status_code != 200:
                r = HTTP.get("https://api.themoviedb.org/3/movie/%s" % tid,
                             params={"api_key": TMDB_API_KEY}, timeout=HTTP_TIMEOUT)
            if r.status_code == 200:
                d = r.json() or {}
                title = d.get("name") or d.get("title") or ""
                date = d.get("first_air_date") or d.get("release_date") or ""
                year = _year_of(date[:4])
    except Exception:
        return None
    title = clean_text(title)
    if not title:
        return None
    value = (title, year)
    C_ID.put(lowered, value, 6 * 3600)
    return value


def _year_of(value):
    m = re.search(r"(19|20)\d{2}", str(value or ""))
    return int(m.group(0)) if m else None


# ------------------------------------------------------------ 4. search/match
def mirror_search(mirror, term):
    """One mirror's results for a term: wp-json first, HTML ?s= fallback."""
    key = (mirror, term.lower())
    hit, value = C_SEARCH.get(key)
    if hit:
        return value
    results = []
    # wp-json live search (movieslinkbd.com / movielink.ch)
    try:
        r = HTTP.get(mirror + SEARCH_PATH, params={"term": term},
                     timeout=HTTP_TIMEOUT,
                     headers={"Referer": mirror + "/"})
        if r.status_code == 200:
            for item in r.json() or []:
                link = str(item.get("link") or "")
                title = clean_text(item.get("title"))
                if link and title and mirror in link:
                    results.append({"link": link, "title": title})
    except Exception:
        results = []
    # Legacy mirror (movielinkbd.net): plain WordPress search page.
    if not results:
        try:
            r = HTTP.get(mirror + "/", params={"s": term},
                         timeout=HTTP_TIMEOUT,
                         headers={"Referer": mirror + "/"})
            if r.status_code == 200:
                for href, label in re.findall(
                        r'<a[^>]+href="(%s/[a-z0-9-]{12,120}/)"[^>]*>(.{0,120}?)</a>'
                        % re.escape(mirror), r.text, re.S):
                    label = clean_text(label)
                    if label and not re.search(r'(?i)^(home|movies|series|contact|dmca|privacy|disclaimer|about)', label):
                        entry = {"link": href, "title": label}
                        if entry not in results:
                            results.append(entry)
        except Exception:
            pass
    if results:
        C_SEARCH.put(key, results, 1800)
    return results


# ----------------------------------------------------- 4b. movielinkbd.li app
def _li_challenge(text):
    """Cloudflare interstitial? (managed challenge blocks all server IPs)"""
    head = (text or "")[:4000]
    return "Just a moment" in head or "cf-challenge" in head


def li_search(term):
    """Search the .li app: /search?q= -> movie-card rows.

    Returns (status, rows) with status in {"ok", "blocked", "empty"}.
    """
    try:
        r = HTTP.get(LI_FRONT + "/search", params={"q": term},
                     timeout=HTTP_TIMEOUT, headers={"Referer": LI_FRONT + "/"})
    except Exception:
        return "blocked", []
    if r.status_code == 403 or _li_challenge(r.text):
        return "blocked", []
    if r.status_code != 200:
        return "empty", []
    rows = []
    # movie-card blocks: <div class="movie-card" ...> ... <a href=".../movie/<id>"
    # class="title">Title (YYYY)</a>
    for m in re.finditer(
            r'<a href="([^"]*?/movie/[A-Za-z0-9_+-]{6,120})"\s+class="title">([^<]+)</a>',
            r.text):
        link, title = m.group(1), clean_text(m.group(2))
        if link and title:
            if not link.startswith("http"):
                link = LI_FRONT + link
            rows.append({"link": link, "title": title})
    return ("ok" if rows else "empty"), rows


def li_page_buttons(page_url):
    """Parse a .li movie page into watch/download buttons.

    Ground truth (wayback snapshot 2026-03-11, 'The Gift (2015)'):
      /getWatch/<b64(b64(cipher))>  — Watch Online
      /getLink/<b64(b64(cipher))>   — Download [720p • 700 MB]
    The button LABEL carries the quality/size (there is no other honest
    source on this app).
    """
    files = []
    try:
        r = HTTP.get(page_url, timeout=HTTP_TIMEOUT,
                     headers={"Referer": LI_FRONT + "/"})
        if r.status_code != 200 or _li_challenge(r.text):
            return []
        body = r.text
    except Exception:
        return []
    for m in re.finditer(
            r'<a[^>]+href="([^"]*?)/(getWatch|getLink)/([A-Za-z0-9+/=_-]{20,})"[^>]*>(.*?)</a>',
            body, re.S):
        front, kind, token = m.group(1), m.group(2), m.group(3)
        label = re.sub(r"<[^>]+>|\s+", " ", m.group(4)).strip()
        quality = (re.search(r"\b(2160|1080|720|480|360)p?\b", label) or [None, ""])[1]
        quality = quality if quality.endswith("p") and quality != "p" else (quality + "p" if quality else "")
        size = (re.search(r"(\d+(?:\.\d+)?\s*(?:MB|GB))", label, re.I) or [None, ""])[1]
        entry = {"front": front, "kind": kind, "token": token,
                 "label": label, "quality": quality, "size": size,
                 "ep_lo": None, "ep_hi": None, "ext": ""}
        if entry not in files:
            files.append(entry)
    return files


FILE_URL_RE = re.compile(
    r'https?://[^"\'<>\s\\]+?\.(?:m3u8|mp4|mkv|webm|avi|m4v)(?:\?[^"\'<>\s\\]*)?',
    re.I)


def li_button_links(entry):
    """Follow one getWatch/getLink token and pull direct file URLs out.

    The response shape is unknown until the Cloudflare gate lifts, so this
    extracts defensively: direct media URLs, meta-refresh targets and plain
    redirects are all accepted.  Positive-only: nothing is guessed.
    """
    url = entry["front"] + "/" + entry["kind"] + "/" + entry["token"]
    try:
        r = HTTP.get(url, timeout=HTTP_TIMEOUT, stream=True,
                     allow_redirects=True, headers={"Referer": LI_FRONT + "/"})
        if r.status_code != 200 or _li_challenge(getattr(r, "text", "")):
            r.close()
            return []
        body = r.text if hasattr(r, "text") else ""
        r.close()
    except Exception:
        return []
    urls = []
    for u in FILE_URL_RE.findall(body):
        if u not in urls:
            urls.append(u)
    # The family's own CDN signs extension-less /download/<b64> URLs.
    for u in re.findall(r'https?://dl\.vircloud\.site/download/[A-Za-z0-9+/=_-]+',
                        body):
        if u not in urls:
            urls.append(u)
    for m in re.finditer(
            r'(?:http-equiv="refresh"[^>]+url=|window\.location(?:\.href)?\s*=\s*["\'])([^"\';]+)',
            body, re.I):
        u = m.group(1).strip()
        if u.startswith("http") and u not in urls:
            urls.append(u)
    return urls


def _title_score(query, candidate):
    a = re.sub(r"[^a-z0-9]+", " ", query.lower()).strip()
    b = re.sub(r"[^a-z0-9]+", " ", candidate.lower()).strip()
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    at, bt = set(a.split()), set(b.split())
    overlap = len(at & bt) / max(1, len(at | bt))
    if a in b or b in a:
        # Containment only counts when the shorter side carries most of the
        # longer one: "Hawa" must not match "Abar Hawa Bodol (2026)".
        ratio = min(len(a), len(b)) / max(len(a), len(b))
        return max(overlap, 0.80 * ratio)
    return overlap


def _title_variants(title):
    """The full title plus its pre-colon head: the sites list 'Dune' where
    Cinemeta says 'Dune: Part One'."""
    variants = [title]
    head = title.split(":")[0].strip()
    if ":" in title and len(head) >= 3 and head.lower() != title.lower():
        variants.append(head)
    return variants


def best_result(results, title, year):
    """Rank search rows: title similarity + '(YYYY)' year hint in the title."""
    best, best_key = None, None
    variants = _title_variants(title)
    for row in results:
        row_title = row["title"]
        m = re.search(r"\((19|20)\d{2}\)", row_title)
        row_year = int(m.group(0)[1:-1]) if m else None
        stripped = re.sub(r"\((19|20)\d{2}\).*", "", row_title)
        score = max(_title_score(v, stripped) for v in variants)
        if year and row_year:
            if abs(row_year - year) <= 1:
                score += 0.22
            elif abs(row_year - year) > 4:
                # A wildly different year is a different title entry.
                score -= 0.65
        # Prefer plainer rows ("Season N" rows rank below the base title).
        if re.search(r"(?i)season\s*\d+", row_title):
            score -= 0.05
        key = (round(score, 3), -len(row_title))
        if best is None or key > best_key:
            best, best_key = row, key
    if best is None or best_key[0] < 0.45:
        return None
    return best


# ------------------------------------------------------------- 5. page files
QUALITY_RE = re.compile(r"(?i)\b(2160|1440|1080|720|576|480|360|240)p?\b|\b4k\b")
EP_RANGE_RE = re.compile(r"(?i)episodes?[-_]?(\d{2,3})(\d{2,3})(?=[^0-9]|$)")
EP_ONE_RE = re.compile(r"(?i)episode[-_]?(\d{1,3})(?=[^0-9]|$)")


def parse_filename(filename):
    """Filename -> {quality, ext, ep_lo, ep_hi}.  The filename is the only
    honest source (buttons are routinely mislabeled)."""
    info = {"quality": "", "ext": "", "ep_lo": None, "ep_hi": None}
    m = QUALITY_RE.search(filename)
    if m:
        token = m.group(0).lower()
        info["quality"] = "2160p" if token == "4k" else (token if token.endswith("p") else token + "p")
    ext = os.path.splitext(filename)[1].lower()
    if ext in (".mkv", ".mp4", ".webm", ".avi", ".m4v"):
        info["ext"] = ext.lstrip(".").upper()
    m = EP_RANGE_RE.search(filename)
    if m:
        lo, hi = int(m.group(1)), int(m.group(2))
        if hi < lo:
            lo, hi = hi, lo
        if 1 <= lo <= hi <= 999:
            info["ep_lo"], info["ep_hi"] = lo, hi
    else:
        m = EP_ONE_RE.search(filename)
        if m:
            info["ep_lo"] = info["ep_hi"] = int(m.group(1))
    return info


def page_files(page_url):
    """Extract the ordered, deduplicated /generate/ file list from a page."""
    hit, value = C_PAGE.get(page_url)
    if hit:
        return value
    files = []
    try:
        r = HTTP.get(page_url, timeout=HTTP_TIMEOUT,
                     headers={"Referer": urlparse(page_url).scheme + "://" +
                              urlparse(page_url).netloc + "/"})
        if r.status_code != 200:
            return []
        seen = set()
        for m in re.finditer(r'[?&]file=([A-Za-z0-9+/=]{16,})', r.text):
            b64 = m.group(1)
            if b64 in seen:
                continue
            seen.add(b64)
            try:
                name = base64.b64decode(b64).decode("utf-8", "ignore")
            except Exception:
                continue
            if "." not in name:
                continue
            entry = {"b64": b64, "name": name}
            entry.update(parse_filename(name))
            files.append(entry)
    except Exception:
        return []
    if files:
        C_PAGE.put(page_url, files, 1800)
    return files


# ------------------------------------------------------------ 6. sign+probe
def sign_file(b64):
    """Signed direct URL from the vircloud worker (or cached copy)."""
    hit, value = C_SIGN.get(b64)
    if hit:
        return value
    url = ""
    try:
        r = HTTP.get(SIGN_API + b64, timeout=HTTP_TIMEOUT)
        if r.status_code == 200:
            url = str((r.json() or {}).get("downloadUrl") or "")
    except Exception:
        url = ""
    if not url.startswith("http"):
        return ""
    # Cache until shortly before the worker's own expiry (default ~6 h).
    ttl = 4 * 3600
    m = re.search(r"[?&]exp=(\d{6,})", url)
    if m:
        ttl = max(60.0, min(4 * 3600, int(m.group(1)) - time.time() - 600))
    C_SIGN.put(b64, url, ttl)
    STATS["signs"] += 1
    return url


def probe_file(url):
    """Positive-only honesty gate: the sign worker signs anything, so a card
    is only emitted after the CDN really serves the first byte(s)."""
    try:
        r = HTTP.get(url, headers={"Range": "bytes=0-1"}, stream=True,
                     timeout=PROBE_TIMEOUT)
        ok = r.status_code in (200, 206)
        if ok:
            # Read a couple of bytes and close: liveness probe, never a relay.
            next(r.iter_content(64), b"")
        r.close()
    except Exception:
        ok = False
    STATS["probes"] += 1
    if not ok:
        STATS["probe_dead"] += 1
    return ok


# ------------------------------------------------------------------ 7. cards
def _card(entry, signed, mirror, is_series):
    label_parts = []
    if is_series and entry.get("ep_lo") is not None:
        if entry["ep_lo"] == entry["ep_hi"]:
            label_parts.append("E%02d" % entry["ep_lo"])
        else:
            label_parts.append("E%02d-E%02d" % (entry["ep_lo"], entry["ep_hi"]))
    if entry.get("quality"):
        label_parts.append(entry["quality"])
    kind = entry.get("ext") or "FILE"
    label_parts.append(kind)
    name = "♧ %s · %s" % (BRAND, " · ".join(label_parts))
    shown = re.sub(r"(?i)^(mls?bd(\.com)?|movielink(\.one|bd)?|mlwbd)-", "",
                   entry["name"])
    stream = {
        "name": name,
        "title": shown[:80],
        "url": signed,
        "behaviorHints": {"notWebReady": entry.get("ext", "") != "MP4"},
    }
    return stream


def _li_card(entry, url):
    parts = [x for x in (entry.get("quality"), entry.get("size")) if x]
    ext = ""
    m = re.search(r"\.(mkv|mp4|webm|m4v|avi)(?:\?|$)", url, re.I)
    if m:
        ext = m.group(1).upper()
        parts.append(ext)
    name = "♧ %s · %s" % (BRAND, " · ".join(parts) if parts else "Direct")
    return {"name": name, "title": entry.get("label", "")[:80], "url": url,
            "behaviorHints": {"notWebReady": ext != "MP4"}}


def _li_try(title, year, is_series, season, episode):
    """Try the movielinkbd.li app first.  Returns a streams dict when it
    produced verified cards, else None so the WordPress mirrors answer."""
    status, rows = li_search(title)
    if status != "ok":
        return None
    row = best_result(rows, title, year)
    if not row:
        return None
    buttons = li_page_buttons(row["link"])
    if not buttons:
        return None
    streams = []
    for entry in buttons:
        for url in li_button_links(entry)[:2]:
            hit, ok = C_PROBE.get(entry["token"] + url[-24:])
            if not hit:
                ok = probe_file(url)
                if ok:
                    C_PROBE.put(entry["token"] + url[-24:], True, 600)
            if ok:
                streams.append(_li_card(entry, url))
    if not streams:
        return None
    return {"streams": streams[:12], "message": ""}


def build_streams(identifier, season=None, episode=None):
    """Full pipeline: id -> mirror search -> page -> signed direct cards.

    Accepts both bare ids ("tt0816692", "tmdb:123") and Stremio series
    identifiers ("tt…:1:4") — the season/episode suffix is parsed here as
    well, so the function is self-contained for tests and for the routes.
    """
    m = re.fullmatch(r"(tt\d+|tmdb:\d+)(?::(\d+):(\d+))?", identifier or "")
    if m:
        identifier = m.group(1)
        if episode is None and m.group(3):
            season, episode = int(m.group(2)), int(m.group(3))
    resolved = resolve_id(identifier)
    if not resolved:
        return {"streams": [], "message": "could not resolve the title id"}
    title, year = resolved
    is_series = episode is not None
    STATS["searches"] += 1
    # The .li app goes first (user directive).  While its Cloudflare gate
    # blocks servers this returns None instantly and the mirrors answer.
    li_out = _li_try(title, year, is_series, season, episode)
    if li_out is not None:
        return li_out
    # The wp-json search is literal: 'Dune: Part One' finds nothing while
    # 'Dune' does.  Try the full title, then its pre-colon head.
    terms = _title_variants(title)
    if len(title.split()) > 5:
        terms.append(" ".join(title.split()[:5]))
    row = None
    results = []
    used_mirror = ""
    for term in dict.fromkeys(terms):
        results = []
        for mirror in MIRRORS:
            results = mirror_search(mirror, term)
            if results:
                used_mirror = mirror
                break
        if not results:
            continue
        row = best_result(results, title, year)
        if row:
            break
    if not used_mirror:
        return {"streams": [], "message":
                "no mirror of the MovieLinkBD family answered for %r" % title}
    if not row:
        return {"streams": [], "message":
                "no MovieLinkBD page matched %r%s" % (
                    title, " (%d)" % year if year else "")}
    files = page_files(row["link"])
    if not files:
        return {"streams": [], "message":
                "the matched page carries no generate links"}
    # Series: prefer the batch that contains the requested episode.
    if is_series:
        in_range = [f for f in files
                    if f.get("ep_lo") is not None
                    and f["ep_lo"] <= int(episode) <= f["ep_hi"]]
        ordered = in_range or [f for f in files if f.get("ep_lo") is not None] or files
    else:
        ordered = files
    quality_rank = {"2160p": 6, "1440p": 5, "1080p": 4, "720p": 3,
                    "576p": 2, "480p": 2, "360p": 1, "240p": 1, "": 0}
    ordered = sorted(ordered, key=lambda f: -quality_rank.get(f.get("quality"), 0))
    streams = []
    for entry in ordered:
        signed = sign_file(entry["b64"])
        if not signed:
            continue
        hit, ok = C_PROBE.get(entry["b64"])
        if not hit:
            ok = probe_file(signed)
            if ok:
                C_PROBE.put(entry["b64"], True, 600)
        if not ok:
            continue
        streams.append(_card(entry, signed, used_mirror, is_series))
    if not streams:
        return {"streams": [], "message":
                "every file behind that page failed its liveness probe"}
    return {"streams": streams[:12], "message": ""}


# ---------------------------------------------------------------- 8. landing
_LANDING_HTML = """<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>MovieLinkBD — Stremio Addon</title>
<style>
body{background:#0b0f14;color:#e6edf3;font:16px/1.6 system-ui,sans-serif;
margin:0;padding:40px 16px}
.wrap{max-width:720px;margin:0 auto}
h1{color:#58d5ff;margin:0 0 6px}
p{margin:10px 0}
a.btn{display:inline-block;background:#16537e;color:#fff;padding:12px 22px;
border-radius:10px;text-decoration:none;font-weight:700;margin:14px 0}
code{background:#161c24;padding:2px 6px;border-radius:6px}
.mir{background:#11161d;border:1px solid #223041;border-radius:10px;
padding:10px 16px;margin:6px 0}
.ok{color:#3fd68f}.no{color:#ff7b72}
footer{margin-top:26px;color:#8b98a5;font-size:13px}
</style></head><body><div class="wrap">
<h1>MovieLinkBD</h1>
<p>Stream-only Stremio addon for the <b>movielinksbd / MovieLinkBD</b> family.
Open any movie or series in Stremio — direct 480p–2160p MKV/MP4 links from
the site's <code>vircloud</code> CDN appear. No catalogs, nothing is
re-hosted; only link metadata is served.</p>
<a class="btn" href="/manifest.json">Install in Stremio</a>
<h3>Mirror family</h3>
<div class="mir"><span class="ok">●</span> movieslinkbd.com — live</div>
<div class="mir"><span class="ok">●</span> movielink.ch — live</div>
<div class="mir"><span class="ok">●</span> movielinkbd.net — live</div>
<div class="mir"><span class="no">●</span> movielinkbd.com / .one / .shop — Cloudflare-gated fallbacks</div>
<div class="mir"><span class="no">●</span> movielinkbd.tv — excluded by request</div>
<div class="mir"><span class="no">●</span> movielinksbd.com (parked), movielinkbd.to / .io, mlsbd.site — dead</div>
<footer>Every card is emitted only after a real first-byte probe of the
signed CDN link (the sign worker signs anything — existence is proven per
file). Signed links expire after ~6&nbsp;hours and are refreshed on the next
request. This addon indexes third-party content and hosts no media.</footer>
</div></body></html>"""

LOGO_B64 = ("iVBORw0KGgoAAAANSUhEUgAAAGQAAABkCAMAAABHPGVmAAAAY1BMVEVnZO+nqHxnZPD/"
            "wL6mpmZiYv////+ioq2lpZ2dnKeFhYqAgIaEhIl0dIWFhomHhyiEhIBaWm1UVF1NTFky"
            "MkhCQkEpKSYAAAIpKSMfHyAcHCUWFhgQEBAICAgGBgYDAwEAAOtnZO/AAADfElEQVRo"
            "u+2Y65qDMAyF5VcQBLQQ2zZt27Zt27a+tP+fdKBbs6RJm/xI0mTi0uWSvJzH9zwdnp5/"
            "fU3wsL6urmZmZhLRTy19S8vRQITD7TiL02W8jLOocLFB1d596H+eOd2N7zJeP6dWXP2j"
            "vQj+JfQG85nwLrSL+AL6iPtCiZ/lRZzwNsSHN3jEl7q4miBBXIL3V1M8xovn0yDyNYQv"
            "ib7qY5y0dz1K7tPTvmGmA9Y6SNujLCcXdwzq1TruXTMwGvhA/JvQcb+A9x0mvHiK+nW"
            "mvthrpT69Lmh+/1i3gXQP8bPPwX5JX2T9gvOZ2V9GgpPnWY8+nfW48r6Qwv3e8/jC8qX"
            "1X+yBIr4xTSwP6TRGJ1vbe2SdGJ9ovrp/nKXwKnSDv2PXFLZlnuky7iZV7RDrFTryrh/"
            "3RqThCb2PUor+JH7wUb2je6Dc1unOvrYzl6v0MX37hZQ36oqGdH4EPOTmw/WNH/oyjM"
            "Nncd3KAD3L/zqzDX23tKyZXDMCCtZ/32VCQ1FdSBXCOdOTkS1p8fFcyxPZ7wQepvYRuk"
            "x+vYQD7KrGhnoa3pavcg7tPBUbsP4Wdw9Wzp8T2oFdXlUwrXT91Q3CH97pC2D9pPZ11e"
            "V15H+wD5mDCdz3BJbW7tGNTBfgK8tH6dwV7Q3gLfeH3Tl72b8jLy6R6yDjVjvCX0R3w"
            "NnwMrwNrwzrwxsTIMY4FrjU8m2Vdqxty1FpqK9sU5ei01FO2Kcvb3yDd/AG5bNKfS0z"
            "3AAAAABJRU5ErkJggg==")


# ----------------------------------------------------------------- 9. server
class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "movielinkbd/" + VERSION

    def log_message(self, fmt, *args):
        pass

    def _send(self, code, body, ctype="application/json", extra=None):
        if isinstance(body, str):
            body = body.encode("utf-8")
        accept = self.headers.get("Accept-Encoding") or ""
        head = {"Content-Type": ctype,
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "*",
                "Cache-Control": "public, max-age=180",
                "Connection": "close"}
        if extra:
            head.update(extra)
        if "gzip" in accept and len(body) > 500:
            body = gzip.compress(body)
            head["Content-Encoding"] = "gzip"
        head["Content-Length"] = str(len(body))
        self.send_response(code)
        for k, v in head.items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = self.path.split("?")[0]
        STATS["requests"] += 1
        try:
            if path in ("/", "/index.html"):
                return self._send(200, _LANDING_HTML, "text/html; charset=utf-8")
            if path == "/manifest.json":
                return self._send(200, json.dumps(MANIFEST))
            if path == "/health":
                return self._send(200, json.dumps(
                    {"ok": True, "addon": BRAND, "version": VERSION,
                     "uptime_s": round(time.time() - _START, 1),
                     "stats": STATS}))
            if path == "/debug/li":
                info = {}
                for tag, url in (
                        ("home", "https://u7n8gg.movielinkbd.li/"),
                        ("wpjson", "https://u7n8gg.movielinkbd.li"
                         "/wp-json/mlmbd/v1/search?term=interstellar"),
                        ("sign", "https://dl.vircloud.site/api/sign/")):
                    try:
                        r = HTTP.get(url, timeout=HTTP_TIMEOUT,
                                     headers={"User-Agent": UA})
                        info[tag] = {"status": r.status_code,
                                     "ctype": r.headers.get("Content-Type", ""),
                                     "head": r.text[:90]}
                    except Exception as exc:
                        info[tag] = {"error": str(exc)[:90]}
                return self._send(200, json.dumps(info))
            if path == "/logo.png":
                return self._send(200, base64.b64decode(LOGO_B64), "image/png",
                                  {"Cache-Control": "public, max-age=86400"})
            m = re.fullmatch(r"/stream/movie/([A-Za-z0-9:]+)\.json", path)
            if m:
                STATS["streams"] += 1
                out = build_streams(m.group(1))
                STATS["cards"] += len(out.get("streams") or [])
                return self._send(200, json.dumps(out))
            m = re.fullmatch(r"/stream/series/([A-Za-z0-9:]+):(\d+):(\d+)\.json", path)
            if m:
                STATS["streams"] += 1
                out = build_streams(m.group(1), int(m.group(2)), int(m.group(3)))
                STATS["cards"] += len(out.get("streams") or [])
                return self._send(200, json.dumps(out))
            return self._send(404, json.dumps(
                {"error": "not found", "paths": ["/", "/manifest.json",
                                                 "/stream/movie/<id>.json",
                                                 "/stream/series/<id>:s:e.json"]}))
        except Exception as exc:
            return self._send(500, json.dumps(
                {"error": "internal", "detail": str(exc)[:200]}))


_START = time.time()


def main():
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print("%s %s listening on :%d" % (BRAND, VERSION, PORT), flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
