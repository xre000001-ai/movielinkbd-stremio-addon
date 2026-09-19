# MovieLinkBD Stremio Addon

A source-aware Stremio addon for the public MovieLinkBD movie and web-series catalogue.

## What it provides

- Latest movie and series catalogs.
- Separate MovieLinkBD catalogs for Bangla, Hindi/Bollywood, Hindi Dubbed, English, Dual Audio, Web Series, Anime, Drama, Animation and Horror.
- Uses MovieLinkBD's public WordPress REST data when available to distinguish movies from series without fetching every detail page during catalog building.
- Search and pagination for the source pages that publish them.
- Direct source poster URLs; lazy placeholders are not used.
- Movie metadata and series episode lists parsed from MovieLinkBD detail pages.
- Source-published browser destinations for the source page and quality/download pages.
- No video-byte relay, DRM bypass, guessed HLS URL, or synthetic native playback URL.
- Adult-marked cards are excluded from catalogs.

MovieLinkBD currently exposes multiple domains. The addon tries `movielinkbd.net` first because it provides the accessible movie/series catalogue, then falls back through `.tv`, `.one`, `.work` and `.shop`. Override the order with `MOVIELINKBD_BASES`, for example:

```text
MOVIELINKBD_BASES=https://movielinkbd.net,https://movielinkbd.tv
```

## Run locally

```bash
python3 -m pip install -r requirements.txt
python3 addon.py
```

The addon listens on `http://127.0.0.1:7070` by default. For a deployed service, use:

```text
https://YOUR_HOST/manifest.json
```

## Tests

```bash
pytest -q
python3 test_movielinkbd.py
python3 -m py_compile addon.py test_movielinkbd.py
```

The tests cover HTML parsing, real lazy poster selection, adult filtering, type inference, source-ID round trips, movie downloads, episode links, cache semantics, mirror fallback, malformed input, route behavior, concurrency and the no-native/no-relay policy.

## Scope note

MovieLinkBD pages publish browser/download destinations rather than a verified portable HLS stream. Therefore the addon returns `externalUrl` cards only. Stremio opens those destinations in a browser; it does not pretend that a download page is native playback.
