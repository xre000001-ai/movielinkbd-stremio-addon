#!/usr/bin/env python3
"""Extensive deterministic tests for the MovieLinkBD Stremio addon."""

import json
import re
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest import mock
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import addon


LISTING = """
<div class="yv-movie-grid">
  <a class="yv-movie-card" data-adult="0" href="/the-awakening-2026/" title="The Awakening (2026)">
    <div class="yv-movie-poster"><img class="yv-movie-img" src="data:image/svg+xml,placeholder" data-src="https://img.test/awakening.jpg" alt="The Awakening (2026)"></div>
    <div class="yv-movie-title">The Awakening (2026)</div>
  </a>
  <a class="yv-movie-card" data-adult="0" href="/bachelor-point-season-5-chapter-15-episode-113-120/" title="Bachelor Point (Season 5) Chapter 15 | Episode 113-120">
    <img src="https://img.test/bachelor.jpg" alt="Bachelor Point">
    <div class="yv-movie-title">Bachelor Point (Season 5) Chapter 15 | Episode 113-120</div>
  </a>
  <a class="yv-movie-card" data-adult="1" href="/download18plus/explicit-show/" title="Explicit Show 18+">
    <img src="https://img.test/adult.jpg" alt="Explicit Show">
  </a>
  <a class="yv-movie-card" data-adult="0" href="/the-awakening-2026/" title="Duplicate">
    <img src="https://img.test/duplicate.jpg" alt="Duplicate">
  </a>
</div>
"""

GENERIC_LISTING = """
<a href="/simple-movie-2024/"><img data-lazy-src="//img.test/simple.jpg" alt="Simple Movie (2024)"></a>
<a href="https://movielinkbd.net/simple-series-season-2/"><img src="https://img.test/series.jpg" alt="Simple Series Season 2"></a>
"""

MOVIE_DETAIL = """
<html><head>
<meta property="og:title" content="The Awakening (2026) - MovieLinkBD">
<meta property="og:description" content="Movie Details &amp; Info: Full Movie Name: The Awakening (2026) Genres: Horror, Action, War Release Date: April 19, 2026 Quality: 480p | 720p | 1080p">
<meta property="og:description" content="Movie Details &amp; Info: Full Movie Name: The Awakening (2026) Genres: Horror, Action, War Release Date: April 19, 2026 Runtime: 1 Hour 36 Minutes Quality: 480p | 720p | 1080p [WEB-DL]">
</head><body>
<header><img src="https://img.test/logo.webp"></header>
<div class="yv-hero-grid"><div class="yv-poster"><img src="https://img.test/awakening.jpg" alt="The Awakening"></div></div>
<h1>The Awakening (2026)</h1>
<div class="yv-panel"><div class="dl-grid">
 <div class="dl-box"><div class="dl-quality-text">480P</div><span class="dl-size-badge">430 MB</span><a class="dl-action-btn" href="https://kitecloud.me/low">Download Link</a></div>
 <div class="dl-box"><div class="dl-quality-text">720P</div><span class="dl-size-badge">940 MB</span><a class="dl-action-btn" href="https://kitecloud.me/mid">Download Link</a></div>
 <div class="dl-box"><div class="dl-quality-text">1080P</div><span class="dl-size-badge">2.2 GB</span><a class="dl-action-btn" href="https://kitecloud.me/high">Download Link</a></div>
</div></div>
<a href="https://evil.example/not-source">Ignore</a>
</body></html>
"""

SERIES_DETAIL = """
<html><head>
<h1>Bachelor Point (Season 5) Chapter 15 | Episode 113-120</h1>
<meta property="og:description" content="Series Info &amp; Technical Details: Series Name: Bachelor Point (Season 5) Genres: Comedy, Drama, Family Release Dates: June 7, 2025 Language: Bengali Quality: 480p | 720p">
</head><body>
<div class="yv-poster"><img src="https://img.test/bachelor.jpg"></div>
<div class="yv-ep-list">
 <div class="yv-ep-row"><span class="yv-tag">Zip Pack</span><span class="yv-ep-title">Ep 113-116</span><a href="https://kitecloud.me/pack">480p Pack</a></div>
 <div class="yv-ep-row"><span class="yv-tag">NEW</span><span class="yv-ep-title">Episode 113</span><span>Bengali</span><a href="https://kitecloud.me/e113-720">720p</a><a href="https://kitecloud.me/e113-1080">1080p</a></div>
 <div class="yv-ep-row"><span class="yv-tag">NEW</span><span class="yv-ep-title">Episode 114</span><span>Bengali</span><a href="https://kitecloud.me/e114-720">720p</a></div>
</div>
</body></html>
"""


class MovieLinkBDTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = addon.ThreadingHTTPServer(("127.0.0.1", 0), addon.Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base = "http://127.0.0.1:%d" % cls.server.server_address[1]

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)

    def setUp(self):
        for cache in (addon.C_HTML, addon.C_CATALOG, addon.C_META, addon.C_WP):
            cache.clear()

    def get(self, path):
        try:
            response = urlopen(Request(self.base + path), timeout=8)
            return response.status, dict(response.headers), response.read()
        except HTTPError as exc:
            return exc.code, dict(exc.headers), exc.read()

    def test_manifest_has_both_types_and_expected_catalogs(self):
        self.assertEqual(addon.MANIFEST["types"], ["movie", "series"])
        self.assertIn("mlbd-latest-movies", {x["id"] for x in addon.MANIFEST["catalogs"]})
        self.assertIn("mlbd-latest-series", {x["id"] for x in addon.MANIFEST["catalogs"]})
        self.assertIn("mlbd-", addon.MANIFEST["idPrefixes"])
        self.assertIn("mlbde-", addon.MANIFEST["idPrefixes"])

    def test_clean_text_handles_entities_tags_and_whitespace(self):
        self.assertEqual(addon.clean_text("  A&nbsp; <b>B</b>\n C "), "A B C")
        self.assertEqual(addon.clean_text(None), "")
        self.assertEqual(addon.clean_text("<script>x</script>Title"), "x Title")

    def test_source_path_is_safe_and_normalized(self):
        cases = {
            "https://movielinkbd.net/a/b/?x=1": "/a/b/",
            "/a//b/": "/a/b/",
            "a/b": "/a/b",
            "": "/",
        }
        for value, expected in cases.items():
            with self.subTest(value=value):
                self.assertEqual(addon.source_path(value), expected)

    def test_absolute_url_supports_relative_protocol_and_data_values(self):
        base = "https://movielinkbd.net"
        self.assertEqual(addon.absolute_url("/poster.jpg", base), "https://movielinkbd.net/poster.jpg")
        self.assertEqual(addon.absolute_url("//cdn.test/poster.jpg", base), "https://cdn.test/poster.jpg")
        self.assertEqual(addon.absolute_url("data:image/svg+xml,placeholder", base), "")
        self.assertEqual(addon.absolute_url("", base), "")

    def test_source_id_round_trip_for_many_paths(self):
        for path in ["/movie-2026/", "/series/show-season-2/", "/drama/mKs_ABC/", "/a_b-c/long-title/"]:
            with self.subTest(path=path):
                self.assertEqual(addon.decode_source_id(addon.make_source_id(path)), addon.source_path(path))

    def test_episode_id_round_trip_and_rejects_invalid_values(self):
        identifier = addon.make_episode_id("/series/show/", "Episode 113")
        self.assertEqual(addon.decode_episode_id(identifier), ("/series/show/", "Episode 113"))
        self.assertIsNone(addon.decode_episode_id("mlbde-not-base64"))
        self.assertIsNone(addon.decode_source_id("mlbd-../../x"))
        self.assertIsNone(addon.decode_source_id("bad-id"))

    def test_source_host_allows_known_mirrors_only(self):
        for host in ("movielinkbd.net", "movielinkbd.tv", "ecptfv.movielinkbd.li", "movielinkbd.one"):
            self.assertTrue(addon.is_source_host("https://" + host + "/x/"))
        self.assertFalse(addon.is_source_host("https://evil.example/x/"))

    def test_challenge_detection(self):
        self.assertTrue(addon.looks_like_challenge("<title>Just a moment...</title>"))
        self.assertTrue(addon.looks_like_challenge("cf-chl-abc"))
        self.assertFalse(addon.looks_like_challenge("<title>MovieLinkBD</title><h1>Latest</h1>"))

    def test_parse_listing_uses_real_lazy_image_not_placeholder(self):
        rows = addon.parse_listing(LISTING, "https://movielinkbd.net")
        self.assertEqual(len(rows), 3)
        first = next(x for x in rows if x["name"].startswith("The Awakening"))
        self.assertEqual(first["poster"], "https://img.test/awakening.jpg")
        self.assertFalse(first["poster"].startswith("data:"))

    def test_parse_listing_deduplicates_and_marks_adult(self):
        rows = addon.parse_listing(LISTING, "https://movielinkbd.net")
        self.assertEqual(len({x["source_path"] for x in rows}), 3)
        adult = next(x for x in rows if "Explicit" in x["name"])
        self.assertTrue(adult["adult"])

    def test_parse_listing_infers_movie_and_series(self):
        rows = addon.parse_listing(GENERIC_LISTING, "https://movielinkbd.net")
        self.assertEqual([x["kind"] for x in rows], ["movie", "series"])
        self.assertEqual(rows[0]["poster"], "https://img.test/simple.jpg")
        self.assertEqual(rows[0]["year"], 2024)

    def test_parse_listing_handles_empty_malformed_and_non_source_html(self):
        for text in ("", "<", "<a>", "<a href='https://evil.example/x'><img src='x'></a>", None):
            with self.subTest(text=text):
                self.assertEqual(addon.parse_listing(text or "", "https://movielinkbd.net"), [])

    def test_parse_listing_generic_fallback_ignores_navigation_image(self):
        html = "<a href='/'><img src='logo.png' alt='logo'></a>" + GENERIC_LISTING
        rows = addon.parse_listing(html, "https://movielinkbd.net")
        self.assertEqual(len(rows), 2)

    def test_infer_kind_matrix(self):
        values = [
            ("/movie/abc/", "Anything", "", "movie"),
            ("/series/abc/", "Anything", "", "series"),
            ("/x/", "A Season 2 Show", "", "series"),
            ("/x/", "A Film", "Series Name: A Film", "series"),
            ("/x/", "A Film", "Movie Details", "movie"),
        ]
        for path, title, text, expected in values:
            with self.subTest(path=path, title=title):
                self.assertEqual(addon.infer_kind(path, title, text), expected)

    def test_adult_detection_is_conservative(self):
        self.assertTrue(addon.is_adult_item("Show 18+"))
        self.assertTrue(addon.is_adult_item("Adult title"))
        self.assertTrue(addon.is_adult_item("Clean title", {"data-adult": "1"}))
        self.assertFalse(addon.is_adult_item("The Adult Education Movie"))
        self.assertFalse(addon.is_adult_item("Normal Movie"))

    def test_parse_movie_detail_prefers_hero_poster_and_display_name(self):
        meta = addon.parse_detail(MOVIE_DETAIL, "/the-awakening-2026/", "https://movielinkbd.net")
        self.assertIsNotNone(meta)
        self.assertEqual(meta["name"], "The Awakening (2026)")
        self.assertEqual(meta["type"], "movie")
        self.assertEqual(meta["poster"], "https://img.test/awakening.jpg")
        self.assertEqual(meta["year"], 2026)
        self.assertEqual(meta["genres"], ["Horror", "Action", "War"])
        self.assertIn("Quality", meta["description"])

    def test_parse_movie_detail_never_uses_site_logo(self):
        meta = addon.parse_detail(MOVIE_DETAIL, "/the-awakening-2026/", "https://movielinkbd.net")
        self.assertNotIn("movielinkbd.webp", meta["poster"])
        self.assertNotIn("simran", meta["poster"])

    def test_parse_series_detail_extracts_episode_ids_and_skips_packs(self):
        meta = addon.parse_detail(SERIES_DETAIL, "/bachelor-point-season-5/", "https://movielinkbd.net")
        self.assertEqual(meta["type"], "series")
        self.assertEqual(meta["name"], "Bachelor Point (Season 5)")
        self.assertEqual(meta["year"], 2025)
        self.assertEqual(len(meta["videos"]), 2)
        self.assertEqual([x["episode"] for x in meta["videos"]], [113, 114])
        self.assertTrue(all(x["season"] == 5 for x in meta["videos"]))
        self.assertTrue(all(x["id"].startswith("mlbde-") for x in meta["videos"]))

    def test_parse_detail_empty_or_titleless_returns_none(self):
        self.assertIsNone(addon.parse_detail("<html><body>nothing</body></html>", "/x/", "https://movielinkbd.net"))
        self.assertIsNone(addon.parse_detail("", "/x/", "https://movielinkbd.net"))

    def test_parse_genres_and_quality(self):
        self.assertEqual(addon.parse_genres("Genres: Horror, Action / War Release Date: today"), ["Horror", "Action", "War"])
        for value, expected in [("720p", "720p"), ("1080P WEB-DL", "1080p"), ("unknown", "")]:
            self.assertEqual(addon.parse_quality(value), expected)

    def test_parse_movie_download_links_keeps_only_kitecloud(self):
        links = addon.parse_download_links(MOVIE_DETAIL, "/the-awakening-2026/")
        self.assertEqual(len(links), 3)
        self.assertEqual([x["quality"] for x in links], ["480p", "720p", "1080p"])
        self.assertEqual(links[2]["size"], "2.2 GB")
        self.assertTrue(all("kitecloud.me" in x["url"] for x in links))

    def test_parse_episode_links_selects_one_episode(self):
        links = addon.parse_download_links(SERIES_DETAIL, "/bachelor-point-season-5/", "Episode 113")
        self.assertEqual(len(links), 2)
        self.assertTrue(all("e113" in x["url"] for x in links))
        self.assertFalse(any("e114" in x["url"] for x in links))

    def test_parse_episode_links_does_not_select_zip_pack(self):
        links = addon.parse_download_links(SERIES_DETAIL, "/bachelor-point-season-5/", "Episode 114")
        self.assertEqual([x["url"] for x in links], ["https://kitecloud.me/e114-720"])

    def test_parse_download_links_fallback_is_deduplicated(self):
        text = "<a href='https://kitecloud.me/a'>A</a><a href='https://kitecloud.me/a'>A again</a><a href='https://evil.example/x'>bad</a>"
        links = addon.parse_download_links(text, "/x/")
        self.assertEqual(len(links), 1)
        self.assertEqual(links[0]["url"], "https://kitecloud.me/a")

    def test_catalog_paths_cover_search_and_pagination(self):
        latest = addon.catalog_definition("mlbd-latest-movies")
        category = addon.catalog_definition("mlbd-anime-series")
        self.assertEqual(addon.catalog_path(latest, 1, ""), "/")
        self.assertEqual(addon.catalog_path(latest, 2, ""), "/page/2/")
        self.assertEqual(addon.catalog_path(latest, 2, "awakening"), "/?s=awakening&page=2")
        self.assertEqual(addon.catalog_path(category, 4, ""), "/category/anime/")

    def test_catalog_definition_rejects_unknown(self):
        self.assertIsNone(addon.catalog_definition("not-real"))
        self.assertEqual(addon.catalog_definition("mlbd-latest-series")["type"], "series")

    def test_wordpress_kind_prefers_authoritative_content_marker(self):
        self.assertEqual(addon.wordpress_kind({"title": {"rendered": "Plain title"}, "content": {"rendered": "<b>Series Name:</b> Plain title"}, "link": "/x/"}), "series")
        self.assertEqual(addon.wordpress_kind({"title": {"rendered": "Plain title"}, "content": {"rendered": "<b>Full Movie Name:</b> Plain title"}, "link": "/x/"}), "movie")

    def test_catalog_items_uses_wordpress_type_and_html_poster_join(self):
        post = {"link": "https://movielinkbd.net/bachelor-point-season-5-chapter-15-episode-113-120/", "title": {"rendered": "Bachelor Point API"}, "content": {"rendered": "Series Info &amp; Technical Details: Series Name: Bachelor Point"}, "categories": []}
        def fake_fetch(path):
            return "https://movielinkbd.net", "https://movielinkbd.net" + path, LISTING
        with mock.patch.object(addon, "fetch_source_path", side_effect=fake_fetch), \
             mock.patch.object(addon, "wordpress_posts", return_value=[post]):
            rows = addon.catalog_items("mlbd-latest-series")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["type"], "series")
        self.assertEqual(rows[0]["name"], "Bachelor Point API")
        self.assertEqual(rows[0]["poster"], "https://img.test/bachelor.jpg")

    def test_catalog_items_filters_type_and_adult(self):
        def fake_fetch(path):
            return "https://movielinkbd.net", "https://movielinkbd.net" + path, LISTING
        with mock.patch.object(addon, "fetch_source_path", side_effect=fake_fetch), \
             mock.patch.object(addon, "wordpress_posts", return_value=None):
            movies = addon.catalog_items("mlbd-latest-movies")
            series = addon.catalog_items("mlbd-latest-series")
        self.assertEqual([x["name"] for x in movies], ["The Awakening (2026)"])
        self.assertEqual([x["name"] for x in series], ["Bachelor Point (Season 5) Chapter 15 | Episode 113-120"])
        self.assertTrue(all(x["type"] == "movie" for x in movies))
        self.assertTrue(all("Explicit" not in x["name"] for x in movies + series))

    def test_latest_filtered_pagination_accumulates_raw_pages(self):
        pages = [[{"id": "a", "name": "A", "poster": "", "kind": "movie", "source_path": "/a/", "adult": False}],
                 [{"id": "b", "name": "B", "poster": "", "kind": "movie", "source_path": "/b/", "adult": False}],
                 [{"id": "c", "name": "C", "poster": "", "kind": "movie", "source_path": "/c/", "adult": False}]]
        with mock.patch.object(addon, "catalog_page_items", side_effect=pages + [[]]):
            rows = addon.catalog_items("mlbd-latest-movies", skip=1)
        self.assertEqual([x["name"] for x in rows], ["B", "C"])

    def test_catalog_items_positive_cache_avoids_second_fetch(self):
        def fake_fetch(path):
            return "https://movielinkbd.net", "https://movielinkbd.net" + path, LISTING
        with mock.patch.object(addon, "fetch_source_path", side_effect=fake_fetch) as fetch, \
             mock.patch.object(addon, "wordpress_posts", return_value=None):
            one = addon.catalog_items("mlbd-latest-movies")
            two = addon.catalog_items("mlbd-latest-movies")
        self.assertEqual(one, two)
        self.assertEqual(fetch.call_count, 1)

    def test_catalog_failure_is_not_negative_cached(self):
        with mock.patch.object(addon, "fetch_source_path", return_value=None) as fetch, \
             mock.patch.object(addon, "wordpress_posts", return_value=None):
            self.assertEqual(addon.catalog_items("mlbd-latest-movies"), [])
            self.assertEqual(addon.catalog_items("mlbd-latest-movies"), [])
        self.assertEqual(fetch.call_count, 2)

    def test_metadata_for_uses_encoded_path_and_positive_cache(self):
        ident = addon.make_source_id("/the-awakening-2026/")
        with mock.patch.object(addon, "fetch_source_path", return_value=("https://movielinkbd.net", "", MOVIE_DETAIL)) as fetch:
            first = addon.metadata_for(ident)
            second = addon.metadata_for(ident)
        self.assertEqual(first, second)
        self.assertEqual(fetch.call_count, 1)

    def test_metadata_failure_is_not_negative_cached(self):
        ident = addon.make_source_id("/missing/")
        with mock.patch.object(addon, "fetch_source_path", return_value=None) as fetch:
            self.assertIsNone(addon.metadata_for(ident))
            self.assertIsNone(addon.metadata_for(ident))
        self.assertEqual(fetch.call_count, 2)

    def test_movie_stream_cards_are_external_only(self):
        ident = addon.make_source_id("/the-awakening-2026/")
        with mock.patch.object(addon, "fetch_source_path", return_value=("https://movielinkbd.net", "https://movielinkbd.net/the-awakening-2026/", MOVIE_DETAIL)):
            cards = addon.stream_cards_for(ident, "movie")
        self.assertEqual(len(cards), 4)
        self.assertTrue(all("externalUrl" in card for card in cards))
        self.assertTrue(all("url" not in card for card in cards))
        self.assertTrue(any("1080p" in card["name"] for card in cards))

    def test_series_stream_cards_select_episode_links(self):
        ident = addon.make_episode_id("/bachelor-point-season-5/", "Episode 114")
        with mock.patch.object(addon, "fetch_source_path", return_value=("https://movielinkbd.net", "https://movielinkbd.net/bachelor-point-season-5/", SERIES_DETAIL)):
            cards = addon.stream_cards_for(ident, "series")
        self.assertEqual(len(cards), 2)
        self.assertTrue(any("Open source page" in card["name"] for card in cards))
        self.assertTrue(all("e114" in card["externalUrl"] or "bachelor-point" in card["externalUrl"] for card in cards))
        self.assertFalse(any("e113" in card["externalUrl"] for card in cards))

    def test_stream_failure_is_empty_not_fake(self):
        ident = addon.make_source_id("/missing/")
        with mock.patch.object(addon, "fetch_source_path", return_value=None):
            self.assertEqual(addon.stream_cards_for(ident, "movie"), [])

    def test_fetch_source_path_falls_through_mirrors(self):
        calls = []
        def fake_fetch(url, referer=""):
            calls.append(url)
            return "<title>MovieLinkBD</title>" if url.endswith(".tv/") else None
        with mock.patch.object(addon, "source_bases", return_value=("https://first.example", "https://movielinkbd.tv")), \
             mock.patch.object(addon, "fetch_html", side_effect=fake_fetch):
            result = addon.fetch_source_path("/")
        self.assertEqual(result[0], "https://movielinkbd.tv")
        self.assertEqual(calls, ["https://first.example/", "https://movielinkbd.tv/"])

    def test_fetch_html_rejects_non_200_challenges_and_keeps_positive(self):
        class Response:
            def __init__(self, status, text):
                self.status_code = status
                self.text = text
        with mock.patch.object(addon.HTTP, "get", side_effect=[Response(403, "no"), Response(200, "<html>MovieLinkBD</html>")]) as get:
            self.assertIsNone(addon.fetch_html("https://x.test/a"))
            self.assertEqual(addon.fetch_html("https://x.test/a"), "<html>MovieLinkBD</html>")
        self.assertEqual(get.call_count, 2)

    def test_health_route_reports_no_video_relay(self):
        status, headers, body = self.get("/health")
        self.assertEqual(status, 200)
        self.assertEqual(headers["Content-Type"], "application/json; charset=utf-8")
        self.assertIn("no video-byte relay", json.loads(body)["egress"])

    def test_manifest_route_is_valid_json(self):
        status, _headers, body = self.get("/manifest.json")
        self.assertEqual(status, 200)
        manifest = json.loads(body)
        self.assertEqual(manifest["id"], "org.movielinkbd.stremio")
        self.assertEqual(len(manifest["catalogs"]), len(addon.CATALOGS))

    def test_root_route(self):
        status, _headers, body = self.get("/")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["addon"], "MovieLinkBD")

    def test_catalog_route_uses_requested_catalog(self):
        with mock.patch.object(addon, "catalog_items", return_value=[{"id": "mlbd-x", "type": "movie", "name": "X"}]) as call:
            status, _headers, body = self.get("/catalog/movie/mlbd-latest-movies.json?search=x&skip=20")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["metas"][0]["name"], "X")
        self.assertEqual(call.call_args.args, ("mlbd-latest-movies", "x", "20"))

    def test_catalog_route_rejects_wrong_type_and_unknown(self):
        self.assertEqual(self.get("/catalog/series/mlbd-latest-movies.json")[0], 404)
        self.assertEqual(self.get("/catalog/movie/not-real.json")[0], 404)

    def test_meta_route_returns_only_matching_source_type(self):
        ident = addon.make_source_id("/the-awakening-2026/")
        meta = {"id": ident, "type": "movie", "name": "The Awakening"}
        with mock.patch.object(addon, "metadata_for", return_value=meta):
            status, _headers, body = self.get("/meta/movie/%s.json" % ident)
            wrong, _headers, _body = self.get("/meta/series/%s.json" % ident)
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["meta"]["name"], "The Awakening")
        self.assertEqual(wrong, 404)

    def test_stream_route_has_no_native_url(self):
        ident = addon.make_source_id("/the-awakening-2026/")
        cards = [{"name": "browser", "externalUrl": "https://kitecloud.me/x"}]
        with mock.patch.object(addon, "stream_cards_for", return_value=cards):
            status, _headers, body = self.get("/stream/movie/%s.json" % ident)
        self.assertEqual(status, 200)
        streams = json.loads(body)["streams"]
        self.assertTrue(all("url" not in x and "externalUrl" in x for x in streams))

    def test_unknown_route_and_invalid_identifiers(self):
        self.assertEqual(self.get("/does-not-exist")[0], 404)
        self.assertEqual(self.get("/meta/movie/mlbd-invalid.json")[0], 404)
        self.assertEqual(self.get("/stream/movie/mlbd-invalid.json")[0], 200)
        self.assertEqual(json.loads(self.get("/stream/movie/mlbd-invalid.json")[2])["streams"], [])

    def test_route_exception_is_sanitized(self):
        with mock.patch.object(addon, "catalog_items", side_effect=RuntimeError("boom")):
            status, _headers, body = self.get("/catalog/movie/mlbd-latest-movies.json")
        self.assertEqual(status, 500)
        self.assertEqual(json.loads(body)["error"], "internal server error")

    def test_concurrent_parser_calls_are_consistent(self):
        with ThreadPoolExecutor(max_workers=20) as pool:
            results = list(pool.map(lambda _: addon.parse_listing(LISTING), range(100)))
        self.assertTrue(all(len(result) == 3 for result in results))
        self.assertTrue(all(result[0]["poster"] for result in results))

    def test_concurrent_metadata_calls_are_valid(self):
        ident = addon.make_source_id("/the-awakening-2026/")
        with mock.patch.object(addon, "fetch_source_path", return_value=("https://movielinkbd.net", "", MOVIE_DETAIL)):
            with ThreadPoolExecutor(max_workers=12) as pool:
                results = list(pool.map(lambda _: addon.metadata_for(ident), range(40)))
        self.assertTrue(all(result and result["name"] == "The Awakening (2026)" for result in results))

    def test_malformed_fuzz_inputs_never_raise(self):
        samples = ["", "<", ">", "\x00", "&amp;", "<article>", "<a href='x'>", "data:image/svg+xml"]
        samples += ["<" + "a" * n + ">" for n in range(1, 80)]
        for value in samples:
            with self.subTest(value=value):
                addon.parse_listing(value)
                addon.parse_detail(value, "/x/", "https://movielinkbd.net")
                addon.parse_download_links(value, "/x/")

    def test_no_browser_card_claims_native_playback(self):
        ident = addon.make_source_id("/the-awakening-2026/")
        with mock.patch.object(addon, "fetch_source_path", return_value=("https://movielinkbd.net", "https://movielinkbd.net/x", MOVIE_DETAIL)):
            cards = addon.stream_cards_for(ident, "movie")
        for card in cards:
            self.assertNotIn("url", card)
            self.assertIn("externalUrl", card)
            self.assertIn("browser", card["name"].lower())

    def test_catalog_cache_key_separates_catalogs_and_search(self):
        def fake_fetch(path):
            return "https://movielinkbd.net", "", LISTING
        with mock.patch.object(addon, "fetch_source_path", side_effect=fake_fetch) as fetch, \
             mock.patch.object(addon, "wordpress_posts", return_value=None):
            addon.catalog_items("mlbd-latest-movies")
            addon.catalog_items("mlbd-latest-series")
            addon.catalog_items("mlbd-latest-movies", "awakening")
        self.assertEqual(fetch.call_count, 3)


if __name__ == "__main__":
    unittest.main(verbosity=2)
