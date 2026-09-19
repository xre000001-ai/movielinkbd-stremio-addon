"""MovieLinkBD addon tests — offline, mocked HTTP, real-shaped fixtures."""

import base64
import json
import sys
import unittest
from unittest import mock

sys.path.insert(0, "/home/user/movielinkbd-addon")
import addon  # noqa: E402


def b64(name):
    return base64.b64encode(name.encode()).decode()


INTERSTELLAL_NAME = "MLMBD.com-Interstellar-2014-Hindi-English-720p.mkv"
SCANDAL_E0104 = ("MovieLink.one-18-The-Scandal-2026-Season-1-Hindi-amp-English"
                 "-Download-amp-Watch-Online-Episodes-0104-720p.mkv")
SCANDAL_E0508 = ("MovieLink.one-18-The-Scandal-2026-Season-1-Hindi-amp-English"
                 "-Download-amp-Watch-Online-Episodes-0508-1080p.mkv")


class FakeResponse:
    def __init__(self, status=200, body=b"", json_value=None):
        self.status_code = status
        self._body = body if isinstance(body, bytes) else str(body).encode()
        self._json = json_value

    @property
    def text(self):
        return self._body.decode("utf-8", "ignore")

    def json(self):
        if self._json is None:
            raise ValueError("no json")
        return self._json

    def iter_content(self, _size):
        yield self._body[:64]

    def close(self):
        pass


def page_html(files, mirror="https://movieslinkbd.com"):
    buttons = "".join(
        '<a href="%s/generate/?file=%s">Download in 1080p</a>' % (mirror, f)
        for f in files)
    return ("<!doctype html><html><body>"
            "<h3>Download Links</h3>" + buttons + "</body></html>")


def reset_caches():
    addon.C_SEARCH = addon.TTLCache("t-search", 1 << 16)
    addon.C_PAGE = addon.TTLCache("t-page", 1 << 16)
    addon.C_SIGN = addon.TTLCache("t-sign", 1 << 16)
    addon.C_PROBE = addon.TTLCache("t-probe", 1 << 16)
    addon.C_ID = addon.TTLCache("t-id", 1 << 16)


class UrlRouter:
    """Route mock: a single callable, or a list of (substring, response)."""

    def __init__(self, routes):
        self.routes = routes
        self.calls = []

    def __call__(self, url, **kwargs):
        self.calls.append(url)
        if callable(self.routes) and not isinstance(self.routes, list):
            return self.routes(url, **kwargs)
        for needle, response in self.routes:
            if needle in url:
                if callable(response):
                    return response(url, kwargs)
                return response
        return FakeResponse(status=404, body="{}")


class ManifestTests(unittest.TestCase):
    def test_stream_only_manifest(self):
        m = addon.MANIFEST
        self.assertEqual(m["catalogs"], [])
        self.assertEqual(m["resources"], ["stream"])
        self.assertEqual(m["idPrefixes"], ["tt", "tmdb:"])
        self.assertEqual(m["version"], addon.VERSION)


class FilenameTests(unittest.TestCase):
    def test_quality_ext_and_range(self):
        f = addon.parse_filename(SCANDAL_E0104)
        self.assertEqual(f["quality"], "720p")
        self.assertEqual(f["ext"], "MKV")
        self.assertEqual((f["ep_lo"], f["ep_hi"]), (1, 4))
        f = addon.parse_filename(SCANDAL_E0508)
        self.assertEqual((f["ep_lo"], f["ep_hi"]), (5, 8))
        self.assertEqual(f["quality"], "1080p")

    def test_movie_file_has_no_episode_range(self):
        f = addon.parse_filename(INTERSTELLAL_NAME)
        self.assertIsNone(f["ep_lo"])
        self.assertEqual(f["quality"], "720p")

    def test_4k_and_mp4(self):
        f = addon.parse_filename("MovieLink.one-Some-Movie-2026-4K.mp4")
        self.assertEqual(f["quality"], "2160p")
        self.assertEqual(f["ext"], "MP4")

    def test_single_episode(self):
        f = addon.parse_filename("MLMBD.com-Series-2026-Episode-07-480p.mkv")
        self.assertEqual((f["ep_lo"], f["ep_hi"]), (7, 7))

    def test_swapped_range_normalised(self):
        f = addon.parse_filename("MLNBD.com-X-2026-Episodes-0906-720p.mkv")
        self.assertEqual((f["ep_lo"], f["ep_hi"]), (6, 9))


class SearchMatchTests(unittest.TestCase):
    def setUp(self):
        reset_caches()

    def test_wpjson_search_and_best_result(self):
        with mock.patch.object(addon.HTTP, "get") as get:
            get.return_value = FakeResponse(json_value=[
                {"link": "https://movieslinkbd.com/interstellar-2014-hindi-english-download-watch-online/",
                 "title": "Interstellar (2014) [Hindi & English] Download & Watch Online"},
                {"link": "https://movieslinkbd.com/the-awakening-2026-hindi-org-download-watch-online/",
                 "title": "The Awakening (2026) [Hindi ORG] Download"},
            ])
            rows = addon.mirror_search("https://movieslinkbd.com", "Interstellar")
        self.assertEqual(len(rows), 2)
        best = addon.best_result(rows, "Interstellar", 2014)
        self.assertIn("interstellar-2014", best["link"])

    def test_colon_title_falls_back_to_head_term(self):
        # 'Dune: Part One' must resolve through the 'Dune' page.
        def route(url, **kw):
            if "cinemeta" in url:
                return FakeResponse(json_value={"meta": {
                    "name": "Dune: Part One", "releaseInfo": "2021"}})
            if "wp-json" in url:
                term = (kw.get("params") or {}).get("term", "")
                if term == "Dune: Part One":
                    return FakeResponse(json_value=[])
                return FakeResponse(json_value=[{
                    "link": "https://movieslinkbd.com/dune-2021-hindi-english-download-watch-online/",
                    "title": "Dune (2021) Dual Audio [Hindi & English] 1080p & 720p"}])
            if "download-watch-online" in url:
                return FakeResponse(body=page_html([b64("MLMBD.com-Dune-2021-Hindi-English-1080p.mkv")]))
            if "/api/sign/" in url:
                return FakeResponse(json_value={"downloadUrl": "https://dl.vircloud.site/download/x?exp=9999999999&sig=1"})
            if "dl.vircloud.site/download/" in url:
                return FakeResponse(status=206)
            return FakeResponse(status=404, body="{}")
        with mock.patch.object(addon.HTTP, "get", side_effect=UrlRouter(route)):
            out = addon.build_streams("tt1160419")
        self.assertEqual(len(out["streams"]), 1)
        self.assertIn("1080p", out["streams"][0]["name"])

    def test_short_title_containment_does_not_match_longer_title(self):
        rows = [{"link": "https://movieslinkbd.com/abar-hawa-bodol-2026-bengali-download-watch-online/",
                 "title": "Abar Hawa Bodol (2026) Bengali Download & Watch Online"}]
        self.assertIsNone(addon.best_result(rows, "Hawa", 2022))

    def test_year_mismatch_penalised(self):
        rows = [{"link": "https://movielink.ch/x-1999-download-watch-online/",
                 "title": "Interstellar (1999) Download & Watch Online"}]
        self.assertIsNone(addon.best_result(rows, "Interstellar", 2014))

    def test_html_fallback_search_for_legacy_mirror(self):
        html = ('<a href="https://movielinkbd.net/interstellar-2014-download-watch-online/">'
                'Interstellar (2014) Download</a>'
                '<a href="https://movielinkbd.net/category/movies/">Movies</a>')
        with mock.patch.object(addon.HTTP, "get") as get:
            # wp-json 404s on the legacy mirror -> HTML ?s= fallback
            def route(url, **kw):
                if "wp-json" in url:
                    return FakeResponse(status=404, body="{}")
                return FakeResponse(body=html)
            get.side_effect = route
            rows = addon.mirror_search("https://movielinkbd.net", "Interstellar")
        self.assertEqual(len(rows), 1)
        self.assertIn("interstellar-2014", rows[0]["link"])

    def test_page_files_dedupe_and_parse(self):
        url = "https://movieslinkbd.com/interstellar-2014-hindi-english-download-watch-online/"
        # Two buttons -> one file (the "1080p" button lies: file is 720p).
        with mock.patch.object(addon.HTTP, "get",
                               return_value=FakeResponse(
                                   body=page_html([b64(INTERSTELLAL_NAME)] * 2))):
            files = addon.page_files(url)
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0]["quality"], "720p")
        self.assertEqual(files[0]["b64"], b64(INTERSTELLAL_NAME))


class StreamTests(unittest.TestCase):
    def setUp(self):
        reset_caches()

    def _router(self, files, probe_status=206, season=False):
        def route(url, **kw):
            if "cinemeta" in url:
                name = "The Scandal" if season else "Interstellar"
                year = "2026" if season else "2014"
                return FakeResponse(json_value={"meta": {"name": name,
                                                         "releaseInfo": year}})
            if "wp-json" in url:
                if "movieslinkbd.com" in url:
                    if season:
                        link = "https://movieslinkbd.com/the-scandal-2026-hindi-english-download-watch-online/"
                        title = "The Scandal (2026) Season 1 [Hindi & English] Download | MovieLinkBD"
                    else:
                        link = "https://movieslinkbd.com/interstellar-2014-hindi-english-download-watch-online/"
                        title = "Interstellar (2014) [Hindi & English] Download & Watch Online"
                    return FakeResponse(json_value=[{"link": link, "title": title}])
                return FakeResponse(status=404, body="{}")
            if "/generate/" in url or "download-watch-online" in url:
                return FakeResponse(body=page_html([b64(f) for f in files]))
            if "/api/sign/" in url:
                return FakeResponse(json_value={
                    "downloadUrl": "https://dl.vircloud.site/download/%s?exp=9999999999&sig=deadbeef"
                    % b64(files[0])})
            if "dl.vircloud.site/download/" in url:
                return FakeResponse(status=probe_status)
            return FakeResponse(status=404, body="{}")
        return UrlRouter(route)

    def test_movie_pipeline_emits_direct_card(self):
        router = self._router([INTERSTELLAL_NAME])
        with mock.patch.object(addon.HTTP, "get", side_effect=router):
            out = addon.build_streams("tt0816692")
        self.assertEqual(out["message"], "")
        self.assertEqual(len(out["streams"]), 1)
        card = out["streams"][0]
        self.assertEqual(card["name"], "♧ MovieLinkBD · 720p · MKV")
        self.assertTrue(card["url"].startswith("https://dl.vircloud.site/download/"))
        self.assertTrue(card["behaviorHints"]["notWebReady"])
        self.assertIn("Interstellar-2014", card["title"])

    def test_series_episode_maps_into_batch(self):
        router = self._router([SCANDAL_E0104, SCANDAL_E0508], season=True)
        with mock.patch.object(addon.HTTP, "get", side_effect=router):
            out = addon.build_streams("tt1234567:1:3")
        self.assertEqual(len(out["streams"]), 1)
        self.assertEqual(out["streams"][0]["name"], "♧ MovieLinkBD · E01-E04 · 720p · MKV")

    def test_series_out_of_range_falls_back_to_all_batches(self):
        router = self._router([SCANDAL_E0104, SCANDAL_E0508], season=True)
        with mock.patch.object(addon.HTTP, "get", side_effect=router):
            out = addon.build_streams("tt1234567:1:9")
        self.assertEqual(len(out["streams"]), 2)
        # 1080p ranks first.
        self.assertIn("1080p", out["streams"][0]["name"])

    def test_dead_probe_means_no_card(self):
        router = self._router([INTERSTELLAL_NAME], probe_status=403)
        with mock.patch.object(addon.HTTP, "get", side_effect=router):
            out = addon.build_streams("tt0816692")
        self.assertEqual(out["streams"], [])
        self.assertIn("probe", out["message"])

    def test_sign_failure_skips_file(self):
        def route(url, **kw):
            if "cinemeta" in url:
                return FakeResponse(json_value={"meta": {"name": "Interstellar",
                                                         "releaseInfo": "2014"}})
            if "wp-json" in url and "movieslinkbd.com" in url:
                return FakeResponse(json_value=[{
                    "link": "https://movieslinkbd.com/interstellar-2014-hindi-english-download-watch-online/",
                    "title": "Interstellar (2014) [Hindi & English] Download"}])
            if "/generate/" in url or "download-watch-online" in url:
                return FakeResponse(body=page_html([INTERSTELLAL_NAME]))
            if "/api/sign/" in url:
                return FakeResponse(status=500, body="{}")
            return FakeResponse(status=404, body="{}")
        with mock.patch.object(addon.HTTP, "get", side_effect=UrlRouter(route)):
            out = addon.build_streams("tt0816692")
        self.assertEqual(out["streams"], [])

    def test_mirror_failover_after_cf_block(self):
        calls = []

        def route(url, **kw):
            calls.append(url)
            if "cinemeta" in url:
                return FakeResponse(json_value={"meta": {"name": "Interstellar",
                                                         "releaseInfo": "2014"}})
            # The movie page must be matched before the mirror branches.
            if "download-watch-online" in url and "wp-json" not in url:
                return FakeResponse(body=page_html([b64(INTERSTELLAL_NAME)],
                                                   mirror="https://movielinkbd.net"))
            if "movieslinkbd.com" in url or "movielink.ch" in url:
                if "wp-json" in url:
                    return FakeResponse(status=403, body="{}")
                return FakeResponse(status=403, body="{}")
            if "movielinkbd.net" in url:
                if "wp-json" in url:
                    return FakeResponse(status=404, body="{}")
                return FakeResponse(body=(
                    '<a href="https://movielinkbd.net/interstellar-2014-download-watch-online/">'
                    'Interstellar (2014) Download</a>'))
            if "/api/sign/" in url:
                return FakeResponse(json_value={
                    "downloadUrl": "https://dl.vircloud.site/download/x?exp=9999999999&sig=1"})
            if "dl.vircloud.site/download/" in url:
                return FakeResponse(status=206)
            return FakeResponse(status=404, body="{}")
        with mock.patch.object(addon.HTTP, "get", side_effect=UrlRouter(route)):
            out = addon.build_streams("tt0816692")
        self.assertEqual(len(out["streams"]), 1)
        self.assertTrue(any("movielinkbd.net" in u for u in calls))

    def test_probe_positive_cache(self):
        router = self._router([INTERSTELLAL_NAME])
        with mock.patch.object(addon.HTTP, "get", side_effect=router):
            addon.build_streams("tt0816692")
            addon.build_streams("tt0816692")
        probes = [u for u in router.calls if "dl.vircloud.site/download/" in u]
        self.assertEqual(len(probes), 1)


class LiAdapterTests(unittest.TestCase):
    """movielinkbd.li adapter — fixtures pinned from the wayback snapshot
    of https://2f2w3j.movielinkbd.li/movie/1is9Hh_… ('The Gift (2015)')."""

    def setUp(self):
        reset_caches()

    def test_challenge_detected(self):
        self.assertTrue(addon._li_challenge(
            "<!DOCTYPE html><html><head><title>Just a moment...</title>"))
        self.assertFalse(addon._li_challenge("<html><title>The Gift</title>"))

    def test_li_search_parses_movie_cards(self):
        body = """
        <div class="movie-card" data-18="0">
          <div class="image-container"><a href="https://u7n8gg.movielinkbd.li/movie/1is9Hh_Tu3VMHsruWQnBCkufSypBVcAJdVxvWGazdLgIvdu6XHvI-xQYhGLpiolRK3gnIA">
          <img alt="The Gift (2015)" title="The Gift (2015)"/></a>
          <span class="quality">WEB-DL</span><span class="type type-movie">Movie</span></div>
          <div class="content"><a href="https://u7n8gg.movielinkbd.li/movie/1is9Hh_Tu3VMHsruWQnBCkufSypBVcAJdVxvWGazdLgIvdu6XHvI-xQYhGLpiolRK3gnIA" class="title">The Gift (2015)</a></div>
        </div>"""
        with mock.patch.object(addon.HTTP, "get",
                               return_value=FakeResponse(body=body)):
            status, rows = addon.li_search("the gift")
        self.assertEqual(status, "ok")
        self.assertEqual(rows[0]["title"], "The Gift (2015)")
        self.assertIn("/movie/1is9Hh_", rows[0]["link"])

    def test_li_search_blocked_by_cloudflare(self):
        cf = "<!DOCTYPE html><html><head><title>Just a moment...</title></head>"
        with mock.patch.object(addon.HTTP, "get",
                               return_value=FakeResponse(status=403, body=cf)):
            status, rows = addon.li_search("the gift")
        self.assertEqual((status, rows), ("blocked", []))

    def test_li_page_buttons_parses_watch_and_download(self):
        tok_w = "bVc2QlNMK2Fq" + "A" * 40
        tok_d = "aVVMTnBXdWZEMDZG" + "B" * 40
        page = """
        <h1>The Gift (2015)</h1>
        <a href="https://u7n8gg.movielinkbd.li/getWatch/%s" class="btn">
          <b><i class="fas fa-play-circle"></i> Watch Online</b></a>
        <a href="https://u7n8gg.movielinkbd.li/getLink/%s" class="btn">
          <b><i class="fas fa-cloud-download-alt"></i> Download [720p • 700 MB]</b></a>
        """ % (tok_w, tok_d)
        with mock.patch.object(addon.HTTP, "get",
                               return_value=FakeResponse(body=page)):
            buttons = addon.li_page_buttons("https://u7n8gg.movielinkbd.li/movie/x")
        self.assertEqual(len(buttons), 2)
        watch = [b for b in buttons if b["kind"] == "getWatch"][0]
        down = [b for b in buttons if b["kind"] == "getLink"][0]
        self.assertEqual(watch["token"], tok_w)
        self.assertEqual(down["quality"], "720p")
        self.assertEqual(down["size"], "700 MB")

    def test_li_button_links_extracts_direct_and_refresh(self):
        body = ('<html><head><meta http-equiv="refresh" content="5;url='
                'https://dl.example.com/file-1080p.mkv"></head>'
                '<body><a href="https://cdn.example.com/video.mp4">go</a></body></html>')
        with mock.patch.object(addon.HTTP, "get",
                               return_value=FakeResponse(body=body)):
            urls = addon.li_button_links(
                {"front": "https://u7n8gg.movielinkbd.li",
                 "kind": "getLink", "token": "x" * 30})
        self.assertIn("https://cdn.example.com/video.mp4", urls)
        self.assertIn("https://dl.example.com/file-1080p.mkv", urls)

    def test_li_try_falls_back_when_blocked(self):
        cf = "<html><title>Just a moment...</title></html>"
        cinemeta = FakeResponse(json_value={"meta": {"name": "Interstellar",
                                                     "releaseInfo": "2014"}})
        wp = FakeResponse(json_value=[{
            "link": "https://movieslinkbd.com/interstellar-2014-hindi-english-download-watch-online/",
            "title": "Interstellar (2014) [Hindi & English] Download & Watch Online"}])
        page = FakeResponse(body=page_html([b64(INTERSTELLAL_NAME)]))
        sign = FakeResponse(json_value={
            "downloadUrl": "https://dl.vircloud.site/download/x?exp=9999999999&sig=1"})
        probe = FakeResponse(status=206)

        def route(url, **kw):
            if "cinemeta" in url:
                return cinemeta
            if addon.LI_FRONT in url:
                return FakeResponse(status=403, body=cf)   # .li is CF-blocked
            if "wp-json" in url:
                return wp
            if "download-watch-online" in url:
                return page
            if "/api/sign/" in url:
                return sign
            if "dl.vircloud.site/download/" in url:
                return probe
            return FakeResponse(status=404, body="{}")
        with mock.patch.object(addon.HTTP, "get", side_effect=UrlRouter(route)):
            out = addon.build_streams("tt0816692")
        # .li blocked -> transparent fallback to the WP mirror library.
        self.assertEqual(len(out["streams"]), 1)
        self.assertIn("MovieLinkBD", out["streams"][0]["name"])

    def test_li_try_serves_cards_when_open(self):
        tok_d = "T" * 60
        search_body = """
        <div class="content"><a href="https://u7n8gg.movielinkbd.li/movie/hashgift" class="title">The Gift (2015)</a></div>"""
        movie_page = """
        <a href="https://u7n8gg.movielinkbd.li/getLink/%s" class="btn">
          Download [720p • 700 MB]</a>""" % tok_d
        getlink_page = ('<a href="https://dl.vircloud.site/download/GIFT?exp=9999999999&sig=2">get</a>')
        cinemeta = FakeResponse(json_value={"meta": {"name": "The Gift",
                                                     "releaseInfo": "2015"}})

        def route(url, **kw):
            if "cinemeta" in url:
                return cinemeta
            if "/search" in url and addon.LI_FRONT in url:
                return FakeResponse(body=search_body)
            if "/movie/hashgift" in url:
                return FakeResponse(body=movie_page)
            if "/getLink/" in url:
                return FakeResponse(body=getlink_page)
            if "dl.vircloud.site/download/" in url:
                return FakeResponse(status=206)
            return FakeResponse(status=404, body="{}")
        with mock.patch.object(addon.HTTP, "get", side_effect=UrlRouter(route)):
            out = addon.build_streams("tt0000000")
        self.assertEqual(len(out["streams"]), 1)
        self.assertIn("720p", out["streams"][0]["name"])
        self.assertIn("700 MB", out["streams"][0]["name"])


class ServerSmokeTests(unittest.TestCase):
    def test_routes_via_handler(self):
        import threading
        import http.client
        addon2 = __import__("importlib").import_module("addon")
        server = addon2.ThreadingHTTPServer(("127.0.0.1", 0), addon2.Handler)
        port = server.server_address[1]
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()
        try:
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            conn.request("GET", "/manifest.json")
            r = conn.getresponse()
            body = json.loads(r.read())
            self.assertEqual(r.status, 200)
            self.assertEqual(body["id"], "movielinksbd.stremio")
            conn.close()
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            conn.request("GET", "/health")
            r = conn.getresponse()
            self.assertEqual(r.status, 200)
            self.assertTrue(json.loads(r.read())["ok"])
            conn.close()
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            conn.request("GET", "/")
            r = conn.getresponse()
            self.assertIn("MovieLinkBD", r.read().decode())
            conn.close()
        finally:
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    unittest.main()
