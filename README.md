# MovieLinkBD — stream-only Stremio addon

Stream-only (Torrentio-style, **no catalogs**) Stremio addon for the
**movielinksbd / MovieLinkBD** mirror family. Open any movie or series from
your own catalogs and direct file links from the site's `vircloud` CDN
appear as native stream cards.

## Mirror family

| Domain | State (probed 2026-09-19) | Role |
|---|---|---|
| `movieslinkbd.com` | live, WordPress | primary (wp-json live search) |
| `movielink.ch` | live, WordPress | primary (wp-json live search, hosts `/generate/`) |
| `movielinkbd.net` | live, WordPress | primary (HTML `?s=` search fallback) |
| `movielinkbd.com` / `.one` / `.shop` | Cloudflare-challenged from datacenter IPs | last-ditch fallbacks |
| `movielinkbd.tv` | — | **excluded by user directive** |
| `movielinksbd.com` | parked (intivesearch) | skipped |
| `movielinkbd.to` / `.io`, `mlsbd.site` | dead | skipped |

Searches walk the mirror list in order; the first mirror that answers wins
(positive-cached per term).

## How it works

1. `tt…` ids resolve through Cinemeta, `tmdb:` ids through TMDB
   (key from `MLSBD_TMDB_KEY`, fleet default baked in).
2. Site search via `/wp-json/mlmbd/v1/search?term=…` (HTML `?s=` fallback on
   the legacy mirror). The literal search is retried with the pre-colon head
   of the title — Cinemeta's *"Dune: Part One"* finds the site's *"Dune"*.
3. Matching scores the title (and its pre-colon head) against the site row
   with a `(YYYY)` year hint; short-title containment is ratio-penalised so
   *"Hawa"* does not match *"Abar Hawa Bodol"*.
4. The matched page's `/generate/?file=<base64>` buttons are decoded into
   file names. **The filename is the only trusted source**: button labels are
   routinely mislabeled (a "1080p" button pointing at the 720p file). Twin
   buttons to the same file are deduped. Quality, container and episode
   ranges (`Episodes-0104` → E01–E04) are parsed from the filename.
5. `dl.vircloud.site/api/sign/<base64>` returns a signed direct URL
   (Cloudflare worker, HMAC, ~6 h expiry, **no referer needed**). Cached
   until shortly before its own `exp`.
6. **Positive-only honesty gate**: the sign worker happily signs nonexistent
   files, so every card is emitted only after a real `Range: bytes=0-1` probe
   of the signed URL returns 200/216. Dead probes never produce cards.
7. Series requests map the requested episode into the page's batch files
   (`E01–E04`); an out-of-range episode falls back to all batches, honestly
   labelled. Cards are capped at 12, best quality first.

Nothing is re-hosted: the addon serves only tiny JSON/HTML/probe traffic;
the player pulls the file directly from the CDN.

## Endpoints

- `/manifest.json` — stream-only manifest (`catalogs: []`)
- `/stream/movie/<tt…|tmdb:…>.json`
- `/stream/series/<id>:<season>:<episode>.json`
- `/` landing, `/health`

## Local run

```
pip install requests
python addon.py            # PORT env respected (default 7000)
pytest test_movielinkbd.py # 20 offline tests (mocked HTTP)
```

## Deploy (beamup)

Procfile build (`web: python addon.py`), no Dockerfile.

The public `git.baby-beamup.club` HTTPS front is not always in DNS; the
durable path is the **dokku SSH remote** (GitHub-key based):

```
# one-time: register your GitHub public key with the dokku host
# (beamup-cli ships the shared sync key for this):
node -e "require('<beamup-cli>/lib/ssh').syncGithubKeys(
    {host: 'a.baby-beamup.club', githubUsername: '<github-user>')"

git remote add beamup dokku@a.baby-beamup.club:<account-hash>/movielinkbd
git push beamup master --force
```

`<account-hash>` = `utils.hash("<github-user>")` from beamup-cli
(`sha256`-derived; e.g. `xre000001-ai` → `3404d3c5dc63`, the prefix in the
app URL `3404d3c5dc63-movielinkbd.baby-beamup.club`).

## Verification snapshots (2026-09-19, live)

- Interstellar `tt0816692` → `♧ MovieLinkBD · 720p · MKV`, direct
  `dl.vircloud.site/download/…?exp=…&sig=…` URL, Range probe 206
  `bytes 0-1/1310671425`, Matroska EBML header confirmed.
- Inception / Oppenheimer / 3 Idiots / Top Gun Maverick → 720p MKV cards.
- Dune: Part One `tt1160419` → matched via head-term fallback.
- Squid Game → honest miss (the family has no such page).
- The Scandal S1 batch files → `Episodes-0104` / `Episodes-0508` parsed
  (unit-tested mapping).

Indexing third-party, publicly listed content; no media is hosted.
