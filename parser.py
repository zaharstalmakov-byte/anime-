"""Anilibria-first parser + auto-update background loop. Python 3.8 compatible.

Behaviour
---------
* **Primary source: Anilibria** (the new ``anilibria.top`` REST API). Every
  catalogue card is built from an Anilibria release — if a release isn't on
  Anilibria, it doesn't enter the catalogue.
* As soon as a release is fetched we materialise an ``Episode`` row for every
  aired episode reported by Anilibria. Each row stores the direct HLS URLs so
  the front-end can switch episodes instantly without re-hitting the API.
* **YummyAnime fallback for missing episodes.** After Anilibria is consulted
  we compare the aired-episode count it reports against the count YummyAnime
  knows about for the same title (matched by name + year). If YummyAnime has
  more episodes — typical for long-running shows like One Piece — we pull
  the missing numbers from YummyAnime and persist them as ``source =
  "yummyanime"`` rows so the player gets the embed URL automatically.
* **Shikimori is used only as enrichment** — to upgrade the description, the
  rating and the poster when Anilibria's data is sparse. A failed Shikimori
  request never blocks creation of the catalogue card or its episodes.
* A background task polls **Anilibria** every 30 minutes for newly aired
  episodes and appends fresh "brick" buttons to existing titles. The
  YummyAnime fallback also runs in this loop so on-going shows that switch
  to YummyAnime mid-season are kept up to date.
* Per-episode logs (``"ep N взят с anilibria"`` / ``"ep N взят с
  yummyanime"``) are written to the live admin log buffer for full
  traceability of every reparse and every auto-update round.
"""
import asyncio
import logging
from collections import deque
from datetime import datetime
from typing import Any, Deque, Dict, List, Optional, Tuple

import httpx
from sqlalchemy import delete, desc, select

from app.models.anime import Anime, Episode
from app.services.proxy import (
    PROXY_EXCEPTIONS,
    make_async_client,
    proxy_label,
)
from database import SessionLocal

logger = logging.getLogger("animeflow.parser")

# --- Anilibria endpoints (new public API, key-less) ------------------------
ANILIBRIA_HOSTS = ("anilibria.top", "api.anilibria.app")
ANILIBRIA_CDN = "https://anilibria.top"
ANILIBRIA_WEB = "https://anilibria.top"
ANILIBRIA_UA = "AnimeFlow/2.0 (+anilibria sync)"

# --- YummyAnime (fallback source for episodes Anilibria doesn't cover) -----
# YummyAnime exposes a public JSON catalogue at yummyani.me. We talk to it
# only to fill gaps in the Anilibria episode list — never as a primary
# source, never for catalogue cards.
YUMMY_HOSTS = ("yummyani.me", "yummyanime.com")
YUMMY_WEB = "https://yummyani.me"
YUMMY_UA = "AnimeFlow/2.0 (+yummyanime fallback)"

# --- Animedia (third-tier fallback) ----------------------------------------
# Animedia is consulted only after YummyAnime if there are STILL missing
# episode numbers. Like YummyAnime, never used as a primary source and
# never for catalogue cards.
ANIMEDIA_HOSTS = ("online.animedia.tv", "animedia.tv")
ANIMEDIA_WEB = "https://online.animedia.tv"
ANIMEDIA_UA = "AnimeFlow/2.0 (+animedia fallback)"

# --- Shikimori (enrichment only) ------------------------------------------
# Shikimori (the russian MAL/AniList mirror) is used ONLY to enrich
# poster/description/rating/genres for catalogue cards. It is never queried
# for episodes or stream URLs — those come strictly from Anilibria,
# YummyAnime, or Animedia. By extension, MAL and AniList themselves are
# never consulted for the primary stream chain — they would only be useful
# for subtitles or non-Russian dubs, neither of which this build serves.
SHIKIMORI_API = "https://shikimori.one/api"
SHIKIMORI_UA = "AnimeFlow/2.0 (shikimori enrichment)"

AUTO_UPDATE_INTERVAL = 30 * 60  # seconds — every 30 minutes
TOP_LIMIT = 100  # auto-update window: refresh the latest 100 titles for new eps
INITIAL_PARSE_LIMIT: Optional[int] = None  # None == ingest every release Anilibria has
REQUEST_DELAY = 0.5  # seconds between Anilibria requests (avoid throttling)
LOG_BUFFER_SIZE = 800

SOURCE_ANILIBRIA = "anilibria"
SOURCE_YUMMY = "yummyanime"
SOURCE_ANIMEDIA = "animedia"


# --------------------------------------------------------------------------- #
#  Series-group derivation (franchise/chronology key)                         #
# --------------------------------------------------------------------------- #

import re as _re

_SERIES_TRIM_RE = _re.compile(
    r"\s+(?:2nd|3rd|\d+(?:st|nd|rd|th)?|season|s\d+|part|movie|film|"
    r"the\s+movie|ova|ona|special|tv|ii+|iv|vi+|ix|x|\d+)$",
    _re.IGNORECASE,
)
_SERIES_PUNCT_RE = _re.compile(r"[^a-zа-яё0-9\s]+", _re.IGNORECASE)
_SERIES_SPACE_RE = _re.compile(r"\s+")


def _series_group_key(name_main: str, name_english: str) -> str:
    """Derive a stable franchise key from a release's titles.

    Strips season/movie/part suffixes, the part of the title after a colon,
    and punctuation, then collapses whitespace. Two releases that resolve to
    the same key are treated as parts of the same franchise.
    """
    base = (name_english or name_main or "").strip()
    if not base:
        return ""
    s = base.lower()
    s = _re.split(r"[:\-—–|]", s, 1)[0]
    while True:
        s2 = _SERIES_TRIM_RE.sub("", s).strip()
        if s2 == s:
            break
        s = s2
    s = _SERIES_PUNCT_RE.sub(" ", s)
    s = _SERIES_SPACE_RE.sub(" ", s).strip()
    return s


# --------------------------------------------------------------------------- #
#  In-memory parser state                                                     #
# --------------------------------------------------------------------------- #

STATE: Dict[str, Any] = {
    "status": "idle",          # idle | running | done | error | stopped
    "progress": 0,             # 0..100
    "processed": 0,            # number of items processed
    "total": 0,                # total items planned
    "message": "",
    "started_at": None,
    "finished_at": None,
    "auto_update": False,      # whether background loop is alive
    "last_auto_update": None,  # ISO timestamp
    "new_episodes_total": 0,   # cumulative episodes added by auto-update
    "source": "anilibria",     # which catalogue source is in use
}

_log_buffer: Deque[Dict[str, Any]] = deque(maxlen=LOG_BUFFER_SIZE)
_log_seq = 0

_run_lock = asyncio.Lock()
_stop_event = asyncio.Event()
_auto_task: Optional["asyncio.Task[None]"] = None
_auto_started = False
_state_listeners: "set[asyncio.Queue[Dict[str, Any]]]" = set()


# --------------------------------------------------------------------------- #
#  Logging primitives                                                         #
# --------------------------------------------------------------------------- #

def _log(level: str, message: str) -> None:
    global _log_seq
    _log_seq += 1
    entry = {
        "id": _log_seq,
        "level": level,
        "ts": datetime.utcnow().isoformat(timespec="seconds"),
        "message": message,
    }
    _log_buffer.append(entry)
    logger.info("[%s] %s", level, message)
    _broadcast()


def _broadcast() -> None:
    snapshot_data = snapshot()
    dead = []
    for q in _state_listeners:
        try:
            q.put_nowait(snapshot_data)
        except asyncio.QueueFull:
            dead.append(q)
    for q in dead:
        _state_listeners.discard(q)


def subscribe() -> "asyncio.Queue[Dict[str, Any]]":
    q: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue(maxsize=64)
    _state_listeners.add(q)
    try:
        q.put_nowait(snapshot())
    except asyncio.QueueFull:
        pass
    return q


def unsubscribe(q: "asyncio.Queue[Dict[str, Any]]") -> None:
    _state_listeners.discard(q)


def snapshot() -> Dict[str, Any]:
    return {
        **STATE,
        "logs": list(_log_buffer)[-80:],
    }


def logs_since(after_id: int = 0) -> List[Dict[str, Any]]:
    return [e for e in _log_buffer if e["id"] > after_id]


# --------------------------------------------------------------------------- #
#  Anilibria HTTP helpers (PRIMARY SOURCE)                                    #
# --------------------------------------------------------------------------- #

async def _anilibria_get(
    client: httpx.AsyncClient, path: str, params: Optional[Dict[str, Any]] = None
) -> Optional[Any]:
    """GET an Anilibria endpoint, transparently failing over between mirrors."""
    last_status = None
    for host in ANILIBRIA_HOSTS:
        url = "https://{}/api/v1{}".format(host, path)
        try:
            resp = await client.get(url, params=params)
        except httpx.HTTPError as exc:
            last_status = "net:{}".format(exc.__class__.__name__)
            continue
        if resp.status_code != 200:
            last_status = resp.status_code
            continue
        try:
            return resp.json()
        except ValueError:
            last_status = "json"
            continue
    if last_status is not None:
        _log("WARN", "Anilibria {} → {}".format(path, last_status))
    return None


async def _fetch_release_detail(
    client: httpx.AsyncClient, release_id_or_alias
) -> Optional[Dict[str, Any]]:
    data = await _anilibria_get(
        client, "/anime/releases/{}".format(release_id_or_alias)
    )
    return data if isinstance(data, dict) and data.get("id") else None


async def _fetch_anilibria_top(
    client: httpx.AsyncClient, limit: Optional[int]
) -> List[Dict[str, Any]]:
    """Pull releases from Anilibria's catalogue.

    The catalogue endpoint is paginated (50 per page). When ``limit`` is
    ``None`` we walk every page until the API reports we've run out — this is
    how the admin "массовый парсинг (без лимита)" mode ingests the entire
    Anilibria library. We sleep ``REQUEST_DELAY`` seconds between page
    requests to stay polite.
    """
    out: List[Dict[str, Any]] = []
    page = 1
    while True:
        if limit is not None and len(out) >= limit:
            break
        per_page = 50 if limit is None else min(50, limit - len(out))
        if per_page <= 0:
            break
        data = await _anilibria_get(
            client,
            "/anime/catalog/releases",
            {"page": page, "limit": per_page},
        )
        if not isinstance(data, dict):
            break
        items = data.get("data") or []
        if not items:
            break
        out.extend(items)
        meta = data.get("meta") or {}
        pag = meta.get("pagination") or {}
        total_pages = int(pag.get("total_pages") or page)
        STATE["message"] = "Каталог: страница {} из {} (получено {} тайтлов)".format(
            page, total_pages, len(out)
        )
        _broadcast()
        if page >= total_pages:
            break
        page += 1
        await asyncio.sleep(REQUEST_DELAY)
    return out if limit is None else out[:limit]


async def _fetch_anilibria_latest(
    client: httpx.AsyncClient, limit: int = 50
) -> List[Dict[str, Any]]:
    data = await _anilibria_get(
        client, "/anime/releases/latest", {"limit": limit}
    )
    if isinstance(data, list):
        return data
    return []


async def _anilibria_search(
    client: httpx.AsyncClient, query: str
) -> Optional[Dict[str, Any]]:
    if not query:
        return None
    data = await _anilibria_get(
        client, "/app/search/releases", {"query": query, "limit": 1}
    )
    if isinstance(data, list) and data:
        return data[0]
    return None


def _release_titles(rec: Dict[str, Any]) -> Tuple[str, str, List[str]]:
    name = rec.get("name") or {}
    ru = (name.get("main") or "").strip()
    en = (name.get("english") or "").strip()
    alt = (name.get("alternative") or "").strip()
    title = ru or en or "Без названия"
    extras: List[str] = []
    if en and en != title:
        extras.append(en)
    if alt and alt not in (title, en):
        extras.append(alt)
    return title, en, extras


def _absolute(url: Optional[str]) -> str:
    if not url:
        return ""
    if url.startswith("http://") or url.startswith("https://"):
        return url
    if url.startswith("//"):
        return "https:" + url
    if url.startswith("/"):
        return ANILIBRIA_CDN + url
    return url


def _release_poster(rec: Dict[str, Any]) -> Tuple[str, str]:
    poster_block = rec.get("poster") or {}
    src = poster_block.get("src") or poster_block.get("preview") or ""
    optimized = poster_block.get("optimized") or {}
    backdrop = optimized.get("src") or optimized.get("preview") or src
    return _absolute(src), _absolute(backdrop)


def _release_genres(rec: Dict[str, Any]) -> str:
    genres = rec.get("genres") or []
    out: List[str] = []
    for g in genres:
        if isinstance(g, dict):
            name = g.get("name")
            if name:
                out.append(str(name))
    return ", ".join(out)


def _release_year(rec: Dict[str, Any]) -> int:
    try:
        return int(rec.get("year") or 0)
    except (TypeError, ValueError):
        return 0


def _release_status(rec: Dict[str, Any]) -> str:
    if rec.get("is_in_production") or rec.get("is_ongoing"):
        return "ongoing"
    return "released"


def _release_episodes(rec: Dict[str, Any]) -> List[Dict[str, Any]]:
    episodes = rec.get("episodes") or []
    out: List[Dict[str, Any]] = []
    if not isinstance(episodes, list):
        return out
    for ep in episodes:
        if not isinstance(ep, dict):
            continue
        try:
            number = int(ep.get("ordinal") or 0)
        except (TypeError, ValueError):
            continue
        if number <= 0:
            continue
        out.append({
            "number": number,
            "name": (ep.get("name") or "").strip(),
            "hls_sd": ep.get("hls_480") or "",
            "hls_hd": ep.get("hls_720") or "",
            "hls_fhd": ep.get("hls_1080") or "",
        })
    out.sort(key=lambda e: e["number"])
    return out


def _build_iframe_url(alias: str, episode_number: int) -> str:
    if not alias:
        return ""
    return "{}/anime/releases/release/{}/episodes".format(ANILIBRIA_WEB, alias)


def _best_video_url(ep_payload: Dict[str, Any], alias: str) -> str:
    for key in ("hls_hd", "hls_fhd", "hls_sd"):
        url = ep_payload.get(key) or ""
        if url:
            return url
    return _build_iframe_url(alias, ep_payload["number"])


# --------------------------------------------------------------------------- #
#  YummyAnime fallback (used only to fill gaps in Anilibria episode lists)    #
# --------------------------------------------------------------------------- #

async def _yummy_get(
    client: httpx.AsyncClient,
    path: str,
    params: Optional[Dict[str, Any]] = None,
) -> Optional[Any]:
    """GET a YummyAnime endpoint, transparently failing over between mirrors."""
    last_status = None
    for host in YUMMY_HOSTS:
        url = "https://{}{}".format(host, path)
        try:
            resp = await client.get(url, params=params)
        except httpx.HTTPError as exc:
            last_status = "net:{}".format(exc.__class__.__name__)
            continue
        if resp.status_code != 200:
            last_status = resp.status_code
            continue
        try:
            return resp.json()
        except ValueError:
            last_status = "json"
            continue
    if last_status is not None:
        _log("WARN", "YummyAnime {} → {}".format(path, last_status))
    return None


def _yummy_match_score(rec: Dict[str, Any], title: str, year: int) -> int:
    """Heuristic score for picking the best YummyAnime hit for a title."""
    score = 0
    norm = (title or "").strip().lower()
    if not norm:
        return 0
    candidates: List[str] = []
    for k in ("title", "title_en", "title_ru", "title_orig", "name", "rus_name", "eng_name"):
        v = rec.get(k)
        if isinstance(v, str) and v.strip():
            candidates.append(v.strip().lower())
    other = rec.get("other_titles") or rec.get("aliases") or []
    if isinstance(other, list):
        for v in other:
            if isinstance(v, str) and v.strip():
                candidates.append(v.strip().lower())
    for cand in candidates:
        if cand == norm:
            score += 100
        elif norm in cand or cand in norm:
            score += 40
    if year:
        try:
            ry = int(rec.get("year") or rec.get("year_int") or 0)
        except (TypeError, ValueError):
            ry = 0
        if ry and abs(ry - year) <= 1:
            score += 25
    return score


async def _yummy_search(
    client: httpx.AsyncClient,
    title: str,
    year: int = 0,
) -> Optional[Dict[str, Any]]:
    """Find the best matching YummyAnime entry for a given title + year."""
    if not title:
        return None
    data = await _yummy_get(
        client, "/api/search", {"q": title, "limit": 10}
    )
    items: List[Dict[str, Any]] = []
    if isinstance(data, list):
        items = [x for x in data if isinstance(x, dict)]
    elif isinstance(data, dict):
        for key in ("data", "results", "response", "items"):
            v = data.get(key)
            if isinstance(v, list):
                items = [x for x in v if isinstance(x, dict)]
                break
    if not items:
        return None
    best: Optional[Dict[str, Any]] = None
    best_score = 0
    for rec in items:
        s = _yummy_match_score(rec, title, year)
        if s > best_score:
            best_score = s
            best = rec
    if best is None or best_score < 40:
        return None
    return best


def _yummy_id_of(rec: Dict[str, Any]) -> Optional[int]:
    for key in ("id", "anime_id", "yummy_id"):
        v = rec.get(key)
        try:
            iv = int(v) if v is not None else 0
        except (TypeError, ValueError):
            iv = 0
        if iv:
            return iv
    return None


def _yummy_slug_of(rec: Dict[str, Any]) -> str:
    for key in ("slug", "code", "alias", "url"):
        v = rec.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip().rsplit("/", 1)[-1]
    return ""


async def _fetch_yummy_episodes(
    client: httpx.AsyncClient,
    yummy_id: int,
    slug: str,
) -> List[Dict[str, Any]]:
    """Return YummyAnime episodes as a list of normalised payload dicts."""
    if not yummy_id and not slug:
        return []

    paths: List[str] = []
    if yummy_id:
        paths.append("/api/anime/{}/episodes".format(yummy_id))
        paths.append("/api/anime/{}".format(yummy_id))
    if slug:
        paths.append("/api/anime/{}/episodes".format(slug))
        paths.append("/api/anime/{}".format(slug))

    raw: Any = None
    for p in paths:
        raw = await _yummy_get(client, p)
        if raw:
            break
    if not raw:
        return []

    candidates: List[Dict[str, Any]] = []
    if isinstance(raw, list):
        candidates = [x for x in raw if isinstance(x, dict)]
    elif isinstance(raw, dict):
        for key in ("episodes", "series", "data", "items", "list"):
            v = raw.get(key)
            if isinstance(v, list):
                candidates = [x for x in v if isinstance(x, dict)]
                break
        if not candidates and isinstance(raw.get("anime"), dict):
            v = (raw["anime"] or {}).get("episodes")
            if isinstance(v, list):
                candidates = [x for x in v if isinstance(x, dict)]

    out: List[Dict[str, Any]] = []
    seen: set = set()
    for ep in candidates:
        n = 0
        for k in ("episode", "number", "ordinal", "ep", "num"):
            try:
                n = int(ep.get(k) or 0)
            except (TypeError, ValueError):
                n = 0
            if n:
                break
        if n <= 0 or n in seen:
            continue
        seen.add(n)

        iframe = ""
        for k in ("iframe", "iframe_url", "embed", "player_url", "video_url"):
            v = ep.get(k)
            if isinstance(v, str) and v.strip():
                iframe = v.strip()
                break
        if not iframe and slug:
            iframe = "{}/embed/{}/{}".format(YUMMY_WEB, slug, n)
        elif not iframe and yummy_id:
            iframe = "{}/embed/{}/{}".format(YUMMY_WEB, yummy_id, n)

        title = (ep.get("title") or ep.get("name") or "").strip()
        out.append({
            "number": n,
            "title": title,
            "iframe": iframe,
        })
    out.sort(key=lambda e: e["number"])
    return out


async def _yummy_fill_gaps(
    client: httpx.AsyncClient,
    title: str,
    year: int,
    anilibria_episodes: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """Return (yummy_episodes_for_missing_numbers, yummy_meta).

    Compares Anilibria's known episode numbers to YummyAnime's list. If
    YummyAnime knows about more episodes — e.g. One Piece, where Anilibria
    only has the latest arcs — we return the YummyAnime payloads for the
    numbers Anilibria didn't cover. The caller is expected to merge them
    with the Anilibria list (Anilibria wins on conflict).
    """
    rec = await _yummy_search(client, title, year)
    if not rec:
        return [], None
    yummy_id = _yummy_id_of(rec) or 0
    slug = _yummy_slug_of(rec)
    yummy_eps = await _fetch_yummy_episodes(client, yummy_id, slug)
    if not yummy_eps:
        return [], None
    have = {int(e["number"]) for e in anilibria_episodes if e.get("number")}
    yummy_have = {int(e["number"]) for e in yummy_eps}
    missing = sorted(yummy_have - have)
    if not missing:
        return [], {"id": yummy_id, "slug": slug, "rec": rec}
    fill: List[Dict[str, Any]] = [
        ep for ep in yummy_eps if int(ep["number"]) in set(missing)
    ]
    return fill, {"id": yummy_id, "slug": slug, "rec": rec}


# --------------------------------------------------------------------------- #
#  Animedia fallback (third-tier — used after Anilibria + YummyAnime still    #
#  leave gaps; only fills the remaining missing episode numbers).             #
# --------------------------------------------------------------------------- #

async def _animedia_get(
    client: httpx.AsyncClient,
    path: str,
    params: Optional[Dict[str, Any]] = None,
) -> Optional[Any]:
    """GET an Animedia endpoint, transparently failing over between mirrors."""
    last_status = None
    for host in ANIMEDIA_HOSTS:
        url = "https://{}{}".format(host, path)
        try:
            resp = await client.get(
                url,
                params=params,
                headers={
                    "User-Agent": ANIMEDIA_UA,
                    "Accept": "application/json,text/plain,*/*",
                },
                timeout=httpx.Timeout(12.0, connect=6.0),
            )
        except httpx.HTTPError as exc:
            last_status = "net:{}".format(exc.__class__.__name__)
            continue
        if resp.status_code != 200:
            last_status = resp.status_code
            continue
        try:
            return resp.json()
        except ValueError:
            # Some Animedia endpoints return HTML — return raw text so the
            # caller can still try a different path. Episode list extraction
            # only works on JSON responses, so non-JSON is treated as a miss.
            last_status = "json"
            continue
    if last_status is not None:
        _log("WARN", "Animedia {} → {}".format(path, last_status))
    return None


def _animedia_match_score(rec: Dict[str, Any], title: str, year: int) -> int:
    """Heuristic score for picking the best Animedia hit for a title."""
    score = 0
    norm = (title or "").strip().lower()
    if not norm:
        return 0
    candidates: List[str] = []
    for k in (
        "title", "title_en", "title_ru", "title_orig",
        "name", "rus_name", "eng_name", "original_name",
    ):
        v = rec.get(k)
        if isinstance(v, str) and v.strip():
            candidates.append(v.strip().lower())
    other = rec.get("other_titles") or rec.get("aliases") or rec.get("synonyms") or []
    if isinstance(other, list):
        for v in other:
            if isinstance(v, str) and v.strip():
                candidates.append(v.strip().lower())
    for cand in candidates:
        if cand == norm:
            score += 100
        elif norm in cand or cand in norm:
            score += 40
    if year:
        try:
            ry = int(rec.get("year") or rec.get("year_int") or 0)
        except (TypeError, ValueError):
            ry = 0
        if ry and abs(ry - year) <= 1:
            score += 25
    return score


async def _animedia_search(
    client: httpx.AsyncClient,
    title: str,
    year: int = 0,
) -> Optional[Dict[str, Any]]:
    """Find the best matching Animedia entry for a given title + year."""
    if not title:
        return None
    data: Any = None
    for path, params in (
        ("/api/search", {"q": title, "limit": 10}),
        ("/api/anime/search", {"q": title, "limit": 10}),
        ("/api/v1/search", {"query": title, "limit": 10}),
    ):
        data = await _animedia_get(client, path, params)
        if data:
            break
    if not data:
        return None

    items: List[Dict[str, Any]] = []
    if isinstance(data, list):
        items = [x for x in data if isinstance(x, dict)]
    elif isinstance(data, dict):
        for key in ("data", "results", "response", "items", "hits"):
            v = data.get(key)
            if isinstance(v, list):
                items = [x for x in v if isinstance(x, dict)]
                break
    if not items:
        return None

    best: Optional[Dict[str, Any]] = None
    best_score = 0
    for rec in items:
        s = _animedia_match_score(rec, title, year)
        if s > best_score:
            best_score = s
            best = rec
    if best is None or best_score < 40:
        return None
    return best


def _animedia_id_of(rec: Dict[str, Any]) -> Optional[int]:
    for key in ("id", "anime_id", "animedia_id"):
        v = rec.get(key)
        try:
            iv = int(v) if v is not None else 0
        except (TypeError, ValueError):
            iv = 0
        if iv:
            return iv
    return None


def _animedia_slug_of(rec: Dict[str, Any]) -> str:
    for key in ("slug", "code", "alias", "url", "permalink"):
        v = rec.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip().rsplit("/", 1)[-1]
    return ""


async def _fetch_animedia_episodes(
    client: httpx.AsyncClient,
    animedia_id: int,
    slug: str,
) -> List[Dict[str, Any]]:
    """Return Animedia episodes as a list of normalised payload dicts."""
    if not animedia_id and not slug:
        return []

    paths: List[str] = []
    if animedia_id:
        paths.append("/api/anime/{}/episodes".format(animedia_id))
        paths.append("/api/anime/{}".format(animedia_id))
        paths.append("/api/v1/anime/{}/episodes".format(animedia_id))
    if slug:
        paths.append("/api/anime/{}/episodes".format(slug))
        paths.append("/api/anime/{}".format(slug))
        paths.append("/api/v1/anime/{}/episodes".format(slug))

    raw: Any = None
    for p in paths:
        raw = await _animedia_get(client, p)
        if raw:
            break
    if not raw:
        return []

    candidates: List[Dict[str, Any]] = []
    if isinstance(raw, list):
        candidates = [x for x in raw if isinstance(x, dict)]
    elif isinstance(raw, dict):
        for key in ("episodes", "series", "data", "items", "list", "playlist"):
            v = raw.get(key)
            if isinstance(v, list):
                candidates = [x for x in v if isinstance(x, dict)]
                break
        if not candidates and isinstance(raw.get("anime"), dict):
            v = (raw["anime"] or {}).get("episodes")
            if isinstance(v, list):
                candidates = [x for x in v if isinstance(x, dict)]

    out: List[Dict[str, Any]] = []
    seen: set = set()
    for ep in candidates:
        n = 0
        for k in ("episode", "number", "ordinal", "ep", "num", "index"):
            try:
                n = int(ep.get(k) or 0)
            except (TypeError, ValueError):
                n = 0
            if n:
                break
        if n <= 0 or n in seen:
            continue
        seen.add(n)

        iframe = ""
        for k in ("iframe", "iframe_url", "embed", "player_url", "video_url", "file"):
            v = ep.get(k)
            if isinstance(v, str) and v.strip():
                iframe = v.strip()
                break
        if not iframe and slug:
            iframe = "{}/embed/{}/{}".format(ANIMEDIA_WEB, slug, n)
        elif not iframe and animedia_id:
            iframe = "{}/embed/{}/{}".format(ANIMEDIA_WEB, animedia_id, n)

        title = (ep.get("title") or ep.get("name") or "").strip()
        out.append({
            "number": n,
            "title": title,
            "iframe": iframe,
        })
    out.sort(key=lambda e: e["number"])
    return out


async def _animedia_fill_gaps(
    client: httpx.AsyncClient,
    title: str,
    year: int,
    already_have_numbers: set,
) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """Return (animedia_episodes_for_missing_numbers, animedia_meta).

    Compares the union of Anilibria + YummyAnime numbers (passed as
    ``already_have_numbers``) to Animedia's list. Returns Animedia payloads
    only for episode numbers that neither Anilibria nor YummyAnime
    supplied. The caller merges these in last; existing Anilibria/YummyAnime
    rows always win on conflict.
    """
    rec = await _animedia_search(client, title, year)
    if not rec:
        return [], None
    animedia_id = _animedia_id_of(rec) or 0
    slug = _animedia_slug_of(rec)
    eps = await _fetch_animedia_episodes(client, animedia_id, slug)
    if not eps:
        return [], None
    animedia_have = {int(e["number"]) for e in eps}
    missing = sorted(animedia_have - already_have_numbers)
    if not missing:
        return [], {"id": animedia_id, "slug": slug, "rec": rec}
    fill: List[Dict[str, Any]] = [
        ep for ep in eps if int(ep["number"]) in set(missing)
    ]
    return fill, {"id": animedia_id, "slug": slug, "rec": rec}


# --------------------------------------------------------------------------- #
#  Shikimori enrichment (SECONDARY — never blocks catalogue creation)         #
# --------------------------------------------------------------------------- #

async def _shikimori_enrich(
    client: httpx.AsyncClient, title_ru: str, title_en: str
) -> Dict[str, Any]:
    query = title_en or title_ru
    if not query:
        return {}
    try:
        resp = await client.get(
            SHIKIMORI_API + "/animes",
            params={"search": query, "limit": 1},
            headers={"User-Agent": SHIKIMORI_UA, "Accept": "application/json"},
            timeout=httpx.Timeout(8.0, connect=5.0),
        )
    except httpx.HTTPError:
        return {}
    if resp.status_code != 200:
        return {}
    try:
        data = resp.json()
    except ValueError:
        return {}
    if not isinstance(data, list) or not data:
        return {}
    rec = data[0]

    detail: Optional[Dict[str, Any]] = None
    aid = rec.get("id")
    if aid:
        try:
            d = await client.get(
                "{}/animes/{}".format(SHIKIMORI_API, aid),
                headers={"User-Agent": SHIKIMORI_UA, "Accept": "application/json"},
                timeout=httpx.Timeout(8.0, connect=5.0),
            )
            if d.status_code == 200:
                detail = d.json()
        except (httpx.HTTPError, ValueError):
            detail = None

    image = rec.get("image") or {}
    poster = image.get("original") or image.get("preview") or ""
    if poster and poster.startswith("/"):
        poster = "https://shikimori.one" + poster

    out: Dict[str, Any] = {}
    if poster:
        out["poster_url_alt"] = poster
    score = rec.get("score")
    try:
        score_val = float(score or 0.0)
    except (TypeError, ValueError):
        score_val = 0.0
    if score_val:
        out["rating"] = score_val
    if detail:
        desc = (detail.get("description") or "").strip()
        if desc:
            out["description"] = desc[:4000]
        genres = detail.get("genres") or []
        gs = ", ".join(
            (g.get("russian") or g.get("name") or "")
            for g in genres if isinstance(g, dict)
        )
        if gs:
            out["genres_alt"] = gs
    return out


# --------------------------------------------------------------------------- #
#  Episode persistence                                                        #
# --------------------------------------------------------------------------- #

def _build_yummy_episode_row(
    anime: Anime,
    ep: Dict[str, Any],
    yummy_meta: Optional[Dict[str, Any]],
) -> Episode:
    number = int(ep["number"])
    yummy_id = (yummy_meta or {}).get("id") or 0
    slug = (yummy_meta or {}).get("slug") or ""
    iframe = ep.get("iframe") or ""
    page_url = ""
    if slug:
        page_url = "{}/anime/{}".format(YUMMY_WEB, slug)
    elif yummy_id:
        page_url = "{}/anime/{}".format(YUMMY_WEB, yummy_id)
    return Episode(
        anime_id=anime.id,
        episode_number=number,
        title=ep.get("title") or "Эпизод {}".format(number),
        video_url=iframe,
        source=SOURCE_YUMMY,
        yummy_id=int(yummy_id) or None,
        yummy_slug=slug,
        yummy_iframe=iframe,
        yummy_page_url=page_url,
    )


def _build_animedia_episode_row(
    anime: Anime,
    ep: Dict[str, Any],
    animedia_meta: Optional[Dict[str, Any]],
) -> Episode:
    number = int(ep["number"])
    animedia_id = (animedia_meta or {}).get("id") or 0
    slug = (animedia_meta or {}).get("slug") or ""
    iframe = ep.get("iframe") or ""
    page_url = ""
    if slug:
        page_url = "{}/anime/{}".format(ANIMEDIA_WEB, slug)
    elif animedia_id:
        page_url = "{}/anime/{}".format(ANIMEDIA_WEB, animedia_id)
    return Episode(
        anime_id=anime.id,
        episode_number=number,
        title=ep.get("title") or "Эпизод {}".format(number),
        video_url=iframe,
        source=SOURCE_ANIMEDIA,
        animedia_id=int(animedia_id) or None,
        animedia_slug=slug,
        animedia_iframe=iframe,
        animedia_page_url=page_url,
    )


async def _ensure_episodes(
    session,
    anime: Anime,
    episodes_payload: List[Dict[str, Any]],
    alias: str,
    yummy_payload: Optional[List[Dict[str, Any]]] = None,
    yummy_meta: Optional[Dict[str, Any]] = None,
    animedia_payload: Optional[List[Dict[str, Any]]] = None,
    animedia_meta: Optional[Dict[str, Any]] = None,
    log_per_episode: bool = False,
) -> Dict[str, int]:
    """Sync ``Episode`` rows from YummyAnime + Anilibria + Animedia payloads.

    **Priority on conflict: YummyAnime > Anilibria > Animedia.** YummyAnime
    is the primary source: it's processed first and wins every collision.
    Anilibria fills only numbers YummyAnime didn't return. Animedia is the
    final fallback for numbers neither YummyAnime nor Anilibria covered.

    Returns a dict
    ``{"added": N, "from_anilibria": N, "from_yummy": N, "from_animedia": N}``.
    """
    yummy_payload = yummy_payload or []
    animedia_payload = animedia_payload or []
    if not episodes_payload and not yummy_payload and not animedia_payload:
        return {"added": 0, "from_anilibria": 0, "from_yummy": 0, "from_animedia": 0}

    existing = await session.execute(
        select(Episode).where(Episode.anime_id == anime.id)
    )
    by_number: Dict[int, Episode] = {
        e.episode_number: e for e in existing.scalars().all()
    }

    added = 0
    from_anilibria = 0
    from_yummy = 0
    from_animedia = 0

    # ---- Pass 1: YummyAnime (primary) — wins every collision. ----
    yummy_numbers: set = set()
    for ep in yummy_payload:
        number = int(ep.get("number") or 0)
        if not number:
            continue
        yummy_id = (yummy_meta or {}).get("id") or 0
        slug = (yummy_meta or {}).get("slug") or ""
        iframe = ep.get("iframe") or ""
        page_url = ""
        if slug:
            page_url = "{}/anime/{}".format(YUMMY_WEB, slug)
        elif yummy_id:
            page_url = "{}/anime/{}".format(YUMMY_WEB, yummy_id)

        row = by_number.get(number)
        if row is None:
            session.add(_build_yummy_episode_row(anime, ep, yummy_meta))
            added += 1
            from_yummy += 1
            yummy_numbers.add(number)
            if log_per_episode:
                _log("INFO", "  ep {}: yummyanime (новая, primary)".format(number))
        else:
            row.title = ep.get("title") or row.title
            row.video_url = iframe or row.video_url
            row.source = SOURCE_YUMMY
            row.yummy_id = int(yummy_id) or row.yummy_id
            row.yummy_slug = slug or row.yummy_slug
            row.yummy_iframe = iframe or row.yummy_iframe
            row.yummy_page_url = page_url or row.yummy_page_url
            from_yummy += 1
            yummy_numbers.add(number)
            if log_per_episode:
                _log("INFO", "  ep {}: yummyanime (обновлена, primary)".format(number))

    # ---- Pass 2: Anilibria — only for numbers YummyAnime didn't supply. ----
    anilibria_numbers: set = set()
    for ep in episodes_payload:
        number = int(ep.get("number") or 0)
        if not number or number in yummy_numbers:
            continue
        hls_hd = ep.get("hls_hd") or ""
        hls_sd = ep.get("hls_sd") or ""
        hls_fhd = ep.get("hls_fhd") or ""
        iframe = _build_iframe_url(alias, number)
        video_url = _best_video_url(ep, alias)
        title = ep.get("name") or "Эпизод {}".format(number)

        row = by_number.get(number)
        if row is None:
            session.add(
                Episode(
                    anime_id=anime.id,
                    episode_number=number,
                    title=title,
                    video_url=video_url,
                    source=SOURCE_ANILIBRIA,
                    anilibria_id=anime.anilibria_id,
                    anilibria_host="",
                    anilibria_hls_hd=hls_hd,
                    anilibria_hls_sd=hls_sd,
                    anilibria_hls_fhd=hls_fhd,
                    anilibria_iframe=iframe,
                )
            )
            added += 1
            from_anilibria += 1
            anilibria_numbers.add(number)
            if log_per_episode:
                _log("INFO", "  ep {}: anilibria (новая, fallback)".format(number))
        else:
            # Don't overwrite a row YummyAnime just wrote in this pass.
            if row.source == SOURCE_YUMMY and row.yummy_iframe:
                continue
            row.video_url = video_url or row.video_url
            row.title = title or row.title
            row.source = SOURCE_ANILIBRIA
            row.anilibria_id = anime.anilibria_id
            row.anilibria_hls_hd = hls_hd or row.anilibria_hls_hd
            row.anilibria_hls_sd = hls_sd or row.anilibria_hls_sd
            row.anilibria_hls_fhd = hls_fhd or row.anilibria_hls_fhd
            row.anilibria_iframe = iframe or row.anilibria_iframe
            from_anilibria += 1
            anilibria_numbers.add(number)
            if log_per_episode:
                _log("INFO", "  ep {}: anilibria (обновлена, fallback)".format(number))

    # ---- Pass 3: Animedia — only for numbers neither Yummy nor Anilibria supplied. ----
    blocked_numbers = yummy_numbers | anilibria_numbers
    for ep in animedia_payload:
        number = int(ep.get("number") or 0)
        if not number or number in blocked_numbers:
            continue
        row = by_number.get(number)
        animedia_id = (animedia_meta or {}).get("id") or 0
        slug = (animedia_meta or {}).get("slug") or ""
        iframe = ep.get("iframe") or ""
        page_url = ""
        if slug:
            page_url = "{}/anime/{}".format(ANIMEDIA_WEB, slug)
        elif animedia_id:
            page_url = "{}/anime/{}".format(ANIMEDIA_WEB, animedia_id)

        if row is None:
            session.add(_build_animedia_episode_row(anime, ep, animedia_meta))
            added += 1
            from_animedia += 1
            if log_per_episode:
                _log("INFO", "  ep {}: animedia (новая, last-resort)".format(number))
        else:
            # Never override YummyAnime / Anilibria rows from this pass.
            if row.source in (SOURCE_YUMMY, SOURCE_ANILIBRIA):
                continue
            row.title = ep.get("title") or row.title
            row.video_url = iframe or row.video_url
            row.source = SOURCE_ANIMEDIA
            row.animedia_id = int(animedia_id) or row.animedia_id
            row.animedia_slug = slug or row.animedia_slug
            row.animedia_iframe = iframe or row.animedia_iframe
            row.animedia_page_url = page_url or row.animedia_page_url
            from_animedia += 1
            if log_per_episode:
                _log("INFO", "  ep {}: animedia (обновлена, last-resort)".format(number))

    return {
        "added": added,
        "from_anilibria": from_anilibria,
        "from_yummy": from_yummy,
        "from_animedia": from_animedia,
    }


# --------------------------------------------------------------------------- #
#  Single-anime full re-parse                                                 #
# --------------------------------------------------------------------------- #

async def reparse_anime(anime_id: int) -> Dict[str, Any]:
    """Wipe and re-import all episodes for one anime.

    All three sources are queried **in parallel** through the residential
    proxy. Priority on conflict: **YummyAnime > Anilibria > Animedia**.
    YummyAnime is primary; Anilibria fills only numbers Yummy didn't
    return; Animedia is a final fallback for numbers nobody else covered.
    Old rows are deleted up front so duplicates can't survive a reparse.

    Returns a structured report and pushes per-episode log lines into the
    live admin log buffer so the operator can see exactly which source
    each episode came from.
    """
    report: Dict[str, Any] = {
        "anime_id": anime_id,
        "ok": False,
        "deleted": 0,
        "added": 0,
        "from_anilibria": 0,
        "from_yummy": 0,
        "from_animedia": 0,
        "total": 0,
        "gaps": [],
        "duplicates": [],
        "title": None,
        "yummy": None,        # {"id":..., "slug":..., "filled":[1,2,3]}
        "animedia": None,     # {"id":..., "slug":..., "filled":[1,2,3]}
        "sources": {},        # {episode_number: "anilibria" | "yummyanime" | "animedia"}
        "error": None,
    }

    async with SessionLocal() as session:
        anime = await session.get(Anime, anime_id)
        if not anime:
            report["error"] = "Аниме не найдено"
            _log("ERROR", "Re-parse: id={} не найдено".format(anime_id))
            return report
        report["title"] = anime.title
        anime_year = int(anime.year or 0)
        anime_title = anime.title
        anime_alt = anime.alternative_titles or ""

    _log(
        "INFO",
        "Re-parse «{}» (id={}) — начинаю (proxy: {})".format(
            report["title"], anime_id, proxy_label()
        ),
    )

    rec: Optional[Dict[str, Any]] = None
    yummy_fill: List[Dict[str, Any]] = []
    yummy_meta: Optional[Dict[str, Any]] = None
    animedia_fill: List[Dict[str, Any]] = []
    animedia_meta: Optional[Dict[str, Any]] = None

    # Build search-title shortlist from main + alternative titles.
    search_titles: List[str] = [anime_title]
    if anime_alt:
        search_titles.extend(
            [t.strip() for t in anime_alt.split(",") if t.strip()][:3]
        )

    try:
        async with make_async_client(
            headers={"User-Agent": ANILIBRIA_UA, "Accept": "application/json"},
        ) as client:

            async def _fetch_yummy() -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
                # Pass an empty "existing" list so the helper returns the FULL
                # YummyAnime episode list (we treat Yummy as primary, not a gap-fill).
                for st in search_titles:
                    try:
                        full, meta = await _yummy_fill_gaps(
                            client, st, anime_year, []
                        )
                    except PROXY_EXCEPTIONS as exc:
                        _log(
                            "WARN",
                            "Re-parse «{}»: YummyAnime прокси/сеть — {}".format(
                                report["title"], exc
                            ),
                        )
                        continue
                    except Exception as exc:  # noqa: BLE001
                        _log(
                            "WARN",
                            "Re-parse «{}»: YummyAnime ошибка — {}".format(
                                report["title"], exc
                            ),
                        )
                        continue
                    if meta:
                        return full, meta
                    await asyncio.sleep(0.2)
                return [], None

            async def _fetch_anilibria() -> Optional[Dict[str, Any]]:
                rec_local: Optional[Dict[str, Any]] = None
                try:
                    if anime.anilibria_id:
                        rec_local = await _fetch_release_detail(
                            client, anime.anilibria_id
                        )
                    if rec_local is None and anime.anilibria_code:
                        rec_local = await _fetch_release_detail(
                            client, anime.anilibria_code
                        )
                    if rec_local is None:
                        rec_local = await _anilibria_search(client, anime_title)
                        if rec_local and rec_local.get("id"):
                            rec_local = (
                                await _fetch_release_detail(client, rec_local["id"])
                                or rec_local
                            )
                except PROXY_EXCEPTIONS as exc:
                    _log(
                        "WARN",
                        "Re-parse «{}»: Anilibria прокси/сеть — {}".format(
                            report["title"], exc
                        ),
                    )
                    return None
                except Exception as exc:  # noqa: BLE001
                    _log(
                        "WARN",
                        "Re-parse «{}»: Anilibria ошибка — {}".format(
                            report["title"], exc
                        ),
                    )
                    return None
                return rec_local

            async def _fetch_animedia() -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
                # Pass an empty "covered" set so the helper returns the FULL
                # Animedia episode list (final fallback).
                for st in search_titles:
                    try:
                        full, meta = await _animedia_fill_gaps(
                            client, st, anime_year, set()
                        )
                    except PROXY_EXCEPTIONS as exc:
                        _log(
                            "WARN",
                            "Re-parse «{}»: Animedia прокси/сеть — {}".format(
                                report["title"], exc
                            ),
                        )
                        continue
                    except Exception as exc:  # noqa: BLE001
                        _log(
                            "WARN",
                            "Re-parse «{}»: Animedia ошибка — {}".format(
                                report["title"], exc
                            ),
                        )
                        continue
                    if meta:
                        return full, meta
                    await asyncio.sleep(0.2)
                return [], None

            # All three sources fired in parallel through the residential proxy.
            yummy_res, rec, animedia_res = await asyncio.gather(
                _fetch_yummy(), _fetch_anilibria(), _fetch_animedia()
            )
            yummy_fill, yummy_meta = yummy_res
            animedia_fill, animedia_meta = animedia_res
    except PROXY_EXCEPTIONS as exc:
        report["error"] = "Прокси/сеть недоступны: {}".format(exc)
        _log(
            "ERROR",
            "Re-parse «{}»: прокси/сеть — {}".format(report["title"], exc),
        )
        return report
    except Exception as exc:  # noqa: BLE001
        report["error"] = "Ошибка сети: {}".format(exc)
        _log("ERROR", "Re-parse «{}»: ошибка сети — {}".format(report["title"], exc))
        return report

    episodes_payload = _release_episodes(rec) if rec else []

    if not episodes_payload and not yummy_fill and not animedia_fill:
        report["error"] = "Серий не найдено ни на Anilibria, ни на YummyAnime, ни на Animedia"
        _log(
            "WARN",
            "Re-parse «{}»: серий нет ни на Anilibria, ни на YummyAnime, ни на Animedia".format(
                report["title"]
            ),
        )
        return report

    alias = ""
    if rec:
        alias = (rec.get("alias") or anime.anilibria_code or "").strip()

    # Detect duplicates and gaps across ALL sources combined.
    combined_numbers: List[int] = (
        [int(ep["number"]) for ep in episodes_payload if ep.get("number")]
        + [int(ep["number"]) for ep in yummy_fill if ep.get("number")]
        + [int(ep["number"]) for ep in animedia_fill if ep.get("number")]
    )
    seen: set = set()
    for n in combined_numbers:
        if n in seen:
            report["duplicates"].append(n)
        seen.add(n)
    if combined_numbers:
        lo, hi = min(combined_numbers), max(combined_numbers)
        present = set(combined_numbers)
        report["gaps"] = [n for n in range(lo, hi + 1) if n not in present]

    # Priority: YummyAnime > Anilibria > Animedia. We dedupe in that order
    # so each episode number ends up sourced from the highest-priority
    # provider that returned it.
    yummy_taken: set = set()
    clean_yummy: List[Dict[str, Any]] = []
    for ep in yummy_fill:
        n = int(ep.get("number") or 0)
        if not n or n in yummy_taken:
            continue
        yummy_taken.add(n)
        clean_yummy.append(ep)
    clean_yummy.sort(key=lambda e: int(e["number"]))

    anilibria_taken: set = set()
    clean_anilibria: List[Dict[str, Any]] = []
    for ep in episodes_payload:
        n = int(ep.get("number") or 0)
        if not n or n in anilibria_taken or n in yummy_taken:
            continue
        anilibria_taken.add(n)
        clean_anilibria.append(ep)
    clean_anilibria.sort(key=lambda e: int(e["number"]))

    animedia_taken: set = set()
    clean_animedia: List[Dict[str, Any]] = []
    for ep in animedia_fill:
        n = int(ep.get("number") or 0)
        if (
            not n
            or n in animedia_taken
            or n in yummy_taken
            or n in anilibria_taken
        ):
            continue
        animedia_taken.add(n)
        clean_animedia.append(ep)
    clean_animedia.sort(key=lambda e: int(e["number"]))

    async with SessionLocal() as session:
        fresh = await session.get(Anime, anime_id)
        if not fresh:
            report["error"] = "Аниме исчезло во время re-parse"
            return report

        # Wipe existing episodes — clean slate, kills any duplicate rows.
        del_res = await session.execute(
            delete(Episode).where(Episode.anime_id == anime_id)
        )
        report["deleted"] = int(del_res.rowcount or 0)

        if rec is not None:
            if not fresh.anilibria_id and rec.get("id"):
                try:
                    fresh.anilibria_id = int(rec["id"])
                except (TypeError, ValueError):
                    pass
            if alias and not fresh.anilibria_code:
                fresh.anilibria_code = alias

        # Re-create everything in priority order: YummyAnime first (primary),
        # then Anilibria (fallback), then Animedia (last resort). We do it
        # directly here (not via _ensure_episodes) so the report can carry
        # exact per-episode source attribution.
        for ep in clean_yummy:
            number = int(ep["number"])
            session.add(_build_yummy_episode_row(fresh, ep, yummy_meta))
            report["sources"][number] = SOURCE_YUMMY
            report["from_yummy"] += 1

        for ep in clean_anilibria:
            number = int(ep["number"])
            session.add(
                Episode(
                    anime_id=anime_id,
                    episode_number=number,
                    title=ep.get("name") or "Эпизод {}".format(number),
                    video_url=_best_video_url(ep, alias),
                    source=SOURCE_ANILIBRIA,
                    anilibria_id=fresh.anilibria_id,
                    anilibria_host="",
                    anilibria_hls_hd=ep.get("hls_hd") or "",
                    anilibria_hls_sd=ep.get("hls_sd") or "",
                    anilibria_hls_fhd=ep.get("hls_fhd") or "",
                    anilibria_iframe=_build_iframe_url(alias, number),
                )
            )
            report["sources"][number] = SOURCE_ANILIBRIA
            report["from_anilibria"] += 1

        for ep in clean_animedia:
            number = int(ep["number"])
            session.add(_build_animedia_episode_row(fresh, ep, animedia_meta))
            report["sources"][number] = SOURCE_ANIMEDIA
            report["from_animedia"] += 1

        # Update episodes_total = max episode number across all sources.
        try:
            fresh.episodes_total = max(
                report["sources"].keys(), default=0
            )
        except Exception:  # noqa: BLE001
            pass

        await session.commit()

    report["added"] = (
        report["from_anilibria"] + report["from_yummy"] + report["from_animedia"]
    )
    report["total"] = report["added"]
    report["ok"] = True

    if yummy_meta:
        report["yummy"] = {
            "id": yummy_meta.get("id"),
            "slug": yummy_meta.get("slug"),
            "filled": [int(ep["number"]) for ep in clean_yummy],
        }
    if animedia_meta:
        report["animedia"] = {
            "id": animedia_meta.get("id"),
            "slug": animedia_meta.get("slug"),
            "filled": [int(ep["number"]) for ep in clean_animedia],
        }

    _log(
        "INFO",
        "Re-parse «{}» готово: удалено {}, добавлено {} "
        "(anilibria={}, yummyanime={}, animedia={}, диапазон 1..{})".format(
            report["title"],
            report["deleted"],
            report["added"],
            report["from_anilibria"],
            report["from_yummy"],
            report["from_animedia"],
            max(combined_numbers) if combined_numbers else 0,
        ),
    )

    # Per-episode source breakdown — written compactly so the admin log
    # stays readable for shows like One Piece (1000+ episodes).
    if report["from_yummy"]:
        nums = [str(n) for n in sorted(int(k) for k in report["sources"].keys()
                                       if report["sources"][k] == SOURCE_YUMMY)]
        head = ", ".join(nums[:30])
        more = "" if len(nums) <= 30 else " … +{} ещё".format(len(nums) - 30)
        _log(
            "INFO",
            "Re-parse «{}»: с YummyAnime взяты серии {}{}".format(
                report["title"], head, more
            ),
        )
    if report["from_animedia"]:
        nums = [str(n) for n in sorted(int(k) for k in report["sources"].keys()
                                       if report["sources"][k] == SOURCE_ANIMEDIA)]
        head = ", ".join(nums[:30])
        more = "" if len(nums) <= 30 else " … +{} ещё".format(len(nums) - 30)
        _log(
            "INFO",
            "Re-parse «{}»: с Animedia взяты серии {}{}".format(
                report["title"], head, more
            ),
        )

    if report["gaps"]:
        _log(
            "WARN",
            "Re-parse «{}»: пропуски в нумерации (ни Anilibria, ни YummyAnime, ни Animedia) — {}".format(
                report["title"],
                ", ".join(str(g) for g in report["gaps"][:20]),
            ),
        )
    if report["duplicates"]:
        _log(
            "WARN",
            "Re-parse «{}»: дубли номеров от источников (отфильтрованы) — {}".format(
                report["title"],
                ", ".join(str(g) for g in report["duplicates"][:20]),
            ),
        )
    _broadcast()
    return report


# --------------------------------------------------------------------------- #
#  Public control API                                                         #
# --------------------------------------------------------------------------- #

async def run_once() -> bool:
    """Kick off a catalogue import. YummyAnime is the primary episode source;
    Anilibria seeds the catalogue card list and fills gaps Yummy missed;
    Animedia is a final fallback. Returns False if a parse is already running.
    """
    if _run_lock.locked():
        return False

    async def _runner() -> None:
        async with _run_lock:
            _stop_event.clear()
            STATE.update(
                status="running",
                progress=0,
                processed=0,
                total=0,
                message="Подключение к Anilibria…",
                started_at=datetime.utcnow().isoformat(timespec="seconds"),
                finished_at=None,
            )
            _log(
                "INFO",
                "Старт парсера: ТОП-{} (proxy: {}, приоритет YummyAnime > Anilibria > Animedia)".format(
                    TOP_LIMIT, proxy_label()
                ),
            )
            _broadcast()

            try:
                async with make_async_client(
                    headers={"User-Agent": ANILIBRIA_UA, "Accept": "application/json"},
                ) as client:
                    catalog = await _fetch_anilibria_top(client, INITIAL_PARSE_LIMIT)
                    if not catalog:
                        _log("ERROR", "Не удалось получить релизы с Anilibria")
                        STATE.update(
                            status="error",
                            message="Не удалось получить релизы с Anilibria",
                        )
                        return

                    # Dedup by Anilibria id so a release that appears on
                    # multiple pages (rare during pagination drift) isn't
                    # processed twice in the same run.
                    seen_ids: set = set()
                    unique_catalog: List[Dict[str, Any]] = []
                    for it in catalog:
                        rid = it.get("id")
                        if rid in seen_ids:
                            continue
                        seen_ids.add(rid)
                        unique_catalog.append(it)
                    catalog = unique_catalog

                    STATE["total"] = len(catalog)
                    _log(
                        "INFO",
                        "Получено {} уникальных релизов с Anilibria (массовый парсинг)".format(
                            len(catalog)
                        ),
                    )
                    _broadcast()

                    for idx, summary_rec in enumerate(catalog, start=1):
                        if _stop_event.is_set():
                            _log("WARN", "Парсер остановлен оператором")
                            STATE.update(
                                status="stopped", message="Остановлено оператором"
                            )
                            return

                        title_ru, title_en, alt_bits = _release_titles(summary_rec)
                        STATE.update(
                            processed=idx - 1,
                            progress=int(((idx - 1) / max(1, len(catalog))) * 100),
                            message="[{}/{}] {}".format(idx, len(catalog), title_ru),
                        )
                        _broadcast()

                        # Catalogue payload doesn't include episodes — fetch detail.
                        rid = summary_rec.get("id")
                        detail = await _fetch_release_detail(client, rid) if rid else None
                        rec = detail or summary_rec

                        # Re-derive titles from the detail response (richer).
                        title_ru, title_en, alt_bits = _release_titles(rec)
                        poster, backdrop = _release_poster(rec)
                        episodes_payload = _release_episodes(rec)

                        kwargs = {
                            "title": title_ru,
                            "alternative_titles": ", ".join(
                                dict.fromkeys(a for a in alt_bits if a)
                            ),
                            "description": (rec.get("description") or "").strip()[:4000],
                            "poster_url": poster,
                            "backdrop_url": backdrop,
                            "genres": _release_genres(rec),
                            "year": _release_year(rec),
                            "status": _release_status(rec),
                            "rating": 0.0,
                            "anilibria_id": int(rec.get("id") or 0) or None,
                            "anilibria_code": (rec.get("alias") or "").strip(),
                            "series_group": _series_group_key(title_ru, title_en),
                        }

                        try:
                            extra = await _shikimori_enrich(
                                client, title_ru, title_en
                            )
                        except Exception:
                            extra = {}
                        if extra:
                            if extra.get("description") and len(extra["description"]) > len(kwargs["description"]):
                                kwargs["description"] = extra["description"]
                            if extra.get("rating"):
                                kwargs["rating"] = extra["rating"]
                            if extra.get("poster_url_alt") and not kwargs["poster_url"]:
                                kwargs["poster_url"] = extra["poster_url_alt"]
                            if extra.get("genres_alt") and not kwargs["genres"]:
                                kwargs["genres"] = extra["genres_alt"]

                        # YummyAnime (primary) and Animedia (last-resort) are
                        # fired in parallel. We pass an empty "existing" list
                        # to Yummy and an empty "covered" set to Animedia so
                        # both return their full episode catalogues; the
                        # priority dedup happens inside `_ensure_episodes`.
                        async def _yummy_call() -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
                            try:
                                return await _yummy_fill_gaps(
                                    client, title_ru, kwargs["year"], []
                                )
                            except PROXY_EXCEPTIONS as exc:
                                _log(
                                    "WARN",
                                    "YummyAnime прокси/сеть для «{}»: {}".format(
                                        title_ru, exc
                                    ),
                                )
                                return [], None
                            except Exception as exc:  # noqa: BLE001
                                _log(
                                    "WARN",
                                    "YummyAnime для «{}»: {}".format(title_ru, exc),
                                )
                                return [], None

                        async def _animedia_call() -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
                            try:
                                return await _animedia_fill_gaps(
                                    client, title_ru, kwargs["year"], set()
                                )
                            except PROXY_EXCEPTIONS as exc:
                                _log(
                                    "WARN",
                                    "Animedia прокси/сеть для «{}»: {}".format(
                                        title_ru, exc
                                    ),
                                )
                                return [], None
                            except Exception as exc:  # noqa: BLE001
                                _log(
                                    "WARN",
                                    "Animedia для «{}»: {}".format(title_ru, exc),
                                )
                                return [], None

                        (yummy_fill, yummy_meta), (
                            animedia_fill,
                            animedia_meta,
                        ) = await asyncio.gather(_yummy_call(), _animedia_call())

                        result = await _persist_anime(
                            kwargs,
                            episodes_payload=episodes_payload,
                            alias=kwargs["anilibria_code"],
                            yummy_payload=yummy_fill,
                            yummy_meta=yummy_meta,
                            animedia_payload=animedia_fill,
                            animedia_meta=animedia_meta,
                        )

                        ep_numbers = [e["number"] for e in episodes_payload]
                        ep_min = min(ep_numbers) if ep_numbers else 0
                        ep_max = max(ep_numbers) if ep_numbers else 0
                        _log(
                            "INFO",
                            "[OK] {}: yummyanime {}, anilibria {} ({}-{}), animedia {}, "
                            "сохранено {} строк (yummy +{}, anilibria +{}, animedia +{})".format(
                                title_ru,
                                len(yummy_fill),
                                len(episodes_payload),
                                ep_min,
                                ep_max,
                                len(animedia_fill),
                                result["added"],
                                result.get("from_yummy", 0),
                                result.get("from_anilibria", 0),
                                result.get("from_animedia", 0),
                            ),
                        )

                        STATE.update(
                            processed=idx,
                            progress=int((idx / max(1, len(catalog))) * 100),
                        )
                        _broadcast()
                        await asyncio.sleep(REQUEST_DELAY)

                STATE.update(
                    status="done",
                    progress=100,
                    message="Загрузка завершена: {}/{}".format(
                        STATE["processed"], STATE["total"]
                    ),
                )
                _log(
                    "INFO",
                    "Готово: импортировано {} аниме с Anilibria".format(
                        STATE["processed"]
                    ),
                )
                ensure_auto_update_started()

            except Exception as exc:  # pragma: no cover
                _log("ERROR", "Сбой парсера: {}".format(exc))
                STATE.update(status="error", message=str(exc))
            finally:
                STATE["finished_at"] = datetime.utcnow().isoformat(timespec="seconds")
                _broadcast()

    asyncio.create_task(_runner())
    return True


async def stop() -> bool:
    if not _run_lock.locked():
        return False
    _stop_event.set()
    _log("WARN", "Получен сигнал остановки парсера")
    return True


async def shutdown() -> None:
    global _auto_task, _auto_started
    _stop_event.set()
    if _auto_task and not _auto_task.done():
        _auto_task.cancel()
        try:
            await _auto_task
        except (asyncio.CancelledError, Exception):
            pass
    _auto_task = None
    _auto_started = False
    STATE["auto_update"] = False


# --------------------------------------------------------------------------- #
#  Persistence                                                                #
# --------------------------------------------------------------------------- #

async def _persist_anime(
    kwargs: Dict[str, Any],
    episodes_payload: List[Dict[str, Any]],
    alias: str,
    yummy_payload: Optional[List[Dict[str, Any]]] = None,
    yummy_meta: Optional[Dict[str, Any]] = None,
    animedia_payload: Optional[List[Dict[str, Any]]] = None,
    animedia_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, int]:
    async with SessionLocal() as session:
        anime: Optional[Anime] = None
        anilibria_id = kwargs.get("anilibria_id")
        if anilibria_id:
            row = await session.execute(
                select(Anime).where(Anime.anilibria_id == anilibria_id)
            )
            anime = row.scalar_one_or_none()
        if anime is None:
            row = await session.execute(
                select(Anime).where(Anime.title == kwargs["title"])
            )
            anime = row.scalar_one_or_none()

        if anime is None:
            anime = Anime(**kwargs)
            session.add(anime)
            await session.flush()
        else:
            for k, v in kwargs.items():
                if v not in (None, ""):
                    setattr(anime, k, v)

        result = await _ensure_episodes(
            session,
            anime,
            episodes_payload=episodes_payload,
            alias=alias,
            yummy_payload=yummy_payload,
            yummy_meta=yummy_meta,
            animedia_payload=animedia_payload,
            animedia_meta=animedia_meta,
        )
        await session.commit()
        return result


# --------------------------------------------------------------------------- #
#  Auto-update background loop                                                #
# --------------------------------------------------------------------------- #

def ensure_auto_update_started() -> None:
    global _auto_task, _auto_started
    if _auto_started and _auto_task and not _auto_task.done():
        return
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        return
    if not loop.is_running():
        return
    _auto_task = loop.create_task(_auto_update_loop())
    _auto_started = True
    STATE["auto_update"] = True
    _log("INFO", "Авто-обновление с Anilibria включено (каждые 30 минут)")


async def _auto_update_loop() -> None:
    while True:
        try:
            await asyncio.sleep(AUTO_UPDATE_INTERVAL)
            await _auto_update_round()
        except asyncio.CancelledError:
            return
        except Exception as exc:  # pragma: no cover
            _log("WARN", "auto-update round failed: {}".format(exc))


async def _auto_update_round() -> None:
    """Poll YummyAnime + Anilibria + Animedia in parallel and append any
    new episodes that have appeared since the last round.
    """
    if _run_lock.locked():
        return

    async with SessionLocal() as session:
        rows = await session.execute(
            select(Anime).order_by(desc(Anime.id)).limit(TOP_LIMIT)
        )
        anime_list = rows.scalars().all()

    if not anime_list:
        return

    _log(
        "INFO",
        "Авто-проверка (YummyAnime > Anilibria > Animedia, proxy: {}): {} тайтлов".format(
            proxy_label(), len(anime_list)
        ),
    )
    new_total = 0

    async with make_async_client(
        headers={"User-Agent": ANILIBRIA_UA, "Accept": "application/json"},
    ) as client:
        for anime in anime_list:

            async def _do_anilibria() -> Optional[Dict[str, Any]]:
                rec_local: Optional[Dict[str, Any]] = None
                try:
                    if anime.anilibria_id:
                        rec_local = await _fetch_release_detail(
                            client, anime.anilibria_id
                        )
                    if rec_local is None and anime.anilibria_code:
                        rec_local = await _fetch_release_detail(
                            client, anime.anilibria_code
                        )
                    if rec_local is None:
                        rec_local = await _anilibria_search(client, anime.title)
                        if rec_local and rec_local.get("id"):
                            rec_local = (
                                await _fetch_release_detail(client, rec_local["id"])
                                or rec_local
                            )
                except PROXY_EXCEPTIONS as exc:
                    _log(
                        "WARN",
                        "Auto-update Anilibria прокси/сеть «{}»: {}".format(
                            anime.title, exc
                        ),
                    )
                    return None
                except Exception as exc:  # noqa: BLE001
                    _log(
                        "WARN",
                        "Auto-update Anilibria «{}»: {}".format(anime.title, exc),
                    )
                    return None
                return rec_local

            async def _do_yummy() -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
                try:
                    return await _yummy_fill_gaps(
                        client, anime.title, int(anime.year or 0), []
                    )
                except PROXY_EXCEPTIONS as exc:
                    _log(
                        "WARN",
                        "Auto-update YummyAnime прокси/сеть «{}»: {}".format(
                            anime.title, exc
                        ),
                    )
                    return [], None
                except Exception as exc:  # noqa: BLE001
                    _log(
                        "WARN",
                        "Auto-update YummyAnime «{}»: {}".format(anime.title, exc),
                    )
                    return [], None

            async def _do_animedia() -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
                try:
                    return await _animedia_fill_gaps(
                        client, anime.title, int(anime.year or 0), set()
                    )
                except PROXY_EXCEPTIONS as exc:
                    _log(
                        "WARN",
                        "Auto-update Animedia прокси/сеть «{}»: {}".format(
                            anime.title, exc
                        ),
                    )
                    return [], None
                except Exception as exc:  # noqa: BLE001
                    _log(
                        "WARN",
                        "Auto-update Animedia «{}»: {}".format(anime.title, exc),
                    )
                    return [], None

            rec, (yummy_fill, yummy_meta), (
                animedia_fill,
                animedia_meta,
            ) = await asyncio.gather(_do_anilibria(), _do_yummy(), _do_animedia())

            episodes_payload = _release_episodes(rec) if rec else []
            alias = ""
            if rec:
                alias = (rec.get("alias") or anime.anilibria_code or "").strip()

            if not episodes_payload and not yummy_fill and not animedia_fill:
                continue

            async with SessionLocal() as s2:
                fresh = await s2.get(Anime, anime.id)
                if not fresh:
                    continue
                if not fresh.anilibria_id and rec.get("id"):
                    try:
                        fresh.anilibria_id = int(rec["id"])
                    except (TypeError, ValueError):
                        pass
                if alias and not fresh.anilibria_code:
                    fresh.anilibria_code = alias

                result = await _ensure_episodes(
                    s2,
                    fresh,
                    episodes_payload=episodes_payload,
                    alias=alias,
                    yummy_payload=yummy_fill,
                    yummy_meta=yummy_meta,
                    animedia_payload=animedia_fill,
                    animedia_meta=animedia_meta,
                )
                added = result["added"]
                if added:
                    await s2.commit()
                    new_total += added
                    suffix = ""
                    if result["from_yummy"]:
                        suffix += " (yummyanime: +{})".format(result["from_yummy"])
                    if result.get("from_animedia"):
                        suffix += " (animedia: +{})".format(result["from_animedia"])
                    _log(
                        "INFO",
                        "Новые серии для «{}»: +{} "
                        "(всего ани {} + ями {} + анимедия {}){}".format(
                            fresh.title,
                            added,
                            len(episodes_payload),
                            len(yummy_fill),
                            len(animedia_fill),
                            suffix,
                        ),
                    )
                else:
                    await s2.commit()
            await asyncio.sleep(0.15)

    STATE["last_auto_update"] = datetime.utcnow().isoformat(timespec="seconds")
    STATE["new_episodes_total"] = STATE.get("new_episodes_total", 0) + new_total
    if new_total:
        _log(
            "INFO",
            "Авто-проверка завершена: добавлено {} новых серий".format(new_total),
        )
    else:
        _log("INFO", "Авто-проверка завершена: новых серий не найдено")
    _broadcast()
