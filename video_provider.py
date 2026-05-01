"""VideoProvider — per-episode video resolver. Python 3.8 compatible.

Anilibria-first, key-less. The parser stores Anilibria HLS URLs on every
``Episode`` row at parse time, so most lookups never need to hit the network.
This resolver only goes to the Anilibria API when the DB has no usable URL
(e.g. for a manually-created Anime row).
"""
import asyncio
import logging
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import httpx

logger = logging.getLogger("animeflow.video_provider")

ANILIBRIA_HOSTS = ("anilibria.top", "api.anilibria.app")
ANILIBRIA_WEB = "https://anilibria.top"

_DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_FORBIDDEN_HOST_RE = re.compile(r"(kodik|alloha|jut\.su|jutsu)", re.IGNORECASE)


def _is_forbidden(url: Optional[str]) -> bool:
    return bool(url and _FORBIDDEN_HOST_RE.search(url))


def _pick_episode(episodes: List[Dict[str, Any]], wanted: int) -> Optional[Dict[str, Any]]:
    if not episodes:
        return None
    for ep in episodes:
        if not isinstance(ep, dict):
            continue
        try:
            if int(ep.get("ordinal") or 0) == wanted:
                return ep
        except (TypeError, ValueError):
            continue
    return None


def _episode_to_url(ep: Dict[str, Any]) -> Optional[str]:
    for key in ("hls_720", "hls_1080", "hls_480"):
        url = ep.get(key)
        if url:
            return url
    return None


class VideoProvider:
    """Resolves (anime title + episode number) to an Anilibria HLS URL."""

    _NORMALIZE_DROP = re.compile(r"[^\w\s\-]+", re.UNICODE)
    _NORMALIZE_SPACES = re.compile(r"\s+", re.UNICODE)

    def __init__(
        self,
        timeout: float = 10.0,
        retries: int = 1,
        delay: float = 0.3,
        cache_ttl: int = 1800,
    ) -> None:
        self.timeout = timeout
        self.retries = max(0, retries)
        self.delay = max(0.0, delay)
        self.cache_ttl = max(0, cache_ttl)
        self._cache: Dict[str, Tuple[float, Optional[str]]] = {}
        self._lock = asyncio.Lock()

    async def find_for_episode(
        self,
        title: str,
        episode_number: int,
        alt_titles: Optional[List[str]] = None,
        anilibria_id: Optional[int] = None,
        anilibria_code: Optional[str] = None,
    ) -> Optional[str]:
        if episode_number <= 0:
            episode_number = 1
        candidates = self._build_candidates(title, alt_titles)

        cache_key = "ep{}::{}::{}::{}".format(
            episode_number, anilibria_id or 0, anilibria_code or "",
            "||".join(candidates),
        )
        async with self._lock:
            cached = self._cache_get(cache_key)
            if cached is not None:
                _, value = cached
                return value

        headers = {
            "User-Agent": _DEFAULT_UA,
            "Accept-Language": "ru,en;q=0.9",
            "Accept": "application/json,*/*",
        }
        timeout_cfg = httpx.Timeout(self.timeout, connect=self.timeout)

        async with httpx.AsyncClient(
            timeout=timeout_cfg,
            headers=headers,
            follow_redirects=True,
            http2=False,
        ) as client:
            url: Optional[str] = None
            if anilibria_id:
                url = await self._fetch_by_release(client, anilibria_id, episode_number)
            if url is None and anilibria_code:
                url = await self._fetch_by_release(client, anilibria_code, episode_number)
            if url is None:
                for cand in candidates:
                    rid = await self._search_release_id(client, cand)
                    if rid:
                        url = await self._fetch_by_release(client, rid, episode_number)
                        if url:
                            break
                    if self.delay:
                        await asyncio.sleep(self.delay)
            if url is None and anilibria_code:
                url = "{}/anime/releases/release/{}/episodes".format(
                    ANILIBRIA_WEB, anilibria_code
                )

        if url and _is_forbidden(url):
            url = None

        async with self._lock:
            self._cache_set(cache_key, url)
        return url

    def clear_cache(self) -> None:
        self._cache.clear()

    # ----- internals -------------------------------------------------------

    async def _anilibria_get(
        self,
        client: httpx.AsyncClient,
        path: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Optional[Any]:
        for host in ANILIBRIA_HOSTS:
            url = "https://{}/api/v1{}".format(host, path)
            try:
                resp = await client.get(url, params=params)
            except httpx.HTTPError:
                continue
            if resp.status_code != 200:
                continue
            try:
                return resp.json()
            except ValueError:
                continue
        return None

    async def _fetch_by_release(
        self, client: httpx.AsyncClient, release_id_or_alias, episode_number: int
    ) -> Optional[str]:
        data = await self._anilibria_get(
            client, "/anime/releases/{}".format(release_id_or_alias)
        )
        if not isinstance(data, dict):
            return None
        ep = _pick_episode(data.get("episodes") or [], episode_number)
        if ep:
            url = _episode_to_url(ep)
            if url:
                return url
        alias = (data.get("alias") or "").strip()
        if alias:
            return "{}/anime/releases/release/{}/episodes".format(
                ANILIBRIA_WEB, alias
            )
        return None

    async def _search_release_id(
        self, client: httpx.AsyncClient, query: str
    ) -> Optional[Any]:
        if not query:
            return None
        data = await self._anilibria_get(
            client, "/app/search/releases", {"query": query, "limit": 1}
        )
        if isinstance(data, list) and data and isinstance(data[0], dict):
            return data[0].get("id")
        return None

    def _build_candidates(self, title, alt_titles):
        raw: List[str] = [title or ""]
        if alt_titles:
            raw.extend([t for t in alt_titles if t])
        out: List[str] = []
        seen: set = set()
        for t in raw:
            for variant in (t, self._normalize(t), self._normalize_ascii(t)):
                v = (variant or "").strip()
                if v and v.lower() not in seen:
                    seen.add(v.lower())
                    out.append(v)
        return out

    @classmethod
    def _normalize(cls, title):
        s = (title or "").strip().lower()
        s = cls._NORMALIZE_DROP.sub(" ", s)
        s = cls._NORMALIZE_SPACES.sub(" ", s)
        return s.strip()

    @classmethod
    def _normalize_ascii(cls, title):
        s = cls._normalize(title)
        s = re.sub(r"[^a-z0-9\s-]", "", s)
        s = cls._NORMALIZE_SPACES.sub(" ", s).strip()
        return s

    def _cache_get(self, key):
        item = self._cache.get(key)
        if not item:
            return None
        ts, value = item
        if self.cache_ttl and (time.time() - ts) > self.cache_ttl:
            self._cache.pop(key, None)
            return None
        return (value is not None, value)

    def _cache_set(self, key, value):
        self._cache[key] = (time.time(), value)


video_provider = VideoProvider()
