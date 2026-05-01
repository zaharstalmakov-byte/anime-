"""JSON API + iframe player. Python 3.8 compatible."""
import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.security import get_current_user
from app.models.anime import Anime, Episode
from app.models.user import Favorite, User, WatchProgress
from app.services.video_provider import video_provider
from database import get_session
from main_templates import templates

logger = logging.getLogger("animeflow.api")

router = APIRouter(prefix="/api")


def _alt_titles(anime: Anime):
    if not anime.alternative_titles:
        return []
    return [
        a.strip()
        for a in anime.alternative_titles.replace(";", ",").split(",")
        if a.strip()
    ]


def _episode_video_url(episode: Episode) -> Optional[str]:
    """Pick the best stored URL for an episode without going to net.

    Honours ``episode.source``: rows that came from YummyAnime use the
    YummyAnime iframe first, rows that came from Animedia use the Animedia
    iframe first, while Anilibria rows use the direct HLS URLs.
    """
    if not episode:
        return None

    source = getattr(episode, "source", "") or "anilibria"
    yummy_iframe = getattr(episode, "yummy_iframe", "") or ""
    animedia_iframe = getattr(episode, "animedia_iframe", "") or ""

    if source == "yummyanime" and yummy_iframe:
        return yummy_iframe
    if source == "animedia" and animedia_iframe:
        return animedia_iframe

    # Prefer direct HD HLS, then full-HD, then SD; fall back to whatever the
    # parser wrote into ``video_url``, then to the Anilibria web embed, then
    # to a YummyAnime/Animedia iframe even on an "anilibria" row (rare —
    # happens only if the row was migrated mid-flight).
    for url in (
        episode.anilibria_hls_hd,
        episode.anilibria_hls_fhd,
        episode.anilibria_hls_sd,
    ):
        if url:
            return url
    if episode.video_url:
        return episode.video_url
    if episode.anilibria_iframe:
        return episode.anilibria_iframe
    if yummy_iframe:
        return yummy_iframe
    if animedia_iframe:
        return animedia_iframe
    return None


async def _resolve_video_url(
    session: AsyncSession,
    episode: Optional[Episode],
    anime: Anime,
    episode_number: int,
) -> Optional[str]:
    """Stored URL first, fall back to a live Anilibria lookup if missing."""
    direct = _episode_video_url(episode) if episode else None
    if direct:
        return direct
    return await video_provider.find_for_episode(
        anime.title,
        episode_number=episode_number,
        alt_titles=_alt_titles(anime),
        anilibria_id=anime.anilibria_id,
        anilibria_code=anime.anilibria_code or None,
    )


# ---------------------------------------------------------------------------
#  Iframe player — internal endpoint that the front-end <iframe> points at.
# ---------------------------------------------------------------------------

@router.get("/player", response_class=HTMLResponse)
async def player_page(
    request: Request,
    episode_id: Optional[int] = None,
    anime_id: Optional[int] = None,
    redirect: int = 0,
    session: AsyncSession = Depends(get_session),
):
    """Resolve a playable URL on demand and render the embed page.

    * ``episode_id`` (preferred) — uses the parent anime's title + the episode
      number to ask :class:`VideoProvider` for a working stream.
    * ``anime_id`` — falls back to episode #1.
    * If the caller passes ``redirect=1``, we 302 straight to the upstream URL
      instead of rendering our wrapper page (useful for opening sources in a
      new tab).
    """
    episode: Optional[Episode] = None
    anime: Optional[Anime] = None
    episode_number = 1

    if episode_id is not None:
        episode = await session.get(Episode, episode_id)
        if episode is None:
            raise HTTPException(status_code=404, detail="Episode not found")
        anime = await session.get(Anime, episode.anime_id)
        episode_number = episode.episode_number

    if anime is None and anime_id is not None:
        anime = await session.get(Anime, anime_id)
        if anime is None:
            raise HTTPException(status_code=404, detail="Anime not found")

    if anime is None:
        raise HTTPException(status_code=400, detail="Нужно episode_id или anime_id")

    video_url = await _resolve_video_url(session, episode, anime, episode_number)

    not_found_reason = None
    if not video_url:
        logger.info(
            "player miss anime=%r ep=%d", anime.title, episode_number,
        )
        not_found_reason = "Видео ещё не загружено."

    if video_url and redirect:
        return RedirectResponse(video_url, status_code=302)

    # Look up the next episode so the player can prefetch and offer
    # an auto-advance countdown.
    next_url = ""
    if episode is not None:
        next_q = await session.execute(
            select(Episode)
            .where(
                Episode.anime_id == anime.id,
                Episode.episode_number > episode.episode_number,
            )
            .order_by(Episode.episode_number.asc())
            .limit(1)
        )
        nxt = next_q.scalar_one_or_none()
        if nxt is not None:
            next_url = "/anime/{}/episode/{}".format(anime.id, nxt.episode_number)

    return templates.TemplateResponse(
        request,
        "player.html",
        {
            "video_url": video_url,
            "title": anime.title,
            "episode_number": episode_number,
            "anime_id": anime.id,
            "episode_id": episode.id if episode else None,
            "not_found_reason": not_found_reason,
            "next_url": next_url,
            "intro_start": 0,
            "intro_end": 90,
            "outro_start": 0,
            "outro_end": 0,
        },
    )


@router.get("/player/data")
async def player_data(
    episode_id: int,
    session: AsyncSession = Depends(get_session),
):
    """JSON payload used by the SPA brick-grid to switch episodes in place."""
    episode = await session.get(Episode, episode_id)
    if episode is None:
        raise HTTPException(status_code=404, detail="Episode not found")
    anime = await session.get(Anime, episode.anime_id)
    if anime is None:
        raise HTTPException(status_code=404, detail="Anime not found")

    video_url = await _resolve_video_url(
        session, episode, anime, episode.episode_number
    )
    kind = "none"
    if video_url:
        if ".m3u8" in video_url:
            kind = "hls"
        else:
            kind = "iframe"
    return {
        "episode_id": episode.id,
        "anime_id": anime.id,
        "episode_number": episode.episode_number,
        "title": episode.title,
        "anime_title": anime.title,
        "video_url": video_url,
        "kind": kind,
        "iframe_url": "/api/player?episode_id={}&anime_id={}".format(
            episode.id, anime.id
        ),
        "page_url": "/anime/{}/episode/{}".format(anime.id, episode.episode_number),
    }


@router.get("/iframe")
async def resolve_iframe(
    anime_id: Optional[int] = None,
    episode_number: int = 1,
    title: Optional[str] = None,
    session: AsyncSession = Depends(get_session),
):
    chosen_title: Optional[str] = title
    alts: list = []
    anilibria_id: Optional[int] = None
    anilibria_code: Optional[str] = None
    if anime_id is not None:
        anime = await session.get(Anime, anime_id)
        if anime is None:
            raise HTTPException(status_code=404, detail="Аниме не найдено")
        chosen_title = anime.title
        alts = _alt_titles(anime)
        anilibria_id = anime.anilibria_id
        anilibria_code = anime.anilibria_code or None
    if not chosen_title:
        raise HTTPException(status_code=400, detail="Нужно anime_id или title")

    url = await video_provider.find_for_episode(
        chosen_title,
        episode_number=episode_number,
        alt_titles=alts,
        anilibria_id=anilibria_id,
        anilibria_code=anilibria_code,
    )
    return {"url": url, "title": chosen_title, "episode_number": episode_number}


@router.get("/healthz")
async def healthz():
    return {"status": "ok"}


@router.get("/search")
async def live_search(
    q: str = "",
    session: AsyncSession = Depends(get_session),
):
    q = q.strip().lower()
    if not q:
        return {"results": []}
    result = await session.execute(select(Anime))
    matched = []
    for a in result.scalars().all():
        haystack = "{} {}".format(a.title or "", a.alternative_titles or "").lower()
        if q in haystack:
            matched.append(a)
            if len(matched) >= 8:
                break
    return {
        "results": [
            {"id": a.id, "title": a.title, "year": a.year, "poster_url": a.poster_url}
            for a in matched
        ]
    }


class FavoriteToggle(BaseModel):
    anime_id: int


@router.post("/favorites/toggle")
async def toggle_favorite(
    payload: FavoriteToggle,
    session: AsyncSession = Depends(get_session),
    user: Optional[User] = Depends(get_current_user),
):
    if not user:
        raise HTTPException(status_code=401, detail="Войдите в аккаунт")

    existing = await session.execute(
        select(Favorite).where(
            Favorite.user_id == user.id, Favorite.anime_id == payload.anime_id
        )
    )
    fav = existing.scalar_one_or_none()
    if fav:
        await session.delete(fav)
        await session.commit()
        return {"favorited": False}

    session.add(Favorite(user_id=user.id, anime_id=payload.anime_id))
    await session.commit()
    return {"favorited": True}


class ProgressPayload(BaseModel):
    episode_id: int
    timestamp: float
    duration: float


@router.post("/progress")
async def save_progress(
    payload: ProgressPayload,
    session: AsyncSession = Depends(get_session),
    user: Optional[User] = Depends(get_current_user),
):
    if not user:
        return {"saved": False}

    result = await session.execute(
        select(WatchProgress).where(
            WatchProgress.user_id == user.id,
            WatchProgress.episode_id == payload.episode_id,
        )
    )
    progress = result.scalar_one_or_none()
    if progress:
        progress.timestamp = payload.timestamp
        progress.duration = payload.duration
    else:
        progress = WatchProgress(
            user_id=user.id,
            episode_id=payload.episode_id,
            timestamp=payload.timestamp,
            duration=payload.duration,
        )
        session.add(progress)
    await session.commit()
    return {"saved": True}


@router.get("/episode/{episode_id}")
async def episode_data(
    episode_id: int,
    session: AsyncSession = Depends(get_session),
):
    result = await session.execute(select(Episode).where(Episode.id == episode_id))
    ep = result.scalar_one_or_none()
    if not ep:
        raise HTTPException(status_code=404, detail="Episode not found")
    return {
        "id": ep.id,
        "anime_id": ep.anime_id,
        "episode_number": ep.episode_number,
        "title": ep.title,
        "video_url": ep.video_url,
    }


@router.get("/episode/{episode_id}/progress")
async def episode_progress(
    episode_id: int,
    session: AsyncSession = Depends(get_session),
    user: Optional[User] = Depends(get_current_user),
):
    if not user:
        return {"timestamp": 0, "duration": 0}

    result = await session.execute(
        select(WatchProgress).where(
            WatchProgress.user_id == user.id,
            WatchProgress.episode_id == episode_id,
        )
    )
    wp = result.scalar_one_or_none()
    if not wp:
        return {"timestamp": 0, "duration": 0}
    return {"timestamp": wp.timestamp, "duration": wp.duration}


@router.get("/anime/{anime_id}/progress")
async def anime_progress_summary(
    anime_id: int,
    session: AsyncSession = Depends(get_session),
    user: Optional[User] = Depends(get_current_user),
):
    """Return per-episode watch progress for the current user.

    Used by the episode grid to mark watched/in-progress episodes.
    Anonymous users get an empty list — no error.
    """
    if not user:
        return {"items": []}

    rows = await session.execute(
        select(WatchProgress, Episode)
        .join(Episode, Episode.id == WatchProgress.episode_id)
        .where(
            WatchProgress.user_id == user.id,
            Episode.anime_id == anime_id,
        )
    )
    items = []
    for wp, ep in rows.all():
        if not wp.duration:
            continue
        percent = int(min(100, max(0, (wp.timestamp / wp.duration) * 100)))
        items.append({
            "episode_id": ep.id,
            "episode_number": ep.episode_number,
            "percent": percent,
            "watched": percent >= 90,
        })
    return {"items": items}


# ---------------------------------------------------------------------------
#  Lightweight client analytics — structured server log only, no DB writes.
# ---------------------------------------------------------------------------

_analytics_logger = logging.getLogger("animeflow.analytics")

_ALLOWED_EVENTS = {
    "play", "pause", "ended", "exit",
    "seek", "quality_change", "share", "fullscreen",
    "error", "double_tap_back", "double_tap_fwd",
}


class AnalyticsEvent(BaseModel):
    event: str
    anime_id: Optional[int] = None
    episode_id: Optional[int] = None
    episode_number: Optional[int] = None
    timestamp: Optional[float] = None
    duration: Optional[float] = None
    detail: Optional[str] = None


@router.post("/analytics/event")
async def analytics_event(
    payload: AnalyticsEvent,
    request: Request,
    user: Optional[User] = Depends(get_current_user),
):
    """Record a player event for analytics. Best-effort, never blocks."""
    if payload.event not in _ALLOWED_EVENTS:
        raise HTTPException(status_code=400, detail="unknown event")
    user_part = "u={}".format(user.id) if user else "anon"
    pos = ""
    if payload.timestamp is not None and payload.duration:
        try:
            pct = int(min(100, max(0, (payload.timestamp / payload.duration) * 100)))
            pos = " pct={} t={:.1f}/{:.1f}".format(pct, payload.timestamp, payload.duration)
        except Exception:  # noqa: BLE001
            pass
    detail = " detail={!r}".format(payload.detail) if payload.detail else ""
    _analytics_logger.info(
        "ev=%s anime=%s ep=%s epnum=%s %s%s%s",
        payload.event,
        payload.anime_id,
        payload.episode_id,
        payload.episode_number,
        user_part,
        pos,
        detail,
    )
    return {"ok": True}
