"""Anime detail + episode pages with reviews + recommendations + resume. Python 3.8 compatible."""
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.auth.security import get_current_user
from app.models.anime import Anime, Episode
from app.models.user import Favorite, Review, User, WatchProgress
from database import get_session
from main_templates import templates

router = APIRouter()


@router.get("/anime/{anime_id}", response_class=HTMLResponse)
async def anime_detail(
    anime_id: int,
    request: Request,
    session: AsyncSession = Depends(get_session),
    user: Optional[User] = Depends(get_current_user),
):
    return await _render_anime(anime_id, None, request, session, user)


@router.get("/anime/{anime_id}/episode/{episode_number}", response_class=HTMLResponse)
async def anime_episode(
    anime_id: int,
    episode_number: int,
    request: Request,
    session: AsyncSession = Depends(get_session),
    user: Optional[User] = Depends(get_current_user),
):
    return await _render_anime(anime_id, episode_number, request, session, user)


async def _recommendations(session: AsyncSession, anime: Anime, limit: int = 4) -> List[Anime]:
    """Return up to N anime sharing the most genres with `anime`."""
    genres = anime.genre_list
    if not genres:
        # fall back to top-rated other titles
        q = await session.execute(
            select(Anime)
            .where(Anime.id != anime.id)
            .order_by(desc(Anime.rating))
            .limit(limit)
        )
        return list(q.scalars().all())

    q = await session.execute(select(Anime).where(Anime.id != anime.id))
    others = q.scalars().all()
    target = set(g.lower() for g in genres)
    scored = []
    for o in others:
        overlap = len(target & set(g.lower() for g in o.genre_list))
        if overlap > 0:
            scored.append((overlap, o.rating or 0.0, o))
    scored.sort(key=lambda t: (-t[0], -t[1]))
    return [s[2] for s in scored[:limit]]


async def _render_anime(
    anime_id: int,
    episode_number: Optional[int],
    request: Request,
    session: AsyncSession,
    user: Optional[User],
):
    result = await session.execute(
        select(Anime).options(selectinload(Anime.episodes)).where(Anime.id == anime_id)
    )
    anime = result.scalar_one_or_none()
    if not anime:
        raise HTTPException(status_code=404, detail="Anime not found")

    episodes = sorted(anime.episodes, key=lambda e: e.episode_number)
    if not episodes:
        current, next_ep, prev_ep = None, None, None
    else:
        current = next(
            (e for e in episodes if e.episode_number == episode_number),
            episodes[0],
        )
        idx = episodes.index(current)
        prev_ep = episodes[idx - 1] if idx > 0 else None
        next_ep = episodes[idx + 1] if idx < len(episodes) - 1 else None

    is_favorite = False
    resume_progress = None
    if user:
        fav_q = await session.execute(
            select(Favorite).where(Favorite.user_id == user.id, Favorite.anime_id == anime.id)
        )
        is_favorite = fav_q.scalar_one_or_none() is not None

        if current:
            prog_q = await session.execute(
                select(WatchProgress).where(
                    WatchProgress.user_id == user.id,
                    WatchProgress.episode_id == current.id,
                )
            )
            wp = prog_q.scalar_one_or_none()
            if (
                wp
                and wp.duration > 0
                and wp.timestamp > 60
                and wp.timestamp < wp.duration * 0.9
            ):
                secs_total = int(wp.timestamp)
                mm, ss = divmod(secs_total, 60)
                hh, mm = divmod(mm, 60)
                if hh:
                    time_str = "{}:{:02d}:{:02d}".format(hh, mm, ss)
                else:
                    time_str = "{}:{:02d}".format(mm, ss)
                resume_progress = {
                    "timestamp": wp.timestamp,
                    "duration": wp.duration,
                    "percent": int((wp.timestamp / wp.duration) * 100),
                    "time_str": time_str,
                }

    # Reviews (joined with user)
    rev_q = await session.execute(
        select(Review)
        .where(Review.anime_id == anime.id)
        .order_by(desc(Review.timestamp))
    )
    reviews = rev_q.scalars().all()

    recommendations = await _recommendations(session, anime, limit=4)

    # Franchise chronology — every release that shares the same series_group,
    # sorted strictly by release year (oldest → newest). Movies get the
    # "Фильм" label, everything else is numbered as 1/2/3 сезон.
    chronology = []
    if anime.series_group:
        sg_q = await session.execute(
            select(Anime)
            .where(Anime.series_group == anime.series_group)
            .order_by(Anime.year, Anime.id)
        )
        siblings = sg_q.scalars().all()
        if len(siblings) > 1:
            season_idx = 0
            for s in siblings:
                t = (s.title or "").lower()
                if "фильм" in t or "movie" in t or "film" in t:
                    label = "Фильм"
                else:
                    season_idx += 1
                    label = "{} сезон".format(season_idx)
                chronology.append({
                    "id": s.id,
                    "year": s.year,
                    "title": s.title,
                    "label": label,
                })

    return templates.TemplateResponse(
        request,
        "anime.html",
        {
            "anime": anime,
            "episodes": episodes,
            "current": current,
            "next_ep": next_ep,
            "prev_ep": prev_ep,
            "is_favorite": is_favorite,
            "user": user,
            "active": "home",
            "reviews": reviews,
            "recommendations": recommendations,
            "resume_progress": resume_progress,
            "chronology": chronology,
        },
    )
