"""Home page route + resume-watching feed + pagination. Python 3.8 compatible."""
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.security import get_current_user
from app.models.anime import Anime, Episode
from app.models.user import User, WatchProgress
from database import get_session
from main_templates import templates

router = APIRouter()

PER_PAGE = 24


async def _resume_items(
    session: AsyncSession,
    user: Optional[User],
    limit: int = 8,
) -> List[Dict[str, Any]]:
    if not user:
        return []
    q = await session.execute(
        select(WatchProgress, Episode, Anime)
        .join(Episode, Episode.id == WatchProgress.episode_id)
        .join(Anime, Anime.id == Episode.anime_id)
        .where(
            WatchProgress.user_id == user.id,
            WatchProgress.duration > 0,
            WatchProgress.timestamp > 60,
            WatchProgress.timestamp < WatchProgress.duration * 0.9,
        )
        .order_by(desc(WatchProgress.updated_at))
        .limit(limit)
    )
    rows = q.all()
    return [
        {
            "anime": a,
            "episode": e,
            "timestamp": p.timestamp,
            "duration": p.duration,
            "percent": int(min(100, (p.timestamp / p.duration) * 100)) if p.duration else 0,
        }
        for (p, e, a) in rows
    ]


def _page_window(page: int, total_pages: int, span: int = 2) -> List[int]:
    """Return a small list of page numbers around ``page`` for the pager UI."""
    if total_pages <= 1:
        return [1]
    lo = max(1, page - span)
    hi = min(total_pages, page + span)
    return list(range(lo, hi + 1))


@router.get("/", response_class=HTMLResponse)
async def home(
    request: Request,
    page: int = Query(1, ge=1),
    session: AsyncSession = Depends(get_session),
    user: Optional[User] = Depends(get_current_user),
):
    total_q = await session.execute(select(func.count()).select_from(Anime))
    total = int(total_q.scalar() or 0)
    total_pages = max(1, (total + PER_PAGE - 1) // PER_PAGE)
    page = min(max(1, page), total_pages)
    offset = (page - 1) * PER_PAGE

    result = await session.execute(
        select(Anime)
        .order_by(Anime.rating.desc(), Anime.id.desc())
        .offset(offset)
        .limit(PER_PAGE)
    )
    anime_list = result.scalars().all()
    resume = await _resume_items(session, user)

    pagination = {
        "page": page,
        "total_pages": total_pages,
        "total": total,
        "has_prev": page > 1,
        "has_next": page < total_pages,
        "prev_page": max(1, page - 1),
        "next_page": min(total_pages, page + 1),
        "pages": _page_window(page, total_pages),
        "first_visible": (page - 1) * PER_PAGE + (1 if anime_list else 0),
        "last_visible": (page - 1) * PER_PAGE + len(anime_list),
    }

    return templates.TemplateResponse(
        request,
        "home.html",
        {
            "anime_list": anime_list,
            "user": user,
            "active": "home",
            "resume": resume,
            "pagination": pagination,
        },
    )


@router.get("/random", response_class=HTMLResponse)
async def random_anime(session: AsyncSession = Depends(get_session)):
    result = await session.execute(select(Anime).order_by(func.random()).limit(1))
    anime = result.scalar_one_or_none()
    if not anime:
        return RedirectResponse("/", status_code=303)
    return RedirectResponse("/anime/{}".format(anime.id), status_code=303)
