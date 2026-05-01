"""Catalog page with multi-select genre filter + debounced search.

Server-rendered on first hit, then incrementally updated client-side via the
JSON endpoint ``/api/catalog/filter``. Filters use AND semantics across
selected genres (an anime must contain *every* selected genre to match), plus
a Unicode-friendly substring match against title/alternative_titles.

Python 3.8 compatible.
"""
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.security import get_current_user
from app.models.anime import Anime
from app.models.user import User
from database import get_session
from main_templates import templates

router = APIRouter()


def _all_genres(animes: List[Anime]) -> List[str]:
    """Distinct sorted list of every genre that appears across the catalog."""
    seen = {}
    for a in animes:
        for g in a.genre_list:
            key = g.strip()
            if not key:
                continue
            seen.setdefault(key.lower(), key)
    return sorted(seen.values(), key=lambda s: s.lower())


def _match(a: Anime, wanted: List[str], q_low: str) -> bool:
    if q_low:
        haystack = "{} {}".format(a.title or "", a.alternative_titles or "").lower()
        if q_low not in haystack:
            return False
    if wanted:
        have = set(g.lower() for g in a.genre_list)
        for w in wanted:
            if w.lower() not in have:
                return False
    return True


def _serialize(a: Anime) -> Dict[str, Any]:
    return {
        "id": a.id,
        "title": a.title,
        "year": a.year,
        "status": a.status,
        "rating": a.rating or 0.0,
        "poster_url": a.poster_url or "",
        "genres": a.genre_list,
    }


@router.get("/catalog", response_class=HTMLResponse)
async def catalog_page(
    request: Request,
    session: AsyncSession = Depends(get_session),
    user: Optional[User] = Depends(get_current_user),
):
    result = await session.execute(select(Anime).order_by(Anime.rating.desc(), Anime.id.desc()))
    animes = result.scalars().all()
    return templates.TemplateResponse(
        request,
        "catalog.html",
        {
            "user": user,
            "active": "catalog",
            "anime_list": animes,
            "all_genres": _all_genres(animes),
            "total": len(animes),
        },
    )


@router.get("/api/catalog/filter")
async def catalog_filter(
    genres: str = Query("", description="Comma-separated genre names, AND logic"),
    q: str = Query("", description="Substring match against title"),
    limit: int = Query(200, ge=1, le=1000),
    session: AsyncSession = Depends(get_session),
):
    wanted = [g.strip() for g in genres.split(",") if g.strip()]
    q_low = q.strip().lower()

    result = await session.execute(select(Anime).order_by(Anime.rating.desc(), Anime.id.desc()))
    matched = [a for a in result.scalars().all() if _match(a, wanted, q_low)]

    return JSONResponse({
        "total": len(matched),
        "items": [_serialize(a) for a in matched[:limit]],
    })
