"""Search page route (full-page results). Python 3.8 compatible.

Performs a Unicode-friendly substring match across title and alternative_titles
(the alternative_titles column lets us find e.g. "Ван Пис" by typing "ван").
SQLite's LIKE/ILIKE is ASCII-only for case folding, so we lowercase in Python.
"""
from typing import List, Optional

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.security import get_current_user
from app.models.anime import Anime
from app.models.user import User
from database import get_session
from main_templates import templates

router = APIRouter()


@router.get("/search", response_class=HTMLResponse)
async def search(
    request: Request,
    q: str = "",
    session: AsyncSession = Depends(get_session),
    user: Optional[User] = Depends(get_current_user),
):
    q_raw = q.strip()
    q_low = q_raw.lower()
    anime_list: List[Anime] = []
    if q_low:
        result = await session.execute(select(Anime))
        for a in result.scalars().all():
            haystack = "{} {}".format(a.title or "", a.alternative_titles or "").lower()
            if q_low in haystack:
                anime_list.append(a)

    return templates.TemplateResponse(
        request,
        "home.html",
        {
            "anime_list": anime_list,
            "user": user,
            "active": "search",
            "page_title": "Поиск: {}".format(q_raw) if q_raw else "Поиск",
            "search_query": q_raw,
        },
    )
