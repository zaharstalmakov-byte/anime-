"""Profile page + favorites listing. Python 3.8 compatible."""
from typing import Optional

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.security import get_current_user
from app.models.anime import Anime
from app.models.user import Favorite, User, WatchProgress
from database import get_session
from main_templates import templates

router = APIRouter()


@router.get("/profile", response_class=HTMLResponse)
async def profile(
    request: Request,
    session: AsyncSession = Depends(get_session),
    user: Optional[User] = Depends(get_current_user),
):
    if not user:
        return RedirectResponse("/login", status_code=303)

    fav_q = await session.execute(
        select(Anime)
        .join(Favorite, Favorite.anime_id == Anime.id)
        .where(Favorite.user_id == user.id)
        .order_by(Favorite.created_at.desc())
    )
    favorites = fav_q.scalars().all()

    progress_q = await session.execute(
        select(WatchProgress)
        .where(WatchProgress.user_id == user.id)
        .order_by(WatchProgress.updated_at.desc())
        .limit(8)
    )
    progress = progress_q.scalars().all()

    return templates.TemplateResponse(
        request,
        "profile.html",
        {
            "user": user,
            "favorites": favorites,
            "progress": progress,
            "active": "profile",
        },
    )
