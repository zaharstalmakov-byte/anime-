"""Review create + delete (own review). Python 3.8 compatible."""
from typing import Optional

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.security import get_current_user
from app.models.anime import Anime
from app.models.user import Review, User
from database import get_session

router = APIRouter()


@router.post("/anime/{anime_id}/review")
async def post_review(
    anime_id: int,
    request: Request,
    text: str = Form(""),
    rating: float = Form(0.0),
    session: AsyncSession = Depends(get_session),
    user: Optional[User] = Depends(get_current_user),
):
    if not user:
        return RedirectResponse("/login", status_code=303)

    anime = await session.get(Anime, anime_id)
    if not anime:
        raise HTTPException(status_code=404, detail="Anime not found")

    text = (text or "").strip()
    if not text:
        return RedirectResponse("/anime/{}".format(anime_id), status_code=303)

    rating = max(0.0, min(10.0, float(rating or 0)))

    review = Review(user_id=user.id, anime_id=anime_id, text=text, rating=rating)
    session.add(review)
    await session.commit()
    return RedirectResponse("/anime/{}#reviews".format(anime_id), status_code=303)


@router.post("/review/{review_id}/delete")
async def delete_own_review(
    review_id: int,
    session: AsyncSession = Depends(get_session),
    user: Optional[User] = Depends(get_current_user),
):
    if not user:
        raise HTTPException(status_code=401)
    review = await session.get(Review, review_id)
    if not review:
        raise HTTPException(status_code=404)
    if review.user_id != user.id and user.id != 1:
        raise HTTPException(status_code=403)
    anime_id = review.anime_id
    await session.delete(review)
    await session.commit()
    return RedirectResponse("/anime/{}#reviews".format(anime_id), status_code=303)
