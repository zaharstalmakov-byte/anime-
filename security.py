"""Password hashing + session-based auth helpers. Python 3.8 compatible."""
from typing import Optional

from fastapi import Depends, Request
from passlib.context import CryptContext
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.user import User
from database import get_session

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain: str, hashed: str) -> bool:
    try:
        return pwd_context.verify(plain, hashed)
    except Exception:
        return False


async def get_current_user(
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> Optional[User]:
    user_id = request.session.get("user_id")
    if not user_id:
        return None
    result = await session.execute(select(User).where(User.id == user_id))
    return result.scalar_one_or_none()
