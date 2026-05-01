"""Register / login / logout routes. Python 3.8 compatible."""
import re
from typing import Optional

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.security import (
    get_current_user,
    hash_password,
    verify_password,
)
from app.models.user import User
from database import get_session
from main_templates import templates

router = APIRouter()

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, user: Optional[User] = Depends(get_current_user)):
    if user:
        return RedirectResponse("/profile", status_code=303)
    return templates.TemplateResponse(
        request,
        "login.html",
        {"user": user, "error": None, "active": "login"},
    )


@router.post("/login", response_class=HTMLResponse)
async def login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    session: AsyncSession = Depends(get_session),
):
    username = username.strip()
    error: Optional[str] = None
    if not username or not password:
        error = "Заполните все поля"

    user = None
    if not error:
        result = await session.execute(
            select(User).where(
                (User.username == username) | (User.email == username)
            )
        )
        user = result.scalar_one_or_none()
        if not user or not verify_password(password, user.password_hash):
            error = "Неверный логин или пароль"

    if error:
        return templates.TemplateResponse(
            request,
            "login.html",
            {"user": None, "error": error, "active": "login"},
            status_code=400,
        )

    request.session["user_id"] = user.id
    return RedirectResponse("/profile", status_code=303)


@router.get("/register", response_class=HTMLResponse)
async def register_page(request: Request, user: Optional[User] = Depends(get_current_user)):
    if user:
        return RedirectResponse("/profile", status_code=303)
    return templates.TemplateResponse(
        request,
        "register.html",
        {"user": user, "error": None, "active": "register"},
    )


@router.post("/register", response_class=HTMLResponse)
async def register_submit(
    request: Request,
    username: str = Form(...),
    email: str = Form(...),
    password: str = Form(...),
    session: AsyncSession = Depends(get_session),
):
    username = username.strip()
    email = email.strip().lower()
    error: Optional[str] = None

    if not username or not email or not password:
        error = "Заполните все поля"
    elif not EMAIL_RE.match(email):
        error = "Неверный формат email"
    elif len(password) < 6:
        error = "Пароль должен быть не короче 6 символов"
    elif len(username) < 3:
        error = "Имя пользователя должно быть не короче 3 символов"

    if not error:
        existing = await session.execute(
            select(User).where(
                (User.username == username) | (User.email == email)
            )
        )
        if existing.scalar_one_or_none():
            error = "Такой пользователь уже существует"

    if error:
        return templates.TemplateResponse(
            request,
            "register.html",
            {"user": None, "error": error, "active": "register"},
            status_code=400,
        )

    # First registered user automatically becomes admin
    user_count = await session.scalar(select(func.count()).select_from(User))
    role = "admin" if (user_count or 0) == 0 else "user"

    user = User(
        username=username,
        email=email,
        password_hash=hash_password(password),
        role=role,
    )
    session.add(user)
    await session.commit()
    request.session["user_id"] = user.id
    return RedirectResponse("/profile", status_code=303)


@router.post("/logout")
@router.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/", status_code=303)
