"""Admin panel: ad management + parser control + review moderation + WS logs.

Python 3.8 compatible.

Access policy: admin features are exposed ONLY to the very first registered
user (user.id == 1). For everyone else the routes return 404 so the panel is
completely invisible.
"""
import asyncio
from typing import Optional

from fastapi import (
    APIRouter,
    Depends,
    Form,
    HTTPException,
    Query,
    Request,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.auth.security import get_current_user
from app.models.settings import Setting
from app.models.user import Review, User
from app.services import parser as parser_service
from database import SessionLocal, get_session
from main_templates import templates

router = APIRouter()


def _require_admin(user: Optional[User]) -> User:
    if not user or user.id != 1:
        raise HTTPException(status_code=404)
    return user


async def _get_setting(session: AsyncSession, key: str, default: str = "") -> str:
    row = await session.get(Setting, key)
    return row.value if row else default


async def _set_setting(session: AsyncSession, key: str, value: str) -> None:
    row = await session.get(Setting, key)
    if row:
        row.value = value
    else:
        session.add(Setting(key=key, value=value))
    await session.commit()


@router.get("/admin", response_class=HTMLResponse)
async def admin_page(
    request: Request,
    session: AsyncSession = Depends(get_session),
    user: Optional[User] = Depends(get_current_user),
):
    _require_admin(user)

    ad_content = await _get_setting(session, "ad_content", "")

    review_q = await session.execute(
        select(Review)
        .options(selectinload(Review.user), selectinload(Review.anime))
        .order_by(desc(Review.timestamp))
        .limit(20)
    )
    reviews = review_q.scalars().all()

    return templates.TemplateResponse(
        request,
        "admin.html",
        {
            "user": user,
            "active": "admin",
            "ad_content": ad_content,
            "parser_state": parser_service.snapshot(),
            "reviews": reviews,
            "forbidden": False,
        },
    )


@router.post("/admin/ad")
async def admin_save_ad(
    request: Request,
    ad_content: str = Form(""),
    session: AsyncSession = Depends(get_session),
    user: Optional[User] = Depends(get_current_user),
):
    _require_admin(user)
    await _set_setting(session, "ad_content", ad_content)
    return RedirectResponse("/admin", status_code=303)


# ---------------------------------------------------------------------------
#  Parser control
# ---------------------------------------------------------------------------

@router.post("/admin/parser/run")
async def admin_run_parser(
    anime_id: Optional[int] = Query(None),
    user: Optional[User] = Depends(get_current_user),
):
    _require_admin(user)
    if anime_id:
        # Single-anime full re-parse (delete + re-import all episodes).
        report = await parser_service.reparse_anime(anime_id)
        return {"reparse": report, "state": parser_service.snapshot()}
    started = await parser_service.run_once()
    return {"started": started, "state": parser_service.snapshot()}


@router.post("/admin/anime/{anime_id}/reparse")
async def admin_reparse_anime(
    anime_id: int,
    user: Optional[User] = Depends(get_current_user),
):
    """Convenience alias used by the in-page «Перепарсить» button."""
    _require_admin(user)
    report = await parser_service.reparse_anime(anime_id)
    return report


@router.post("/admin/parser/stop")
async def admin_stop_parser(user: Optional[User] = Depends(get_current_user)):
    _require_admin(user)
    stopped = await parser_service.stop()
    return {"stopped": stopped, "state": parser_service.snapshot()}


@router.get("/admin/parser/status")
async def admin_parser_status(
    after: int = Query(0, ge=0),
    user: Optional[User] = Depends(get_current_user),
):
    """Polling fallback for environments without WebSocket support."""
    _require_admin(user)
    state = parser_service.snapshot()
    state["logs"] = parser_service.logs_since(after)
    return state


@router.websocket("/admin/parser/ws")
async def admin_parser_ws(websocket: WebSocket):
    """Live progress + log stream. Auth via session cookie."""
    # Manually verify the admin session.
    session_data = websocket.session if hasattr(websocket, "session") else {}
    user_id = session_data.get("user_id")
    if user_id != 1:
        # Try to load to be sure (covers the rare race where id != 1 but admin).
        if user_id:
            async with SessionLocal() as s:
                user = await s.get(User, user_id)
                if not user or user.id != 1:
                    await websocket.close(code=4403)
                    return
        else:
            await websocket.close(code=4403)
            return

    await websocket.accept()
    queue = parser_service.subscribe()
    try:
        # Initial snapshot
        await websocket.send_json(parser_service.snapshot())
        while True:
            try:
                payload = await asyncio.wait_for(queue.get(), timeout=20.0)
                await websocket.send_json(payload)
            except asyncio.TimeoutError:
                # Heartbeat to keep proxies happy.
                await websocket.send_json({"heartbeat": True})
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        parser_service.unsubscribe(queue)


# ---------------------------------------------------------------------------
#  Reviews moderation
# ---------------------------------------------------------------------------

@router.post("/admin/review/{review_id}/delete")
async def admin_delete_review(
    review_id: int,
    session: AsyncSession = Depends(get_session),
    user: Optional[User] = Depends(get_current_user),
):
    _require_admin(user)
    review = await session.get(Review, review_id)
    if review:
        await session.delete(review)
        await session.commit()
    return RedirectResponse("/admin", status_code=303)
