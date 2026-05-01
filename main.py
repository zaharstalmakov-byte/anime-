"""ANIMEFLOW — FastAPI entrypoint. Python 3.8 compatible."""
import logging
import logging.handlers
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from sqlalchemy import select
from starlette.middleware.sessions import SessionMiddleware

# --- Production-grade file logging -----------------------------------------
# Errors get persisted to data/errors.log so transient API failures (Anilibria
# blips, Shikimori 5xx, …) never bring the site down silently. We use a
# rotating handler so the log can never grow without bound.
_LOG_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(_LOG_DIR, exist_ok=True)
_LOG_PATH = os.path.join(_LOG_DIR, "errors.log")

_root = logging.getLogger()
if not any(isinstance(h, logging.handlers.RotatingFileHandler) for h in _root.handlers):
    _file_handler = logging.handlers.RotatingFileHandler(
        _LOG_PATH, maxBytes=2 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    _file_handler.setLevel(logging.WARNING)
    _file_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    )
    _root.addHandler(_file_handler)
    if _root.level > logging.INFO or _root.level == logging.NOTSET:
        _root.setLevel(logging.INFO)

from app.models.settings import Setting
from app.routes import admin as admin_routes
from app.routes import anime as anime_routes
from app.routes import api as api_routes
from app.routes import catalog as catalog_routes
from app.routes import auth as auth_routes
from app.routes import home as home_routes
from app.routes import profile as profile_routes
from app.routes import reviews as reviews_routes
from app.routes import search as search_routes
from app.services import parser as parser_service
from database import SessionLocal, init_db
from main_templates import templates

BASE_DIR = os.path.dirname(__file__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    # Resume the auto-update loop if it was running before restart
    parser_service.ensure_auto_update_started()
    yield
    await parser_service.shutdown()


app = FastAPI(title="ANIMEFLOW", lifespan=lifespan)

app.add_middleware(
    SessionMiddleware,
    secret_key=os.environ.get("SESSION_SECRET", "dev-secret-change-me"),
    same_site="lax",
    https_only=False,
    max_age=60 * 60 * 24 * 30,
)


@app.middleware("http")
async def load_global_settings(request: Request, call_next):
    """Load admin-controlled ad content into request.state for all templates."""
    request.state.ad_content = ""
    try:
        async with SessionLocal() as session:
            row = await session.execute(
                select(Setting).where(Setting.key == "ad_content")
            )
            setting = row.scalar_one_or_none()
            if setting:
                request.state.ad_content = setting.value or ""
    except Exception:
        pass
    return await call_next(request)


app.mount(
    "/static",
    StaticFiles(directory=os.path.join(BASE_DIR, "app", "static")),
    name="static",
)

# Routers
app.include_router(api_routes.router)
app.include_router(home_routes.router)
app.include_router(anime_routes.router)
app.include_router(auth_routes.router)
app.include_router(profile_routes.router)
app.include_router(search_routes.router)
app.include_router(catalog_routes.router)
app.include_router(reviews_routes.router)
app.include_router(admin_routes.router)


@app.exception_handler(404)
async def not_found(request: Request, exc):
    return templates.TemplateResponse(
        request,
        "404.html",
        {"user": None, "active": ""},
        status_code=404,
    )
