"""Async SQLAlchemy database setup using SQLite (aiosqlite). Python 3.8 compatible."""
import os
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "animeflow.db")

DATABASE_URL = "sqlite+aiosqlite:///{}".format(DB_PATH)

engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    future=True,
    connect_args={"check_same_thread": False},
)

SessionLocal = async_sessionmaker(
    engine, expire_on_commit=False, class_=AsyncSession
)


class Base(DeclarativeBase):
    """Shared declarative base for all ORM models."""


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with SessionLocal() as session:
        yield session


async def init_db() -> None:
    # Import models so they're registered with Base.metadata
    from app.models import anime as _anime  # noqa: F401
    from app.models import settings as _settings  # noqa: F401
    from app.models import user as _user  # noqa: F401

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # Lightweight idempotent migrations for SQLite — add columns added
        # after the original schema was created so existing DBs keep working.
        await _ensure_columns(conn)


async def _ensure_columns(conn) -> None:
    """Add new columns to existing SQLite tables if they're missing."""
    from sqlalchemy import text

    pending = {
        "anime": [
            ("studio", "VARCHAR(120) DEFAULT ''"),
            ("episodes_total", "INTEGER DEFAULT 0"),
        ],
        "episodes": [
            ("source", "VARCHAR(32) DEFAULT 'anilibria'"),
            ("yummy_id", "INTEGER"),
            ("yummy_slug", "VARCHAR(160) DEFAULT ''"),
            ("yummy_iframe", "VARCHAR(500) DEFAULT ''"),
            ("yummy_page_url", "VARCHAR(500) DEFAULT ''"),
            ("animedia_id", "INTEGER"),
            ("animedia_slug", "VARCHAR(160) DEFAULT ''"),
            ("animedia_iframe", "VARCHAR(500) DEFAULT ''"),
            ("animedia_page_url", "VARCHAR(500) DEFAULT ''"),
        ],
    }
    for table, cols in pending.items():
        res = await conn.exec_driver_sql(
            "PRAGMA table_info({})".format(table)
        )
        existing = {row[1] for row in res.fetchall()}
        for name, ddl in cols:
            if name in existing:
                continue
            try:
                await conn.exec_driver_sql(
                    "ALTER TABLE {} ADD COLUMN {} {}".format(table, name, ddl)
                )
            except Exception:
                # Already exists or unsupported — ignore so startup never blocks.
                pass
