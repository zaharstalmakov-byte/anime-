"""Key/value settings storage (used for ad banner content, etc.). Python 3.8 compatible."""
from sqlalchemy import String, Text
from sqlalchemy.orm import Mapped, mapped_column

from database import Base


class Setting(Base):
    __tablename__ = "settings"

    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    value: Mapped[str] = mapped_column(Text, default="")
