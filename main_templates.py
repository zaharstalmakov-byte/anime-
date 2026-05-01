"""Shared Jinja2Templates instance to avoid circular imports. Python 3.8 compatible."""
import os
from datetime import datetime

from fastapi.templating import Jinja2Templates

BASE_DIR = os.path.dirname(__file__)
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "app", "templates"))

# Globals available in every template — keeps individual routes lean.
templates.env.globals["now_year"] = datetime.utcnow().year
