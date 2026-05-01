"""Cross-platform launcher for ANIMEFLOW. Python 3.8 compatible.

Usage:
    python run.py

It will:
  * make sure dependencies are installed (if requirements.txt exists)
  * start uvicorn on http://127.0.0.1:8080
  * open the browser

Designed to also work on low-spec machines (e.g. Windows 7 + Python 3.8).
"""
import os
import sys
import webbrowser
from pathlib import Path

HERE = Path(__file__).resolve().parent
os.chdir(str(HERE))
sys.path.insert(0, str(HERE))

HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "8080"))

# Make sure SESSION_SECRET has *something* for local use.
os.environ.setdefault("SESSION_SECRET", "local-dev-secret-change-me")


def _ensure_deps():
    try:
        import fastapi  # noqa: F401
        import uvicorn  # noqa: F401
    except ImportError:
        print("[ANIMEFLOW] Installing dependencies from requirements.txt ...")
        import subprocess
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "-r", str(HERE / "requirements.txt")]
        )


def main():
    _ensure_deps()
    import uvicorn

    url = "http://{}:{}/".format(HOST, PORT)
    print("[ANIMEFLOW] starting on {}".format(url))
    try:
        webbrowser.open(url, new=2)
    except Exception:
        pass

    uvicorn.run("main:app", host=HOST, port=PORT, reload=False)


if __name__ == "__main__":
    main()
