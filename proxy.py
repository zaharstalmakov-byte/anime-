"""Outbound HTTP proxy configuration for the parser. Python 3.8 compatible.

Every request the parser makes to YummyAnime / Anilibria / Animedia /
Shikimori is routed through a residential proxy so the upstream sources
can't IP-block our deployment.

Credentials default to the Geonode residential gateway provided in the
project brief, but every value can be overridden via env vars without
touching code:

* ``PARSER_PROXY_DISABLED``  — set to ``1`` / ``true`` to bypass the proxy.
* ``PARSER_PROXY_URL``       — full proxy URL, wins over the pieces below.
                               (e.g. ``http://user:pass@host:port``)
* ``PARSER_PROXY_USER``      — proxy username
* ``PARSER_PROXY_PASS``      — proxy password
* ``PARSER_PROXY_HOST``      — proxy hostname
                               (default: ``premium-residential.geonode.com``)
* ``PARSER_PROXY_PORT``      — proxy port (default: ``9000``)

The module also exposes ``PROXY_EXCEPTIONS`` — the tuple of httpx
exception classes that callers should catch with a clear "proxy /
network" log message (RemoteProtocolError, ConnectError, ProxyError,
TimeoutException, etc.).
"""
import logging
import os
from typing import Any, Dict, Optional, Tuple
from urllib.parse import quote

import httpx

logger = logging.getLogger("animeflow.proxy")

# Defaults are the Geonode residential gateway from the project brief.
DEFAULT_USER = "geonode_SgbsncVlMl"
DEFAULT_PASS = "5c2652eb-9083-4148-b8a4-71b231e9e5d8"
DEFAULT_HOST = "premium-residential.geonode.com"
DEFAULT_PORT = "9000"


def _truthy(val: str) -> bool:
    return val.strip().lower() in ("1", "true", "yes", "on")


def _build_proxy_url() -> Optional[str]:
    if _truthy(os.getenv("PARSER_PROXY_DISABLED", "")):
        return None
    explicit = os.getenv("PARSER_PROXY_URL", "").strip()
    if explicit:
        return explicit
    user = os.getenv("PARSER_PROXY_USER", DEFAULT_USER).strip()
    pwd = os.getenv("PARSER_PROXY_PASS", DEFAULT_PASS).strip()
    host = os.getenv("PARSER_PROXY_HOST", DEFAULT_HOST).strip()
    port = os.getenv("PARSER_PROXY_PORT", DEFAULT_PORT).strip()
    if not (user and pwd and host and port):
        return None
    return "http://{}:{}@{}:{}".format(
        quote(user, safe=""), quote(pwd, safe=""), host, port
    )


PROXY_URL: Optional[str] = _build_proxy_url()


# Exception classes that should be treated as "proxy / network unavailable"
# rather than a parser bug. Caller code is expected to catch these and emit
# a clear WARN log instead of letting them bubble up as ERRORs.
PROXY_EXCEPTIONS: Tuple[type, ...] = (
    httpx.RemoteProtocolError,
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.ReadTimeout,
    httpx.WriteTimeout,
    httpx.PoolTimeout,
    httpx.TimeoutException,
    httpx.ProxyError,
    ConnectionError,
    OSError,
)


def httpx_proxies() -> Optional[Dict[str, str]]:
    """Return the dict you pass to ``httpx.AsyncClient(proxies=...)``.

    Returns ``None`` if the proxy is intentionally disabled, in which case
    callers should construct the client without the ``proxies`` kwarg.
    """
    if not PROXY_URL:
        return None
    return {"all://": PROXY_URL}


def proxy_label() -> str:
    """Human-readable proxy hint for logs (no password)."""
    if not PROXY_URL:
        return "—"
    try:
        rest = PROXY_URL.split("://", 1)[1]
        creds, hostport = rest.split("@", 1)
        user = creds.split(":", 1)[0]
        return "{}@{}".format(user, hostport)
    except Exception:  # noqa: BLE001
        return "configured"


def make_async_client(
    headers: Optional[Dict[str, str]] = None,
    timeout_total: float = 20.0,
    timeout_connect: float = 10.0,
    follow_redirects: bool = True,
) -> httpx.AsyncClient:
    """Build an ``httpx.AsyncClient`` pre-wired to the residential proxy.

    Always use this helper inside the parser instead of constructing
    ``httpx.AsyncClient`` directly — it makes the proxy switch a one-line
    change and keeps every outbound request consistent.
    """
    kwargs: Dict[str, Any] = {
        "timeout": httpx.Timeout(timeout_total, connect=timeout_connect),
        "headers": headers or {},
        "follow_redirects": follow_redirects,
    }
    proxies = httpx_proxies()
    if proxies:
        kwargs["proxies"] = proxies
    return httpx.AsyncClient(**kwargs)
