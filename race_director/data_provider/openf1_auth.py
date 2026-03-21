"""OpenF1 OAuth2 token management."""

from __future__ import annotations

import time

import httpx
import structlog

log = structlog.get_logger()

TOKEN_URL = "https://api.openf1.org/token"
REFRESH_BUFFER_SEC = 600  # Refresh 10 min before 1h expiry


async def fetch_token(username: str, password: str) -> tuple[str, int]:
    """Fetch OAuth2 access token from OpenF1.

    Returns:
        Tuple of (access_token, expires_in_seconds).
    """
    async with httpx.AsyncClient() as client:
        r = await client.post(
            TOKEN_URL,
            data={"username": username, "password": password},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        if r.status_code == 401:
            log.warning("openf1_token_auth_failed", hint="Check username/password")
            raise httpx.HTTPStatusError("401 Unauthorized", request=r.request, response=r)
        r.raise_for_status()
        data = r.json()
        token = data.get("access_token") or ""
        expires_in = int(data.get("expires_in", 3600))
        if not token:
            raise ValueError("No access_token in OpenF1 token response")
        return token, expires_in


class OpenF1TokenManager:
    """Manages OpenF1 OAuth2 token lifecycle with automatic refresh."""

    def __init__(self, username: str, password: str) -> None:
        self._username = username
        self._password = password
        self._token: str | None = None
        self._expires_at: float = 0.0

    async def get_valid_token(self) -> str:
        """Return a valid access token, refreshing if expired or missing."""
        now = time.time()
        if self._token and self._expires_at > now:
            return self._token
        try:
            token, expires_in = await fetch_token(self._username, self._password)
            self._token = token
            self._expires_at = time.time() + (expires_in - REFRESH_BUFFER_SEC)
            log.info("openf1_token_obtained", expires_in=expires_in)
            return self._token
        except Exception as e:
            log.warning("openf1_token_refresh_failed", error=str(e))
            if self._token:
                log.info("using_stale_token")
                return self._token
            raise
