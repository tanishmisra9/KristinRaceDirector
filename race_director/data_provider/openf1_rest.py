"""OpenF1 REST API polling provider."""

from __future__ import annotations

import asyncio

import httpx
import structlog

from race_director.config.schema import AppConfig, OpenF1Config
from race_director.data_provider.openf1_auth import OpenF1TokenManager
from race_director.data_provider.state_manager import StateManager
from race_director.models.session import SessionInfo

log = structlog.get_logger()

RATE_LIMIT_DELAY_SEC = 0.2  # 200ms between requests to stay under 6 req/sec
ENDPOINTS = [
    ("drivers", "ingest_drivers"),
    ("intervals", "ingest_intervals"),
    ("position", "ingest_positions"),
    ("location", "ingest_locations"),
    ("overtakes", "ingest_overtakes"),
    ("pit", "ingest_pit"),
    ("race_control", "_ingest_race_control"),
    ("car_data", "ingest_car_data"),
]


class OpenF1RestProvider:
    """Polls OpenF1 REST API and feeds StateManager."""

    def __init__(self, config: AppConfig) -> None:
        self._config = config
        self._state = StateManager(config.scoring_params)
        self._session_key: int | None = None
        self._running = False
        self._token_manager: OpenF1TokenManager | None = None
        if config.openf1.username and config.openf1.password:
            self._token_manager = OpenF1TokenManager(
                config.openf1.username, config.openf1.password
            )

    async def start(self) -> None:
        self._running = True
        await self._fetch_session()

    async def stop(self) -> None:
        self._running = False

    async def _auth_headers(self) -> dict[str, str]:
        """Return headers with Bearer token when authenticated."""
        headers: dict[str, str] = {"accept": "application/json"}
        if self._token_manager:
            token = await self._token_manager.get_valid_token()
            headers["Authorization"] = f"Bearer {token}"
        return headers

    async def poll(self) -> None:
        await self._fetch_session()
        if not self._session_key:
            return
        headers = await self._auth_headers()
        async with httpx.AsyncClient(
            timeout=self._config.openf1.request_timeout_sec
        ) as c:
            for endpoint, handler_name in ENDPOINTS:
                handler = getattr(self._state, handler_name, None)
                if handler_name == "_ingest_race_control":
                    handler = self._ingest_race_control
                if handler is not None:
                    await self._fetch(c, endpoint, handler, headers)
                    await asyncio.sleep(RATE_LIMIT_DELAY_SEC)
        self._state.expire_stale_events()
        self._update_session_context()

    async def _fetch_session(self) -> None:
        cfg = self._config.openf1
        url = f"{cfg.base_url}/sessions"
        params: dict[str, str | int] = {}
        if cfg.username and cfg.password:
            params["session_key"] = "latest"
        else:
            params["session_name"] = "Race"
        headers = await self._auth_headers()
        async with httpx.AsyncClient(timeout=cfg.request_timeout_sec) as c:
            try:
                r = await c.get(url, params=params, headers=headers)
                if r.status_code == 401:
                    log.warning("openf1_unauthorized_live_data_requires_subscription")
                    return
                r.raise_for_status()
                data = r.json()
                if isinstance(data, list) and data:
                    s = data[0]
                    self._session_key = s.get("session_key")
                    name = s.get("session_name", "")
                    stype = "Sprint" if "sprint" in name.lower() else "Race"
                    if "qualifying" in name.lower():
                        stype = "Qualifying"
                    self._state.set_session(
                        SessionInfo(
                            session_key=s.get("session_key", 0),
                            session_name=name,
                            session_type=stype,
                        )
                    )
                    self._state.set_session_type(stype)
            except httpx.HTTPError as e:
                log.debug("session_fetch_failed", error=str(e))

    def _update_session_context(self) -> None:
        """Set lap_number, grid_position from laps when available."""
        if not self._session_key:
            return

    def _ingest_race_control(self, records: list[dict]) -> None:
        self._state.ingest_race_control(records)

    async def _fetch(
        self,
        client: httpx.AsyncClient,
        endpoint: str,
        handler: object,
        headers: dict[str, str],
    ) -> None:
        if not self._session_key:
            return
        url = f"{self._config.openf1.base_url}/{endpoint}"
        try:
            r = await client.get(
                url,
                params={"session_key": self._session_key},
                headers=headers,
            )
            if r.status_code == 401:
                log.warning("openf1_unauthorized_live_data_requires_subscription")
                return
            r.raise_for_status()
            data = r.json()
            if isinstance(data, list):
                handler(data)
        except httpx.HTTPError:
            pass

    def get_driver_states(self):
        return self._state.get_driver_states()

    def get_session_info(self):
        return self._state.get_session_info()

    def is_live(self) -> bool:
        return True
