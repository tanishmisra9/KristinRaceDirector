"""OpenF1 REST API polling provider."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import httpx
import structlog

from race_director.config.schema import AppConfig
from race_director.data_provider.openf1_auth import OpenF1TokenManager
from race_director.data_provider.state_manager import StateManager
from race_director.models.session import SessionInfo

log = structlog.get_logger()

RATE_LIMIT_DELAY_SEC = 0.2  # 200ms between requests to stay under 6 req/sec
# Fix #4: Critical endpoints that trigger staleness if they fail repeatedly
CRITICAL_ENDPOINTS = {"intervals", "position"}
ENDPOINTS = [
    ("drivers", "ingest_drivers"),
    ("intervals", "ingest_intervals"),
    ("position", "ingest_positions"),
    ("location", "ingest_locations"),
    ("overtakes", "ingest_overtakes"),
    ("pit", "ingest_pit"),
    ("race_control", "_ingest_race_control"),
    ("car_data", "ingest_car_data"),
    ("laps", "ingest_laps"),
]


def _dedup_latest_per_driver_for_test(records: list[dict]) -> list[dict]:
    """Keep latest record per driver_number by date (test mode / capture_data pattern)."""
    per_driver: dict[int, dict] = {}
    best_date: dict[int, str] = {}
    for r in records:
        num = r.get("driver_number")
        if num is None:
            continue
        d = r.get("date", "") or ""
        if num not in per_driver or d > best_date.get(num, ""):
            best_date[num] = d
            per_driver[num] = r
    return list(per_driver.values())


class OpenF1RestProvider:
    """Polls OpenF1 REST API and feeds StateManager."""

    def __init__(self, config: AppConfig) -> None:
        self._config = config
        self._state = StateManager(config.scoring_params)
        self._session_key: int | None = None
        self._grid_fetched = False
        self._running = False
        self._token_manager: OpenF1TokenManager | None = None
        if config.openf1.username and config.openf1.password:
            self._token_manager = OpenF1TokenManager(
                config.openf1.username, config.openf1.password
            )
        
        # Fix #12: Persistent HTTP client for connection reuse
        self._client: httpx.AsyncClient | None = None
        
        # Fix #4 & #13: Endpoint health tracking
        self._consecutive_failures: dict[str, int] = {}
        self._last_success: dict[str, datetime] = {}
        self._data_stale: bool = False
        self._poll_count: int = 0
        # Test mode: raw API payloads per poll tick
        self._tick_api_data: dict[str, list[dict]] = {}
        self._session_meta: dict | None = None
        # Fix #28: Replay cursor = session date_start + commentary player time (seconds)
        self._replay_cursor: datetime | None = None

    def set_replay_cursor(self, commentary_seconds: float | None) -> None:
        """Compute replay cursor from commentary offset + session date_start (Fix #28)."""
        if commentary_seconds is None or self._session_meta is None:
            self._replay_cursor = None
        else:
            date_start = self._session_meta.get("date_start")
            if not date_start:
                self._replay_cursor = None
            else:
                try:
                    start = datetime.fromisoformat(str(date_start).replace("Z", "+00:00"))
                    self._replay_cursor = start + timedelta(seconds=float(commentary_seconds))
                except (ValueError, TypeError):
                    self._replay_cursor = None

    async def start(self) -> None:
        self._running = True
        # Fix #12: Create persistent HTTP client
        self._client = httpx.AsyncClient(timeout=self._config.openf1.request_timeout_sec)
        await self._fetch_session()

    async def stop(self) -> None:
        self._running = False
        # Fix #12: Close persistent HTTP client
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _auth_headers(self) -> dict[str, str]:
        """Return headers with Bearer token when authenticated."""
        headers: dict[str, str] = {"accept": "application/json"}
        if self._token_manager:
            token = await self._token_manager.get_valid_token()
            headers["Authorization"] = f"Bearer {token}"
        return headers

    async def poll(self) -> None:
        self._poll_count += 1
        if self._config.orchestrator.test_mode:
            self._tick_api_data.clear()

        # Fix #28: StateManager must see cursor before any ingest this poll
        self._state.set_replay_cursor(self._replay_cursor)

        # Fix #12: Ensure client exists before any fetches (session, grid, endpoints)
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=self._config.openf1.request_timeout_sec)
        
        await self._fetch_session()
        if not self._session_key:
            return
        headers = await self._auth_headers()
        await self._fetch_starting_grid(headers)
        
        for endpoint, handler_name in ENDPOINTS:
            handler = getattr(self._state, handler_name, None)
            if handler_name == "_ingest_race_control":
                handler = self._ingest_race_control
            if handler is not None:
                await self._fetch(self._client, endpoint, handler, headers)
                await asyncio.sleep(RATE_LIMIT_DELAY_SEC)
        
        self._state.expire_stale_events()
        
        # Fix #13: Log periodic health summary every 30 ticks
        if self._poll_count % 30 == 0:
            self._log_endpoint_health()

    async def _fetch_session(self) -> None:
        cfg = self._config.openf1
        url = f"{cfg.base_url}/sessions"
        params: dict[str, str | int] = {}
        if cfg.username and cfg.password:
            params["session_key"] = "latest"
        else:
            params["session_name"] = "Race"
        headers = await self._auth_headers()
        # Issue B: Use shared client (guaranteed by poll() or start())
        if self._client is None:
            return
        try:
            r = await self._client.get(url, params=params, headers=headers)
            if r.status_code == 401:
                log.warning("openf1_unauthorized_live_data_requires_subscription")
                return
            r.raise_for_status()
            data = r.json()
            if isinstance(data, list) and data:
                s = data[0]
                self._session_meta = dict(s)
                if self._config.orchestrator.test_mode:
                    self._tick_api_data["sessions"] = list(data)
                new_key = s.get("session_key")
                if new_key != self._session_key:
                    self._grid_fetched = False
                    # Fix #2: Full state reset on session change, not just filters
                    self._state.reset()
                    # Also reset endpoint health tracking for new session
                    self._consecutive_failures.clear()
                    self._last_success.clear()
                    self._data_stale = False
                self._session_key = new_key
                name = s.get("session_name", "")
                stype = "Sprint" if "sprint" in name.lower() else "Race"
                if "qualifying" in name.lower():
                    stype = "Qualifying"
                self._state.set_session(SessionInfo(session_type=stype))
        except httpx.HTTPError as e:
            log.debug("session_fetch_failed", error=str(e))

    async def _fetch_starting_grid(self, headers: dict[str, str]) -> None:
        """Fetch starting grid once per session."""
        if self._grid_fetched or not self._session_key or self._client is None:
            return
        url = f"{self._config.openf1.base_url}/starting_grid"
        try:
            # Issue B: Use shared client instead of creating a new one
            r = await self._client.get(
                url,
                params={"session_key": self._session_key},
                headers=headers,
            )
            if r.status_code == 401:
                return
            r.raise_for_status()
            data = r.json()
            if isinstance(data, list) and data:
                if self._config.orchestrator.test_mode:
                    self._tick_api_data["starting_grid"] = list(data)
                grid: dict[int, int] = {}
                for rec in data:
                    num = rec.get("driver_number")
                    pos = rec.get("position")
                    if num is not None and pos is not None:
                        grid[int(num)] = int(pos)
                if grid:
                    self._state.set_grid_positions(grid)
                    self._grid_fetched = True
                    log.info(
                        "starting_grid_loaded",
                        count=len(grid),
                        sample=dict(list(grid.items())[:5]),
                    )
        except httpx.HTTPError as e:
            log.debug("starting_grid_fetch_failed", error=str(e))

        if not self._grid_fetched and self._session_key:
            url = f"{self._config.openf1.base_url}/position"
            try:
                # Issue B: Use shared client for fallback grid fetch
                r = await self._client.get(
                    url,
                    params={"session_key": self._session_key},
                    headers=headers,
                )
                r.raise_for_status()
                data = r.json()
                if isinstance(data, list) and data:
                    earliest: dict[int, tuple[str, int]] = {}
                    for rec in data:
                        num = rec.get("driver_number")
                        pos = rec.get("position")
                        date_str = rec.get("date", "")
                        if num is not None and pos is not None:
                            date_key = (
                                date_str if date_str else "9999-12-31T23:59:59"
                            )
                            if (
                                num not in earliest
                                or date_key < earliest[num][0]
                            ):
                                earliest[num] = (date_key, int(pos))
                    grid = {num: pos for num, (_, pos) in earliest.items()}
                    if grid:
                        if self._config.orchestrator.test_mode and "starting_grid" not in self._tick_api_data:
                            self._tick_api_data["starting_grid"] = _dedup_latest_per_driver_for_test(
                                list(data)
                            )
                        self._state.set_grid_positions(grid)
                        self._grid_fetched = True
                        log.info("grid_from_positions", count=len(grid))
            except httpx.HTTPError:
                pass

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
                try:
                    handler(data)
                    # Fix #4: Reset failure count on success
                    self._consecutive_failures[endpoint] = 0
                    self._last_success[endpoint] = datetime.now(UTC)
                    # Check if we can clear staleness
                    self._update_staleness()
                    if self._config.orchestrator.test_mode:
                        to_store = list(data)
                        if endpoint in ("location", "car_data"):
                            to_store = _dedup_latest_per_driver_for_test(to_store)
                        self._tick_api_data[endpoint] = to_store
                except Exception:
                    log.warning("ingest_failed", endpoint=endpoint, exc_info=True)
        except httpx.HTTPError as e:
            # Fix #4: Track consecutive failures
            self._consecutive_failures[endpoint] = self._consecutive_failures.get(endpoint, 0) + 1
            log.debug("fetch_failed", endpoint=endpoint, failures=self._consecutive_failures[endpoint], error=str(e))
            self._update_staleness()
    
    def _update_staleness(self) -> None:
        """Fix #4: Update data staleness flag based on critical endpoint failures."""
        for ep in CRITICAL_ENDPOINTS:
            if self._consecutive_failures.get(ep, 0) > 5:
                if not self._data_stale:
                    log.warning("data_marked_stale", failed_endpoint=ep, consecutive_failures=self._consecutive_failures[ep])
                self._data_stale = True
                return
        # All critical endpoints are healthy
        if self._data_stale:
            log.info("data_freshness_restored")
        self._data_stale = False
    
    def _log_endpoint_health(self) -> None:
        """Fix #13: Log periodic summary of endpoint health status."""
        health = {}
        for endpoint, _ in ENDPOINTS:
            failures = self._consecutive_failures.get(endpoint, 0)
            last_ok = self._last_success.get(endpoint)
            status = "ok" if failures == 0 else f"failing({failures})"
            health[endpoint] = {"status": status, "last_success": last_ok.isoformat() if last_ok else None}
        log.info("endpoint_health_summary", health=health, data_stale=self._data_stale)
    
    def is_data_fresh(self) -> bool:
        """Fix #4: Return True if data is fresh (no critical endpoint failures)."""
        return not self._data_stale

    def get_reference_time(self):
        return self._state.get_reference_time()

    def is_lights_out(self) -> bool:
        return self._state.is_lights_out()

    def get_driver_states(self):
        return self._state.get_driver_states()

    def get_session_info(self):
        return self._state.get_session_info()

    def get_session_tlas(self) -> set[str]:
        return self._state.get_session_tlas()

    def get_sc_phase(self) -> str:
        return self._state.get_sc_phase()

    def get_session_meta(self) -> dict | None:
        """Raw session dict from OpenF1 /sessions (first row), for test recorder naming."""
        return self._session_meta

    def get_tick_api_data(self) -> dict[str, list[dict]]:
        """Snapshot of raw endpoint payloads for this poll; clears the buffer."""
        out = dict(self._tick_api_data)
        self._tick_api_data.clear()
        return out
