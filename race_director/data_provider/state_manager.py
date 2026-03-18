"""Maintains per-driver state snapshots and trend ring buffers."""

from __future__ import annotations

import math
from collections import deque
from datetime import UTC, datetime, timedelta

import structlog

from race_director.config.schema import ScoringParams
from race_director.models.driver import (
    DriverInfo,
    DriverState,
    IntervalSample,
    LocationSample,
)
from race_director.models.session import SessionInfo, SessionStatus

log = structlog.get_logger()

INCIDENT_KEYWORDS = ("spin", "contact", "collision", "off", "stranded", "stopped")


class StateManager:
    """Aggregates raw OpenF1 data into normalized DriverState snapshots."""

    def __init__(self, scoring_params: ScoringParams) -> None:
        self._params = scoring_params
        self._drivers: dict[int, DriverInfo] = {}
        self._states: dict[int, DriverState] = {}
        self._session: SessionInfo | None = None

        # Ring buffers for interval trend computation
        self._interval_history: dict[int, deque[IntervalSample]] = {}

        # Track global race phase
        self._safety_car = False
        self._vsc = False
        self._session_status = "Unknown"
        self._lap_number = 0
        self._restart_lap = False
        self._sc_ended_recently = False

        # Recent race control events per driver
        self._driver_flags: dict[int, tuple[str, datetime]] = {}

        # Overtake history
        self._recent_overtakes: dict[int, tuple[datetime, bool]] = {}

        # Pit events
        self._pit_exits: dict[int, datetime] = {}
        self._in_pit: set[int] = set()
        self._in_pit_since: dict[int, datetime] = {}

        # Battle duration: consecutive samples with small gap
        self._battle_start: dict[int, datetime] = {}

    def set_session(self, info: SessionInfo) -> None:
        self._session = info

    def get_session_info(self) -> SessionInfo | None:
        return self._session

    def get_driver_states(self) -> dict[int, DriverState]:
        return dict(self._states)

    def ingest_drivers(self, records: list[dict]) -> None:
        for rec in records:
            num = rec.get("driver_number")
            if num is None:
                continue
            self._drivers[num] = DriverInfo(
                driver_number=num,
                name_acronym=rec.get("name_acronym", ""),
                full_name=rec.get("full_name", ""),
                team_name=rec.get("team_name", ""),
                team_colour=rec.get("team_colour", ""),
            )
            if num not in self._states:
                self._states[num] = DriverState(
                    driver_number=num,
                    tla=rec.get("name_acronym", ""),
                    team_name=rec.get("team_name", ""),
                )

    def ingest_intervals(self, records: list[dict]) -> None:
        for rec in records:
            num = rec.get("driver_number")
            if num is None or num not in self._states:
                continue

            interval_raw = rec.get("interval")
            gap_raw = rec.get("gap_to_leader")
            date_str = rec.get("date", "")

            try:
                date = datetime.fromisoformat(date_str) if date_str else datetime.now(UTC)
            except (ValueError, TypeError):
                date = datetime.now(UTC)

            interval_val: float | None = None
            is_lapped = False
            if isinstance(interval_raw, (int, float)):
                interval_val = float(interval_raw)
            elif isinstance(interval_raw, str) and "LAP" in interval_raw:
                is_lapped = True

            gap_val: float | None = None
            if isinstance(gap_raw, (int, float)):
                gap_val = float(gap_raw)

            state = self._states[num]
            state.interval_to_ahead = interval_val
            state.gap_to_leader = gap_val
            state.is_lapped = is_lapped
            state.last_updated = date

            if num not in self._interval_history:
                self._interval_history[num] = deque(
                    maxlen=self._params.trend_window_samples
                )
            self._interval_history[num].append(
                IntervalSample(interval=interval_val, gap_to_leader=gap_val, date=date)
            )

            state.interval_trend = self._compute_trend(num)
            state.battle_duration_seconds = self._compute_battle_duration(num, state)

        self._derive_interval_behind()

    def _compute_trend(self, driver_number: int) -> float:
        history = self._interval_history.get(driver_number)
        if not history or len(history) < 3:
            return 0.0

        values = [s.interval for s in history if s.interval is not None]
        if len(values) < 3:
            return 0.0

        n = len(values)
        x_mean = (n - 1) / 2.0
        y_mean = sum(values) / n
        num = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(values))
        den = sum((i - x_mean) ** 2 for i in range(n))
        if den == 0:
            return 0.0
        return num / den

    def _compute_battle_duration(self, driver_number: int, state: DriverState) -> float:
        """Time spent in close battle (interval ahead or behind < 3s)."""
        small_gap = 3.0
        in_battle = False
        if state.interval_to_ahead is not None and 0.1 <= state.interval_to_ahead <= small_gap:
            in_battle = True
        elif state.interval_behind is not None and 0.1 <= state.interval_behind <= small_gap:
            in_battle = True

        now = datetime.now(UTC)
        if in_battle:
            if driver_number not in self._battle_start:
                self._battle_start[driver_number] = now
            return (now - self._battle_start[driver_number]).total_seconds()
        else:
            self._battle_start.pop(driver_number, None)
            return 0.0

    def _derive_interval_behind(self) -> None:
        by_position: dict[int, int] = {}
        for num, st in self._states.items():
            if st.position > 0:
                by_position[st.position] = num

        for pos, num in by_position.items():
            behind_num = by_position.get(pos + 1)
            if behind_num is not None:
                self._states[num].interval_behind = self._states[
                    behind_num
                ].interval_to_ahead
            else:
                self._states[num].interval_behind = None

    def ingest_positions(self, records: list[dict]) -> None:
        for rec in records:
            num = rec.get("driver_number")
            pos = rec.get("position")
            if num is None or num not in self._states:
                continue
            if pos is not None:
                self._states[num].position = int(pos)

    def set_grid_positions(self, grid_positions: dict[int, int]) -> None:
        """Set start/grid position per driver (from session start order)."""
        for num, grid_pos in grid_positions.items():
            if num in self._states:
                self._states[num].grid_position = grid_pos

    def ingest_laps(self, records: list[dict]) -> None:
        """Ingest lap data — tracks current lap number per driver."""
        max_lap = self._lap_number
        for rec in records:
            lap = rec.get("lap_number")
            if lap is None:
                continue
            if isinstance(lap, int) and lap > max_lap:
                max_lap = lap
        if max_lap > self._lap_number:
            self.set_lap_number(max_lap)
            log.info("lap_number_updated", lap=max_lap)

    def set_lap_number(self, lap: int) -> None:
        self._lap_number = lap
        if self._session:
            self._session.lap_number = lap

    def set_restart_lap(self, is_restart: bool) -> None:
        self._restart_lap = is_restart
        if self._session:
            self._session.restart_lap = is_restart

    def set_session_type(self, session_type: str) -> None:
        if self._session:
            self._session.session_type = session_type

    def ingest_locations(self, records: list[dict]) -> None:
        for rec in records:
            num = rec.get("driver_number")
            if num is None or num not in self._states:
                continue

            date_str = rec.get("date", "")
            try:
                date = datetime.fromisoformat(date_str) if date_str else datetime.now(UTC)
            except (ValueError, TypeError):
                date = datetime.now(UTC)

            self._states[num].location = LocationSample(
                x=rec.get("x", 0),
                y=rec.get("y", 0),
                z=rec.get("z", 0),
                date=date,
            )

    def ingest_overtakes(self, records: list[dict]) -> None:
        for rec in records:
            date_str = rec.get("date", "")
            try:
                date = datetime.fromisoformat(date_str) if date_str else datetime.now(UTC)
            except (ValueError, TypeError):
                date = datetime.now(UTC)

            overtaker = rec.get("overtaking_driver_number")
            overtaken = rec.get("overtaken_driver_number")

            if overtaker and overtaker in self._states:
                self._recent_overtakes[overtaker] = (date, True)
                self._states[overtaker].last_overtake_time = date
                self._states[overtaker].was_overtaker = True

            if overtaken and overtaken in self._states:
                self._recent_overtakes[overtaken] = (date, False)
                self._states[overtaken].last_overtake_time = date
                self._states[overtaken].was_overtaker = False

    def ingest_pit(self, records: list[dict]) -> None:
        for rec in records:
            num = rec.get("driver_number")
            if num is None or num not in self._states:
                continue

            date_str = rec.get("date", "")
            try:
                date = datetime.fromisoformat(date_str) if date_str else datetime.now(UTC)
            except (ValueError, TypeError):
                date = datetime.now(UTC)

            pit_duration = rec.get("pit_duration")

            if pit_duration is not None and pit_duration > 0:
                self._pit_exits[num] = date
                self._states[num].pit_exit_time = date
                self._states[num].last_pit_lap = rec.get("lap_number")
                self._states[num].in_pit = False
                self._in_pit.discard(num)
                self._in_pit_since.pop(num, None)
            else:
                self._states[num].in_pit = True
                self._in_pit.add(num)
                self._in_pit_since[num] = date

    def ingest_race_control(self, records: list[dict]) -> None:
        now = datetime.now(UTC)
        for rec in records:
            category = rec.get("category", "")
            flag = rec.get("flag")
            message = (rec.get("message", "") or "").lower()
            driver_num = rec.get("driver_number")

            if category == "SafetyCar":
                if "ending" in message or "in this lap" in message:
                    self._sc_ended_recently = True
                elif "virtual safety car" in message or "vsc" in message:
                    self._vsc = True
                    self._safety_car = False
                elif "deployed" in message:
                    self._safety_car = True
                    self._vsc = False
                else:
                    self._safety_car = False
                    self._vsc = False

            if category == "SessionStatus":
                for status_val in SessionStatus:
                    if status_val.value.lower() in message:
                        self._session_status = status_val.value
                        break

            if driver_num and driver_num in self._states and flag:
                self._driver_flags[driver_num] = (flag, now)
                self._states[driver_num].has_active_flag = True
                self._states[driver_num].active_flag_type = flag

            if driver_num and driver_num in self._states:
                for kw in INCIDENT_KEYWORDS:
                    if kw in message:
                        self._states[driver_num].recent_incident_time = now
                        break

        for st in self._states.values():
            st.safety_car_active = self._safety_car
            st.vsc_active = self._vsc
            st.session_status = self._session_status

    def ingest_car_data(self, records: list[dict]) -> None:
        drs_overtake_values = {10, 12, 14}
        per_driver: dict[int, int] = {}
        for rec in records:
            num = rec.get("driver_number")
            drs = rec.get("drs")
            overtake = rec.get("overtake_mode")  # 2026 if OpenF1 adds it
            if num is not None:
                val = overtake if overtake is not None else drs
                if val is not None:
                    per_driver[num] = val

        for num, val in per_driver.items():
            if num in self._states:
                self._states[num].drs_open = val in drs_overtake_values
                self._states[num].overtake_mode_active = val in drs_overtake_values

    def ingest_championship(self, records: list[dict]) -> None:
        for rec in records:
            num = rec.get("driver_number")
            if num is None or num not in self._states:
                continue
            self._states[num].championship_position = rec.get("position_current")
            self._states[num].championship_points = rec.get("points_current")

    def expire_stale_events(self) -> None:
        now = datetime.now(UTC)
        flag_window = timedelta(seconds=60)
        incident_window = timedelta(seconds=self._params.incident_recovery_window_sec)

        for num in list(self._driver_flags):
            flag_type, ts = self._driver_flags[num]
            if now - ts > flag_window and num in self._states:
                self._states[num].has_active_flag = False
                self._states[num].active_flag_type = None
                del self._driver_flags[num]

        for num, st in list(self._states.items()):
            if st.recent_incident_time and now - st.recent_incident_time > incident_window:
                st.recent_incident_time = None

        pit_timeout = timedelta(seconds=60)
        for num in list(self._in_pit):
            if num in self._states and num in self._in_pit_since:
                if now - self._in_pit_since[num] > pit_timeout:
                    self._states[num].in_pit = False
                    self._in_pit.discard(num)
                    del self._in_pit_since[num]
