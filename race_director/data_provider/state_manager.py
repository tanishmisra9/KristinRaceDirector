"""Maintains per-driver state snapshots and trend ring buffers."""

from __future__ import annotations

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
from race_director.models.session import SessionInfo

log = structlog.get_logger()

INCIDENT_KEYWORDS = ("spin", "contact", "collision", "off", "stranded", "stopped")
_SESSION_STATUS_LABELS = ("Unknown", "Inactive", "Active", "Started", "Ended")


class StateManager:
    """Aggregates raw OpenF1 data into normalized DriverState snapshots."""

    def __init__(self, scoring_params: ScoringParams) -> None:
        self._params = scoring_params
        self._drivers: dict[int, DriverInfo] = {}
        self._states: dict[int, DriverState] = {}
        self._session: SessionInfo | None = None

        # Ring buffers for interval trend computation
        self._interval_history: dict[int, deque[IntervalSample]] = {}
        # Ring buffers for interval_behind trend (Fix #21: leader closing trend)
        self._interval_behind_history: dict[int, deque[IntervalSample]] = {}

        # Track global race phase
        self._safety_car = False
        self._vsc = False
        self._session_status = "Unknown"
        self._lap_number = 0

        # Recent race control events per driver
        self._driver_flags: dict[int, tuple[str, datetime]] = {}

        # Overtake history
        self._recent_overtakes: dict[int, tuple[datetime, bool]] = {}

        # Pit events
        self._in_pit: set[int] = set()

        # Battle duration: consecutive samples with small gap
        self._battle_start: dict[int, datetime] = {}

        # Timestamp tracking for replay: only process new records per endpoint
        self._last_processed_date: dict[str, str] = {}

        # Track which endpoints have completed their first (baseline) ingest (Fix #3)
        self._first_ingest_done: set[str] = set()

        # Latest data timestamp — used as reference time for scoring (replay/live)
        self._latest_data_time: datetime = datetime.now(UTC)

        # Fix #28: Upper bound on API record dates when following a replay (MultiViewer commentary time)
        self._replay_cursor: datetime | None = None

        # Lights out: SESSION STARTED from race_control
        self._lights_out: bool = False

        # SC phase for display notifications: "none", "deployed", "ending", "green"
        self._sc_phase: str = "none"

    def get_reference_time(self) -> datetime:
        """Latest timestamp seen in any data feed. Used as 'now' for scoring."""
        return self._latest_data_time

    def is_lights_out(self) -> bool:
        """True if SESSION STARTED (lights out) has been detected."""
        return self._lights_out

    def get_sc_phase(self) -> str:
        """Return current SC phase: 'none', 'deployed', 'ending', 'green'."""
        return self._sc_phase

    def reset(self) -> None:
        """Full reset of all per-driver state — called on session change (Fix #2)."""
        self._drivers.clear()
        self._states.clear()
        self._interval_history.clear()
        self._interval_behind_history.clear()
        self._battle_start.clear()
        self._recent_overtakes.clear()
        self._in_pit.clear()
        self._driver_flags.clear()
        self._first_ingest_done.clear()
        self._last_processed_date.clear()
        self._latest_data_time = datetime.now(UTC)
        self._lights_out = False
        self._safety_car = False
        self._vsc = False
        self._sc_phase = "none"
        self._session_status = "Unknown"
        self._lap_number = 0
        self._replay_cursor = None
        log.info("state_manager_reset")

    def set_replay_cursor(self, cursor: datetime | None) -> None:
        """Set max API record time for replay mode (Fix #28). None = live / no filter."""
        self._replay_cursor = cursor
        # Fix #28: when entering replay mode, clamp future reference time down to cursor.
        # Otherwise _latest_data_time can remain at wall-clock now and retire almost all drivers.
        if cursor is not None and self._latest_data_time > cursor:
            self._latest_data_time = cursor

    def _update_latest_time(self, date: datetime) -> None:
        """Advance _latest_data_time, capped at replay cursor when replaying (Fix #28)."""
        if date > self._latest_data_time:
            if self._replay_cursor is None or date <= self._replay_cursor:
                self._latest_data_time = date

    def _parse_date(self, date_str: str) -> datetime:
        """Parse ISO date string with fallback to latest data time (Fix #7).
        
        Uses _latest_data_time as fallback instead of datetime.now(UTC) to avoid
        corrupting time-based scoring during replay mode.
        """
        if not date_str:
            return self._latest_data_time
        try:
            return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return self._latest_data_time

    def _filter_new_records(self, endpoint: str, records: list[dict]) -> list[dict]:
        """Filter records to only those newer than the last processed date.

        First call: process ALL records to establish baseline state.
        Subsequent calls: only process new records (delta).

        Fix #28: Drop records after the replay cursor (future relative to MultiViewer playback).
        """
        if self._replay_cursor is not None:
            filtered: list[dict] = []
            for r in records:
                ds = r.get("date", "")
                if not ds:
                    filtered.append(r)
                    continue
                try:
                    rd = datetime.fromisoformat(ds.replace("Z", "+00:00"))
                    if rd <= self._replay_cursor:
                        filtered.append(r)
                except (ValueError, TypeError):
                    filtered.append(r)
            records = filtered

        last_date = self._last_processed_date.get(endpoint)

        if last_date is None:
            all_dates = [r.get("date", "") for r in records if r.get("date")]
            if all_dates:
                self._last_processed_date[endpoint] = max(all_dates)
            return records

        new_records = [r for r in records if r.get("date", "") > last_date]
        if new_records:
            dates = [r.get("date", "") for r in new_records if r.get("date")]
            if dates:
                self._last_processed_date[endpoint] = max(dates)
        return new_records

    def set_session(self, info: SessionInfo) -> None:
        self._session = info

    def get_session_info(self) -> SessionInfo | None:
        return self._session

    def get_driver_states(self) -> dict[int, DriverState]:
        return dict(self._states)

    def get_session_tlas(self) -> set[str]:
        """Return TLAs of drivers confirmed in this session."""
        return {info.name_acronym.upper() for info in self._drivers.values() if info.name_acronym}

    def ingest_drivers(self, records: list[dict]) -> None:
        for rec in records:
            num = rec.get("driver_number")
            if num is None:
                continue
            self._drivers[num] = DriverInfo(
                driver_number=num,
                name_acronym=rec.get("name_acronym", ""),
            )
            if num not in self._states:
                self._states[num] = DriverState(
                    driver_number=num,
                    tla=rec.get("name_acronym", ""),
                )

    def ingest_intervals(self, records: list[dict]) -> None:
        is_first_ingest = "intervals" not in self._first_ingest_done
        records = self._filter_new_records("intervals", records)
        if len(records) > 1000:
            latest_per_driver: dict[int, dict] = {}
            for rec in records:
                num = rec.get("driver_number")
                date = rec.get("date", "")
                if num is not None and (
                    num not in latest_per_driver or date > latest_per_driver[num].get("date", "")
                ):
                    latest_per_driver[num] = rec
            records = list(latest_per_driver.values())
        for rec in records:
            num = rec.get("driver_number")
            if num is None:
                continue
            # Issue D: Auto-create DriverState for unknown drivers (mirrors ingest_positions)
            if num not in self._states:
                self._states[num] = DriverState(driver_number=num, tla=str(num))

            interval_raw = rec.get("interval")
            date_str = rec.get("date", "")

            date = self._parse_date(date_str)
            self._update_latest_time(date)

            interval_val: float | None = None
            is_lapped = False
            if isinstance(interval_raw, (int, float)):
                interval_val = float(interval_raw)
            elif isinstance(interval_raw, str) and "LAP" in interval_raw:
                is_lapped = True

            state = self._states[num]
            state.interval_to_ahead = interval_val
            state.is_lapped = is_lapped
            state.last_updated = date

            if num not in self._interval_history:
                self._interval_history[num] = deque(
                    maxlen=self._params.trend_window_samples
                )
            self._interval_history[num].append(
                IntervalSample(interval=interval_val, date=date)
            )

            state.interval_trend = self._compute_trend(num)
            state.battle_duration_seconds = self._compute_battle_duration(num, state)

        self._derive_interval_behind()

        # Populate interval_behind_history after deriving interval_behind (Fix #21)
        for num, state in self._states.items():
            if num not in self._interval_behind_history:
                self._interval_behind_history[num] = deque(
                    maxlen=self._params.trend_window_samples
                )
            if state.interval_behind is not None:
                self._interval_behind_history[num].append(
                    IntervalSample(interval=state.interval_behind, date=self._latest_data_time)
                )
            state.interval_behind_trend = self._compute_behind_trend(num)

        # Fix #3: Clear trend history after first ingest to avoid pollution from historical data
        if is_first_ingest:
            self._interval_history.clear()
            self._interval_behind_history.clear()
            self._first_ingest_done.add("intervals")

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

    def _compute_behind_trend(self, driver_number: int) -> float:
        """Compute trend for interval_behind (Fix #21: leader closing trend)."""
        history = self._interval_behind_history.get(driver_number)
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

        ref = self._latest_data_time
        if in_battle:
            if driver_number not in self._battle_start:
                self._battle_start[driver_number] = ref
            return (ref - self._battle_start[driver_number]).total_seconds()
        else:
            self._battle_start.pop(driver_number, None)
            return 0.0

    def _derive_interval_behind(self) -> None:
        """Derive interval_behind for each driver from the car behind's interval_to_ahead.
        
        Fix #19: Handle position gaps (e.g., P1, P2, P4 with no P3 due to retirement)
        by using sorted position order instead of assuming continuous numbering.
        """
        # Build list of (position, driver_number) sorted by position
        position_order = sorted(
            [(st.position, num) for num, st in self._states.items() if st.position > 0],
            key=lambda x: x[0]
        )
        
        # Link each driver to the next driver in sorted order
        for i, (pos, num) in enumerate(position_order):
            if i + 1 < len(position_order):
                behind_num = position_order[i + 1][1]
                self._states[num].interval_behind = self._states[behind_num].interval_to_ahead
            else:
                self._states[num].interval_behind = None

    def ingest_positions(self, records: list[dict]) -> None:
        is_first_ingest = "positions" not in self._first_ingest_done
        records = self._filter_new_records("positions", records)
        latest_by_driver: dict[int, tuple[str, int]] = {}
        for rec in records:
            num = rec.get("driver_number")
            pos = rec.get("position")
            date_str = rec.get("date", "")
            if num is None or pos is None:
                continue
            # Fix #6: Auto-create DriverState for unknown drivers
            if num not in self._states:
                self._states[num] = DriverState(driver_number=num, tla=str(num))
            new_pos = int(pos)
            date_key = date_str if date_str else "0000-01-01T00:00:00"
            if num not in latest_by_driver or date_key > latest_by_driver[num][0]:
                latest_by_driver[num] = (date_key, new_pos)

        for num, (date_str, new_pos) in latest_by_driver.items():
            overtake_date = self._parse_date(date_str)
            self._update_latest_time(overtake_date)

            old_pos = self._states[num].position
            if old_pos > 0 and new_pos > 0 and old_pos != new_pos:
                positions_gained = old_pos - new_pos
                if positions_gained >= 1:
                    self._states[num].last_overtake_time = overtake_date
                    self._states[num].was_overtaker = True
                    self._recent_overtakes[num] = (overtake_date, True)
                    # Fix #15: Mark as interesting action
                    self._states[num].last_interesting_action = overtake_date
                elif positions_gained <= -1:
                    self._states[num].last_overtake_time = overtake_date
                    self._states[num].was_overtaker = False
                    self._recent_overtakes[num] = (overtake_date, False)
                    # Fix #15: Mark as interesting action
                    self._states[num].last_interesting_action = overtake_date
            self._states[num].position = new_pos
            # Fix #29: Update last_updated from position data so drivers don't
            # get falsely marked as retired when intervals endpoint is down (404).
            self._states[num].last_updated = overtake_date

        # Fix #3: Clear recent overtakes after first ingest to avoid pollution
        if is_first_ingest:
            self._recent_overtakes.clear()
            self._first_ingest_done.add("positions")

    def set_grid_positions(self, grid_positions: dict[int, int]) -> None:
        """Set start/grid position per driver (from session start order)."""
        for num, grid_pos in grid_positions.items():
            if num in self._states:
                self._states[num].grid_position = grid_pos

    def ingest_laps(self, records: list[dict]) -> None:
        """Ingest lap data — derive current lap from most recent records."""
        records = self._filter_new_records("laps", records)
        if not records:
            return
        latest_date: datetime | None = None
        latest_lap = self._lap_number
        for rec in records:
            lap = rec.get("lap_number")
            date_str = rec.get("date", "")
            if lap is None:
                continue
            date = self._parse_date(date_str) if date_str else None
            if date is not None and isinstance(lap, int):
                if latest_date is None or date > latest_date:
                    latest_date = date
                    latest_lap = lap
        if latest_lap != self._lap_number:
            self.set_lap_number(latest_lap)
            log.info("lap_number_updated", lap=latest_lap)
        if latest_date is not None:
            self._update_latest_time(latest_date)

    def set_lap_number(self, lap: int) -> None:
        self._lap_number = lap
        if self._session:
            self._session.lap_number = lap

    def ingest_locations(self, records: list[dict]) -> None:
        for rec in records:
            num = rec.get("driver_number")
            if num is None or num not in self._states:
                continue

            date_str = rec.get("date", "")
            date = self._parse_date(date_str)
            self._update_latest_time(date)

            self._states[num].location = LocationSample(
                x=rec.get("x", 0),
                y=rec.get("y", 0),
                z=rec.get("z", 0),
                date=date,
            )

    def ingest_overtakes(self, records: list[dict]) -> None:
        is_first_ingest = "overtakes" not in self._first_ingest_done
        records = self._filter_new_records("overtakes", records)
        for rec in records:
            date_str = rec.get("date", "")
            date = self._parse_date(date_str)
            self._update_latest_time(date)

            overtaker = rec.get("overtaking_driver_number")
            overtaken = rec.get("overtaken_driver_number")

            if overtaker and overtaker in self._states:
                self._recent_overtakes[overtaker] = (date, True)
                self._states[overtaker].last_overtake_time = date
                self._states[overtaker].was_overtaker = True
                # Fix #15: Mark as interesting action
                self._states[overtaker].last_interesting_action = date

            if overtaken and overtaken in self._states:
                self._recent_overtakes[overtaken] = (date, False)
                self._states[overtaken].last_overtake_time = date
                self._states[overtaken].was_overtaker = False
                # Fix #15: Mark as interesting action
                self._states[overtaken].last_interesting_action = date

        # Fix #3: Clear overtake recency after first ingest
        if is_first_ingest:
            self._recent_overtakes.clear()
            for st in self._states.values():
                st.last_overtake_time = None
                st.was_overtaker = False
                st.last_interesting_action = None
            self._first_ingest_done.add("overtakes")

    def ingest_pit(self, records: list[dict]) -> None:
        """Track pit activity. OpenF1 fires on pit EXIT with duration."""
        for rec in records:
            num = rec.get("driver_number")
            if num is None or num not in self._states:
                continue

            date_str = rec.get("date", "")
            date = self._parse_date(date_str)
            self._update_latest_time(date)

            self._states[num].pit_exit_time = date
            self._states[num].in_pit = False
            self._in_pit.discard(num)

    def ingest_race_control(self, records: list[dict]) -> None:
        # Fix #27: Track first ingest to avoid setting lights_out from historical data
        is_first_ingest = "race_control" not in self._first_ingest_done
        records = self._filter_new_records("race_control", records)
        for rec in records:
            date_str = rec.get("date", "")
            rec_date = self._parse_date(date_str)
            self._update_latest_time(rec_date)

            category = rec.get("category", "")
            flag = rec.get("flag")
            message = (rec.get("message", "") or "").lower()
            driver_num = rec.get("driver_number")

            if category == "SafetyCar" and not is_first_ingest:
                if "ending" in message or "in this lap" in message:
                    self._sc_phase = "ending"
                elif "virtual safety car" in message or "vsc" in message:
                    self._vsc = True
                    self._safety_car = False
                    self._sc_phase = "deployed"
                elif "deployed" in message:
                    self._safety_car = True
                    self._vsc = False
                    self._sc_phase = "deployed"
                else:
                    self._safety_car = False
                    self._vsc = False
                    self._sc_phase = "none"

            if category == "SessionStatus":
                if "started" in message and not is_first_ingest:
                    self._lights_out = True
                for status_val in _SESSION_STATUS_LABELS:
                    if status_val.lower() in message:
                        self._session_status = status_val
                        break
                if "ended" in message or "finalised" in message:
                    self._safety_car = False
                    self._vsc = False
                    self._sc_phase = "none"

            # Detect green flag / track clear - clears SC and VSC state
            if not is_first_ingest:
                if flag == "GREEN" and "track clear" in message:
                    self._safety_car = False
                    self._vsc = False
                    self._sc_phase = "green"
                if "overtake enabled" in message:
                    self._safety_car = False
                    self._vsc = False
                    self._sc_phase = "green"

            if driver_num and driver_num in self._states and flag:
                self._driver_flags[driver_num] = (flag, rec_date)
                self._states[driver_num].has_active_flag = True
                self._states[driver_num].active_flag_type = flag
                # Fix #15: Mark as interesting action
                self._states[driver_num].last_interesting_action = rec_date

            if driver_num and driver_num in self._states:
                for kw in INCIDENT_KEYWORDS:
                    if kw in message:
                        self._states[driver_num].recent_incident_time = rec_date
                        # Fix #15: Mark as interesting action
                        self._states[driver_num].last_interesting_action = rec_date
                        break

        for st in self._states.values():
            st.safety_car_active = self._safety_car
            st.vsc_active = self._vsc
            st.session_status = self._session_status

        # Fix #27: Reset flags set from historical data on first ingest
        if is_first_ingest:
            self._lights_out = False
            self._safety_car = False
            self._vsc = False
            self._sc_phase = "none"
            for st in self._states.values():
                st.safety_car_active = False
                st.vsc_active = False
            self._first_ingest_done.add("race_control")

    def ingest_car_data(self, records: list[dict]) -> None:
        # Fix #23: Apply _filter_new_records to car_data (highest volume endpoint)
        records = self._filter_new_records("car_data", records)
        
        # Dedup to latest per driver (same pattern as intervals first-ingest)
        drs_overtake_values = {10, 12, 14}
        per_driver: dict[int, tuple[str, int]] = {}  # driver_number -> (date, value)
        for rec in records:
            num = rec.get("driver_number")
            drs = rec.get("drs")
            overtake = rec.get("overtake_mode")  # 2026 if OpenF1 adds it
            date = rec.get("date", "")
            if num is not None:
                val = overtake if overtake is not None else drs
                if val is not None:
                    if num not in per_driver or date > per_driver[num][0]:
                        per_driver[num] = (date, val)

        for num, (_, val) in per_driver.items():
            if num in self._states:
                self._states[num].drs_open = val in drs_overtake_values
                self._states[num].overtake_mode_active = val in drs_overtake_values

    def expire_stale_events(self) -> None:
        ref = self._latest_data_time
        flag_window = timedelta(seconds=60)
        incident_window = timedelta(seconds=self._params.incident_recovery_window_sec)

        for num in list(self._driver_flags):
            flag_type, ts = self._driver_flags[num]
            if ref - ts > flag_window and num in self._states:
                self._states[num].has_active_flag = False
                self._states[num].active_flag_type = None
                del self._driver_flags[num]

        for num, st in list(self._states.items()):
            if st.recent_incident_time and ref - st.recent_incident_time > incident_window:
                st.recent_incident_time = None

        # Fix #14: Increase threshold to 300s (5 min) and skip pitting drivers
        stale_threshold = timedelta(seconds=300)
        for num, st in self._states.items():
            # Never mark a driver as retired if they're known to be in the pits
            if num in self._in_pit:
                st.is_retired = False
            elif st.last_updated and ref - st.last_updated > stale_threshold:
                st.is_retired = True
            else:
                st.is_retired = False
