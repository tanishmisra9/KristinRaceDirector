"""Driver information and live state models."""

from __future__ import annotations

from datetime import UTC, datetime

from pydantic import BaseModel, Field


class DriverInfo(BaseModel):
    """Static driver metadata for the current session."""

    driver_number: int
    name_acronym: str  # 3-letter TLA, e.g. "VER"
    full_name: str = ""
    team_name: str = ""
    team_colour: str = ""


class LocationSample(BaseModel):
    """A single (x, y) location sample with timestamp."""

    x: int
    y: int
    z: int = 0
    date: datetime


class IntervalSample(BaseModel):
    """A single interval measurement with timestamp."""

    interval: float | None = None  # seconds to car ahead; None for leader
    gap_to_leader: float | None = None
    date: datetime


class DriverState(BaseModel):
    """Full live state for one driver at the current tick."""

    driver_number: int
    tla: str = ""
    team_name: str = ""

    # Position & intervals
    position: int = 0
    interval_to_ahead: float | None = None
    gap_to_leader: float | None = None
    interval_behind: float | None = None  # derived: next car's interval
    is_lapped: bool = False

    # Location
    location: LocationSample | None = None

    # Trend data (populated by StateManager from ring buffer)
    interval_trend: float = 0.0  # negative = closing on car ahead

    # Pit status
    in_pit: bool = False
    pit_exit_time: datetime | None = None
    last_pit_lap: int | None = None

    # DRS (pre-2026) / Overtake Mode (2026+)
    drs_open: bool = False
    overtake_mode_active: bool = False  # 2026: within 1s, attacking

    # Overtakes
    last_overtake_time: datetime | None = None
    was_overtaker: bool = False

    # Race control events
    has_active_flag: bool = False
    active_flag_type: str | None = None

    # Session phase context (shared, not per-driver, but convenient here)
    safety_car_active: bool = False
    vsc_active: bool = False
    session_status: str = "Started"

    # Metadata
    last_updated: datetime = Field(default_factory=lambda: datetime.now(UTC))
    is_retired: bool = False

    # 2026 scoring additions
    grid_position: int = 0  # start position; 0 = unknown
    recent_incident_time: datetime | None = None
    battle_duration_seconds: float = 0.0  # time in close battle (small gap)
