"""Session info and status models."""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel


class SessionStatus(StrEnum):
    """F1 session lifecycle status."""

    Unknown = "Unknown"
    Inactive = "Inactive"
    Active = "Active"
    Started = "Started"
    Ended = "Ended"


class SessionInfo(BaseModel):
    """Current session metadata."""

    session_key: int = 0
    session_name: str = ""
    session_type: str = "Race"  # Race, Sprint, Qualifying
    status: SessionStatus = SessionStatus.Unknown
    lap_number: int = 0
    restart_lap: bool = False  # first lap after SC/VSC ended
