"""Session info models."""

from __future__ import annotations

from pydantic import BaseModel


class SessionInfo(BaseModel):
    """Current session metadata."""

    session_type: str = "Race"  # Race, Sprint, Qualifying
    lap_number: int = 0
    restart_lap: bool = False  # first lap after SC/VSC ended
