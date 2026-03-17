"""Scoring result and window slot models."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class ScoringBreakdown(BaseModel):
    """Per-dimension scores for one driver."""

    interval_ahead: float = 0.0
    interval_behind: float = 0.0
    closing_trend: float = 0.0
    proximity_cluster: float = 0.0
    overtake_recency: float = 0.0
    pit_exit_traffic: float = 0.0
    race_control_event: float = 0.0
    position_importance: float = 0.0
    anti_churn_penalty: float = 0.0
    on_screen_retention: float = 0.0
    overtake_mode_attack: float = 0.0
    position_gain: float = 0.0
    prolonged_battle: float = 0.0
    session_phase: float = 0.0
    defending_bonus: float = 0.0
    incident_recovery: float = 0.0
    screen_time_penalty: float = 0.0


class ScoringResult(BaseModel):
    """Ranked scoring result for one driver."""

    driver_number: int
    tla: str
    total_score: float
    breakdown: ScoringBreakdown


class WindowSlot(BaseModel):
    """Represents one MultiViewer player window managed by the daemon."""

    slot_index: int
    player_id: int | None = None
    current_tla: str = ""
    current_driver_number: int | None = None
    assigned_at: datetime | None = None
    is_sticky: bool = False
    sticky_target: str | None = None


class SwapCommand(BaseModel):
    """Command to switch a window to a different driver."""

    slot_index: int
    player_id: int
    old_tla: str
    new_tla: str
    new_driver_number: int
    score_improvement: float
