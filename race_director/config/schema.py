"""Configuration schema (loads from config.yaml)."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class OpenF1Config(BaseModel):
    base_url: str = "https://api.openf1.org/v1"
    username: str = ""
    password: str = ""
    use_mqtt: bool = False
    poll_interval_sec: float = 4.0
    request_timeout_sec: float = 5.0


class MultiViewerConfig(BaseModel):
    uri: str = "http://localhost:10101/api/graphql"
    player_ids: list[int] = Field(default_factory=list)
    num_windows: int | None = None
    sync_delay_sec: float = 1.2
    sync_strategy: Literal["seek_only", "global_fallback"] = "global_fallback"


class ScoringWeights(BaseModel):
    interval_ahead: float = 1.2
    interval_behind: float = 0.9
    closing_trend: float = 1.0
    proximity_cluster: float = 0.8
    overtake_recency: float = 2.0
    pit_exit_traffic: float = 1.0
    race_control_event: float = 1.2
    position_importance: float = 1.0
    anti_churn_penalty: float = -0.3
    on_screen_retention: float = 0.3
    overtake_mode_attack: float = 1.0
    position_gain: float = 1.5
    prolonged_battle: float = 1.0
    session_phase: float = 1.5
    defending_bonus: float = 1.0
    incident_recovery: float = 1.5
    screen_time_penalty: float = -0.8


class ScoringParams(BaseModel):
    interval_sigmoid_midpoint_sec: float = 1.2
    interval_sigmoid_steepness: float = 3.5
    trend_window_samples: int = 6
    overtake_decay_seconds: float = 45.0
    pit_exit_window_seconds: float = 45.0
    proximity_radius_units: float = 500
    prolonged_battle_midpoint_sec: float = 45.0
    position_gain_max: int = 10
    incident_recovery_window_sec: float = 60.0
    screen_time_penalty_midpoint_sec: float = 75.0
    screen_time_penalty_steepness: float = 0.08


class HysteresisConfig(BaseModel):
    min_dwell_seconds: float = 12.0
    swap_improvement_threshold: float = 0.12
    max_switches_per_cycle: int = 2
    max_switches_per_minute: int = 8
    removal_cooldown_seconds: float = 20.0
    sprint_min_dwell_seconds: float = 8.0


class StickySlotConfig(BaseModel):
    slot: int
    driver: str | None = None


class OrchestratorConfig(BaseModel):
    tick_interval_sec: float = 4.0
    dry_run: bool = False
    manual_override_file: str = "/tmp/race_director_pause"


class LoggingConfig(BaseModel):
    level: str = "INFO"
    format: str = "json"
    file: str | None = None


class AppConfig(BaseModel):
    openf1: OpenF1Config = Field(default_factory=OpenF1Config)
    multiviewer: MultiViewerConfig = Field(default_factory=MultiViewerConfig)
    scoring_weights: ScoringWeights = Field(default_factory=ScoringWeights)
    scoring_params: ScoringParams = Field(default_factory=ScoringParams)
    hysteresis: HysteresisConfig = Field(default_factory=HysteresisConfig)
    sticky_slots: list[StickySlotConfig] = Field(default_factory=list)
    orchestrator: OrchestratorConfig = Field(default_factory=OrchestratorConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
