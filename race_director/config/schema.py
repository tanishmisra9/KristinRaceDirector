"""Configuration schema (loads from config.yaml)."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class OpenF1Config(BaseModel):
    model_config = ConfigDict(extra="ignore")

    base_url: str = "https://api.openf1.org/v1"
    username: str = ""
    password: str = ""
    poll_interval_sec: float = 4.0
    request_timeout_sec: float = 5.0


class MultiViewerConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")
    uri: str = "http://localhost:10101/api/graphql"
    player_ids: list[int] = Field(default_factory=list)
    num_windows: int | None = None
    sync_delay_sec: float = 1.2


class ScoringWeights(BaseModel):
    model_config = ConfigDict(extra="ignore")
    interval_ahead: float = 1.0
    interval_behind: float = 0.7
    closing_trend: float = 0.8
    proximity_cluster: float = 0.5
    overtake_recency: float = 3.0
    pit_exit_traffic: float = 0.8
    race_control_event: float = 1.0
    position_importance: float = 1.5
    anti_churn_penalty: float = -0.6
    on_screen_retention: float = 0.4
    overtake_mode_attack: float = 0.8
    position_gain: float = 2.0
    prolonged_battle: float = 0.7
    session_phase: float = 1.5
    defending_bonus: float = 0.6
    incident_recovery: float = 2.0
    screen_time_penalty: float = -1.0
    stale_battle_penalty: float = -0.8


class ScoringParams(BaseModel):
    model_config = ConfigDict(extra="ignore")
    interval_sigmoid_midpoint_sec: float = 1.5
    interval_sigmoid_steepness: float = 3.0
    trend_window_samples: int = 8
    overtake_decay_seconds: float = 30.0
    pit_exit_window_seconds: float = 45.0
    proximity_radius_units: float = 500
    prolonged_battle_midpoint_sec: float = 45.0
    position_gain_max: int = 10
    incident_recovery_window_sec: float = 60.0
    screen_time_penalty_midpoint_sec: float = 60.0
    screen_time_penalty_steepness: float = 0.08


class HysteresisConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")
    min_dwell_seconds: float = 12.0
    swap_improvement_threshold: float = 0.15
    # Fix #18: Relative improvement threshold (15% = require 15% better score)
    swap_improvement_ratio: float = 0.15
    max_switches_per_cycle: int = 1
    max_switches_per_minute: int = 6
    removal_cooldown_seconds: float = 25.0
    sprint_min_dwell_seconds: float = 8.0


class StickySlotConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")
    slot: int
    driver: str | None = None


class OrchestratorConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")
    tick_interval_sec: float = 5.0
    dry_run: bool = False
    manual_override_file: str = "/tmp/race_director_pause"
    startup_grace_ticks: int = 1
    # Fix #25: Health monitoring
    health_port: int | None = None  # Optional HTTP health endpoint port
    watchdog_timeout_sec: float = 60.0  # Log CRITICAL if tick takes longer


class LoggingConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")
    level: str = "INFO"
    format: str = "console"
    file: str | None = None


class AppConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")
    openf1: OpenF1Config = Field(default_factory=OpenF1Config)
    multiviewer: MultiViewerConfig = Field(default_factory=MultiViewerConfig)
    scoring_weights: ScoringWeights = Field(default_factory=ScoringWeights)
    scoring_params: ScoringParams = Field(default_factory=ScoringParams)
    hysteresis: HysteresisConfig = Field(default_factory=HysteresisConfig)
    sticky_slots: list[StickySlotConfig] = Field(default_factory=list)
    orchestrator: OrchestratorConfig = Field(default_factory=OrchestratorConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
