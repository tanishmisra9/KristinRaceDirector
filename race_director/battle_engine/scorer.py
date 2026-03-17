"""Composite battle scorer that ranks all drivers by watchability."""

from __future__ import annotations

from datetime import UTC, datetime

import structlog

from race_director.battle_engine.dimensions import (
    score_anti_churn_penalty,
    score_closing_trend,
    score_defending_bonus,
    score_incident_recovery,
    score_interval_ahead,
    score_interval_behind,
    score_on_screen_retention,
    score_overtake_mode_attack,
    score_overtake_recency,
    score_pit_exit_traffic,
    score_position_gain,
    score_position_importance,
    score_prolonged_battle,
    score_proximity_cluster,
    score_race_control_event,
    score_screen_time_penalty,
    score_session_phase,
)
from race_director.config.schema import ScoringParams, ScoringWeights
from race_director.models.driver import DriverState
from race_director.models.scoring import ScoringBreakdown, ScoringResult, WindowSlot
from race_director.models.session import SessionInfo

log = structlog.get_logger()


class BattleScorer:
    """Evaluates all drivers and produces ranked scoring results."""

    def __init__(self, weights: ScoringWeights, params: ScoringParams) -> None:
        self._weights = weights
        self._params = params
        self._recently_removed: dict[int, datetime] = {}

    def record_removal(self, driver_number: int) -> None:
        """Mark a driver as recently removed from a window (for anti-churn)."""
        self._recently_removed[driver_number] = datetime.now(UTC)

    def cleanup_removals(self, cooldown_seconds: float) -> None:
        """Expire old removal records."""
        now = datetime.now(UTC)
        expired = [
            num
            for num, ts in self._recently_removed.items()
            if (now - ts).total_seconds() > cooldown_seconds
        ]
        for num in expired:
            del self._recently_removed[num]

    def score_all(
        self,
        states: dict[int, DriverState],
        current_windows: list[WindowSlot],
        session: SessionInfo | None = None,
        cooldown_seconds: float = 30.0,
    ) -> list[ScoringResult]:
        """Score every driver, return results sorted by total_score descending."""
        self.cleanup_removals(cooldown_seconds)

        results: list[ScoringResult] = []
        w = self._weights
        p = self._params

        for num, state in states.items():
            bd = ScoringBreakdown(
                interval_ahead=score_interval_ahead(state, p),
                interval_behind=score_interval_behind(state, p),
                closing_trend=score_closing_trend(state, p),
                proximity_cluster=score_proximity_cluster(state, states, p),
                overtake_recency=score_overtake_recency(state, p),
                pit_exit_traffic=score_pit_exit_traffic(state, p),
                race_control_event=score_race_control_event(state, p),
                position_importance=score_position_importance(state, p),
                anti_churn_penalty=score_anti_churn_penalty(
                    state, self._recently_removed, cooldown_seconds
                ),
                on_screen_retention=score_on_screen_retention(
                    state, current_windows, states, p
                ),
                overtake_mode_attack=score_overtake_mode_attack(state, p),
                position_gain=score_position_gain(state, p),
                prolonged_battle=score_prolonged_battle(state, p),
                session_phase=score_session_phase(state, session),
                defending_bonus=score_defending_bonus(state, p),
                incident_recovery=score_incident_recovery(state, p),
                screen_time_penalty=score_screen_time_penalty(
                    state, current_windows, p
                ),
            )

            total = (
                bd.interval_ahead * w.interval_ahead
                + bd.interval_behind * w.interval_behind
                + bd.closing_trend * w.closing_trend
                + bd.proximity_cluster * w.proximity_cluster
                + bd.overtake_recency * w.overtake_recency
                + bd.pit_exit_traffic * w.pit_exit_traffic
                + bd.race_control_event * w.race_control_event
                + bd.position_importance * w.position_importance
                + bd.anti_churn_penalty * w.anti_churn_penalty
                + bd.on_screen_retention * w.on_screen_retention
                + bd.overtake_mode_attack * w.overtake_mode_attack
                + bd.position_gain * w.position_gain
                + bd.prolonged_battle * w.prolonged_battle
                + bd.session_phase * w.session_phase
                + bd.defending_bonus * w.defending_bonus
                + bd.incident_recovery * w.incident_recovery
                + bd.screen_time_penalty * w.screen_time_penalty
            )

            results.append(
                ScoringResult(
                    driver_number=num,
                    tla=state.tla,
                    total_score=total,
                    breakdown=bd,
                )
            )

        results.sort(key=lambda r: r.total_score, reverse=True)
        return results
