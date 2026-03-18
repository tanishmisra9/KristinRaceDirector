"""Individual scoring dimension functions.

Each function takes driver state + context and returns a score in [0.0, 1.0].
The composite scorer multiplies these by configurable weights.
"""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta

from race_director.config.schema import ScoringParams
from race_director.models.driver import DriverState
from race_director.models.scoring import WindowSlot
from race_director.models.session import SessionInfo


def sigmoid(value: float, midpoint: float, steepness: float) -> float:
    """Inverse sigmoid: 1.0 when value=0, ~0 when value >> midpoint."""
    return 1.0 / (1.0 + math.exp(steepness * (value - midpoint)))


def score_interval_ahead(state: DriverState, params: ScoringParams) -> float:
    """Score based on gap to the car ahead. Closer = higher."""
    if state.interval_to_ahead is None or state.is_lapped:
        return 0.0
    gap = abs(state.interval_to_ahead)
    return sigmoid(gap, params.interval_sigmoid_midpoint_sec, params.interval_sigmoid_steepness)


def score_interval_behind(state: DriverState, params: ScoringParams) -> float:
    """Score based on pressure from the car behind."""
    if state.interval_behind is None:
        return 0.0
    gap = abs(state.interval_behind)
    return sigmoid(gap, params.interval_sigmoid_midpoint_sec, params.interval_sigmoid_steepness)


def score_closing_trend(state: DriverState, _params: ScoringParams) -> float:
    """Score based on whether the gap is closing. Negative trend = closing = good."""
    trend = state.interval_trend
    if trend >= 0:
        return 0.0
    return min(1.0, abs(trend) / 0.1)


def score_proximity_cluster(
    state: DriverState,
    all_states: dict[int, DriverState],
    params: ScoringParams,
) -> float:
    """Score based on how many cars are physically near this driver on track."""
    if state.location is None:
        return 0.0

    x0, y0 = state.location.x, state.location.y
    radius = params.proximity_radius_units
    count = 0
    cutoff = datetime.now(UTC) - timedelta(seconds=15)

    for num, other in all_states.items():
        if num == state.driver_number:
            continue
        if other.location is None or other.location.date < cutoff:
            continue
        dist = math.sqrt((other.location.x - x0) ** 2 + (other.location.y - y0) ** 2)
        if dist <= radius:
            count += 1

    if count == 0:
        return 0.0
    if count == 1:
        return 0.4
    if count == 2:
        return 0.7
    return 1.0


def score_overtake_recency(state: DriverState, params: ScoringParams) -> float:
    """Decaying bonus for drivers involved in a recent overtake."""
    if state.last_overtake_time is None:
        return 0.0

    elapsed = (datetime.now(UTC) - state.last_overtake_time).total_seconds()
    if elapsed > params.overtake_decay_seconds:
        return 0.0

    base = 1.0 - (elapsed / params.overtake_decay_seconds)
    if state.was_overtaker:
        return min(1.0, base * 1.2)
    return base


def score_pit_exit_traffic(state: DriverState, params: ScoringParams) -> float:
    """Boost for drivers who recently exited the pits and have close gaps."""
    if state.pit_exit_time is None:
        return 0.0

    elapsed = (datetime.now(UTC) - state.pit_exit_time).total_seconds()
    if elapsed > params.pit_exit_window_seconds:
        return 0.0

    time_factor = 1.0 - (elapsed / params.pit_exit_window_seconds)

    traffic_factor = 0.5
    if state.interval_to_ahead is not None and state.interval_to_ahead < 3.0:
        traffic_factor = 1.0
    elif state.interval_behind is not None and state.interval_behind < 3.0:
        traffic_factor = 0.8

    return min(1.0, time_factor * traffic_factor)


def score_race_control_event(state: DriverState, _params: ScoringParams) -> float:
    """Boost during safety car restarts and for drivers with active flags."""
    score = 0.0

    if state.safety_car_active or state.vsc_active:
        score += 0.3
    if state.session_status == "Started" and (state.safety_car_active or state.vsc_active):
        score += 0.3

    if state.has_active_flag:
        if state.active_flag_type in ("YELLOW", "DOUBLE YELLOW"):
            score += 0.5
        elif state.active_flag_type == "BLACK AND WHITE":
            score += 0.3
        else:
            score += 0.2

    return min(1.0, score)


def score_position_importance(state: DriverState, _params: ScoringParams) -> float:
    """Higher score for key positions. Stronger top-end bias."""
    pos = state.position
    if pos <= 0:
        return 0.0

    if pos == 1:
        return 1.0
    if pos == 2:
        return 0.85
    if pos == 3:
        return 0.75
    if 4 <= pos <= 5:
        return 0.5
    if 6 <= pos <= 9:
        return 0.35
    if pos == 10:
        return 0.6
    if pos == 11:
        return 0.5
    if 12 <= pos <= 15:
        return 0.15
    return 0.05


def score_defending_bonus(state: DriverState, params: ScoringParams) -> float:
    """Extra boost when defending a key position (P1-P3) with close pressure behind."""
    if state.position not in (1, 2, 3):
        return 0.0
    if state.interval_behind is None or state.interval_behind <= 0:
        return 0.0
    gap = abs(state.interval_behind)
    if gap > 1.5:
        return 0.0
    return sigmoid(gap, params.interval_sigmoid_midpoint_sec, params.interval_sigmoid_steepness)


def score_anti_churn_penalty(
    state: DriverState,
    recently_removed: dict[int, datetime],
    cooldown_seconds: float,
) -> float:
    """Penalty for recently removed drivers (positive value, applied with negative weight)."""
    removal_time = recently_removed.get(state.driver_number)
    if removal_time is None:
        return 0.0

    elapsed = (datetime.now(UTC) - removal_time).total_seconds()
    if elapsed > cooldown_seconds:
        return 0.0

    return 1.0 - (elapsed / cooldown_seconds)


def score_on_screen_retention(
    state: DriverState,
    current_windows: list[WindowSlot],
    all_states: dict[int, DriverState],
    params: ScoringParams,
) -> float:
    """Bonus for drivers currently on screen who are still interesting."""
    on_screen = any(w.current_driver_number == state.driver_number for w in current_windows)
    if not on_screen:
        return 0.0

    base_interest = 0.3
    if state.interval_to_ahead is not None and state.interval_to_ahead < 2.0:
        base_interest = 0.8
    elif state.interval_behind is not None and state.interval_behind < 2.0:
        base_interest = 0.6
    elif state.last_overtake_time is not None:
        elapsed = (datetime.now(UTC) - state.last_overtake_time).total_seconds()
        if elapsed < params.overtake_decay_seconds:
            base_interest = 0.7

    return min(1.0, base_interest)


def score_overtake_mode_attack(state: DriverState, params: ScoringParams) -> float:
    """2026: Boost when Overtake Mode active and attacking (small gap ahead)."""
    if not (state.overtake_mode_active or state.drs_open):
        return 0.0
    if state.interval_to_ahead is None or state.is_lapped:
        return 0.0
    gap = abs(state.interval_to_ahead)
    if gap > 2.0:
        return 0.0
    return sigmoid(gap, params.interval_sigmoid_midpoint_sec, params.interval_sigmoid_steepness)


def score_position_gain(state: DriverState, params: ScoringParams) -> float:
    """Bonus for positions gained OR lost vs grid. Big movers are interesting."""
    if state.grid_position <= 0 or state.position <= 0:
        return 0.0
    delta = abs(state.grid_position - state.position)
    if delta <= 0:
        return 0.0
    return min(1.0, delta / params.position_gain_max)


def score_prolonged_battle(state: DriverState, params: ScoringParams) -> float:
    """Boost for drivers in prolonged battle (stable small gap over time)."""
    dur = state.battle_duration_seconds
    if dur <= 0:
        return 0.0
    # High when duration is high; inverse of interval sigmoid
    return 1.0 - sigmoid(dur, params.prolonged_battle_midpoint_sec, 0.05)


def score_session_phase(
    state: DriverState,
    session: SessionInfo | None,
) -> float:
    """Opening lap and restart bonus. Graduated: lap 1 highest, fading by lap 3."""
    if session is None:
        return 0.0
    score = 0.0
    lap = session.lap_number
    if lap == 1:
        score += 0.8
    elif lap == 2:
        score += 0.5
    elif lap == 3:
        score += 0.3
    if session.restart_lap:
        score += 0.5
    return min(1.0, score)


def score_incident_recovery(state: DriverState, params: ScoringParams) -> float:
    """Spike for recent incidents. Highest in first 20s, then decay."""
    if state.recent_incident_time is None:
        return 0.0

    elapsed = (datetime.now(UTC) - state.recent_incident_time).total_seconds()
    if elapsed > params.incident_recovery_window_sec:
        return 0.0

    if elapsed <= 20.0:
        return 1.0
    remaining = params.incident_recovery_window_sec - elapsed
    total_decay = params.incident_recovery_window_sec - 20.0
    return max(0.0, remaining / total_decay)


def score_screen_time_penalty(
    state: DriverState,
    current_windows: list[WindowSlot],
    params: ScoringParams,
) -> float:
    """Penalty for drivers who have been on screen a long time (applied with negative weight).
    Encourages variety by reducing score for drivers we've been showing.
    """
    now = datetime.now(UTC)
    time_on_screen_sec = 0.0
    for w in current_windows:
        if w.current_driver_number == state.driver_number and w.assigned_at:
            at = w.assigned_at
            if at.tzinfo is None:
                at = at.replace(tzinfo=UTC)
            elapsed = (now - at).total_seconds()
            time_on_screen_sec = max(time_on_screen_sec, elapsed)
    if time_on_screen_sec <= 0:
        return 0.0
    midpoint = params.screen_time_penalty_midpoint_sec
    steepness = params.screen_time_penalty_steepness
    return 1.0 - sigmoid(time_on_screen_sec, midpoint, steepness)
