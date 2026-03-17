"""Hysteresis engine: dwell timers, switch budget, and churn control."""

from __future__ import annotations

from collections import deque
from datetime import UTC, datetime, timedelta

import structlog

from race_director.config.schema import HysteresisConfig
from race_director.models.scoring import ScoringResult, SwapCommand, WindowSlot
from race_director.models.session import SessionInfo

log = structlog.get_logger()


class HysteresisEngine:
    """Decides which window swaps to actually execute, enforcing stability rules."""

    def __init__(self, config: HysteresisConfig) -> None:
        self._config = config
        self._recent_switches: deque[datetime] = deque()

    def _min_dwell(self, session: SessionInfo | None) -> float:
        """Sprint uses shorter dwell for faster action."""
        if session and session.session_type == "Sprint":
            return self._config.sprint_min_dwell_seconds
        return self._config.min_dwell_seconds

    def plan_swaps(
        self,
        current_windows: list[WindowSlot],
        ranked_drivers: list[ScoringResult],
        driver_tla_map: dict[int, str],
        session: SessionInfo | None = None,
    ) -> list[SwapCommand]:
        """Given current windows and ranked candidates, produce swap commands."""
        self._prune_old_switches()

        now = datetime.now(UTC)
        cfg = self._config
        min_dwell = self._min_dwell(session)

        minute_budget = cfg.max_switches_per_minute - len(self._recent_switches)
        cycle_budget = min(cfg.max_switches_per_cycle, minute_budget)
        if cycle_budget <= 0:
            log.debug("switch_budget_exhausted")
            return []

        score_by_num: dict[int, ScoringResult] = {
            r.driver_number: r for r in ranked_drivers
        }

        swappable: list[tuple[int, float]] = []
        for slot in current_windows:
            if slot.is_sticky:
                continue
            if slot.assigned_at:
                dwell = (now - slot.assigned_at).total_seconds()
                if dwell < min_dwell:
                    continue
            current_score = 0.0
            if slot.current_driver_number and slot.current_driver_number in score_by_num:
                current_score = score_by_num[slot.current_driver_number].total_score
            swappable.append((slot.slot_index, current_score))

        if not swappable:
            return []

        swappable.sort(key=lambda x: x[1])

        on_screen: set[int] = {
            w.current_driver_number for w in current_windows if w.current_driver_number
        }

        swaps: list[SwapCommand] = []

        for candidate in ranked_drivers:
            if len(swaps) >= cycle_budget:
                break
            if not swappable:
                break

            if candidate.driver_number in on_screen:
                continue

            for i, (slot_idx, slot_score) in enumerate(swappable):
                improvement = candidate.total_score - slot_score
                if improvement >= cfg.swap_improvement_threshold:
                    slot = current_windows[slot_idx]
                    swap = SwapCommand(
                        slot_index=slot_idx,
                        player_id=slot.player_id or 0,
                        old_tla=slot.current_tla,
                        new_tla=candidate.tla,
                        new_driver_number=candidate.driver_number,
                        score_improvement=improvement,
                    )
                    swaps.append(swap)
                    on_screen.add(candidate.driver_number)
                    if slot.current_driver_number:
                        on_screen.discard(slot.current_driver_number)
                    swappable.pop(i)
                    break

        return swaps

    def record_swaps(self, count: int) -> None:
        """Record that N swaps were executed at the current time."""
        now = datetime.now(UTC)
        for _ in range(count):
            self._recent_switches.append(now)

    def resolve_sticky(
        self,
        current_windows: list[WindowSlot],
        ranked_drivers: list[ScoringResult],
    ) -> list[SwapCommand]:
        """Handle sticky slots (e.g. "leader" alias) that may need updating.
        Enforces uniqueness: multiple "leader" stickies get P1, P2, P3, etc.
        """
        swaps: list[SwapCommand] = []
        assigned_tlas: set[str] = set()

        sticky_slots = [
            s for s in current_windows
            if s.is_sticky and s.sticky_target
        ]

        for slot in sticky_slots:
            target_tla: str | None = None
            target_result: ScoringResult | None = None

            if slot.sticky_target.lower() == "leader":
                for r in ranked_drivers:
                    if r.tla not in assigned_tlas and r.breakdown.position_importance >= 0.8:
                        target_result = r
                        target_tla = r.tla
                        break
                if not target_tla:
                    for r in ranked_drivers:
                        if r.tla not in assigned_tlas:
                            target_result = r
                            target_tla = r.tla
                            break
            else:
                tla = slot.sticky_target.upper()
                if tla not in assigned_tlas:
                    for r in ranked_drivers:
                        if r.tla == tla:
                            target_result = r
                            target_tla = tla
                            break
                    if not target_result:
                        target_tla = tla
                else:
                    for r in ranked_drivers:
                        if r.tla not in assigned_tlas:
                            target_result = r
                            target_tla = r.tla
                            break

            if target_tla and target_tla.upper() != slot.current_tla.upper():
                driver_num = target_result.driver_number if target_result else 0
                swaps.append(
                    SwapCommand(
                        slot_index=slot.slot_index,
                        player_id=slot.player_id or 0,
                        old_tla=slot.current_tla,
                        new_tla=target_tla,
                        new_driver_number=driver_num,
                        score_improvement=0.0,
                    )
                )
                assigned_tlas.add(target_tla.upper())

        return swaps

    def _prune_old_switches(self) -> None:
        cutoff = datetime.now(UTC) - timedelta(seconds=60)
        while self._recent_switches and self._recent_switches[0] < cutoff:
            self._recent_switches.popleft()
