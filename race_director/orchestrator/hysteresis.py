"""Hysteresis engine: dwell timers, switch budget, and churn control."""

from __future__ import annotations

from collections import deque
from datetime import UTC, datetime, timedelta

import structlog

from race_director.config.schema import HysteresisConfig
from race_director.models.scoring import ScoringResult, SwapCommand, WindowSlot
from race_director.models.session import SessionInfo

log = structlog.get_logger()

STICKY_OVERRIDE_THRESHOLD = 0.5


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
        on_screen_nums: set[int] = {
            w.current_driver_number for w in current_windows if w.current_driver_number
        }
        on_screen_tlas: set[str] = {
            w.current_tla.upper() for w in current_windows if w.current_tla
        }

        best_off_screen_score = 0.0
        for r in ranked_drivers:
            if r.driver_number not in on_screen_nums and r.tla.upper() not in on_screen_tlas:
                best_off_screen_score = r.total_score
                break

        swappable: list[tuple[int, float]] = []
        for slot in current_windows:
            if slot.is_sticky:
                if slot.current_driver_number and slot.current_driver_number in score_by_num:
                    current_score = score_by_num[slot.current_driver_number].total_score
                    if best_off_screen_score - current_score < STICKY_OVERRIDE_THRESHOLD:
                        continue
                else:
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

        log.info(
            "plan_swaps_start",
            on_screen=list(on_screen_nums),
            swappable_slots=[s[0] for s in swappable],
            cycle_budget=cycle_budget,
        )

        swaps: list[SwapCommand] = []

        for candidate in ranked_drivers:
            if len(swaps) >= cycle_budget:
                break
            if not swappable:
                break

            if candidate.driver_number in on_screen_nums or candidate.tla.upper() in on_screen_tlas:
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
                    on_screen_nums.add(candidate.driver_number)
                    on_screen_tlas.add(candidate.tla.upper())
                    if slot.current_driver_number:
                        on_screen_nums.discard(slot.current_driver_number)
                    if slot.current_tla:
                        on_screen_tlas.discard(slot.current_tla.upper())
                    swappable.pop(i)
                    break

        if swaps:
            log.info(
                "plan_swaps_result",
                planned=[
                    {
                        "slot": s.slot_index,
                        "old": s.old_tla,
                        "new": s.new_tla,
                        "improvement": round(s.score_improvement, 3),
                    }
                    for s in swaps
                ],
            )

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
        Evicts duplicates: if a sticky assigns a driver already on a non-sticky slot,
        that non-sticky slot is evicted and replaced with next-best driver.
        """
        swaps: list[SwapCommand] = []
        sticky_claimed_tlas: set[str] = set()
        on_screen_nums = {w.current_driver_number for w in current_windows if w.current_driver_number}
        on_screen_tlas = {w.current_tla.upper() for w in current_windows if w.current_tla}
        best_off_screen_score = 0.0
        for r in ranked_drivers:
            if r.driver_number not in on_screen_nums and r.tla.upper() not in on_screen_tlas:
                best_off_screen_score = r.total_score
                break

        sticky_slots = [
            s for s in current_windows
            if s.is_sticky and s.sticky_target
        ]

        for slot in sticky_slots:
            target_tla: str | None = None
            target_result: ScoringResult | None = None

            if slot.sticky_target.lower() == "leader":
                for r in ranked_drivers:
                    if r.tla.upper() not in sticky_claimed_tlas and r.breakdown.position_importance >= 0.8:
                        target_result = r
                        target_tla = r.tla
                        break
                if not target_tla:
                    for r in ranked_drivers:
                        if r.tla.upper() not in sticky_claimed_tlas:
                            target_result = r
                            target_tla = r.tla
                            break
            else:
                tla = slot.sticky_target.upper()
                if tla not in sticky_claimed_tlas:
                    for r in ranked_drivers:
                        if r.tla == tla:
                            target_result = r
                            target_tla = tla
                            break
                    if not target_result:
                        target_tla = tla
                else:
                    for r in ranked_drivers:
                        if r.tla.upper() not in sticky_claimed_tlas:
                            target_result = r
                            target_tla = r.tla
                            break

            if target_tla and target_tla.upper() != slot.current_tla.upper():
                leader_boring = False
                if slot.sticky_target and slot.sticky_target.lower() == "leader" and target_result:
                    leader_score = target_result.total_score
                    if best_off_screen_score - leader_score >= STICKY_OVERRIDE_THRESHOLD:
                        leader_boring = True
                if not leader_boring:
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
            if target_tla:
                sticky_claimed_tlas.add(target_tla.upper())

        all_assigned: set[str] = set(sticky_claimed_tlas)
        for slot in current_windows:
            if not slot.is_sticky and slot.current_tla:
                all_assigned.add(slot.current_tla.upper())

        non_sticky_slots = [
            w for w in current_windows
            if not w.is_sticky
        ]
        for slot in non_sticky_slots:
            tla_upper = (slot.current_tla or "").upper()
            if tla_upper in sticky_claimed_tlas:
                for r in ranked_drivers:
                    if r.tla.upper() not in all_assigned:
                        eviction = SwapCommand(
                            slot_index=slot.slot_index,
                            player_id=slot.player_id or 0,
                            old_tla=slot.current_tla,
                            new_tla=r.tla,
                            new_driver_number=r.driver_number,
                            score_improvement=0.0,
                        )
                        swaps.append(eviction)
                        all_assigned.add(r.tla.upper())
                        break

        return swaps

    def _prune_old_switches(self) -> None:
        cutoff = datetime.now(UTC) - timedelta(seconds=60)
        while self._recent_switches and self._recent_switches[0] < cutoff:
            self._recent_switches.popleft()
