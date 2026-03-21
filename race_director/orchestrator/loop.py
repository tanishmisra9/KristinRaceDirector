"""Main orchestrator event loop."""

from __future__ import annotations

import asyncio
import signal
from datetime import datetime
from pathlib import Path

import structlog

from race_director import display
from race_director.battle_engine.scorer import BattleScorer
from race_director.config.schema import AppConfig
from race_director.data_provider.openf1_rest import OpenF1RestProvider
from race_director.orchestrator.hysteresis import HysteresisEngine

log = structlog.get_logger()


class Orchestrator:
    """Ties together data provider, scorer, hysteresis, and MultiViewer adapter."""

    def __init__(self, config: AppConfig) -> None:
        self._config = config
        self._shutting_down = False
        self._provider = OpenF1RestProvider(config)
        self._scorer = BattleScorer(config.scoring_weights, config.scoring_params)
        self._hysteresis = HysteresisEngine(config.hysteresis)
        self._tick_count = 0
        self._lights_out_seen: bool = False
        self._last_leader: int | None = None
        self._adapter = None
        if config.orchestrator.dry_run:
            from race_director.multiviewer_adapter.dry_run import DryRunAdapter
            self._adapter = DryRunAdapter(config.multiviewer, config.sticky_slots)
        else:
            from race_director.multiviewer_adapter.mvf1_adapter import Mvf1Adapter
            self._adapter = Mvf1Adapter(config.multiviewer, config.sticky_slots)

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        def shutdown():
            self._shutting_down = True
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, shutdown)
            except NotImplementedError:
                pass
        log.info("orchestrator_starting")
        await self._provider.start()
        if self._adapter and self._adapter.is_available():
            windows = self._adapter.get_current_windows()
            log.info("windows_detected", count=len(windows))
            display.show_startup(len(windows))
            session_tlas = self._provider.get_session_tlas()
            if session_tlas:
                display.show_driver_list(sorted(session_tlas))
        try:
            while not self._shutting_down:
                await self._tick()
                await asyncio.sleep(self._config.orchestrator.tick_interval_sec)
        except asyncio.CancelledError:
            pass
        finally:
            await self._provider.stop()
            log.info("orchestrator_stopped")

    async def _tick(self) -> None:
        self._tick_count += 1
        override_path = Path(self._config.orchestrator.manual_override_file)
        if override_path.exists():
            return
        try:
            await self._provider.poll()
        except Exception:
            log.error("poll_failed", exc_info=True)
            return
        states = self._provider.get_driver_states()
        session = self._provider.get_session_info()
        if not states:
            log.debug("no_driver_states_yet")
            return
        windows = self._adapter.get_current_windows() if self._adapter else []
        if not windows:
            return
        log.info(
            "tick_start",
            tick=self._tick_count,
            num_states=len(states),
            num_windows=len(windows),
        )
        ref_time = self._provider.get_reference_time()
        failed = self._adapter.get_failed_tlas() if self._adapter else set()
        session_tlas = self._provider.get_session_tlas()
        # If we have a session driver list, exclude anyone not in it
        excluded = failed
        if session_tlas:
            all_tlas = {st.tla.upper() for st in states.values()}
            non_session = all_tlas - session_tlas
            excluded = failed | non_session
        ranked = self._scorer.score_all(
            states,
            windows,
            session=session,
            cooldown_seconds=self._config.hysteresis.removal_cooldown_seconds,
            reference_time=ref_time,
            excluded_tlas=excluded,
        )
        if not self._lights_out_seen:
            if self._provider.is_lights_out():
                self._lights_out_seen = True
                self._tick_count = 0  # Reset tick counter — grace period starts now
                display.show_lights_out()
            else:
                # Still waiting for lights out — don't process swaps yet
                on_screen = [w.current_tla for w in windows if w.current_tla]
                if self._tick_count % 6 == 0:
                    display.show_waiting_for_start()
                log.info("tick_end", tick=self._tick_count, on_screen=on_screen, swaps_executed=0)
                return
        if self._tick_count <= self._config.orchestrator.startup_grace_ticks:
            log.info(
                "startup_grace_period",
                tick=self._tick_count,
                remaining=self._config.orchestrator.startup_grace_ticks - self._tick_count,
            )
            display.show_grace_period(
                self._tick_count,
                self._config.orchestrator.startup_grace_ticks - self._tick_count,
            )
            on_screen = [w.current_tla for w in windows if w.current_tla]
            log.info("tick_end", tick=self._tick_count, on_screen=on_screen, swaps_executed=0)
            return
        # Freeze swaps during Safety Car, VSC, or Red Flag
        is_neutralized = False
        for state in states.values():
            if state.safety_car_active or state.vsc_active:
                is_neutralized = True
                break
        if session and session.status in ("Inactive", "Ended"):
            is_neutralized = True
        if is_neutralized:
            on_screen = [w.current_tla for w in windows if w.current_tla]
            if self._tick_count % 6 == 0:
                display.show_neutralized()
                display.show_tick_status(self._tick_count, on_screen)
            log.info("tick_end", tick=self._tick_count, on_screen=on_screen, swaps_executed=0)
            return
        current_leader = None
        for num, state in states.items():
            if state.position == 1:
                current_leader = num
                break
        if current_leader and self._last_leader and current_leader != self._last_leader:
            leader_tla = states[current_leader].tla if current_leader in states else None
            if leader_tla:
                already_on_screen = any(
                    w.current_tla.upper() == leader_tla.upper() for w in windows if w.current_tla
                )
                if not already_on_screen:
                    ranked_by_num = {r.driver_number: r for r in ranked}
                    worst_slot = None
                    worst_score = float("inf")
                    for w in windows:
                        if w.current_driver_number and w.current_driver_number in ranked_by_num:
                            score = ranked_by_num[w.current_driver_number].total_score
                            if score < worst_score:
                                worst_score = score
                                worst_slot = w
                    if worst_slot and self._adapter:
                        log.info(
                            "lead_change_detected",
                            old_leader=self._last_leader,
                            new_leader=current_leader,
                            new_tla=leader_tla,
                            replacing_slot=worst_slot.slot_index,
                        )
                        display.show_lead_change(
                            states[self._last_leader].tla if self._last_leader in states else None,
                            leader_tla,
                        )
                        if self._adapter.switch_window(
                            worst_slot.slot_index,
                            leader_tla,
                            player_id=worst_slot.player_id,
                        ):
                            if worst_slot.slot_index < len(windows) and windows[worst_slot.slot_index].current_driver_number:
                                self._scorer.record_removal(windows[worst_slot.slot_index].current_driver_number, removed_at=ref_time)
                            windows[worst_slot.slot_index].current_tla = leader_tla
                            windows[worst_slot.slot_index].current_driver_number = current_leader
                            windows[worst_slot.slot_index].assigned_at = ref_time
                            self._hysteresis.record_swaps(1)
        self._last_leader = current_leader
        if self._tick_count % 6 == 0:
            log.info(
                "scoring_snapshot",
                top=[{"tla": r.tla, "score": round(r.total_score, 3)} for r in ranked[:5]],
            )
            display.show_scoring_snapshot([(r.tla, r.total_score) for r in ranked])
            if ranked:
                top = ranked[0]
                log.info(
                    "top_driver_breakdown",
                    tla=top.tla,
                    interval_ahead=round(top.breakdown.interval_ahead, 3),
                    interval_behind=round(top.breakdown.interval_behind, 3),
                    overtake_recency=round(top.breakdown.overtake_recency, 3),
                    position_gain=round(top.breakdown.position_gain, 3),
                    session_phase=round(top.breakdown.session_phase, 3),
                    position_importance=round(top.breakdown.position_importance, 3),
                    screen_time_penalty=round(top.breakdown.screen_time_penalty, 3),
                    incident_recovery=round(top.breakdown.incident_recovery, 3),
                )
        has_sticky_targets = any(
            w.is_sticky and w.sticky_target for w in windows
        )
        if has_sticky_targets:
            sticky_swaps = self._hysteresis.resolve_sticky(windows, ranked)
        else:
            sticky_swaps = []
        for swap in sticky_swaps:
            if self._adapter and self._adapter.switch_window(swap.slot_index, swap.new_tla, player_id=swap.player_id):
                if swap.slot_index < len(windows):
                    windows[swap.slot_index].current_tla = swap.new_tla
                    windows[swap.slot_index].current_driver_number = swap.new_driver_number
                    windows[swap.slot_index].assigned_at = ref_time
                self._hysteresis.record_swaps(1)
        if sticky_swaps:
            log.info(
                "sticky_swaps_executed",
                swaps=[
                    {"slot": s.slot_index, "old": s.old_tla, "new": s.new_tla}
                    for s in sticky_swaps
                ],
            )
        tla_map = {n: st.tla for n, st in states.items()}
        swaps = self._hysteresis.plan_swaps(windows, ranked, tla_map, session=session, reference_time=ref_time)
        swaps_executed = 0
        for swap in swaps:
            if self._adapter and self._adapter.switch_window(swap.slot_index, swap.new_tla, player_id=swap.player_id):
                log.info(
                    "swap_executed",
                    slot=swap.slot_index,
                    old_tla=swap.old_tla,
                    new_tla=swap.new_tla,
                    score_improvement=round(swap.score_improvement, 3),
                )
                display.show_swap(swap.old_tla, swap.new_tla, swap.slot_index)
                if swap.slot_index < len(windows) and windows[swap.slot_index].current_driver_number:
                    self._scorer.record_removal(windows[swap.slot_index].current_driver_number, removed_at=ref_time)
                windows[swap.slot_index].current_tla = swap.new_tla
                windows[swap.slot_index].current_driver_number = swap.new_driver_number
                windows[swap.slot_index].assigned_at = ref_time
                self._hysteresis.record_swaps(1)
                swaps_executed += 1
            else:
                log.warning(
                    "swap_failed",
                    slot=swap.slot_index,
                    new_tla=swap.new_tla,
                )
        on_screen = [w.current_tla for w in windows if w.current_tla]
        log.info(
            "tick_end",
            tick=self._tick_count,
            on_screen=on_screen,
            swaps_executed=swaps_executed,
        )
        if self._tick_count % 6 == 0 or swaps_executed > 0:
            display.show_tick_status(self._tick_count, on_screen)
