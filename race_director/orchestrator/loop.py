"""Main orchestrator event loop."""

from __future__ import annotations

import asyncio
import signal
from datetime import UTC, datetime
from pathlib import Path

import structlog

from race_director.battle_engine.scorer import BattleScorer
from race_director.config.schema import AppConfig
from race_director.data_provider.openf1_rest import OpenF1RestProvider
from race_director.orchestrator.hysteresis import HysteresisEngine

log = structlog.get_logger()


class Orchestrator:
    """Ties together data provider, scorer, hysteresis, and MultiViewer adapter."""

    def __init__(self, config: AppConfig, replay_path: str | None = None) -> None:
        self._config = config
        self._shutting_down = False
        self._provider = OpenF1RestProvider(config)
        self._scorer = BattleScorer(config.scoring_weights, config.scoring_params)
        self._hysteresis = HysteresisEngine(config.hysteresis)
        self._tick_count = 0
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
        ranked = self._scorer.score_all(
            states,
            windows,
            session=session,
            cooldown_seconds=self._config.hysteresis.removal_cooldown_seconds,
        )
        if self._tick_count % 6 == 0:
            log.info(
                "scoring_snapshot",
                top=[{"tla": r.tla, "score": round(r.total_score, 3)} for r in ranked[:5]],
            )
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
                    windows[swap.slot_index].assigned_at = datetime.now(UTC)
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
        swaps = self._hysteresis.plan_swaps(windows, ranked, tla_map, session=session)
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
                if swap.slot_index < len(windows) and windows[swap.slot_index].current_driver_number:
                    self._scorer.record_removal(windows[swap.slot_index].current_driver_number)
                windows[swap.slot_index].current_tla = swap.new_tla
                windows[swap.slot_index].current_driver_number = swap.new_driver_number
                windows[swap.slot_index].assigned_at = datetime.now(UTC)
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
