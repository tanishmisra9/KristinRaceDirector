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
        sticky_swaps = self._hysteresis.resolve_sticky(windows, ranked)
        for swap in sticky_swaps:
            if self._adapter and self._adapter.switch_window(swap.slot_index, swap.new_tla):
                if swap.slot_index < len(windows):
                    windows[swap.slot_index].current_tla = swap.new_tla
                    windows[swap.slot_index].current_driver_number = swap.new_driver_number
                    windows[swap.slot_index].assigned_at = datetime.now(UTC)
                self._hysteresis.record_swaps(1)
        tla_map = {n: st.tla for n, st in states.items()}
        swaps = self._hysteresis.plan_swaps(windows, ranked, tla_map, session=session)
        for swap in swaps:
            if self._adapter and self._adapter.switch_window(swap.slot_index, swap.new_tla):
                if swap.slot_index < len(windows) and windows[swap.slot_index].current_driver_number:
                    self._scorer.record_removal(windows[swap.slot_index].current_driver_number)
                windows[swap.slot_index].current_tla = swap.new_tla
                windows[swap.slot_index].current_driver_number = swap.new_driver_number
                windows[swap.slot_index].assigned_at = datetime.now(UTC)
                self._hysteresis.record_swaps(1)
