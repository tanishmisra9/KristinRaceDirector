"""Main orchestrator event loop."""

from __future__ import annotations

import asyncio
import json
import signal
import time
from datetime import UTC, datetime
from pathlib import Path

import structlog

from race_director import display
from race_director.battle_engine.scorer import BattleScorer
from race_director.config.schema import AppConfig
from race_director.data_provider.openf1_rest import OpenF1RestProvider
from race_director.data_provider.test_recorder import TestRecorder
from race_director.orchestrator.hysteresis import HysteresisEngine

log = structlog.get_logger()

# Fix #25: Heartbeat file location
HEARTBEAT_FILE = Path("/tmp/kristin_heartbeat")


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
        self._last_sc_phase: str = "none"
        self._consecutive_no_windows: int = 0
        # Fix #25: Watchdog tracking
        self._last_tick_completed_at: float = time.monotonic()
        self._last_swap_at: datetime | None = None
        # Fix #26: Track TLAs that recently failed swaps to avoid retrying immediately
        self._swap_cooldown: dict[str, int] = {}  # tla -> tick when cooldown expires
        self._adapter = None
        if config.orchestrator.dry_run:
            from race_director.multiviewer_adapter.dry_run import DryRunAdapter
            self._adapter = DryRunAdapter(config.multiviewer, config.sticky_slots)
        else:
            from race_director.multiviewer_adapter.mvf1_adapter import Mvf1Adapter
            self._adapter = Mvf1Adapter(config.multiviewer, config.sticky_slots)
        self._recorder: TestRecorder | None = None
        if config.orchestrator.test_mode:
            self._recorder = TestRecorder(Path(config.orchestrator.test_data_dir))
        # Fix #28: Warn once if no commentary time (replay cursor) available
        self._commentary_time_warned: bool = False
        self._last_session_key: int | None = None
        self._was_data_stale: bool = False

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
        for attempt in range(5):
            try:
                await self._provider.start()
                break
            except Exception as e:
                display.show_connection_retry(attempt + 1, str(e))
                await asyncio.sleep(5)
        else:
            display.show_connection_failed()
            return
        if self._config.orchestrator.quali_mode:
            display.show_quali_startup()
        elif self._config.orchestrator.monitor_mode:
            display.show_monitor_startup()
        elif self._adapter and self._adapter.is_available():
            windows = self._adapter.get_current_windows()
            log.info("windows_detected", count=len(windows))
            if windows:
                display.show_startup(len(windows))
                session_tlas = self._provider.get_session_tlas()
                if session_tlas:
                    display.show_driver_list(sorted(session_tlas))
            else:
                display.show_no_windows()
        else:
            display.show_multiviewer_not_found()
        try:
            while not self._shutting_down:
                # Fix #25: Watchdog - check if previous tick took too long
                tick_start = time.monotonic()
                elapsed_since_last = tick_start - self._last_tick_completed_at
                if elapsed_since_last > self._config.orchestrator.watchdog_timeout_sec:
                    log.critical("watchdog_tick_timeout", 
                                elapsed_sec=round(elapsed_since_last, 1),
                                timeout_threshold=self._config.orchestrator.watchdog_timeout_sec)
                
                await self._tick()
                
                # Fix #25: Update watchdog and write heartbeat
                self._last_tick_completed_at = time.monotonic()
                self._write_heartbeat()
                
                await asyncio.sleep(self._config.orchestrator.tick_interval_sec)
        except asyncio.CancelledError:
            pass
        finally:
            display.close_test_log()
            if self._recorder:
                self._recorder.close()
            await self._provider.stop()
            log.info("orchestrator_stopped")

    def _write_heartbeat(self) -> None:
        """Fix #25: Write heartbeat file for external monitoring."""
        try:
            heartbeat = {
                "tick": self._tick_count,
                "timestamp": datetime.now(UTC).isoformat(),
                "last_swap_at": self._last_swap_at.isoformat() if self._last_swap_at else None,
                "data_fresh": self._provider.is_data_fresh(),
            }
            HEARTBEAT_FILE.write_text(json.dumps(heartbeat))
        except Exception as e:
            log.debug("heartbeat_write_failed", error=str(e))

    async def _tick(self) -> None:
        self._tick_count += 1
        override_path = Path(self._config.orchestrator.manual_override_file)
        if override_path.exists():
            return
        commentary_time = None
        if self._adapter:
            commentary_time = await self._adapter.get_commentary_time()
        self._provider.set_replay_cursor(commentary_time)
        if commentary_time is None and not self._commentary_time_warned:
            log.warning(
                "no_commentary_time",
                hint="Open F1 Live player in MultiViewer for replay sync",
            )
            self._commentary_time_warned = True
        try:
            await self._provider.poll()
        except Exception:
            log.error("poll_failed", exc_info=True)
            if self._tick_count % 6 == 0:
                display.show_poll_error()
            return
        meta = self._provider.get_session_meta()
        raw_key = meta.get("session_key") if meta else None
        try:
            current_key: int | None = int(raw_key) if raw_key is not None else None
        except (TypeError, ValueError):
            current_key = None
        if current_key is not None and current_key != self._last_session_key:
            if self._last_session_key is not None:
                display.show_session_changed(self._last_session_key, current_key)
            self._last_session_key = current_key

        api_data: dict[str, list[dict]] = {}
        if self._config.orchestrator.test_mode and self._recorder:
            if meta:
                self._recorder.init_session(meta)
            elif not self._recorder.is_initialized:
                self._recorder.init_session({})
            if self._recorder.session_dir:
                display.set_test_log(self._recorder.session_dir / "terminal_output.log")
            api_data = self._provider.get_tick_api_data()
            self._recorder.record_api_tick(self._tick_count, api_data)
        
        # Fix #4: Skip scoring/swaps if data is stale
        if not self._provider.is_data_fresh():
            log.warning("data_stale_skipping_swaps", tick=self._tick_count)
            if self._config.orchestrator.monitor_mode or self._config.orchestrator.quali_mode:
                states = self._provider.get_driver_states()
                session = self._provider.get_session_info()
                sc_phase = self._provider.get_sc_phase()
                should_show_tick = False
                if not self._was_data_stale:
                    display.show_data_stale()
                    self._was_data_stale = True
                    should_show_tick = True
                elif self._tick_count % 30 == 0:
                    should_show_tick = True
                if should_show_tick:
                    display.show_monitor_tick(
                        tick=self._tick_count,
                        num_drivers=len(states),
                        session_type=session.session_type if session else "unknown",
                        lap=session.lap_number if session else 0,
                        data_fresh=False,
                        commentary_time=commentary_time,
                        sc_phase=sc_phase,
                        session_key=current_key,
                    )
            return

        if self._config.orchestrator.quali_mode:
            if self._was_data_stale:
                display.show_data_fresh_restored()
                self._was_data_stale = False
            states = self._provider.get_driver_states()
            session = self._provider.get_session_info()
            sc_phase = self._provider.get_sc_phase()
            ref_time = self._provider.get_reference_time()
            session_tlas = self._provider.get_session_tlas()
            excluded: set[str] = set()
            if session_tlas:
                all_tlas = {st.tla.upper() for st in states.values()}
                excluded = all_tlas - session_tlas
            ranked = self._scorer.score_all(
                states,
                [],
                session=session,
                cooldown_seconds=self._config.hysteresis.removal_cooldown_seconds,
                reference_time=ref_time,
                excluded_tlas=excluded,
            )
            if self._recorder:
                self._recorder.record_scoring(self._tick_count, ranked, [])
                self._recorder.record_quali_validation_tick(
                    tick=self._tick_count,
                    session_key=current_key,
                    data_fresh=self._provider.is_data_fresh(),
                    driver_state_count=len(states),
                    ranked_count=len(ranked),
                    qualifying_phase=self._provider.get_latest_qualifying_phase(),
                    session_result_rows=len(api_data.get("session_result", [])),
                )
            display.show_monitor_tick(
                tick=self._tick_count,
                num_drivers=len(states),
                session_type=session.session_type if session else "unknown",
                lap=session.lap_number if session else 0,
                data_fresh=True,
                commentary_time=commentary_time,
                sc_phase=sc_phase,
                session_key=current_key,
            )
            if self._tick_count % 6 == 0:
                display.show_scoring_snapshot([(r.tla, r.total_score) for r in ranked])
            if self._tick_count % 10 == 0:
                self._provider._log_endpoint_health()
            return

        if self._config.orchestrator.monitor_mode:
            if self._was_data_stale:
                display.show_data_fresh_restored()
                self._was_data_stale = False
            states = self._provider.get_driver_states()
            session = self._provider.get_session_info()
            sc_phase = self._provider.get_sc_phase()
            display.show_monitor_tick(
                tick=self._tick_count,
                num_drivers=len(states),
                session_type=session.session_type if session else "unknown",
                lap=session.lap_number if session else 0,
                data_fresh=True,
                commentary_time=commentary_time,
                sc_phase=sc_phase,
                session_key=current_key,
            )
            if self._tick_count % 10 == 0:
                self._provider._log_endpoint_health()
            return

        states = self._provider.get_driver_states()
        session = self._provider.get_session_info()
        if not states:
            log.debug("no_driver_states_yet")
            return
        windows = self._adapter.get_current_windows() if self._adapter else []
        if not windows:
            self._consecutive_no_windows += 1
            if self._consecutive_no_windows == 1 or self._consecutive_no_windows % 6 == 0:
                display.show_no_windows()
            return
        self._consecutive_no_windows = 0
        log.info(
            "tick_start",
            tick=self._tick_count,
            num_states=len(states),
            num_windows=len(windows),
        )
        ref_time = self._provider.get_reference_time()
        failed = self._adapter.get_failed_tlas() if self._adapter else set()
        session_tlas = self._provider.get_session_tlas()
        
        # Fix #26: Clean expired cooldowns and exclude cooled-down TLAs
        expired = [tla for tla, expire_tick in self._swap_cooldown.items() if self._tick_count >= expire_tick]
        for tla in expired:
            del self._swap_cooldown[tla]
        cooled_down = set(self._swap_cooldown.keys())
        
        # If we have a session driver list, exclude anyone not in it
        excluded = failed | cooled_down
        if session_tlas:
            all_tlas = {st.tla.upper() for st in states.values()}
            non_session = all_tlas - session_tlas
            excluded = failed | cooled_down | non_session
        ranked = self._scorer.score_all(
            states,
            windows,
            session=session,
            cooldown_seconds=self._config.hysteresis.removal_cooldown_seconds,
            reference_time=ref_time,
            excluded_tlas=excluded,
        )
        if self._recorder:
            self._recorder.record_scoring(self._tick_count, ranked, windows)
        if not self._lights_out_seen:
            if self._provider.is_lights_out():
                if self._recorder:
                    self._recorder.record_event(self._tick_count, "lights_out", {})
                self._lights_out_seen = True
                self._tick_count = 0  # Reset tick counter — grace period starts now
                display.show_lights_out()
            else:
                # Fix #27: Infer lights out from data in replay mode
                # If after 5 ticks we have valid position data but no SESSION STARTED event,
                # the race is already running and we should start managing cameras
                replay_implies_started = (
                    commentary_time is not None
                    and commentary_time > 30.0
                    and any(st.position > 0 for st in states.values())
                )
                delayed_fallback = self._tick_count > 5 and any(
                    st.position > 0 for st in states.values()
                )
                if replay_implies_started or delayed_fallback:
                    if self._recorder:
                        self._recorder.record_event(
                            self._tick_count, "lights_out_inferred", {}
                        )
                    self._lights_out_seen = True
                    self._tick_count = 0
                    log.info("lights_out_inferred_from_data")
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
            if self._recorder:
                self._recorder.record_event(
                    self._tick_count,
                    "grace_period",
                    {
                        "remaining_ticks": self._config.orchestrator.startup_grace_ticks
                        - self._tick_count,
                    },
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

        # Check SC phase for display notifications
        sc_phase = self._provider.get_sc_phase()
        if sc_phase != self._last_sc_phase:
            if sc_phase == "deployed":
                display.show_safety_car_deployed()
                if self._recorder:
                    self._recorder.record_event(
                        self._tick_count, "safety_car", {"phase": "deployed"}
                    )
            elif sc_phase == "ending":
                display.show_safety_car_ending()
                if self._recorder:
                    self._recorder.record_event(
                        self._tick_count, "safety_car", {"phase": "ending"}
                    )
            elif sc_phase == "green" and self._last_sc_phase in ("deployed", "ending"):
                display.show_racing_resumed()
                if self._recorder:
                    self._recorder.record_event(
                        self._tick_count, "safety_car", {"phase": "green"}
                    )
            self._last_sc_phase = sc_phase

        if is_neutralized:
            on_screen = [w.current_tla for w in windows if w.current_tla]
            if self._tick_count % 6 == 0:
                display.show_tick_status(self._tick_count, on_screen)
            log.info("tick_end", tick=self._tick_count, on_screen=on_screen, swaps_executed=0)
            return
        current_leader = None
        for num, state in states.items():
            if state.position == 1:
                current_leader = num
                break
        
        # Fix #10: Defer lead change swaps until after grace period + 3 ticks for stabilization
        lead_change_grace = self._config.orchestrator.startup_grace_ticks + 3
        if current_leader and self._last_leader and current_leader != self._last_leader:
            if self._tick_count > lead_change_grace:
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
                            old_lc = (
                                states[self._last_leader].tla
                                if self._last_leader in states
                                else None
                            )
                            if self._recorder:
                                self._recorder.record_event(
                                    self._tick_count,
                                    "lead_change",
                                    {"old_tla": old_lc, "new_tla": leader_tla},
                                )
                            # Fix #1: await async switch_window
                            ok = await self._adapter.switch_window(
                                worst_slot.slot_index,
                                leader_tla,
                                player_id=worst_slot.player_id,
                            )
                            if self._recorder:
                                self._recorder.record_lead_change_swap(
                                    self._tick_count,
                                    worst_slot.slot_index,
                                    old_lc,
                                    leader_tla,
                                    ok,
                                    "" if ok else "switch_failed",
                                )
                            if ok:
                                if worst_slot.slot_index < len(windows) and windows[worst_slot.slot_index].current_driver_number:
                                    self._scorer.record_removal(windows[worst_slot.slot_index].current_driver_number, removed_at=ref_time)
                                windows[worst_slot.slot_index].current_tla = leader_tla
                                windows[worst_slot.slot_index].current_driver_number = current_leader
                                windows[worst_slot.slot_index].assigned_at = ref_time
                                self._hysteresis.record_swaps(1)
                            else:
                                # Fix #26: Lead-change swap failed - add cooldown
                                log.warning("lead_change_swap_failed", new_tla=leader_tla, slot=worst_slot.slot_index)
                                self._swap_cooldown[leader_tla.upper()] = self._tick_count + 3
        # Always update last_leader (during grace period too, just don't trigger swaps)
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
            # Fix #1: await async switch_window
            ok = bool(
                self._adapter
                and await self._adapter.switch_window(
                    swap.slot_index, swap.new_tla, player_id=swap.player_id
                )
            )
            if self._recorder:
                self._recorder.record_swap(
                    self._tick_count,
                    swap,
                    ok,
                    "" if ok else "sticky_swap_failed",
                )
            if ok:
                if swap.slot_index < len(windows):
                    windows[swap.slot_index].current_tla = swap.new_tla
                    windows[swap.slot_index].current_driver_number = swap.new_driver_number
                    windows[swap.slot_index].assigned_at = ref_time
                self._hysteresis.record_swaps(1)
            else:
                # Fix #9: Log sticky swap failure without updating window state
                # Fix #26: Add cooldown for failed sticky swap
                log.warning("sticky_swap_failed", slot=swap.slot_index, new_tla=swap.new_tla)
                self._swap_cooldown[swap.new_tla.upper()] = self._tick_count + 3
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
            # Fix #1: await async switch_window
            ok = bool(
                self._adapter
                and await self._adapter.switch_window(
                    swap.slot_index, swap.new_tla, player_id=swap.player_id
                )
            )
            if self._recorder:
                self._recorder.record_swap(
                    self._tick_count,
                    swap,
                    ok,
                    "" if ok else "plan_swap_failed",
                )
            if ok:
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
                # Fix #25: Track last swap time for heartbeat (use ref_time for consistency)
                self._last_swap_at = ref_time
            else:
                # Fix #26: Add cooldown for failed swap
                log.warning(
                    "swap_failed",
                    slot=swap.slot_index,
                    new_tla=swap.new_tla,
                )
                self._swap_cooldown[swap.new_tla.upper()] = self._tick_count + 3
        on_screen = [w.current_tla for w in windows if w.current_tla]
        log.info(
            "tick_end",
            tick=self._tick_count,
            on_screen=on_screen,
            swaps_executed=swaps_executed,
        )
        if self._tick_count % 6 == 0 or swaps_executed > 0:
            display.show_tick_status(self._tick_count, on_screen)
