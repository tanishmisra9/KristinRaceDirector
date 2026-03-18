"""MultiViewer for F1 adapter via mvf1 package."""

from __future__ import annotations

import math
import time
from datetime import UTC, datetime

import httpx
import structlog

from race_director.config.schema import MultiViewerConfig, StickySlotConfig
from race_director.models.scoring import WindowSlot

log = structlog.get_logger()


class Mvf1Adapter:
    """Controls MultiViewer via mvf1 package."""

    def __init__(
        self,
        config: MultiViewerConfig,
        sticky_slots: list[StickySlotConfig] | None = None,
    ) -> None:
        self._config = config
        self._sticky_configs = sticky_slots or []
        self._slot_assignments: dict[int, str] = {}  # slot_index -> player_id (str)
        self._slot_state: dict[int, WindowSlot] = {}  # slot_index -> WindowSlot for assigned_at
        self._initialized: bool = False
        self._schema_discovered: bool = False

    def _discover_mutations(self) -> None:
        """Log available GraphQL mutations for debugging."""
        if self._schema_discovered:
            return
        try:
            result = self._graphql_request("""
                query {
                    __schema {
                        mutationType {
                            fields {
                                name
                                args {
                                    name
                                    type {
                                        name
                                        kind
                                        ofType { name }
                                    }
                                }
                            }
                        }
                    }
                }
            """)
            data = result.get("data", {}).get("__schema", {}).get("mutationType", {})
            mutations = data.get("fields", [])
            log.info("multiviewer_mutations", names=[m["name"] for m in mutations])
            seek = next((m for m in mutations if m["name"] == "playerSeekTo"), None)
            if seek:
                log.info("playerSeekTo_args", args=seek.get("args", []))
            else:
                log.warning("playerSeekTo_not_in_schema")
            self._schema_discovered = True
        except Exception as e:
            log.debug("schema_discovery_failed", error=str(e))

    def is_available(self) -> bool:
        try:
            from mvf1 import MultiViewerForF1
            mv = MultiViewerForF1()
            available = bool(list(mv.players))
            if available:
                self._discover_mutations()
            return available
        except Exception:
            return False

    def _is_onboard_feed(self, player) -> bool:
        """True if player is a driver onboard feed (excludes F1 Live, International, etc.)."""
        if not getattr(player, "driver_data", None):
            return False
        return bool(player.driver_data and player.driver_data.get("tla"))

    def _get_onboard_players(self, mv=None):
        """Fetch and filter onboard players from MultiViewer."""
        if mv is None:
            from mvf1 import MultiViewerForF1
            mv = MultiViewerForF1()
        players = [p for p in mv.players if self._is_onboard_feed(p)]
        if self._config.player_ids:
            config_ids_str = {str(pid) for pid in self._config.player_ids}
            players = [p for p in players if str(p.id) in config_ids_str]
        elif self._config.num_windows is not None:
            players = sorted(players, key=lambda p: str(p.id))[: self._config.num_windows]
        return players

    def get_current_windows(self) -> list[WindowSlot]:
        try:
            from mvf1 import MultiViewerForF1
            mv = MultiViewerForF1()
            players = self._get_onboard_players(mv)
            # Only onboard driver feeds - never touch F1 Live, International, Data Channel, etc.
            player_by_id = {str(p.id): p for p in players}
            current_player_ids = set(player_by_id.keys())
            if not self._initialized or not self._slot_assignments:
                sorted_players = sorted(players, key=lambda p: str(p.id))
                self._slot_assignments = {i: str(p.id) for i, p in enumerate(sorted_players)}
                self._initialized = True
            else:
                dead_slots = [s for s, pid in self._slot_assignments.items() if pid not in current_player_ids]
                for s in dead_slots:
                    del self._slot_assignments[s]
                assigned_pids = set(self._slot_assignments.values())
                unassigned_pids = current_player_ids - assigned_pids
                used_slots = set(self._slot_assignments.keys())
                max_slot = max(used_slots) if used_slots else -1
                available_slots = sorted(set(range(max_slot + 2)) - used_slots)
                for pid in sorted(unassigned_pids):
                    slot_idx = available_slots.pop(0) if available_slots else max(self._slot_assignments.keys(), default=-1) + 1
                    self._slot_assignments[slot_idx] = pid
            slots = []
            for slot_idx in sorted(self._slot_assignments.keys()):
                pid = self._slot_assignments[slot_idx]
                p = player_by_id.get(pid)
                if p is None:
                    continue
                cfg = self._sticky_configs[slot_idx] if slot_idx < len(self._sticky_configs) else None
                current_driver_number = p.driver_data.get("driverNumber") if p.driver_data else None
                assigned_at = None
                prev = self._slot_state.get(slot_idx)
                if prev and prev.current_driver_number == current_driver_number and prev.assigned_at:
                    assigned_at = prev.assigned_at
                else:
                    assigned_at = datetime.now(UTC)
                slot = WindowSlot(
                    slot_index=slot_idx,
                    player_id=int(pid) if pid.isdigit() else 0,
                    current_tla=getattr(p, "title", ""),
                    current_driver_number=current_driver_number,
                    assigned_at=assigned_at,
                    is_sticky=cfg is not None and cfg.driver is not None,
                    sticky_target=cfg.driver if cfg else None,
                )
                slots.append(slot)
                self._slot_state[slot_idx] = slot
            active_slots = set(self._slot_assignments.keys())
            for stale in [k for k in self._slot_state if k not in active_slots]:
                del self._slot_state[stale]
            return slots
        except Exception as e:
            log.warning("mvf1_get_windows_failed", error=str(e))
            return []

    def _find_commentary_player(self, mv):
        """Find F1 Live or International player (case-insensitive)."""
        for p in mv.players:
            title = (getattr(p, "stream_data", None) or {}).get("title", "")
            if title and title.upper().replace(" ", "") in ("F1LIVE", "INTERNATIONAL"):
                log.info("commentary_player_found", title=title, player_id=p.id)
                return p
        all_titles = [
            (getattr(p, "stream_data", None) or {}).get("title", "???")
            for p in mv.players
        ]
        log.warning("commentary_player_not_found", available_titles=all_titles)
        return None

    def _graphql_request(self, query: str, variables: dict | None = None) -> dict:
        """POST GraphQL request to MultiViewer API."""
        payload: dict = {"query": query}
        if variables:
            payload["variables"] = variables
        try:
            r = httpx.post(
                self._config.uri,
                json=payload,
                timeout=5.0,
            )
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log.warning("graphql_request_failed", error=str(e))
            raise

    def _sync_player_to_time(self, player_id: int | str, target_time: float) -> bool:
        """Seek a player to absolute time via GraphQL playerSeekTo mutation."""
        query = """
        mutation PlayerSeekTo($id: ID!, $absolute: Float) {
            playerSeekTo(id: $id, absolute: $absolute)
        }
        """
        try:
            result = self._graphql_request(
                query,
                variables={"id": str(player_id), "absolute": target_time},
            )
            if result.get("errors"):
                log.warning(
                    "playerSeekTo_errors",
                    player_id=player_id,
                    errors=result["errors"],
                )
                return False
            return True
        except Exception:
            return False

    def _sync_via_player_sync(self, commentary_player_id: str) -> bool:
        """Sync all players to commentary via playerSync mutation (same as pressing S)."""
        query = """
        mutation PlayerSync($id: ID!) {
            playerSync(id: $id)
        }
        """
        try:
            result = self._graphql_request(
                query,
                variables={"id": str(commentary_player_id)},
            )
            if result.get("errors"):
                log.warning("playerSync_errors", errors=result["errors"])
                return False
            return True
        except Exception as e:
            log.warning("playerSync_failed", error=str(e))
            return False

    def _sync_all_to_main_broadcast(self, mv) -> bool:
        """Sync all players to main broadcast. Retries with backoff."""
        commentary = self._find_commentary_player(mv)
        if not commentary:
            log.warning("no_commentary_player_found", hint="Ensure F1 Live or International is open")
            return False
        try:
            commentary.sync()
            return True
        except Exception as e:
            log.warning("sync_to_commentary_failed", error=str(e))
            return False

    def switch_window(self, slot_index: int, new_tla: str, player_id: int | None = None) -> bool:
        try:
            from mvf1 import MultiViewerForF1
            mv = MultiViewerForF1()
            players = self._get_onboard_players(mv)
            player_by_id = {str(p.id): p for p in players}

            player = None
            pid_str = str(player_id) if player_id is not None else None
            if pid_str and pid_str in player_by_id:
                player = player_by_id[pid_str]
            elif slot_index in self._slot_assignments:
                assigned_pid = self._slot_assignments[slot_index]
                player = player_by_id.get(assigned_pid)
            if player is None:
                sorted_players = sorted(players, key=lambda p: str(p.id))
                if 0 <= slot_index < len(sorted_players):
                    player = sorted_players[slot_index]
            if player is None:
                if pid_str:
                    log.warning(
                        "player_id_not_found",
                        player_id=player_id,
                        available=[str(p.id) for p in players],
                    )
                log.warning("no_player_for_switch", slot=slot_index, player_id=player_id)
                return False

            old_player_id = str(player.id)

            commentary = self._find_commentary_player(mv)
            target_time = None
            if commentary and hasattr(commentary, "state") and commentary.state:
                target_time = commentary.state.get("interpolatedCurrentTime") or commentary.state.get("currentTime")
            if target_time is not None and (math.isnan(target_time) or math.isinf(target_time)):
                log.warning("commentary_time_invalid", target_time=target_time)
                target_time = None

            old_ids = {str(p.id) for p in players}
            player.switch_stream(new_tla)

            time.sleep(self._config.sync_delay_sec)
            mv = MultiViewerForF1()
            new_players = self._get_onboard_players(mv)

            new_player = None
            for p in sorted(new_players, key=lambda x: str(x.id), reverse=True):
                if str(p.id) not in old_ids:
                    new_player = p
                    break
            if new_player is None and new_players:
                new_player = new_players[-1]

            if new_player:
                new_pid = str(new_player.id)
                self._slot_assignments[slot_index] = new_pid
                log.info(
                    "slot_assignment_updated",
                    slot=slot_index,
                    old_player_id=old_player_id,
                    new_player_id=new_pid,
                )

            sync_success = False
            sync_path = "none"
            if new_player and target_time is not None:
                seek_ok = self._sync_player_to_time(new_player.id, target_time)
                commentary_id = str(commentary.id) if commentary else None
                if commentary_id:
                    sync_ok = self._sync_via_player_sync(commentary_id)
                    sync_success = sync_ok
                    sync_path = "seek_then_playerSync" if seek_ok else "playerSync_only"
                else:
                    try:
                        mv.player_sync_to_commentary()
                        sync_success = True
                        sync_path = "seek_then_global" if seek_ok else "global_only"
                    except Exception as e:
                        log.warning("sync_fallback_failed", error=str(e))
                        sync_success = seek_ok
                        sync_path = "seek_only"
                log.info(
                    "sync_result",
                    player_id=new_player.id,
                    target_time=target_time,
                    success=sync_success,
                    path=sync_path,
                )

            log.info(
                "mvf1_switch_success",
                slot=slot_index,
                new_tla=new_tla,
                player_id=new_player.id if new_player else 0,
            )
            return True
        except Exception as e:
            log.warning("mvf1_switch_failed", slot=slot_index, tla=new_tla, error=str(e))
        return False
