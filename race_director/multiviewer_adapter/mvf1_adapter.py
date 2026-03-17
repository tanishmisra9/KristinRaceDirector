"""MultiViewer for F1 adapter via mvf1 package."""

from __future__ import annotations

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
        self._player_ids: list[int] = []
        self._slot_state: dict[int, WindowSlot] = {}
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

    def get_current_windows(self) -> list[WindowSlot]:
        try:
            from mvf1 import MultiViewerForF1
            mv = MultiViewerForF1()
            players = list(mv.players)
            # Only onboard driver feeds - never touch F1 Live, International, Data Channel, etc.
            players = [p for p in players if self._is_onboard_feed(p)]
            if self._config.player_ids:
                players = [p for p in players if p.id in self._config.player_ids]
            elif self._config.num_windows is not None:
                players = players[: self._config.num_windows]
            players = sorted(players, key=lambda p: p.id)
            self._player_ids = [p.id for p in players]
            slots = []
            for i, p in enumerate(players):
                cfg = self._sticky_configs[i] if i < len(self._sticky_configs) else None
                current_driver_number = p.driver_data.get("driverNumber") if p.driver_data else None
                assigned_at = None
                prev = self._slot_state.get(p.id)
                if prev and prev.current_driver_number == current_driver_number and prev.assigned_at:
                    assigned_at = prev.assigned_at
                else:
                    assigned_at = datetime.now(UTC)
                slot = WindowSlot(
                    slot_index=i,
                    player_id=p.id,
                    current_tla=getattr(p, "title", ""),
                    current_driver_number=current_driver_number,
                    assigned_at=assigned_at,
                    is_sticky=cfg is not None and cfg.driver is not None,
                    sticky_target=cfg.driver if cfg else None,
                )
                slots.append(slot)
                self._slot_state[p.id] = slot
            current_player_ids = {p.id for p in players}
            for stale_key in [k for k in self._slot_state if k not in current_player_ids]:
                del self._slot_state[stale_key]
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

    def _sync_player_to_time(self, player_id: int, target_time: float) -> bool:
        """Seek a player to absolute time via GraphQL playerSeekTo mutation."""
        query = """
        mutation PlayerSeekTo($id: Int!, $absolute: Float) {
            playerSeekTo(id: $id, absolute: $absolute)
        }
        """
        try:
            result = self._graphql_request(
                query,
                variables={"id": player_id, "absolute": target_time},
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
            players = sorted(
                [p for p in mv.players if self._is_onboard_feed(p)],
                key=lambda p: p.id,
            )
            if self._config.player_ids:
                players = [p for p in players if p.id in self._config.player_ids]
            elif self._config.num_windows is not None:
                players = players[: self._config.num_windows]

            player = None
            if player_id is not None:
                player = next((p for p in players if p.id == player_id), None)
                if player is None:
                    log.warning(
                        "player_id_not_found",
                        player_id=player_id,
                        available=[p.id for p in players],
                    )
            if player is None and 0 <= slot_index < len(players):
                player = players[slot_index]
            if player is None:
                log.warning("no_player_for_switch", slot=slot_index, player_id=player_id)
                return False

            commentary = self._find_commentary_player(mv)
            target_time = None
            if commentary and hasattr(commentary, "state") and commentary.state:
                target_time = commentary.state.get("interpolatedCurrentTime") or commentary.state.get("currentTime")

            old_ids = set(self._player_ids)
            player.switch_stream(new_tla)

            time.sleep(self._config.sync_delay_sec)
            mv = MultiViewerForF1()
            new_players = sorted(
                [p for p in mv.players if self._is_onboard_feed(p)],
                key=lambda p: p.id,
            )
            if self._config.player_ids:
                new_players = [p for p in new_players if p.id in self._config.player_ids]
            elif self._config.num_windows is not None:
                new_players = new_players[: self._config.num_windows]

            new_player = None
            for p in sorted(new_players, key=lambda x: x.id, reverse=True):
                if p.id not in old_ids:
                    new_player = p
                    break
            if new_player is None and new_players:
                new_player = new_players[-1]

            sync_success = False
            sync_path = "none"
            if new_player and target_time is not None:
                sync_success = self._sync_player_to_time(new_player.id, target_time)
                sync_path = "graphql"
                if not sync_success and self._config.sync_strategy == "global_fallback":
                    sync_success = self._sync_all_to_main_broadcast(mv)
                    sync_path = "global_fallback"
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
