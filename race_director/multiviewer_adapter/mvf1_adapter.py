"""MultiViewer for F1 adapter via mvf1 package."""

from __future__ import annotations

import time
from datetime import UTC, datetime

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

    def is_available(self) -> bool:
        try:
            from mvf1 import MultiViewerForF1
            mv = MultiViewerForF1()
            return bool(list(mv.players))
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
            self._player_ids = [p.id for p in players]
            slots = []
            for i, p in enumerate(players):
                cfg = self._sticky_configs[i] if i < len(self._sticky_configs) else None
                slots.append(
                    WindowSlot(
                        slot_index=i,
                        player_id=p.id,
                        current_tla=getattr(p, "title", ""),
                        current_driver_number=p.driver_data.get("driverNumber") if p.driver_data else None,
                        assigned_at=None,
                        is_sticky=cfg is not None and cfg.driver is not None,
                        sticky_target=cfg.driver if cfg else None,
                    )
                )
            return slots
        except Exception as e:
            log.warning("mvf1_get_windows_failed", error=str(e))
            return []

    def _find_commentary_player(self, mv):
        """Find F1 Live or International player (case-insensitive)."""
        for p in mv.players:
            title = (getattr(p, "stream_data", None) or {}).get("title", "")
            if title and title.upper().replace(" ", "") in ("F1LIVE", "INTERNATIONAL"):
                return p
        return None

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

    def switch_window(self, slot_index: int, new_tla: str) -> bool:
        try:
            from mvf1 import MultiViewerForF1
            mv = MultiViewerForF1()
            players = list(mv.players)
            if self._player_ids:
                players = [p for p in players if p.id in self._player_ids]
            if 0 <= slot_index < len(players):
                player = players[slot_index]
                bounds = (player.x, player.y, player.width, player.height)

                commentary = self._find_commentary_player(mv)
                target_time = None
                if commentary and hasattr(commentary, "state") and commentary.state:
                    target_time = commentary.state.get("interpolatedCurrentTime") or commentary.state.get("currentTime")

                player.switch_stream(new_tla)

                delay = self._config.sync_delay_sec
                retry_delays = [delay * 0.5, delay, delay * 1.5]
                for d in retry_delays:
                    time.sleep(d)
                    mv = MultiViewerForF1()
                    if self._sync_all_to_main_broadcast(mv):
                        break

                if target_time is not None:
                    time.sleep(0.3)
                    mv = MultiViewerForF1()
                    for p in mv.players:
                        if not self._is_onboard_feed(p):
                            continue
                        if (p.x, p.y, p.width, p.height) == bounds:
                            try:
                                p.seek(absolute=target_time)
                            except Exception:
                                pass
                            break

                return True
        except Exception as e:
            log.warning("mvf1_switch_failed", slot=slot_index, tla=new_tla, error=str(e))
        return False
