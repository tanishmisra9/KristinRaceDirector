"""Dry-run MultiViewer adapter - logs only, no actual switches."""

from __future__ import annotations

from datetime import UTC, datetime

import structlog

from race_director.config.schema import MultiViewerConfig, StickySlotConfig
from race_director.models.scoring import WindowSlot

log = structlog.get_logger()


class DryRunAdapter:
    """Logs swap decisions without touching MultiViewer."""

    def __init__(
        self,
        config: MultiViewerConfig,
        sticky_slots: list[StickySlotConfig] | None = None,
        num_windows: int | None = None,
    ) -> None:
        self._config = config
        self._sticky_configs = sticky_slots or []
        self._slots: list[WindowSlot] = []
        count = num_windows or config.num_windows or 6
        sticky_by_slot = {c.slot: c for c in self._sticky_configs}
        for i in range(count):
            cfg = sticky_by_slot.get(i)
            slot = WindowSlot(
                slot_index=i,
                player_id=i + 1,
                current_tla="",
                current_driver_number=None,
                assigned_at=datetime.now(UTC),
                is_sticky=cfg is not None and cfg.driver is not None,
                sticky_target=cfg.driver if cfg else None,
            )
            self._slots.append(slot)

    def is_available(self) -> bool:
        return True

    def get_current_windows(self) -> list[WindowSlot]:
        return self._slots

    def switch_window(self, slot_index: int, new_tla: str, player_id: int | None = None) -> bool:
        if 0 <= slot_index < len(self._slots):
            old = self._slots[slot_index].current_tla
            self._slots[slot_index].current_tla = new_tla
            self._slots[slot_index].current_driver_number = None
            self._slots[slot_index].assigned_at = datetime.now(UTC)
            log.info("dry_run_swap", slot=slot_index, old=old, new=new_tla)
            return True
        return False

    def get_failed_tlas(self) -> set[str]:
        return set()
