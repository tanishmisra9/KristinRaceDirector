"""Record API data and orchestrator decisions when test mode is enabled."""

from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import structlog

from race_director.models.scoring import ScoringResult, SwapCommand, WindowSlot

log = structlog.get_logger()


def _json_default(obj: Any) -> Any:
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    if isinstance(obj, datetime):
        return obj.isoformat()
    return str(obj)


def _normalize_session_dir_name(meta: dict) -> str:
    """Build folder name: {year}_{country}_{session_type} (lowercase, underscores)."""
    year = meta.get("year")
    country = meta.get("country_name") or meta.get("circuit_short_name") or "unknown"
    session_type = (meta.get("session_type") or "").strip()
    if not session_type:
        name = (meta.get("session_name") or "").lower()
        if "sprint" in name and "shootout" not in name:
            session_type = "sprint"
        elif "qualifying" in name:
            session_type = "qualifying"
        else:
            session_type = "race"

    def norm(part: str) -> str:
        return "_".join(part.lower().replace(" ", "_").split())

    if year is None:
        return f"unknown_session_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}"
    return f"{year}_{norm(str(country))}_{norm(str(session_type))}"


class TestRecorder:
    """Records all data and decisions during a test session."""

    def __init__(self, base_dir: Path = Path("data")) -> None:
        self._base_dir = Path(base_dir)
        self._session_dir: Path | None = None
        self._swaps_file: Any = None
        self._events_file: Any = None
        self._initialized = False
        self._pending_unknown_dir: Path | None = None

    def init_session(self, session_meta: dict) -> None:
        """Create session directory and initialize files. Safe to call again to rename unknown folder."""
        try:
            name = _normalize_session_dir_name(session_meta)

            if self._initialized and self._pending_unknown_dir is None:
                return

            if not self._initialized:
                self._base_dir.mkdir(parents=True, exist_ok=True)
                if name.startswith("unknown_session_"):
                    self._session_dir = self._base_dir / name
                    self._pending_unknown_dir = self._session_dir
                else:
                    self._session_dir = self._base_dir / name
                    self._pending_unknown_dir = None
                assert self._session_dir is not None
                self._session_dir.mkdir(parents=True, exist_ok=True)
                (self._session_dir / "api_responses").mkdir(exist_ok=True)
                (self._session_dir / "scoring").mkdir(exist_ok=True)

                meta_path = self._session_dir / "session_meta.json"
                meta_path.write_text(
                    json.dumps(session_meta, indent=2, default=str),
                    encoding="utf-8",
                )

                self._swaps_file = open(self._session_dir / "swaps.jsonl", "a", encoding="utf-8")
                self._events_file = open(self._session_dir / "events.jsonl", "a", encoding="utf-8")
                self._initialized = True
                log.info("test_recorder_session", path=str(self._session_dir))
                return

            # Rename unknown_session_* once we have a proper folder name
            if self._pending_unknown_dir is not None and not name.startswith("unknown_session_"):
                new_dir = self._base_dir / name
                if new_dir != self._pending_unknown_dir:
                    if self._swaps_file:
                        self._swaps_file.close()
                    if self._events_file:
                        self._events_file.close()
                    shutil.move(str(self._pending_unknown_dir), str(new_dir))
                    self._session_dir = new_dir
                    self._pending_unknown_dir = None
                    (self._session_dir / "session_meta.json").write_text(
                        json.dumps(session_meta, indent=2, default=str),
                        encoding="utf-8",
                    )
                    self._swaps_file = open(self._session_dir / "swaps.jsonl", "a", encoding="utf-8")
                    self._events_file = open(self._session_dir / "events.jsonl", "a", encoding="utf-8")
                    log.info("test_recorder_renamed", path=str(self._session_dir))
        except Exception as e:
            log.warning("test_recorder_init_failed", error=str(e))

    def record_api_tick(self, tick: int, endpoint_data: dict[str, list[dict]]) -> None:
        """Save raw API responses for this tick."""
        if not self._session_dir:
            return
        try:
            path = self._session_dir / "api_responses" / f"tick_{tick:04d}.json"
            path.write_text(
                json.dumps(endpoint_data, indent=2, default=str),
                encoding="utf-8",
            )
        except Exception as e:
            log.warning("test_recorder_api_tick_failed", tick=tick, error=str(e))

    def record_scoring(
        self,
        tick: int,
        ranked: list[ScoringResult],
        windows: list[WindowSlot],
    ) -> None:
        """Save scoring results and current window state for this tick."""
        if not self._session_dir:
            return
        try:
            payload = {
                "tick": tick,
                "wall_time": datetime.now(UTC).isoformat(),
                "ranked": [r.model_dump() for r in ranked],
                "windows": [w.model_dump() for w in windows],
            }
            path = self._session_dir / "scoring" / f"tick_{tick:04d}.json"
            path.write_text(json.dumps(payload, indent=2, default=_json_default), encoding="utf-8")
        except Exception as e:
            log.warning("test_recorder_scoring_failed", tick=tick, error=str(e))

    def record_swap(
        self,
        tick: int,
        swap: SwapCommand,
        success: bool,
        reason: str = "",
    ) -> None:
        """Append a swap decision to swaps.jsonl."""
        if not self._swaps_file:
            return
        try:
            line = {
                "tick": tick,
                "wall_time": datetime.now(UTC).isoformat(),
                "slot_index": swap.slot_index,
                "old_tla": swap.old_tla,
                "new_tla": swap.new_tla,
                "score_improvement": swap.score_improvement,
                "success": success,
                "reason": reason,
            }
            self._swaps_file.write(json.dumps(line, default=str) + "\n")
            self._swaps_file.flush()
        except Exception as e:
            log.warning("test_recorder_swap_failed", tick=tick, error=str(e))

    def record_lead_change_swap(
        self,
        tick: int,
        slot_index: int,
        old_tla: str | None,
        new_tla: str,
        success: bool,
        reason: str = "",
    ) -> None:
        """Record lead-change swap (not a SwapCommand from hysteresis)."""
        if not self._swaps_file:
            return
        try:
            line = {
                "tick": tick,
                "wall_time": datetime.now(UTC).isoformat(),
                "kind": "lead_change",
                "slot_index": slot_index,
                "old_tla": old_tla,
                "new_tla": new_tla,
                "success": success,
                "reason": reason,
            }
            self._swaps_file.write(json.dumps(line, default=str) + "\n")
            self._swaps_file.flush()
        except Exception as e:
            log.warning("test_recorder_lead_swap_failed", tick=tick, error=str(e))

    def record_event(self, tick: int, event_type: str, detail: dict) -> None:
        """Append a key event to events.jsonl."""
        if not self._events_file:
            return
        try:
            line = {
                "tick": tick,
                "wall_time": datetime.now(UTC).isoformat(),
                "event_type": event_type,
                "detail": detail,
            }
            self._events_file.write(json.dumps(line, default=str) + "\n")
            self._events_file.flush()
        except Exception as e:
            log.warning("test_recorder_event_failed", tick=tick, error=str(e))

    @property
    def session_dir(self) -> Path | None:
        return self._session_dir

    @property
    def is_initialized(self) -> bool:
        return self._initialized

    def close(self) -> None:
        """Flush and close open file handles."""
        try:
            if self._swaps_file:
                self._swaps_file.close()
                self._swaps_file = None
            if self._events_file:
                self._events_file.close()
                self._events_file = None
        except Exception as e:
            log.warning("test_recorder_close_failed", error=str(e))
