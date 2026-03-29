"""Human-readable terminal output for non-technical users."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import TextIO

_test_log_file: TextIO | None = None
_test_log_path: Path | None = None


def set_test_log(path: Path | None) -> None:
    """When set, mirror all display lines to this file (test mode)."""
    global _test_log_file, _test_log_path
    if path is None:
        close_test_log()
        return
    if _test_log_path == path.resolve() and _test_log_file is not None:
        return
    close_test_log()
    path.parent.mkdir(parents=True, exist_ok=True)
    _test_log_path = path.resolve()
    _test_log_file = open(path, "a", encoding="utf-8")


def close_test_log() -> None:
    """Close the test-mode terminal log file."""
    global _test_log_file, _test_log_path
    if _test_log_file:
        try:
            _test_log_file.close()
        except OSError:
            pass
        _test_log_file = None
    _test_log_path = None


def _print_and_log(msg: str) -> None:
    print(msg, flush=True)
    if _test_log_file:
        _test_log_file.write(msg + "\n")
        _test_log_file.flush()


def _ts() -> str:
    """Short timestamp for display: HH:MM:SS AM/PM."""
    return datetime.now().strftime("%I:%M:%S %p")


def show_tick_status(tick: int, on_screen: list[str]) -> None:
    """Show current onboard lineup."""
    drivers = ", ".join(on_screen) if on_screen else "(none)"
    _print_and_log(f"[{_ts()}]  Showing: {drivers}")


def show_swap(old_tla: str, new_tla: str, slot: int) -> None:
    """Show a camera swap."""
    _print_and_log(f"[{_ts()}]  Swap: {old_tla} -> {new_tla} (window {slot + 1})")


def show_sync_result(tla: str, success: bool, drift: float | None = None) -> None:
    """Show sync result."""
    if success:
        drift_str = f" ({drift:.1f}s drift)" if drift is not None else ""
        _print_and_log(f"[{_ts()}]  Synced {tla}{drift_str}")
    else:
        _print_and_log(f"[{_ts()}]  Sync failed for {tla}")


def show_lead_change(old_tla: str | None, new_tla: str) -> None:
    """Show a lead change."""
    if old_tla:
        _print_and_log(f"[{_ts()}]  Lead change: {old_tla} -> {new_tla}")
    else:
        _print_and_log(f"[{_ts()}]  Leader: {new_tla}")


def show_scoring_snapshot(ranked: list[tuple[str, float]], count: int = 5) -> None:
    """Show top-N driver scores in a readable line."""
    parts = [f"{tla} {score:.1f}" for tla, score in ranked[:count]]
    _print_and_log(f"[{_ts()}]  Top {count} Scores: {', '.join(parts)}")


def show_startup(num_windows: int) -> None:
    """Show startup info."""
    _print_and_log(
        f"[{_ts()}]  Ready - managing {num_windows} onboard window{'s' if num_windows != 1 else ''}"
    )


def show_grace_period(tick: int, remaining: int) -> None:
    """Show grace period countdown."""
    _print_and_log(
        f"[{_ts()}]  Grace period: {remaining} tick{'s' if remaining != 1 else ''} remaining (preserving your cameras)"
    )


def show_no_swaps_needed() -> None:
    """Show that current lineup is optimal."""
    pass  # Stay quiet when nothing is happening — less noise


def show_lights_out() -> None:
    """Show lights out moment."""
    _print_and_log(f"[{_ts()}]  Lights out!")


def show_waiting_for_start() -> None:
    """Show that we're waiting for the race to start."""
    _print_and_log(f"[{_ts()}]  Waiting for lights out...")


def show_stream_unavailable(tla: str) -> None:
    """Show that a driver's stream is not available."""
    _print_and_log(f"[{_ts()}]  Stream unavailable: {tla} (skipping)")


def show_driver_list(tlas: list[str]) -> None:
    """Show confirmed driver list."""
    _print_and_log(f"[{_ts()}]  Drivers confirmed: {', '.join(tlas)} ({len(tlas)} drivers)")


def show_safety_car_deployed() -> None:
    """Show SC/VSC deployment."""
    _print_and_log(f"[{_ts()}]  Safety Car / VSC deployed - cameras frozen")


def show_safety_car_ending() -> None:
    """Show SC ending this lap."""
    _print_and_log(f"[{_ts()}]  Safety Car ending this lap")


def show_racing_resumed() -> None:
    """Show green flag."""
    _print_and_log(f"[{_ts()}]  Green flag - cameras active")


def show_chequered_flag() -> None:
    """Show chequered flag / session end."""
    _print_and_log(f"[{_ts()}]  Chequered flag - session complete")


def show_poll_error() -> None:
    """Show connection/poll error."""
    _print_and_log(f"[{_ts()}]  Connection issue - retrying...")


def show_connection_retry(attempt: int, error: str) -> None:
    """Show connection retry attempt."""
    _print_and_log(f"[{_ts()}]  Connection failed (attempt {attempt}/5) - retrying in 5s...")


def show_connection_failed() -> None:
    """Show connection failure after retries."""
    _print_and_log(
        f"[{_ts()}]  Could not connect after 5 attempts. Check your internet and credentials."
    )


def show_no_windows() -> None:
    """Show no onboard windows detected."""
    _print_and_log(f"[{_ts()}]  No onboard windows detected. Open some in MultiViewer.")


def show_multiviewer_not_found() -> None:
    """Show MultiViewer not found."""
    _print_and_log(f"[{_ts()}]  MultiViewer not found. Make sure it's running.")


def show_no_commentary() -> None:
    """Show F1 LIVE player not found warning."""
    _print_and_log(f"[{_ts()}]  Warning: F1 LIVE player not found. Sync may be inaccurate.")


def show_swap_failed(tla: str, reason: str) -> None:
    """Fix #26: Show that a swap failed for a driver."""
    _print_and_log(f"[{_ts()}]  Swap failed for {tla}: {reason}")


def show_monitor_startup() -> None:
    """One-time banner for monitor mode."""
    _print_and_log(
        f"[{_ts()}]  MONITOR MODE: validating infrastructure "
        "— scoring and camera control disabled"
    )


def show_quali_startup() -> None:
    """One-time banner for qualifying dry-score mode."""
    _print_and_log(
        f"[{_ts()}]  QUALIFYING MODE: dry-scoring with full data logging "
        "-- no camera control"
    )


def show_session_changed(old_key: int | None, new_key: int) -> None:
    """Show OpenF1 session key change."""
    _print_and_log(f"[{_ts()}]  Session changed: {old_key} -> {new_key}")


def show_token_refreshed(expires_in: int) -> None:
    """Show token refresh event."""
    _print_and_log(f"[{_ts()}]  Token refreshed (expires in {expires_in}s)")


def show_data_stale() -> None:
    """Show one-time stale-data transition line."""
    _print_and_log(f"[{_ts()}]  Data stale - critical endpoint failing")


def show_data_fresh_restored() -> None:
    """Show one-time stale->fresh transition line."""
    _print_and_log(f"[{_ts()}]  Data freshness restored")


def show_monitor_tick(
    tick: int,
    num_drivers: int,
    session_type: str,
    lap: int,
    data_fresh: bool,
    commentary_time: float | None,
    sc_phase: str,
    session_key: int | None = None,
) -> None:
    """Per-tick status line in monitor mode."""
    fresh_str = "fresh" if data_fresh else "STALE"
    parts = [
        f"tick {tick}",
        f"{num_drivers} drivers",
        session_type,
    ]
    if session_key is not None:
        parts.append(f"session: {session_key}")
    if lap > 0:
        parts.append(f"lap {lap}")
    parts.append(f"data: {fresh_str}")
    if commentary_time is not None:
        parts.append(f"commentary: {commentary_time:.1f}s")
    if sc_phase.lower() != "none":
        parts.append(f"SC: {sc_phase}")

    _print_and_log(f"[{_ts()}]  Monitor | {' | '.join(parts)}")
