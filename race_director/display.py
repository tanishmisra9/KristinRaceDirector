"""Human-readable terminal output for non-technical users."""

from __future__ import annotations

from datetime import datetime


def _ts() -> str:
    """Short timestamp for display: HH:MM:SS AM/PM."""
    return datetime.now().strftime("%I:%M:%S %p")


def show_tick_status(tick: int, on_screen: list[str]) -> None:
    """Show current onboard lineup."""
    drivers = ", ".join(on_screen) if on_screen else "(none)"
    print(f"[{_ts()}]  Showing: {drivers}")


def show_swap(old_tla: str, new_tla: str, slot: int) -> None:
    """Show a camera swap."""
    print(f"[{_ts()}]  Swap: {old_tla} -> {new_tla} (window {slot + 1})")


def show_sync_result(tla: str, success: bool, drift: float | None = None) -> None:
    """Show sync result."""
    if success:
        drift_str = f" ({drift:.1f}s drift)" if drift is not None else ""
        print(f"[{_ts()}]  Synced {tla}{drift_str}")
    else:
        print(f"[{_ts()}]  Sync failed for {tla}")


def show_lead_change(old_tla: str | None, new_tla: str) -> None:
    """Show a lead change."""
    if old_tla:
        print(f"[{_ts()}]  Lead change: {old_tla} -> {new_tla}")
    else:
        print(f"[{_ts()}]  Leader: {new_tla}")


def show_scoring_snapshot(ranked: list[tuple[str, float]], count: int = 5) -> None:
    """Show top-N driver scores in a readable line."""
    parts = [f"{tla} {score:.1f}" for tla, score in ranked[:count]]
    print(f"[{_ts()}]  Top {count} Scores: {', '.join(parts)}")


def show_startup(num_windows: int) -> None:
    """Show startup info."""
    print(f"[{_ts()}]  Ready - managing {num_windows} onboard window{'s' if num_windows != 1 else ''}")


def show_grace_period(tick: int, remaining: int) -> None:
    """Show grace period countdown."""
    print(f"[{_ts()}]  Grace period: {remaining} tick{'s' if remaining != 1 else ''} remaining (preserving your cameras)")


def show_no_swaps_needed() -> None:
    """Show that current lineup is optimal."""
    pass  # Stay quiet when nothing is happening — less noise


def show_lights_out() -> None:
    """Show lights out moment."""
    print(f"[{_ts()}]  Lights out!")


def show_waiting_for_start() -> None:
    """Show that we're waiting for the race to start."""
    print(f"[{_ts()}]  Waiting for lights out...")


def show_stream_unavailable(tla: str) -> None:
    """Show that a driver's stream is not available."""
    print(f"[{_ts()}]  Stream unavailable: {tla} (skipping)")


def show_driver_list(tlas: list[str]) -> None:
    """Show confirmed driver list."""
    print(f"[{_ts()}]  Drivers confirmed: {', '.join(tlas)} ({len(tlas)} drivers)")


def show_safety_car_deployed() -> None:
    """Show SC/VSC deployment."""
    print(f"[{_ts()}]  Safety Car / VSC deployed - cameras frozen")


def show_safety_car_ending() -> None:
    """Show SC ending this lap."""
    print(f"[{_ts()}]  Safety Car ending this lap")


def show_racing_resumed() -> None:
    """Show green flag."""
    print(f"[{_ts()}]  Green flag - cameras active")


def show_poll_error() -> None:
    """Show connection/poll error."""
    print(f"[{_ts()}]  Connection issue - retrying...")


def show_connection_retry(attempt: int, error: str) -> None:
    """Show connection retry attempt."""
    print(f"[{_ts()}]  Connection failed (attempt {attempt}/5) - retrying in 5s...")


def show_connection_failed() -> None:
    """Show connection failure after retries."""
    print(f"[{_ts()}]  Could not connect after 5 attempts. Check your internet and credentials.")


def show_no_windows() -> None:
    """Show no onboard windows detected."""
    print(f"[{_ts()}]  No onboard windows detected. Open some in MultiViewer.")


def show_multiviewer_not_found() -> None:
    """Show MultiViewer not found."""
    print(f"[{_ts()}]  MultiViewer not found. Make sure it's running.")


def show_no_commentary() -> None:
    """Show F1 LIVE player not found warning."""
    print(f"[{_ts()}]  Warning: F1 LIVE player not found. Sync may be inaccurate.")
