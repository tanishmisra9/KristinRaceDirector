#!/usr/bin/env python3
"""
Data capture script for KristinRaceDirector.

Records timestamped data from all OpenF1 API endpoints during a replay/live session.
Outputs a single JSON file that can be analyzed to build a better scoring algorithm.

Usage:
    python scripts/capture_data.py --duration 600  # capture 10 minutes
    python scripts/capture_data.py --laps 15        # capture ~15 laps worth

The script polls all endpoints every 4 seconds (matching the daemon's tick interval)
and stores every record with wall-clock timestamps for correlation with your viewing notes.

You watch the race in MultiViewer, take notes like:
    "0:32 - HAM passes RUS for P2, exciting"
    "1:15 - VER lockup into T1, must-see"
    "2:00 - boring, nothing happening on any feed"

Then provide both the data file and your notes for analysis.
"""

from __future__ import annotations

import argparse
from typing import Any
import asyncio
import json
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


ENDPOINTS = [
    "drivers",
    "intervals",
    "position",
    "laps",
    "location",
    "overtakes",
    "pit",
    "race_control",
    "car_data",
    "stints",
    "starting_grid",
]

# High-frequency endpoints sampled every tick
EVERY_TICK = {"intervals", "position", "car_data", "location"}
# Lower-frequency endpoints sampled every 3rd tick
EVERY_3RD = {"laps", "overtakes", "pit", "race_control", "stints"}
# One-shot endpoints
ONCE = {"drivers", "starting_grid"}


async def fetch_endpoint(
    client,
    base_url: str,
    endpoint: str,
    session_key: int,
    headers: dict,
    timeout: float = 8.0,
) -> list[dict] | None:
    """Fetch data from a single endpoint."""

    url = f"{base_url}/{endpoint}"
    try:
        r = await client.get(
            url,
            params={"session_key": session_key},
            headers=headers,
            timeout=timeout,
        )
        if r.status_code == 401:
            return None
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list):
            return data
        return None
    except Exception as e:
        print(f"  [warn] {endpoint}: {e}")
        return None


async def get_token(username: str, password: str) -> tuple[str, int]:
    """Get OAuth2 token."""
    import httpx

    async with httpx.AsyncClient() as client:
        r = await client.post(
            "https://api.openf1.org/token",
            data={"username": username, "password": password},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        r.raise_for_status()
        data = r.json()
        return data["access_token"], data.get("expires_in", 3600)


async def get_session_key(base_url: str, headers: dict) -> int | None:
    """Get the latest session key."""
    import httpx

    async with httpx.AsyncClient() as client:
        r = await client.get(
            f"{base_url}/sessions",
            params={"session_key": "latest"},
            headers=headers,
        )
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list) and data:
            sk = data[0].get("session_key")
            name = data[0].get("session_name", "unknown")
            print(f"Session: {name} (key={sk})")
            return sk
    return None


async def main_loop(
    base_url: str,
    headers: dict,
    session_key: int,
    duration_sec: int,
    poll_interval: float = 4.0,
    username: str = "",
    password: str = "",
    token_obtained_at: float = 0.0,
    expires_in: int = 3600,
):
    """Main capture loop.
    
    Fix #16: Supports token refresh during long captures by tracking token expiry.
    """
    import httpx

    capture = {
        "meta": {
            "session_key": session_key,
            "capture_start": datetime.now(UTC).isoformat(),
            "poll_interval_sec": poll_interval,
            "base_url": base_url,
        },
        "ticks": [],
    }

    # Track last seen dates per endpoint for delta detection
    last_dates: dict[str, str] = {}
    
    # Fix #16: Token refresh tracking
    # IMPORTANT: fetch_endpoint must always use current_headers (not the original headers)
    # so that token refreshes are applied to subsequent requests
    current_headers = dict(headers)
    token_time = token_obtained_at
    token_expires = expires_in

    tick = 0
    start_time = time.time()
    print(f"\nCapturing data for {duration_sec}s (Ctrl+C to stop early)...\n")

    async with httpx.AsyncClient(timeout=10.0) as client:
        # One-shot fetches
        for ep in ONCE:
            print(f"  Fetching {ep}...")
            data = await fetch_endpoint(client, base_url, ep, session_key, current_headers)
            if data:
                capture[f"static_{ep}"] = data
                print(f"    {ep}: {len(data)} records")

        try:
            while time.time() - start_time < duration_sec:
                # Fix #16: Check if token needs refresh (10 min before expiry)
                if username and password and token_time > 0:
                    elapsed_since_token = time.time() - token_time
                    if elapsed_since_token > (token_expires - 600):
                        print("  Refreshing token...")
                        try:
                            new_token, new_expires = await get_token(username, password)
                            current_headers["Authorization"] = f"Bearer {new_token}"
                            token_time = time.time()
                            token_expires = new_expires
                            print(f"  Token refreshed (expires in {new_expires}s)")
                        except Exception as e:
                            print(f"  [warn] Token refresh failed: {e}")
                
                tick += 1
                tick_start = datetime.now(UTC)
                tick_data: dict[str, Any] = {
                    "tick": tick,
                    "wall_time": tick_start.isoformat(),
                    "elapsed_sec": round(time.time() - start_time, 1),
                }

                # Determine which endpoints to fetch this tick
                endpoints_this_tick = list(EVERY_TICK)
                if tick % 3 == 0:
                    endpoints_this_tick.extend(EVERY_3RD)

                for ep in endpoints_this_tick:
                    data = await fetch_endpoint(
                        client, base_url, ep, session_key, current_headers
                    )
                    if data is None:
                        continue

                    # For high-volume endpoints, only store new records
                    last_date = last_dates.get(ep)
                    if last_date and ep in EVERY_TICK:
                        new_records = [
                            r for r in data if r.get("date", "") > last_date
                        ]
                    else:
                        new_records = data

                    if new_records:
                        # Update last seen date
                        dates = [r.get("date", "") for r in new_records if r.get("date")]
                        if dates:
                            last_dates[ep] = max(dates)
                        tick_data[ep] = new_records

                    # For location and car_data, only keep last record per driver (too much data otherwise)
                    if ep in ("location", "car_data") and ep in tick_data:
                        per_driver: dict[int, dict] = {}
                        for r in tick_data[ep]:
                            num = r.get("driver_number")
                            if num is not None:
                                per_driver[num] = r
                        tick_data[ep] = list(per_driver.values())

                capture["ticks"].append(tick_data)

                # Progress
                elapsed = round(time.time() - start_time, 0)
                ep_counts = {
                    k: len(v)
                    for k, v in tick_data.items()
                    if isinstance(v, list)
                }
                if ep_counts:
                    summary = ", ".join(f"{k}:{v}" for k, v in sorted(ep_counts.items()))
                else:
                    summary = "no new data"
                print(
                    f"  tick {tick} ({elapsed:.0f}s): {summary}"
                )

                await asyncio.sleep(poll_interval)

        except KeyboardInterrupt:
            print("\n\nCapture stopped by user.")

    capture["meta"]["capture_end"] = datetime.now(UTC).isoformat()
    capture["meta"]["total_ticks"] = tick
    capture["meta"]["duration_sec"] = round(time.time() - start_time, 1)

    return capture


def main():
    parser = argparse.ArgumentParser(
        description="Capture OpenF1 data during a replay for scoring analysis"
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=600,
        help="Duration to capture in seconds (default: 600 = 10 minutes)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output JSON file path (default: capture_TIMESTAMP.json)",
    )
    parser.add_argument(
        "--config",
        type=str,
        default="config.local.yaml",
        help="Config file for credentials (default: config.local.yaml)",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=4.0,
        help="Seconds between polls (default: 4.0)",
    )
    args = parser.parse_args()

    # Load config for credentials
    from race_director.config import load_config

    config = load_config(Path(args.config))
    username = config.openf1.username
    password = config.openf1.password
    base_url = config.openf1.base_url

    if not username or not password:
        print("ERROR: No OpenF1 credentials found.")
        print("Set OPENF1_USERNAME/OPENF1_PASSWORD or use config.local.yaml")
        sys.exit(1)

    print(f"OpenF1 base URL: {base_url}")
    print(f"Authenticating as: {username}")

    # Get token
    token, expires_in = asyncio.run(get_token(username, password))
    token_obtained_at = time.time()
    print(f"Token obtained (expires in {expires_in}s)")
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {token}",
    }

    # Get session key
    session_key = asyncio.run(get_session_key(base_url, headers))
    if not session_key:
        print("ERROR: No session found")
        sys.exit(1)

    # Run capture (Fix #16: pass credentials for token refresh during long captures)
    capture = asyncio.run(
        main_loop(
            base_url,
            headers,
            session_key,
            args.duration,
            args.poll_interval,
            username=username,
            password=password,
            token_obtained_at=token_obtained_at,
            expires_in=expires_in,
        )
    )

    # Save
    output_path = args.output or f"capture_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_path, "w") as f:
        json.dump(capture, f, indent=2, default=str)

    file_size = os.path.getsize(output_path) / (1024 * 1024)
    print(f"\nSaved to: {output_path}")
    print(f"File size: {file_size:.1f} MB")
    print(f"Total ticks: {capture['meta']['total_ticks']}")
    print(f"Duration: {capture['meta']['duration_sec']}s")
    print(
        "\nNext: watch the replay, take notes on what cameras should show, "
        "then provide both the JSON and your notes for analysis."
    )


if __name__ == "__main__":
    main()
