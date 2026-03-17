#!/usr/bin/env python3
"""Quick test that OpenF1 auth and config work. Run before starting the daemon."""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def main() -> None:
    from race_director.config import load_config
    from race_director.data_provider.openf1_auth import fetch_token

    config = load_config(Path("config.yaml"))
    username = config.openf1.username
    password = config.openf1.password

    if not username or not password:
        print("ERROR: No OpenF1 credentials.")
        print("  Set OPENF1_USERNAME and OPENF1_PASSWORD env vars, or")
        print("  use config.local.yaml with openf1.username and openf1.password")
        sys.exit(1)

    print("Config: credentials loaded")
    print("Fetching token...")

    async def run() -> None:
        token, expires_in = await fetch_token(username, password)
        print(f"Token obtained (expires in {expires_in}s)")
        print("Auth test PASSED")

    asyncio.run(run())


if __name__ == "__main__":
    main()
