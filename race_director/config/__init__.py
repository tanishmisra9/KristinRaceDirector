"""Configuration loading."""

from __future__ import annotations

import os
from pathlib import Path

import yaml

from race_director.config.schema import AppConfig


def load_config(path: Path) -> AppConfig:
    """Load AppConfig from a YAML file.

    Environment variables override config file values:
    - OPENF1_USERNAME overrides openf1.username
    - OPENF1_PASSWORD overrides openf1.password
    """
    if not path.exists():
        config = AppConfig()
    else:
        with open(path) as f:
            data = yaml.safe_load(f) or {}
        config = AppConfig.model_validate(data)

    # Env vars override for credentials (keep secrets out of committed config)
    username = os.environ.get("OPENF1_USERNAME")
    password = os.environ.get("OPENF1_PASSWORD")
    if username is not None or password is not None:
        openf1 = config.openf1.model_copy()
        if username is not None:
            openf1.username = username
        if password is not None:
            openf1.password = password
        config = config.model_copy(update={"openf1": openf1})

    return config
