"""Entry point: python -m race_director."""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

import structlog

from race_director.config import load_config
from race_director.orchestrator.loop import Orchestrator


def setup_logging(level: str, fmt: str, log_file: str | None) -> None:
    import logging

    processors: list = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
    ]
    if fmt == "json":
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer(colors=False))

    lvl_map = {"DEBUG": logging.DEBUG, "INFO": logging.INFO, "WARNING": logging.WARNING}
    numeric_level = lvl_map.get(level.upper(), logging.INFO)

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(numeric_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(
            file=open(log_file, "a") if log_file else sys.stderr
        ),
        cache_logger_on_first_use=True,
    )


def main() -> None:
    print("Race Director starting...", flush=True)
    parser = argparse.ArgumentParser(description="MultiViewer for F1 camera automation")
    parser.add_argument("-c", "--config", default="config.yaml")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    config = load_config(Path(args.config))
    if args.dry_run:
        config.orchestrator.dry_run = True
    setup_logging(config.logging.level, config.logging.format, config.logging.file)
    log = structlog.get_logger()
    log.info("starting_race_director", dry_run=config.orchestrator.dry_run)
    orchestrator = Orchestrator(config=config)
    try:
        asyncio.run(orchestrator.run())
    except KeyboardInterrupt:
        log.info("shutdown_requested")
    except Exception as e:
        log.error("fatal_error", error=str(e), exc_info=True)
        raise


if __name__ == "__main__":
    main()
