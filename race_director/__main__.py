"""Entry point: python -m race_director."""

from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path

import structlog

from race_director.config import load_config
from race_director.orchestrator.loop import Orchestrator


def setup_logging(level: str, fmt: str, log_file: str | None) -> None:
    import logging

    # Default: write structlog to file, keep stdout clean for display.py
    if log_file is None:
        log_file = "kristen.log"

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
        logger_factory=structlog.PrintLoggerFactory(file=open(log_file, "a")),
        cache_logger_on_first_use=True,
    )

    # Redirect ALL Python logging (root, sgqlc, etc.) to the log file
    # so only display.py print() lines appear on the terminal
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    file_handler = logging.FileHandler(log_file, mode="a")
    file_handler.setLevel(numeric_level)
    root_logger.addHandler(file_handler)
    root_logger.setLevel(numeric_level)


def main() -> None:
    os.environ.setdefault("PYTHONUNBUFFERED", "1")
    BANNER = """
╔══════════════════════════════════════════════════════╗
║                                                      ║
║            KRISTEN RACE DIRECTOR                     ║
║       MultiViewer Onboard Camera Automation          ║
║                                                      ║
║               Formula 1 @ Purdue                     ║
║                                                      ║
╚══════════════════════════════════════════════════════╝
"""
    print(BANNER, flush=True)
    parser = argparse.ArgumentParser(
        prog="kristen",
        description="MultiViewer for F1 camera automation",
    )
    parser.add_argument("-c", "--config", default=None, help="Config file path")
    parser.add_argument("--dry-run", action="store_true")
    subparsers = parser.add_subparsers(dest="command")

    start_parser = subparsers.add_parser("start", help="Start the race director")
    start_parser.add_argument("-c", "--config", default=None, help="Config file path")
    start_parser.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()

    if args.command is None:
        args.command = "start"
        if not hasattr(args, "config") or args.config is None:
            args.config = None
        if not hasattr(args, "dry_run"):
            args.dry_run = False

    if args.config is None:
        if Path("config.local.yaml").exists():
            args.config = "config.local.yaml"
        else:
            args.config = "config.yaml"

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
