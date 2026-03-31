"""Entry point: python -m race_director."""

from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path

import structlog

from race_director.config import load_config
from race_director.orchestrator.loop import Orchestrator


_log_file_handle = None  # Fix #24: Module-level handle to prevent leak


def setup_logging(level: str, fmt: str, log_file: str | None) -> None:
    """Configure structured logging.
    
    Fix #24: Properly manage file handles to prevent leaks.
    """
    import logging
    from logging.handlers import RotatingFileHandler
    global _log_file_handle

    # Default: write structlog to file, keep stdout clean for display.py
    if log_file is None:
        log_file = "director.log"
    if log_file == "director.log":
        # Fix #24: Use Path.write_text instead of unnamed open().close()
        Path(log_file).write_text("")

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

    # Fix #24: Store file handle in module-level variable to prevent leak
    _log_file_handle = open(log_file, "a")
    
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(numeric_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=_log_file_handle),
        cache_logger_on_first_use=True,
    )

    # Redirect ALL Python logging (root, sgqlc, etc.) to the log file
    # so only display.py print() lines appear on the terminal
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    file_handler = RotatingFileHandler(
        log_file, mode="a", maxBytes=10 * 1024 * 1024, backupCount=3
    )
    file_handler.setLevel(numeric_level)
    root_logger.addHandler(file_handler)
    root_logger.setLevel(numeric_level)


def main() -> None:
    os.environ.setdefault("PYTHONUNBUFFERED", "1")
    BANNER = """
╔══════════════════════════════════════════════════════╗
║                                                      ║
║                    DIRECTOR                          ║
║       MultiViewer Onboard Camera Automation          ║
║                                                      ║
╚══════════════════════════════════════════════════════╝
"""
    print(BANNER, flush=True)
    parser = argparse.ArgumentParser(
        prog="director",
        description="MultiViewer for F1 camera automation",
    )
    parser.add_argument("-c", "--config", default=None, help="Config file path")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--test",
        action="store_true",
        help="Enable test mode: log all API data and decisions to data/",
    )
    parser.add_argument(
        "--monitor",
        action="store_true",
        help="Monitor mode: validate infrastructure without scoring or camera control",
    )
    parser.add_argument(
        "--quali",
        action="store_true",
        help="Qualifying mode: dry-scoring with full data logging and no camera control",
    )
    subparsers = parser.add_subparsers(dest="command")

    start_parser = subparsers.add_parser("start", help="Start the race director")
    start_parser.add_argument("-c", "--config", default=None, help="Config file path")
    start_parser.add_argument("--dry-run", action="store_true")
    start_parser.add_argument(
        "--test",
        action="store_true",
        help="Enable test mode: log all API data and decisions to data/",
    )
    start_parser.add_argument(
        "--monitor",
        action="store_true",
        help="Monitor mode: validate infrastructure without scoring or camera control",
    )
    start_parser.add_argument(
        "--quali",
        action="store_true",
        help="Qualifying mode: dry-scoring with full data logging and no camera control",
    )

    args = parser.parse_args()

    if args.command is None:
        args.command = "start"
        if not hasattr(args, "config") or args.config is None:
            args.config = None
        if not hasattr(args, "dry_run"):
            args.dry_run = False
        if not hasattr(args, "test"):
            args.test = False
        if not hasattr(args, "monitor"):
            args.monitor = False
        if not hasattr(args, "quali"):
            args.quali = False

    if args.config is None:
        if Path("config.local.yaml").exists():
            args.config = "config.local.yaml"
        else:
            args.config = "config.yaml"

    config = load_config(Path(args.config))
    if args.dry_run:
        config.orchestrator.dry_run = True
    if getattr(args, "test", False):
        config.orchestrator.test_mode = True
    if getattr(args, "monitor", False):
        config.orchestrator.monitor_mode = True
        config.orchestrator.dry_run = True
        print(
            "MONITOR MODE -- infrastructure validation only, no scoring or camera control\n",
            flush=True,
        )
    if getattr(args, "quali", False):
        config.orchestrator.quali_mode = True
        config.orchestrator.monitor_mode = True
        config.orchestrator.test_mode = True
        config.orchestrator.dry_run = True
        print(
            "QUALIFYING MODE -- dry-scoring with full data logging, no camera control\n",
            flush=True,
        )
    setup_logging(config.logging.level, config.logging.format, config.logging.file)
    log = structlog.get_logger()
    log.info(
        "starting_race_director",
        dry_run=config.orchestrator.dry_run,
        test_mode=config.orchestrator.test_mode,
        monitor_mode=config.orchestrator.monitor_mode,
        quali_mode=config.orchestrator.quali_mode,
    )
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
