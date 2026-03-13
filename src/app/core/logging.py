"""Centralized loguru logger for the entire app.

Import `logger` from this module everywhere. Call `setup_logger()` once
at app startup to configure file sinks. Until setup is called, loguru
still logs to stderr (its default).

All sinks use:
  - enqueue=True    → async/thread safe (messages go through a queue)
  - rotation at midnight CT
  - 10 days retention
"""

import sys
from datetime import time
from pathlib import Path
from zoneinfo import ZoneInfo

from loguru import logger

# Remove loguru's default stderr handler so we control all output
logger.remove()

# Re-add stderr with a clean format
logger.add(
    sys.stderr,
    format="<level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    level="INFO",
    enqueue=True,
)


_logger_configured = False


def setup_logger(log_dir: Path, timezone: str = "America/Chicago") -> None:
    """Configure file sinks for the app logger.

    Safe to call multiple times — only adds sinks on the first call.

    Creates in log_dir:
      - app.log            (all levels, daily rotation at midnight)
      - app_error.log      (WARNING and above, daily rotation)

    Args:
        log_dir: directory to write log files into
        timezone: IANA timezone for rotation schedule
    """
    global _logger_configured
    if _logger_configured:
        return
    _logger_configured = True

    log_dir.mkdir(parents=True, exist_ok=True)
    tz = ZoneInfo(timezone)
    midnight = time(0, 0, tzinfo=tz)

    # Main log — all levels
    logger.add(
        log_dir / "app.log",
        rotation=midnight,
        retention="10 days",
        enqueue=True,
        backtrace=True,
        diagnose=True,
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
    )

    # Error log — WARNING and above
    logger.add(
        log_dir / "app_error.log",
        level="WARNING",
        rotation=midnight,
        retention="10 days",
        enqueue=True,
        backtrace=True,
        diagnose=True,
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
    )
