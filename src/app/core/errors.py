from pathlib import Path

from .logging import logger


def track_fail(fail_file: Path, line: str) -> None:
    """Append a failure record to the pipeline's fail log."""
    fail_file.parent.mkdir(parents=True, exist_ok=True)
    with open(fail_file, "a") as f:
        f.write(line)
    logger.warning(f"Failure tracked: {line.strip()} → {fail_file}")
