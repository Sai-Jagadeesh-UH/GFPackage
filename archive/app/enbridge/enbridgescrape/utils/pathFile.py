from dataclasses import dataclass
from pathlib import Path


@dataclass
class Paths:
    """
    Enbridge Paths of Package - contains paths for src, root, logs, downloads, processed files and artifacts of enbridge.
    Creates logs and downloads folders if not exist.
    """
    root = Path('.').resolve()

    src = root / 'src'
    logs = root / 'logs'
    fail_file = logs / 'Enbridge_fails.csv'
    downloads = root / 'downloads' / 'enbridge'

    processed = downloads / 'processed'

    artifacts = src / 'artifacts'
    base = src / 'enbridgescrape'

    # models = src / 'Models'
    # configs = base / 'configs'
    # dbFile = models / "EnbridgeMeta.db"
    # dbName = "EnbridgeMeta.db"


paths = Paths()

paths.logs.mkdir(exist_ok=True, parents=True)
paths.downloads.mkdir(exist_ok=True, parents=True)
