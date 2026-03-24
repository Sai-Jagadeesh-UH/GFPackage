from pathlib import Path
from dataclasses import dataclass


@dataclass
class Directories:
    '''
    Contains Project level directories as path objects
    '''
    root = Path('.').resolve()

    src = root / 'src'
    logs = root / 'logs'
    downloads = root / 'downloads'

    artifacts = src / 'artifacts'
    # models = src / 'Models'

    configFiles = artifacts / 'configFiles'


dirs = Directories()

dirs.logs.mkdir(exist_ok=True, parents=True)
dirs.downloads.mkdir(exist_ok=True, parents=True)
# dirs.models.mkdir(exist_ok=True, parents=True)
dirs.configFiles.mkdir(exist_ok=True, parents=True)
