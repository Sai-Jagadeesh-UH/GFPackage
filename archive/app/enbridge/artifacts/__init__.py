import shutil

from dotenv import load_dotenv

from .dirsFile import dirs
from .detLog import error_detailed
from .azureDump import dumpPipeConfigs, dumpSegmentConfigs, updateSegmentConfigs
from .BaseLogWriters import baseLogger
from .runnerContext import openPage


load_dotenv(dirs.root / 'archives/.env')

# cleanse before dumping
if (dirs.configFiles.exists()):
    shutil.rmtree(dirs.configFiles)
    dirs.configFiles.mkdir(exist_ok=True, parents=True)


dumpPipeConfigs()
dumpSegmentConfigs()


__all__ = ["dirs", 'error_detailed', 'baseLogger',
           'openPage', 'updateSegmentConfigs']
