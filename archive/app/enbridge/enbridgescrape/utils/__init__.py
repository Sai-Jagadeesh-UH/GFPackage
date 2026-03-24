import pandas as pd

from src.artifacts import error_detailed, dirs, openPage
# dumpPipeConfigs

from .pathFile import paths
from .logWriters import logger

# if not (dirs.configFiles / 'PipeConfigs.parquet').exists():
#     dumpPipeConfigs()

pipeConfigs_df = pd.read_parquet(dirs.configFiles / 'PipeConfigs.parquet')


def trackFail(line: str):
    with open(paths.fail_file, 'a') as file:
        file.write(line)


__all__ = ["paths", "openPage", "error_detailed",
           "logger", 'dirs', 'pipeConfigs_df', 'trackFail']
