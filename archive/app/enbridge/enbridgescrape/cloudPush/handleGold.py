import os
import shutil

from datetime import datetime
import polars as pl
from dotenv import load_dotenv

from ..utils import paths, logger, error_detailed
from ..deltaHandler.deltapush import LakeMerge

oa_path = paths.downloads / 'OA'
oc_path = paths.downloads / 'OC'
nn_path = paths.downloads / 'NN'

load_dotenv('archives/.env')

staging_storage_options = {
    "AZURE_STORAGE_ACCOUNT_NAME": r'gasfundiesdeltalake',
    "AZURE_STORAGE_ACCOUNT_KEY": os.getenv('ACCESS_KEY', '')

}


def getOA():
    '''
    scan normalized OA fliles, remove 'Timestamp' column and return LazyFrame
    '''
    return pl.scan_parquet(oa_path).select(pl.exclude('Timestamp')).unique()


def getOC():
    '''
    scan normalized OC fliles, remove 'Timestamp' column and return LazyFrame
    '''
    return pl.scan_parquet(oc_path).select(pl.exclude('Timestamp')).unique()


def getNN():
    '''
    scan normalized NN fliles, remove 'Timestamp' column and return LazyFrame
    '''
    return pl.scan_parquet(nn_path).select(pl.exclude('Timestamp')).unique()


def pushGold():
    '''
    merge OA OC NN from getOA(), getOC() & getNN(). Add new Timestamp & RowID and push into gold table in 'goldlayer' ''Enbridge' table
    For each day in the source data, delete all existing rows for that day and insert fresh data to ensure complete daily refresh
    '''
    try:
        LakeMerge(
            pl.concat([getOA(), getOC(), getNN()])
            .with_columns(
                pl.lit(datetime.now()).cast(pl.Datetime).alias('Timestamp')
            ).collect())

        cleanDirectory()

    except Exception as e:
        logger.critical(f"Something Failed with eror - {error_detailed(e)}")


def cleanDirectory():
    '''
    Goes through all directories in paths.downloads and removes them for next run
    '''

    for folder in paths.downloads.iterdir():
        if folder.is_dir():
            for file in folder.iterdir():
                try:
                    file.unlink()
                except OSError as e:
                    logger.critical(
                        f"Error deleting {folder}: {error_detailed(e)}")

    for file in (paths.artifacts / 'configFiles').iterdir():
        if file.is_file():
            try:
                file.unlink()
            except OSError as e:
                logger.critical(
                    f"Error deleting configFiles: {error_detailed(e)}")
