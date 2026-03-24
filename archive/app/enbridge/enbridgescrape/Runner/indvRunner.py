import os
from datetime import datetime

import asyncio
import pandas as pd

from src.enbridgescrape.Scraper.enbridgeScrape import enbridgePipeScrape
from src.enbridgescrape.Scraper.NoNotice import runNN_Scrape
# from src.enbridgescrape import runEnbridgeScrape
# from src.enbridgescrape import runNN_Scrape

from ..utils import logger, error_detailed
from ..cloudPush import pushEnbridge


async def scrapeFailedDate(head_less: bool = True):
    """
    to run the failed date from fail stats

    :param head_less: Description
    :type head_less: bool
    """
    try:
        os.rename('logs/Enbridge_fails.csv', 'logs/Enbridge_fails_run1.csv')
        df = pd.read_csv('logs/Enbridge_fails_run1.csv', sep='|', header=None)\
            .drop_duplicates()\
            .rename(columns={0: 'pipecode', 1: 'type', 2: 'scrape_date'})[['pipecode', 'scrape_date']]

        df['scrape_date'] = df['scrape_date'].apply(
            lambda x: datetime.strptime(x, "%Y/%m/%d"))

        for record in df.to_dict(orient='records'):
            logger.info(f"scraping - {record}")
            print(f"scraping - {record}")
            async with asyncio.TaskGroup() as group:
                group.create_task(enbridgePipeScrape(pipecode=record.get(
                    'pipecode', ''), scrape_date=record.get('scrape_date', ''), head_less=head_less))
                group.create_task(runNN_Scrape(
                    scrape_date=record.get('scrape_date', ''), head_less=head_less))

        await pushEnbridge()

        os.remove('logs/Enbridge_fails_run1.csv')
    except Exception as e:
        os.rename('logs/Enbridge_fails_run1.csv', 'logs/Enbridge_fails.csv')
        logger.critical(f"something failed - {error_detailed(e)}")
