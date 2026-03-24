import asyncio
import aiohttp
import aiofiles
import ssl
import time

from ..utils import paths, pipeConfigs_df
from ..utils import logger
from ..Munger import metaMunge
# from ..Munger import metaMunge


meta_download_path = paths.downloads / "MetaData"
meta_download_path.mkdir(exist_ok=True, parents=True)


async def runDump(file_url, local_filename):
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    try:
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context)) as session:
            async with session.get(url=file_url, raise_for_status=True,) as response:
                async with aiofiles.open(local_filename, mode='wb') as f:
                    # 1MB chunks
                    async for chunk in response.content.iter_chunked(8192):
                        await f.write(chunk)
    except aiohttp.ClientError as e:
        logger.error(f"Error downloading {file_url}: {e}")


async def metaDump():
    start_time = time.perf_counter()
    async with asyncio.TaskGroup() as group:
        # for pipe_code in metacodes:
        for pipe_meta_code in pipeConfigs_df['MetaCode'].dropna(ignore_index=True).to_list():
            pipeCode = pipeConfigs_df\
                .loc[pipeConfigs_df['MetaCode'] == pipe_meta_code, ['PipeCode']].values[0][0]
            file_url = f"https://linkwc.enbridge.com/Pointdata/{pipe_meta_code}AllPoints.csv"
            local_filename = meta_download_path / f"{pipeCode}_AllPoints.csv"
            group.create_task(runDump(file_url, local_filename))

    # metaMunge()

    logger.info(
        f"metaDump completed in {time.perf_counter()-start_time: .2f}s  {'-'*10}")

    # metaMunge()
