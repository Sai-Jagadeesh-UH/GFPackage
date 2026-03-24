from loguru import logger

from datetime import time
from zoneinfo import ZoneInfo

logger.add("app.log",
           rotation=time(0, 0, tzinfo=ZoneInfo("America/Chicago")), # Rotate logs at midnight (local time)
           retention="10 days", enqueue=True)  # Automatically rotate and compress logs

 
