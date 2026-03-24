import logging
from datetime import datetime
from .dirsFile import dirs

baseLogger = logging.getLogger(__name__)
baseLogger.setLevel(logging.INFO)

timestamp = datetime.today().strftime("%Y%m%d")
LOG_DIR = dirs.logs
LOG_DIR.mkdir(exist_ok=True, parents=True)

ERROR_FILE = LOG_DIR / f"Error_{timestamp}.txt"

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

error_handler = logging.FileHandler(ERROR_FILE)
error_handler.setLevel(logging.CRITICAL)
error_handler.setFormatter(formatter)

baseLogger.addHandler(error_handler)
