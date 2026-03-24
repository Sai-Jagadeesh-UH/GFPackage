import logging
from datetime import datetime
from .pathFile import paths
# --- Configuration ---

# 1. Define the base logger
# We set the overall logger level to INFO, so it captures both INFO and ERROR messages initially
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

timestamp = datetime.today().strftime("%Y%m%d")
# Define file paths
LOG_DIR = paths.logs
LOG_DIR.mkdir(exist_ok=True, parents=True)

SUCCESS_FILE = LOG_DIR / f"Enbridge_success_{timestamp}.txt"
ERROR_FILE = LOG_DIR / f"Enbridge_error_{timestamp}.txt"


# 2. Define a common formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# 3. Define and configure Handlers

# A. Handler for success/info messages (INFO level and up)
success_handler = logging.FileHandler(SUCCESS_FILE)
success_handler.setLevel(logging.INFO)
success_handler.setFormatter(formatter)

# B. Handler for error messages (ERROR level and up)
error_handler = logging.FileHandler(ERROR_FILE)
# Crucially, set this handler's level to ERROR
error_handler.setLevel(logging.CRITICAL)
error_handler.setFormatter(formatter)

# C. Optional: Console Handler for real-time monitoring
console_handler = logging.StreamHandler()
# Only show warnings/errors in console by default
console_handler.setLevel(logging.WARNING)
console_handler.setFormatter(formatter)


# 4. Add handlers to the logger
logger.addHandler(success_handler)
logger.addHandler(error_handler)
logger.addHandler(console_handler)

# # --- Example Usage ---


# def process_data(item, should_fail=False):
#     if should_fail:
#         # This message will be caught by *both* error_handler and success_handler (but success_handler won't write it due to level filtering)
#         logger.error(f"Failed to process item: {item}. An error occurred.")
#     else:
#         # This message will be caught by *only* the success_handler's level filter
#         logger.info(f"Successfully processed item: {item}.")


# if __name__ == "__main__":
#     print(f"Logging messages to {SUCCESS_FILE} and {ERROR_FILE}...")
#     process_data("Item A", should_fail=False)
#     process_data("Item B", should_fail=True)
#     process_data("Item C", should_fail=False)

#     # Example of a critical log that will appear in both files and console
#     logger.critical("This is a critical system alert!")
