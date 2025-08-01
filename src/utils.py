
# src/utils.py
# Contains reusable functions like WebDriver setup, waiting mechanisms, and HTML parsing helpers.

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime
from collections import defaultdict
import logging
import os
from datetime import datetime, date
from config import LOG_FILE, LOG_LEVEL


def setup_logging():

# DEBUG: Detailed logs for development and debugging.
# INFO: High-level events (like app startup, task completion).
# WARNING: Non-critical issues that should be looked at.
# ERROR: Serious issues that affect functionality but the app can continue.
# CRITICAL: Fatal errors, the app cannot continue.

# %(asctime)s: Timestamp when the log message was created.
# %(levelname)s: The log level (e.g., DEBUG, INFO, WARNING, etc.).
# %(message)s: The log message itself.
# %(filename)s: The name of the file where the log call was made (not including the path).
# %(module)s: The name of the module (same as the filename, but without the .py extension).
# %(funcName)s: The name of the function where the log call was made.
# %(lineno)d: The line number where the log call was made.
# %(process)d: Process ID (useful for multi-process logging).
# %(thread)d: Thread ID (useful for multi-threaded logging).

    # # Ensure log directory exists
    # log_dir = os.path.dirname(LOG_FILE)
    # if log_dir and not os.path.exists(log_dir):
    #     os.makedirs(log_dir)
    #     logging.info(f"utils: Created log directory: {log_dir}")
    
    # # Configure logging
    # logging.basicConfig(
    #     filename=LOG_FILE,
    #     level=getattr(logging, LOG_LEVEL),
    #     # format='[%(asctime)s] [%(levelname)s] [%(filename)s/%(funcName)s/%(lineno)d] - %(message)s',
    #     format='[%(asctime)s] %(levelname)-8s %(filename)-20.20s%(lineno)-5d%(funcName)-35.35s: %(message)-100s',
    #     datefmt='%b %d %a] [%H:%M:%S'
    # )
    # print(f"ℹ️  Logging configured to {LOG_FILE} at level {LOG_LEVEL}")
    # logging.info("")
    # logging.info("")
    # logging.info("")
    # logging.info(f"Logging configured to {LOG_FILE} at level {LOG_LEVEL}")
    # logging.info("-------------------------------------------------------------------")

    # Create log directory if not exists (derive from LOG_FILE)
    log_dir = os.path.dirname(os.path.abspath(LOG_FILE))
    os.makedirs(log_dir, exist_ok=True)
    
    # Clear any existing handlers to avoid duplicates
    logging.getLogger().handlers = []
    
    # File handler with UTF-8 encoding
    file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8', mode='a')  # 'a' for append
    file_handler.setLevel(LOG_LEVEL)
    # file_formatter = logging.Formatter('[%(asctime)s] [%(levelname)-8s] %(module)s.py %(lineno)-4d %(funcName)-35s : %(message)s', datefmt='%b %d %a %H:%M:%S')
    file_formatter = logging.Formatter('[%(asctime)s] %(levelname)-8s %(filename)-32.32s%(lineno)-5d%(funcName)-35.35s: %(message)-100s', datefmt='%b %d %a] [%H:%M:%S')
    file_handler.setFormatter(file_formatter)
    
    # Console handler for real-time output
    console_handler = logging.StreamHandler()
    console_handler.setLevel(LOG_LEVEL)
    console_formatter = logging.Formatter('[%(asctime)s] [%(levelname)-7s] %(funcName)-35s : %(message)s', datefmt='%b %d %a %H:%M:%S')
    console_handler.setFormatter(console_formatter)
    
    # Add handlers to root logger
    logging.getLogger().addHandler(file_handler)
    # logging.getLogger().addHandler(console_handler)
    logging.getLogger().setLevel(LOG_LEVEL)
    
    logging.info(f"Logging configured to {LOG_FILE} at level {LOG_LEVEL}")
    logging.info("-------------------------------------------------------------------")
    print(f"Logging configured to {LOG_FILE} at level {LOG_LEVEL}")
    print("-------------------------------------------------------------------")

def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36")
    
    # Disable images to reduce load time
    prefs = {"profile.managed_default_content_settings.images": 2}
    chrome_options.add_experimental_option("prefs", prefs)

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    logging.info("WebDriver initialized")
    logging.info("-------------------------------------------------------------------")
    return driver

def parse_date(date_str, context=None):
    """Parse a date string in 'YYYY-MM-DD' or 'YYYY.MM.DD' format into a datetime.date object.
    If input is already a datetime.date, returns it unchanged."""
    if isinstance(date_str, date):
        # Already a date object, no need to parse
        return date_str

    date_str = date_str.strip() if date_str else "None"
    for fmt in ("%Y-%m-%d", "%Y.%m.%d"):
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    logging.warning(f"Invalid date format: {date_str} (context: {context or 'unknown calling function'})")
    return None

def print_db_insert_results(db_results):
    # Initialize with all expected statuses
    all_statuses = ["success", "failed", "skipped"]
    summary = {status: defaultdict(int) for status in all_statuses}

    # Build reason-level summary
    for result in db_results:
        status = result.get("status", "unknown")
        reason = result.get("reason", "No reason provided")
        if status not in summary:
            summary[status] = defaultdict(int)
        summary[status][reason] += 1

    # Log/print overall summary and breakdown
    logging.info("Database Summary:")
    print("📊 Database Summary:")

    for status in all_statuses:
        total = sum(summary[status].values())
        capitalized_status = status.capitalize()
        logging.info(f"   - {capitalized_status}: {total}")

        # Determine prefix for print based on status
        if status == "success":
            prefix = "✅"
        elif status == "failed":
            prefix = "❌"
        elif status == "skipped":
            prefix = "⚠️ "
        else:
            prefix = "-"

        print(f"   {prefix} {capitalized_status}: {total}")

        for reason, count in summary[status].items():
            logging.info(f"      • {reason}: {count}")
            print(f"      • {reason}: {count}")


def sanitize_name(name: str) -> str:
    """Sanitize a name by stripping, splitting, and title-casing each word."""
    return ' '.join(word.strip().title() for word in name.split())