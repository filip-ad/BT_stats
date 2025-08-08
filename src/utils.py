
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
import re
import unicodedata
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

# def print_db_insert_results(db_results):
    # # Initialize with all expected statuses
    # all_statuses = ["success", "failed", "skipped", "warning"]
    # summary = {status: defaultdict(int) for status in all_statuses}

    # # Build reason-level summary
    # for result in db_results:
    #     status = result.get("status", "unknown")
    #     reason = result.get("reason", "No reason provided")
    #     if status not in summary:
    #         summary[status] = defaultdict(int)
    #     summary[status][reason] += 1

    # # Log/print overall summary and breakdown
    # logging.info("Database Summary:")
    # print("📊 Database Summary:")

    # for status in all_statuses:
    #     total = sum(summary[status].values())
    #     capitalized_status = status.capitalize()
    #     logging.info(f"   - {capitalized_status}: {total}")

    #     # Determine prefix for print based on status
    #     if status == "success":
    #         prefix = "✅"
    #     elif status == "failed":
    #         prefix = "❌"
    #     elif status == "skipped":
    #         prefix = "⏭️ " 
    #     elif status == "warning":
    #         prefix = "⚠️ "
    #     else:
    #         prefix = "-"

    #     print(f"   {prefix} {capitalized_status}: {total}")

    #     for reason, count in summary[status].items():
    #         logging.info(f"      • {reason}: {count}")
    #         print(f"      • {reason}: {count}")

from collections import defaultdict
import logging

# def print_db_insert_results(db_results):
#     # Only the “real” statuses here
#     all_statuses = ["success", "failed", "skipped"]
#     summary = {status: defaultdict(int) for status in all_statuses}

#     # Build reason‐level summary
#     for result in db_results:
#         status = result.get("status", "unknown")
#         reason = result.get("reason", "No reason provided")
#         if status not in summary:
#             summary[status] = defaultdict(int)
#         summary[status][reason] += 1

#     # Log/print overall summary and breakdown
#     logging.info("Database Summary:")
#     print("📊 Database Summary:")

#     for status in all_statuses:
#         total = sum(summary[status].values())
#         cap   = status.capitalize()
#         prefix = {
#             "success": "✅",
#             "failed":  "❌",
#             "skipped": "⏭️ "
#         }[status]

#         print(f"   {prefix} {cap}: {total}")
#         logging.info(f"   - {cap}: {total}")

#         for reason, count in summary[status].items():
#             print(f"      • {reason}: {count}")
#             logging.info(f"      • {reason}: {count}")

#     # — Always print warning count —
#     warning_msgs = [r["warning"] for r in db_results if "warning" in r]
#     total_warns = len(warning_msgs)

#     print(f"   ⚠️  Warning: {total_warns}")
#     logging.info(f"   - Warning: {total_warns}")

#     if total_warns > 0:
#         warn_counts = defaultdict(int)
#         for msg in warning_msgs:
#             warn_counts[msg] += 1

#         for reason, cnt in warn_counts.items():
#             print(f"      • {reason}: {cnt}")
#             logging.info(f"      • {reason}: {cnt}")

from collections import defaultdict
import logging

def print_db_insert_results(db_results):
    """
    Print a summary of database insertion results.

    This function takes a list of result dicts—each with keys:
      - "status": one of "success", "failed", or "skipped"
      - "reason": a string describing why that status was assigned
      - "warning": an optional warning message (empty string if none)

    It performs three steps:
    1. **Aggregate** counts of each status/reason combination.
    2. **Print** the overall totals for Success (✅), Failed (❌), and Skipped (⏭️),
       along with a breakdown by reason.
    3. **Print** a Warning (⚠️) section enumerating any non-empty warnings,
       grouped by warning message, always showing “⚠️ Warning: 0” if there are none.
    """
    # existing statuses
    all_statuses = ["success", "failed", "skipped"]
    summary = {st: defaultdict(int) for st in all_statuses}

    # tally status/reasons
    for r in db_results:
        st, rs = r["status"], r["reason"]
        summary.setdefault(st, defaultdict(int))[rs] += 1

    # print main blocks
    print("📊 Database Summary:")
    logging.info("Database Summary:")
    for st, emoji in [("success","✅"),("failed","❌"),("skipped","⏭️ ")]:
        total = sum(summary.get(st,{}).values())
        print(f"   {emoji} {st.capitalize()}: {total}")
        logging.info(f"   - {st.capitalize()}: {total}")
        for reason, cnt in summary[st].items():
            print(f"      • {reason}: {cnt}")
            logging.info(f"      • {reason}: {cnt}")

    warning_list = []
    for r in db_results:
        # if they used "warnings":[...]
        if isinstance(r.get("warnings"), list):
            warning_list.extend(r["warnings"])
        # fall back to the old single‐string "warning"
        elif isinstance(r.get("warning"), str) and r["warning"]:
            warning_list.append(r["warning"])

    total_warns = len(warning_list)
    print(f"   ⚠️  Warning: {total_warns}")
    logging.info(f"   - Warning: {total_warns}")
    if total_warns:
        warn_counts = defaultdict(int)
        for msg in warning_list:
            warn_counts[msg] += 1
        for reason, cnt in warn_counts.items():
            print(f"      • {reason}: {cnt}")
            logging.info(f"      • {reason}: {cnt}")

def sanitize_name(name: str) -> str:
    """ 
    Sanitize a name by stripping, splitting, and title-casing each word.
    Example: 'harry hamrén' -> 'Harry Hamrén' 
    """
    return ' '.join(word.strip().title() for word in name.split())

def normalize_key(name: str) -> str:
    """
    Produce a matching key by:
        1) stripping and collapsing whitespace
        2) decomposing Unicode and removing diacritics
        3) lowercasing
    Example: 'Harry Hamrén' -> 'harry hamren'
    """
    s = name.strip()
    s = re.sub(r"\s+", " ", s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.lower()