
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
from typing import List, Dict, Any, Optional
import sqlite3
import uuid


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
        print(f"   {emoji} {st.capitalize()}: {total:,}")
        logging.info(f"   - {st.capitalize()}: {total}")
        for reason, cnt in summary[st].items():
            print(f"      • {reason}: {cnt:,}")
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

def name_keys_for_lookup_all_splits(name: str) -> list[str]:
    """
    Generate normalized name keys:
      - the raw normalized string
      - for every split point i (1..n-1): 
          * "lastname firstname" where lastname = tokens[i:], firstname = tokens[:i]
          * "firstname lastname" where firstname = tokens[:i], lastname = tokens[i:]
    This covers any number of first-/last-name tokens.
    """
    n = normalize_key(name)
    parts = n.split()
    if len(parts) <= 1:
        return [n]

    keys: set[str] = {n}
    for i in range(1, len(parts)):
        first = " ".join(parts[:i])
        last  = " ".join(parts[i:])
        keys.add(f"{last} {first}")   # lastname firstname
        keys.add(f"{first} {last}")   # firstname lastname
    return list(keys)


class ResultAggregator:
    """
        Aggregates operation results (e.g., from scrape, parse, upsert) and summarizes them.
        Configurable for verbosity, logging, and printing.

        Usage:
        - Initialize with parameters:
        aggregator = ResultAggregator(verbosity=2, print_output=True, log_level="DEBUG")
        - Add results from each step:
        aggregator.add_result({"status": "success", "key": "item1", "reason": "OK", "warnings": ["minor issue"]})
        - Call summarize() at the end to print/log the aggregated summary.
        - Optionally call print_individual_details() for detailed per-result output (requires verbosity >= 3).

        Parameters:
        - verbosity (int): Controls detail level:
            0: Summary totals only (no reasons/warnings).
            1: Totals + reason breakdowns (default).
            2: Level 1 + warning breakdowns.
            3: Level 2 + individual result details (via print_individual_details).
        - print_output (bool): If True, prints to console; else only logs (default: True).
        - log_level (str): Logging level (e.g., "INFO", "DEBUG") for logging module (default: "INFO").

        {
        "status":       "success" | "failed" | "skipped",           # Required
        "key":          str,                                        # Unique identifier (e.g., "tournament_shortname_2025-08-15")
        "reason":       str,                                        # Explanation (e.g., "Missing longname")
        "warnings":     List[str],                                  # Optional list of non-fatal issues
        "step":         "scrape" | "parse" | "validate" | "upsert"  # For filtering by phase
        }       

        """
    def __init__(
            self, 
            verbosity:      int = 1, 
            print_output:   bool = True,
            log_level:      str = "INFO"
        ):
        self.results: List[Dict[str, Any]] = []
        self.verbosity = verbosity
        self.print_output = print_output
        self.log_level = log_level
        logging.basicConfig(level=self.log_level)

    def add_result(
            self, 
            result: Dict[str, Any]
        ) -> None:

        """Add a result dict from any operation step."""
        self.results.append(result)

    def print_individual_details(self) -> None:
        """
        Print detailed individual results based on verbosity level.
        Only executes if verbosity >= 2. At verbosity=2, shows warnings per key if any. At verbosity>=3, shows full details including status, reason, and step.
        Useful for debugging specific items (e.g., which tournament caused a skip).
        """
        if self.verbosity < 2:
            return  # No individual details below verbosity 2

        for r in self.results:
            key = r.get("key", "Unknown")
            warnings = r.get("warnings", [])

            if warnings:
                warn_header = f"Warnings for Key: {key} | Step: {r.get('step', 'Unknown')}"
                logging.info(warn_header)
                if self.print_output:
                    print(warn_header)
                for w in warnings:
                    warn_msg = f"  - {w}"
                    logging.info(warn_msg)
                    if self.print_output:
                        print(warn_msg)

            if self.verbosity >= 3:
                status = r["status"].upper()
                reason = r.get("reason", "No reason provided")
                step = r.get("step", "Unknown")
                log_message = f"[{status}] Key: {key} | Step: {step} | Reason: {reason}"
                logging.info(log_message)
                if self.print_output:
                    print(log_message)

    def summarize(self) -> None:
        """Aggregate and log/print summary based on verbosity.
        Warnings are logged and printed at verbosity >= 2 if any exist.
        The summary is always printed to console if print_output=True, regardless of verbosity.
        """
        all_statuses = ["success", "failed", "skipped"]
        summary = {st: defaultdict(int) for st in all_statuses}
        warnings_list: List[str] = []

        for r in self.results:
            st, rs = r["status"], r.get("reason", "Unknown")
            summary[st][rs] += 1
            warnings_list.extend(r.get("warnings", []))

        output = "📊 Operation Summary:\n"
        logging.info("Operation Summary:")
        for st, emoji in [("success", "✅"), ("failed", "❌"), ("skipped", "⏭️ ")]:
            total = sum(summary[st].values())
            log_message = f"   - {st.capitalize()}: {total}"
            logging.info(log_message)
            output += f"   {emoji} {st.capitalize()}: {total:,}\n"
            if self.verbosity >= 1:
                for reason, cnt in summary[st].items():
                    reason_msg = f"      • {reason}: {cnt:,}"
                    logging.info(reason_msg)
                    output += reason_msg + "\n"

        warn_counts = defaultdict(int)
        for msg in warnings_list:
            warn_counts[msg] += 1
        total_warns = len(warnings_list)
        log_warn = f"   - Warning: {total_warns}"
        logging.info(log_warn)
        output += f"   ⚠️  Warning: {total_warns}\n"
        if self.verbosity >= 2 and total_warns:
            for reason, cnt in warn_counts.items():
                warn_msg = f"      • {reason}: {cnt}"
                logging.info(warn_msg)
                output += warn_msg + "\n"

        print(output)

class OperationLogger:
    """
    A general logging class for tracking success, failed, skipped, and warnings in operations like scrapers and updates.
    
    Usage:
    - Initialize at the start of a script:
      logger = OperationLogger(verbosity=1, print_output=True, log_to_db=False, cursor=None)
    - Add messages during processing:
      logger.success('item1', 'Processed OK')
      logger.failure('item2', 'Invalid data')
      logger.skip('item3', 'Duplicate')
      logger.warning('item4', 'Minor issue')
    - Call summarize() at the end to print/log the summary.
    
    Parameters:
    - verbosity (int): Controls detail level:
        0: Summary totals only.
        1: Totals + reason breakdowns (default).
        2: Level 1 + individual details for failed/skipped/warnings.
        3: Level 2 + detailed output for all items.
    - print_output (bool): If True, prints to console (default: True).
    - log_to_db (bool): If True, logs details to DB (requires cursor).
    - cursor (sqlite3.Cursor): DB cursor for logging to table (required if log_to_db=True).

    The class generates a unique run_id per instance for grouping logs in DB.
    """
    def __init__(
        self,
        verbosity:      int = 1,
        print_output:   bool = True,
        log_to_db:      bool = False,
        cursor:         Optional[sqlite3.Cursor] = None
    ):
        self.run_id             = str(uuid.uuid4())  # Unique ID for this run/script execution
        self.verbosity          = verbosity
        self.print_output       = print_output
        self.log_to_db          = log_to_db
        self.cursor             = cursor if log_to_db else None
        self.results            = defaultdict(lambda: {"success": 0, "failed": 0, "skipped": 0, "warnings": []})
        self.reasons            = {"success": defaultdict(int), "failed": defaultdict(int), "skipped": defaultdict(int)}
        self.individual_logs    = []  # For detailed output

        if log_to_db and not cursor:
            raise ValueError("Cursor required if log_to_db is True")

    def _log_to_db(self, item_key: str, status: str, reason: Optional[str] = None, message: Optional[str] = None):
        if self.log_to_db:
            try:
                self.cursor.execute('''
                    INSERT INTO log_events (run_id, item_key, status, reason, message)
                    VALUES (?, ?, ?, ?, ?)
                ''', (self.run_id, item_key, status, reason, message))
            except Exception as e:
                logging.error(f"Error logging to DB: {e}")

    def success(self, item_key: str, reason: Optional[str] = "Success"):
        self.results[item_key]["success"] += 1
        self.reasons["success"][reason] += 1
        self._log_to_db(item_key, "success", reason)
        if self.verbosity > 2:
            msg = f"[SUCCESS] {item_key}: {reason}"
            logging.info(msg)
            if self.print_output:
                print(msg)

    def failed(self, item_key: str, reason: Optional[str] = "Failed"):
        self.results[item_key]["failed"] += 1
        self.reasons["failed"][reason] += 1
        self._log_to_db(item_key, "failed", reason)
        if self.verbosity >= 1:
            msg = f"[FAILED] {item_key}: {reason}"
            logging.error(msg)
            if self.print_output:
                print(msg)

    def skipped(self, item_key: str, reason: Optional[str] = "Skipped"):
        self.results[item_key]["skipped"] += 1
        self.reasons["skipped"][reason] += 1
        self._log_to_db(item_key, "skipped", reason)
        if self.verbosity <= 2:
            msg = f"[SKIPPED] {item_key}: {reason}"
            logging.warning(msg)
            if self.print_output:
                print(msg)

    def warning(self, item_key: str, message: str):
        self.results[item_key]["warnings"].append(message)
        self._log_to_db(item_key, "warning", None, message)
        # logging.warning(f"[WARNING] {item_key}: {message}", stacklevel=2)
        if self.verbosity >= 2:
            msg = f"[WARNING] {item_key}: {message}"
            logging.warning(msg, stacklevel=2)
            if self.print_output:
                print(msg)

    def summarize(self):
        """Generate and print/log the full summary, always including totals."""
        total_success = sum(d["success"] for d in self.results.values())
        total_failed = sum(d["failed"] for d in self.results.values())
        total_skipped = sum(d["skipped"] for d in self.results.values())
        total_warnings = sum(len(d["warnings"]) for d in self.results.values())

        output = "\n📊 Operation Summary:\n"
        output += f"   ✅ Success: {total_success}\n"
        if self.verbosity >= 1:
            for reason, count in self.reasons["success"].items():
                output += f"      • {reason}: {count}\n"
        output += f"   ❌ Failed: {total_failed}\n"
        if self.verbosity >= 1:
            for reason, count in self.reasons["failed"].items():
                output += f"      • {reason}: {count}\n"
        output += f"   ⏭️  Skipped: {total_skipped}\n"
        if self.verbosity >= 1:
            for reason, count in self.reasons["skipped"].items():
                output += f"      • {reason}: {count}\n"
        output += f"   ⚠️  Warnings: {total_warnings}\n"

        # Always print summary to console
        print(output)
        logging.info(output)