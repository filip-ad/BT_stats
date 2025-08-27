
# src/utils.py
# Contains reusable functions like WebDriver setup, waiting mechanisms, and HTML parsing helpers.

import sys
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

def parse_date(date_str, context=None, return_iso=False):
    """
    Parse a date string in 'YYYY-MM-DD', 'YYYY.MM.DD', or ISO variants into a datetime.date object.
    If input is already a datetime.date, returns it unchanged (or as ISO string if requested).
    Optionally returns the date in ISO format ('YYYY-MM-DD') string.
    """
    if isinstance(date_str, date):
        return date_str.isoformat() if return_iso else date_str

    date_str = date_str.strip() if date_str else "None"
    for fmt in ("%Y-%m-%d", "%Y.%m.%d", "%Y%m%d"):
        try:
            parsed = datetime.strptime(date_str, fmt).date()
            return parsed.isoformat() if return_iso else parsed
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
    Updated: Produce a matching key by:
        1) stripping and collapsing whitespace
        2) lowercasing
        3) decomposing Unicode and removing diacritics *except* for Nordic letters (ÅÄÖåäö), which are preserved as-is.
    Example: 'Harry Hamrén ÅÄÖ' -> 'harry hamren åäö'
    """
    s = name.strip()
    s = re.sub(r"\s+", " ", s)
    s = s.lower()  # Lowercase first to simplify

    # Decompose and remove combining only for non-Nordic
    normalized = []
    for ch in s:
        if ch in 'åäö':  # Preserve lowercase Nordic as-is
            normalized.append(ch)
        else:
            decomp = unicodedata.normalize("NFKD", ch)
            normalized.append("".join(c for c in decomp if not unicodedata.combining(c)))
    
    return "".join(normalized)

def name_keys_for_lookup_all_splits(name: str) -> List[str]:
    """
    Generate normalized name keys:
      - the raw normalized string
      - for every split point i (1..n-1): 
          * "prefix suffix" where prefix = tokens[:i] (firstname(s)), suffix = tokens[i:] (lastname(s))
          * "suffix prefix" (reversed for lastname firstname order)
    This covers any number of first-/last-name tokens and possible order flips in PDFs.
    Example: For "John Doe Smith": ['smith john doe', 'john doe smith', 'doe smith john']
    Deduplicates unique keys.
    """
    n = normalize_key(name)
    parts = n.split()
    if len(parts) <= 1:
        return [n]
    keys = [n]  # Include raw normalized full string
    for i in range(1, len(parts)):
        prefix = " ".join(parts[:i])
        suffix = " ".join(parts[i:])
        fn_ln = f"{prefix} {suffix}"  # firstname(s) lastname(s)
        ln_fn = f"{suffix} {prefix}"  # lastname(s) firstname(s)
        keys.append(fn_ln)
        if fn_ln != ln_fn:  # Avoid dup if symmetric
            keys.append(ln_fn)
    return list(set(keys))  # Dedup and return as list

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
        self.reasons            = {"success": defaultdict(int), "failed": defaultdict(int), "skipped": defaultdict(int), "warning": defaultdict(int)}
        self.individual_logs    = []

        if log_to_db and not cursor:
            raise ValueError("Cursor required if log_to_db is True")

    def _log_to_db(
            self, 
            item_key:   str, 
            status:     str, 
            reason:     Optional[str] = None, 
            message:    Optional[str] = None
        ):
        if self.log_to_db:
            try:
                function_name = sys._getframe(1).f_code.co_name
                self.cursor.execute('''
                    INSERT INTO log_events (run_id, function_name, item_key, status, reason, message)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (self.run_id, function_name, item_key, status, reason, message))
            except Exception as e:
                logging.error(f"Error logging to DB: {e}")

    def success(
            self, 
            item_key:   str, 
            reason:     Optional[str] = "Success"
        ):
        self.results[item_key]["success"] += 1
        self.reasons["success"][reason] += 1     
        if self.verbosity >= 3:
            self._log_to_db(item_key, "success", reason)
            msg = f"[SUCCESS] {item_key}: {reason}"
            logging.info(msg, stacklevel=2)
            if self.print_output:
                print(msg)

    def failed(
            self, 
            item_key: str, 
            reason: Optional[str] = "Failed"
        ):
        self.results[item_key]["failed"] += 1
        self.reasons["failed"][reason] += 1
        self._log_to_db(item_key, "failed", reason)
        if self.verbosity >= 1:
            msg = f"[FAILED] {item_key}: {reason}"
            logging.error(msg,stacklevel=2)
            if self.print_output:
                print(msg)

    def skipped(
            self, 
            item_key: str, 
            reason: Optional[str] = "Skipped"
        ):
        self.results[item_key]["skipped"] += 1
        self.reasons["skipped"][reason] += 1
        self._log_to_db(item_key, "skipped", reason)
        if self.verbosity <= 3:
            msg = f"[SKIPPED] {item_key}: {reason}"
            logging.warning(msg)
            if self.print_output:
                print(msg)

    def warning(
            self, 
            item_key: str, 
            reason: str
        ):
        self.results[item_key]["warnings"].append(reason)
        self.reasons["warning"][reason] += 1
        self._log_to_db(item_key, "warning", reason)
        if self.verbosity >= 2:
            msg = f"[WARNING] {item_key}: {reason}"
            logging.warning(msg, stacklevel=2)
            if self.print_output:
                print(msg)

    def summarize(self):
        """Generate and print/log the full summary, always including totals."""
        total_success   = sum(d["success"]          for d in self.results.values())
        total_failed    = sum(d["failed"]           for d in self.results.values())
        total_skipped   = sum(d["skipped"]          for d in self.results.values())
        total_warnings  = sum(len(d["warnings"])    for d in self.results.values())

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
        if self.verbosity >= 1:
            for reason, count in self.reasons["warning"].items():
                output += f"      • {reason}: {count}\n"

        # Always print summary to console
        print(output)
        logging.info(output)