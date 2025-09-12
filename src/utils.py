
# src/utils.py
# Contains reusable functions like WebDriver setup, waiting mechanisms, and HTML parsing helpers.

from dataclasses import fields
import hashlib
import inspect
import json
from pathlib import Path
import time
import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime
from collections import defaultdict
import logging
import os
import re
import unicodedata
from datetime import datetime, date
from config import LOG_FILE, LOG_LEVEL, PDF_CACHE_DIR
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
import sqlite3
import uuid
from db import get_conn

CACHE_DIR = Path(PDF_CACHE_DIR)


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
    # logging.warning(f"Invalid date format: {date_str} (context: {context or 'unknown calling function'})")
    return None

def sanitize_name(
        name: str
    ) -> str:
    """ 
    Sanitize a name by stripping, splitting, and title-casing each word.
    Example: 'harry hamrén' -> 'Harry Hamrén' 
    """
    return ' '.join(word.strip().title() for word in name.split())

def normalize_key(
        name: str,
        *,
        preserve_diacritics: bool = False,
        preserve_nordic: bool = True
    ) -> str:
    """
    Normalize for matching.

    Parameters
    ----------
    preserve_diacritics : bool
        If True, keep all diacritics (Å, Ä, Ö, Ø, É, etc.)
    preserve_nordic : bool
        When stripping diacritics, optionally preserve å, ä, ö, ø

    Examples
    --------
    "Virum"             -> "virum"
    "Nørre"             -> "norre"  (unless preserve_diacritics=True)
    "Åby"               -> "åby"    (if preserve_nordic=True)
    """
    s = name.strip()
    s = re.sub(r"\s+", " ", s)
    s = s.lower()

    if preserve_diacritics:
        return s

    normalized = []
    for ch in s:
        if preserve_nordic and ch in "åäöø":
            normalized.append(ch)
        else:
            decomp = unicodedata.normalize("NFKD", ch)
            normalized.append("".join(c for c in decomp if not unicodedata.combining(c)))
    return "".join(normalized)

def compute_content_hash(obj: Any, exclude_fields: Iterable[str] = None) -> str:
    """
    Compute a stable SHA256 hash for a dataclass-like object.
    Dynamically includes all fields except those in exclude_fields.
    - normalize_key() is used for strings
    - dates use ISO format
    - booleans are cast to int (0/1)
    - None → empty string
    """
    if exclude_fields is None:
        exclude_fields = []

    parts = []
    for field in fields(obj):
        if field.name in exclude_fields:
            continue
        value = getattr(obj, field.name)
        if value is None:
            parts.append("")
        elif isinstance(value, str):
            parts.append(normalize_key(value))
        elif isinstance(value, date):
            parts.append(value.isoformat())
        elif isinstance(value, bool):
            parts.append(str(int(value)))
        else:
            parts.append(str(value))  # int, float, fallback

    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()

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


###################################
### PDF Downloading and Caching 
###################################

def _format_size(bytes_size: int) -> str:
    """Format size into KB/MB/GB string."""
    if bytes_size < 1024:
        return f"{bytes_size} B"
    elif bytes_size < 1024 * 1024:
        return f"{bytes_size / 1024:.1f} KB"
    elif bytes_size < 1024 * 1024 * 1024:
        return f"{bytes_size / (1024 * 1024):.2f} MB"
    else:
        return f"{bytes_size / (1024 * 1024 * 1024):.2f} GB"

def _is_valid_pdf(path: Path) -> bool:
    """Check if file starts with %PDF- header."""
    try:
        with open(path, "rb") as f:
            header = f.read(5)
            return header == b"%PDF-"
    except Exception:
        return False

def _get_pdf_path(tournament_id_ext: str, class_id_ext: str, stage: int) -> Path:
    """Generate the path for a PDF file."""
    return CACHE_DIR / f"tournament_{tournament_id_ext}" / f"class_{class_id_ext}" / f"stage_{stage}.pdf"

def _download_pdf_ondata_by_tournament_class_and_stage(tournament_id_ext: str, class_id_ext: str, stage: int, force_download: bool = False) -> Tuple[Optional[Path], bool, Optional[str]]:
    """Download a PDF if available. Returns (path, downloaded, message) where:
    - path: Path to the PDF or None if failed
    - downloaded: True if newly downloaded, False if cached or skipped
    - message: Status or error message for logging (None if no special message)
    """
    pdf_path = _get_pdf_path(tournament_id_ext, class_id_ext, stage)

    # If file exists but is invalid, remove it
    if pdf_path.exists() and not _is_valid_pdf(pdf_path):
        pdf_path.unlink()

    if pdf_path.exists() and not force_download:
        return pdf_path, False, f"Cached PDF used: {pdf_path}"

    url = f"https://resultat.ondata.se/ViewClassPDF.php?tournamentID={tournament_id_ext}&classID={class_id_ext}&stage={stage}"
    pdf_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        resp = requests.get(url, timeout=20)
        if resp.status_code != 200 or not resp.content.startswith(b"%PDF-"):
            return None, False, f"No valid PDF for stage {stage} (status: {resp.status_code})"

        with open(pdf_path, "wb") as f:
            f.write(resp.content)
        return pdf_path, True, f"Downloaded PDF: {pdf_path} ({_format_size(pdf_path.stat().st_size)})"

    except Exception as e:
        return None, False, f"Failed to download PDF from {url}: {e}"

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
        cursor:         Optional[sqlite3.Cursor] = None,
        object_type:    Optional[str] = None,   # e.g., 'tournament_raw',
        run_type:       Optional[str] = None    # e.g., 'scrape', 'resolve', 'update'
    ):
        self.run_id             = str(uuid.uuid4())  # Unique ID for this run/script execution
        self.verbosity          = verbosity
        self.print_output       = print_output
        self.log_to_db          = log_to_db
        self.cursor             = cursor if log_to_db else None
        self.results            = defaultdict(lambda: {"success": 0, "failed": 0, "skipped": 0})
        self.reasons            = {"success": defaultdict(int), "failed": defaultdict(int), "skipped": defaultdict(int), "warning": defaultdict(int)}
        self.individual_logs    = []
        self.object_type        = object_type   
        self.run_type           = run_type
        self.processed          = 0
        self.start_time         = time.time()

        if log_to_db and not cursor:
            raise ValueError("Cursor required if log_to_db is True")

    def inc_processed(self, n: int = 1):
        """Increment number of processed records (used for overhead tracking)."""
        self.processed += n

    def _format_msg(self, context: dict, reason: str) -> str:
        return f"({', '.join(f'{k}: {v}' for k,v in context.items())}): {reason}"
        
    def _enrich_context(self, context: dict) -> dict:
        """
        Enrich context dict with name conversions by querying DB.
        Adds fields like 'player_name', 'yearborn', 'club_shortname' if IDs present.
        Skips if no cursor or no matching row.
        """
        enriched = context.copy()

        # Convert dates to strings
        for key, value in enriched.items():
            if isinstance(value, date):
                enriched[key] = value.isoformat()  # Convert to YYYY-MM-DD
        
        if 'player_id' in enriched and self.cursor:
            try:
                self.cursor.execute(
                    "SELECT firstname, lastname, yearborn FROM players WHERE id = ?",
                    (enriched['player_id'],)
                )
                row = self.cursor.fetchone()
                if row:
                    enriched['player_name'] = f"{row[0]} {row[1]}".strip()
                    enriched['yearborn'] = row[2]
            except Exception:
                pass
        
        if 'club_id' in enriched and self.cursor:
            try:
                self.cursor.execute(
                    "SELECT shortname FROM club WHERE club_id = ?",
                    (enriched['club_id'],)
                )
                row = self.cursor.fetchone()
                if row:
                    enriched['club_shortname'] = row[0]
            except Exception:
                pass
        
        return enriched
    
    def _parse_context_str(self, context_str: str) -> Dict[str, any]:
        """
        Parse old concatenated string to dict (e.g., "(player_id: None, club_id: 482)" -> {'player_id': None, 'club_id': 482}).
        Handles common formats; falls back to {'key': context_str} if parsing fails.
        """
        try:
            # Remove outer parens if present
            cleaned = context_str.strip("() ")
            pairs = cleaned.split(", ")
            parsed = {}
            for pair in pairs:
                if ":" in pair:
                    key, value = pair.split(":", 1)
                    key = key.strip()
                    value = value.strip()
                    # Convert common types (None, int)
                    if value == 'None':
                        value = None
                    elif value.isdigit():
                        value = int(value)
                    parsed[key] = value
            return parsed
        except Exception:
            # Fallback for compat
            return {'key': context_str}
        
    # def log_error_to_db(
    #         self,
    #         severity: str,
    #         message: str,
    #         context_dict: dict,
    #         msg_id: Optional[str] = None
    #     ):
    #     """Structured error/warning/skipped logging to log_details."""
    #     frame = inspect.currentframe().f_back
    #     function_name = frame.f_code.co_name
    #     filename = os.path.basename(inspect.getfile(frame))

    #     enriched_context = self._enrich_context(context_dict)
    #     context_json = json.dumps(enriched_context)

    #     if self.cursor:
    #         try:
    #             self.cursor.execute('''
    #                 INSERT INTO log_details (
    #                     run_id, run_date, object_type, process_type,
    #                     function_name, filename, context_json, status, message, msg_id
    #                 ) VALUES (
    #                     ?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?
    #                 )
    #             ''', (
    #                 self.run_id,
    #                 self.object_type or "unknown",
    #                 self.run_type or "unknown",
    #                 function_name,
    #                 filename,
    #                 context_json,
    #                 severity,
    #                 message,
    #                 msg_id
    #             ))
    #             self.cursor.connection.commit()
    #         except Exception as e:
    #             logging.error(f"Error in log_error_to_db: {e}")

    #     self.individual_logs.append({
    #         'status': severity,
    #         'context': enriched_context,
    #         'message': message,
    #         'msg_id': msg_id,
    #         'function_name': function_name,
    #         'filename': filename
    #     })

    def clear_previous_logs(
            self,
            by: str = 'filename',  # 'filename', 'function_name', 'all', or 'run_id'
            value: Optional[str] = None  # Specific value to match (e.g., script name or run_id)
        ):
            """
            Clear previous logs from log_output table based on criteria.
            - by: 'filename' (default, clears for current script), 'function_name', 'all', or 'run_id'.
            - value: Optional filter value (e.g., specific filename or run_id); auto-detects if None.
            Example: logger.clear_previous_logs()  # Clears for current filename
            """
            if not self.cursor:
                logging.warning("No cursor; skipping log cleanup")
                return
            
            try:
                if by == 'all':
                    self.cursor.execute("DELETE FROM log_output")
                else:
                    if value is None:
                        # Auto-detect based on caller
                        frame = inspect.currentframe().f_back
                        if by == 'filename':
                            value = os.path.basename(inspect.getfile(frame))
                        elif by == 'function_name':
                            value = frame.f_code.co_name
                        elif by == 'run_id':
                            value = self.run_id  # For current run (though rare)
                    
                    if value:
                        if by == 'filename':
                            self.cursor.execute("DELETE FROM log_output WHERE filename = ?", (value,))
                        elif by == 'function_name':
                            self.cursor.execute("DELETE FROM log_output WHERE function_name = ?", (value,))
                        elif by == 'run_id':
                            self.cursor.execute("DELETE FROM log_output WHERE run_id = ?", (value,))
                    else:
                        raise ValueError(f"Value required for 'by={by}'")
                
                self.cursor.connection.commit()
                logging.info(f"Cleared previous logs by {by}={value}")
            except Exception as e:
                logging.error(f"Error clearing logs: {e}")

    def info(
        self,
        item_key_or_message: str,
        reason: Optional[str] = None,
        *,
        show_key: bool = True,
        to_console: Optional[bool] = True,
        emoji: str = "ℹ️ ",
    ):
        """
        Usage:
        logger.info("global", "Scraping player licenses...", to_console=True)  # with key
        logger.info("Scraping player licenses...", to_console=True)            # message-only
        logger.info("global", "Starting...", show_key=False, to_console=True)  # hide key in output

        Notes:
        - Does NOT affect counters/summaries.
        - Still logs to DB via _log_to_db with status 'info'.
        """
        if reason is None:
            # Single-arg: treat as message-only
            message = item_key_or_message
            db_key  = "global"   # grouping key for DB; adjust if you prefer ""
            log_msg = message
        else:
            # Two-arg: item_key + reason
            db_key  = item_key_or_message or "global"
            log_msg = f"{item_key_or_message}: {reason}" if (item_key_or_message and show_key) else reason

        # Write to log (keeps your formatter clean)
        logging.info(log_msg, stacklevel=2)

        # Console printing control
        should_print = self.print_output if to_console is None else to_console
        if should_print:
            print(f"{emoji} {log_msg}")


    def success(
        self, 
        context: Union[dict, str], 
        reason: Optional[str] = "Success",
        msg_id: Optional[str] = None,
        *,  # keyword-only after this
        to_console: Optional[bool] = None,
        emoji: str = "✅ ",
        show_key: bool = True,
    ):
        if isinstance(context, str):
            context = self._parse_context_str(context)
        self.results[str(context)]["success"] += 1
        self.reasons["success"][reason] += 1
        
        # Get caller info
        frame = inspect.currentframe().f_back
        function_name = frame.f_code.co_name
        filename = os.path.basename(inspect.getfile(frame))
        
        # Enrich context
        enriched_context = self._enrich_context(context)
        context_json = json.dumps(enriched_context)
        
        # Prepare message
        if show_key:
            msg = self._format_msg(enriched_context, reason) if isinstance(enriched_context, dict) else f"{enriched_context}: {reason}"
        else:
            msg = reason
        
        # DB and file logging controlled by verbosity
        if self.verbosity >= 3:
            if self.cursor:
                try:
                    self.cursor.execute('''
                        INSERT INTO log_details (
                            run_id, run_date, object_type, process_type,
                            function_name, filename, context_json, status, message, msg_id
                        ) VALUES (?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        self.run_id,
                        self.object_type or "unknown",
                        self.run_type or "unknown",
                        function_name,
                        filename,
                        context_json,
                        'success',
                        reason,
                        msg_id
                    ))
                    self.cursor.connection.commit()
                except Exception as e:
                    logging.error(f"Error logging success to DB: {e}")
            
            logging.info(msg, stacklevel=2)
        
        # Console printing controlled solely by to_console (print if True, ignore if None or False)
        should_print = to_console if to_console is not None else False
        if should_print:
            print(f"{emoji} {msg}")
        
        # Store for export/summary
        self.individual_logs.append({
            'status': 'success',
            'context': enriched_context,
            'message': reason,
            'msg_id': msg_id,
            'function_name': function_name,
            'filename': filename
        })
 
    def failed(
        self, 
        context: Union[dict, str],
        reason: Optional[str] = "Failed",
        msg_id: Optional[str] = None,
        *,  # keyword-only after this
        to_console: Optional[bool] = None,
        emoji: str = "❌ ",
        show_key: bool = True,
    ):
        if isinstance(context, str):
            context = {'key': context}  # Backward compat: Treat string as simple key
        self.results[str(context)]["failed"] += 1  # Keep for counters (use str(key) for dict)
        self.reasons["failed"][reason] += 1
        
        # Get caller info (like log_error_to_db)
        frame = inspect.currentframe().f_back
        function_name = frame.f_code.co_name
        filename = os.path.basename(inspect.getfile(frame))
        
        # Enrich context
        enriched_context = self._enrich_context(context)
        context_json = json.dumps(enriched_context)
        
        # Write to log_details table (structured DB log, always for failed)
        if self.cursor:
            try:
                self.cursor.execute('''
                    INSERT INTO log_details (
                        run_id, run_date, object_type, process_type,
                        function_name, filename, context_json, status, message, msg_id
                    ) VALUES (?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    self.run_id,
                    self.object_type or "unknown",
                    self.run_type or "unknown",
                    function_name,
                    filename,
                    context_json,
                    'error',
                    reason,
                    msg_id
                ))
                self.cursor.connection.commit()
            except Exception as e:
                logging.error(f"Error logging failed to DB: {e}")
        
        # Prepare message
        if show_key:
            msg = self._format_msg(enriched_context, reason) if isinstance(enriched_context, dict) else f"{enriched_context}: {reason}"
        else:
            msg = reason
        
        # File logging controlled by verbosity
        if self.verbosity >= 1:
            logging.error(msg, stacklevel=2)
        
        # Console printing controlled solely by to_console (print if True, ignore if None or False)
        should_print = to_console if to_console is not None else False
        if should_print:
            print(f"{emoji} {msg}")
        
        # Store for export/summary
        self.individual_logs.append({
            'status': 'error',
            'context': enriched_context,
            'message': reason,
            'msg_id': msg_id,
            'function_name': function_name,
            'filename': filename
        })


    def skipped(
        self, 
        context: Union[dict, str],
        reason: Optional[str] = "Skipped",
        msg_id: Optional[str] = None,
        *,  # keyword-only after this
        to_console: Optional[bool] = None,
        emoji: str = "⏭️  ",
        show_key: bool = True,
    ):
        if isinstance(context, str):
            context = self._parse_context_str(context)
        self.results[str(context)]["skipped"] += 1
        self.reasons["skipped"][reason] += 1
        
        # Get caller info
        frame = inspect.currentframe().f_back
        function_name = frame.f_code.co_name
        filename = os.path.basename(inspect.getfile(frame))
        
        # Enrich context
        enriched_context = self._enrich_context(context)
        context_json = json.dumps(enriched_context)
        
        # Prepare message
        if show_key:
            msg = self._format_msg(enriched_context, reason) if isinstance(enriched_context, dict) else f"{enriched_context}: {reason}"
        else:
            msg = reason
        
        # DB logging controlled by verbosity
        if self.cursor and self.verbosity >= 3:
            try:
                self.cursor.execute('''
                    INSERT INTO log_details (
                        run_id, run_date, object_type, process_type,
                        function_name, filename, context_json, status, message, msg_id
                    ) VALUES (?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    self.run_id,
                    self.object_type or "unknown",
                    self.run_type or "unknown",
                    function_name,
                    filename,
                    context_json,
                    'skipped',
                    reason,
                    msg_id
                ))
                self.cursor.connection.commit()
            except Exception as e:
                logging.error(f"Error logging skipped to DB: {e}")
        
        # File logging controlled by verbosity
        if self.verbosity >= 3:
            logging.warning(msg, stacklevel=2)
        
        # Console printing controlled solely by to_console (print if True, ignore if None or False)
        should_print = to_console if to_console is not None else False
        if should_print:
            print(f"{emoji} {msg}")
        
        # Store for export/summary
        self.individual_logs.append({
            'status': 'skipped',
            'context': enriched_context,
            'message': reason,
            'msg_id': msg_id,
            'function_name': function_name,
            'filename': filename
        })

    def warning(
        self, 
        context: Union[dict, str], 
        reason: str,
        msg_id: Optional[str] = None,
        *,
        to_console: Optional[bool] = None,  # Optional console print control
        emoji: str = "⚠️  ",  # Optional emoji for warning
        show_key: bool = True,  # Optional show_key, similar to others
    ):
        if isinstance(context, str):
            context = self._parse_context_str(context)
        self.reasons["warning"][reason] += 1  # Keep for total counts
        
        frame = inspect.currentframe().f_back
        function_name = frame.f_code.co_name
        filename = os.path.basename(inspect.getfile(frame))
        
        enriched_context = self._enrich_context(context)
        context_json = json.dumps(enriched_context)
        
        # Prepare message
        if show_key:
            msg = self._format_msg(enriched_context, reason) if isinstance(enriched_context, dict) else f"{enriched_context}: {reason}"
        else:
            msg = reason
        
        # Write to log_output table (always for warning)
        if self.cursor:
            try:
                self.cursor.execute('''
                    INSERT INTO log_output (run_id, function_name, filename, context_json, status, message, msg_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (self.run_id, function_name, filename, context_json, 'warning', reason, msg_id))
                self.cursor.connection.commit()
            except Exception as e:
                logging.error(f"Error logging warning to DB: {e}")
        
        # File logging controlled by verbosity
        if self.verbosity >= 2:
            logging.warning(msg, stacklevel=2)
        
        # Console printing controlled solely by to_console (print if True, ignore if None or False)
        should_print = to_console if to_console is not None else False
        if should_print:
            print(f"{emoji} {msg}")
        
        self.individual_logs.append({
            'status': 'warning',
            'context': enriched_context,
            'message': reason,
            'msg_id': msg_id,
            'function_name': function_name,
            'filename': filename
        })


    def summarize(self):
        """Generate and print/log the full summary, always including totals, one line at a time."""
        total_success   = sum(d["success"]          for d in self.results.values())
        total_failed    = sum(d["failed"]           for d in self.results.values())
        total_skipped   = sum(d["skipped"]          for d in self.results.values())
        # total_warnings  = sum(len(d["warnings"])    for d in self.results.values())
        total_warnings  = sum(self.reasons["warning"].values())

        lines = []
        lines.append("📊 Operation Summary:")
        lines.append(f"   ✅ Success: {total_success}")
        if self.verbosity >= 1:
            for reason, count in self.reasons["success"].items():
                lines.append(f"      • {reason}: {count}")

        lines.append(f"   ❌ Failed: {total_failed}")
        if self.verbosity >= 1:
            for reason, count in self.reasons["failed"].items():
                lines.append(f"      • {reason}: {count}")

        lines.append(f"   ⏭️  Skipped: {total_skipped}")
        if self.verbosity >= 1:
            for reason, count in self.reasons["skipped"].items():
                lines.append(f"      • {reason}: {count}")

        lines.append(f"   ⚠️  Warnings: {total_warnings}")
        if self.verbosity >= 1:
            for reason, count in self.reasons["warning"].items():
                lines.append(f"      • {reason}: {count}")

        if hasattr(self, "start_time"):
            runtime_seconds = time.time() - self.start_time
            lines.append("")
            lines.append(f"   ⏱️  Runtime: {runtime_seconds:.1f}s")
            if hasattr(self, "processed"):
                lines.append(f"   📦 Records processed: {self.processed}")
                if runtime_seconds > 0:
                    throughput = self.processed / runtime_seconds
                    lines.append(f"   ⚡ Throughput: {throughput:.1f} records/sec")

        # if hasattr(self, "start_time"):
        #     runtime_seconds = time.time() - self.start_time
        #     lines.append(f"   ⏱️  Runtime: {runtime_seconds:.1f}s")
        #     lines.append(f"   ⚡ Throughput: {throughput:.1f} records/sec")

        # if hasattr(self, "processed"):
        #     runtime_seconds = time.time() - self.start_time
        #     lines.append(f"   📦 Records processed: {self.processed}")
        #     lines.append(f"   ⏱️ Runtime: {runtime_seconds:.1f}s")
        #     if runtime_seconds > 0:
        #         throughput = self.processed / runtime_seconds
        #         lines.append(f"   ⚡ Throughput: {throughput:.1f} records/sec")
                
        logging.info("")
        print("")
        for line in lines:
            logging.info(line, stacklevel=2)
            print(line)
        logging.info("")
        print("")

    def commit_run_summary(self, cursor: sqlite3.Cursor, remarks: Optional[str] = None):
        runtime_seconds = time.time() - self.start_time
        total_success   = sum(d["success"] for d in self.results.values())
        total_failed    = sum(d["failed"] for d in self.results.values())
        total_skipped   = sum(d["skipped"] for d in self.results.values())
        total_warnings  = sum(self.reasons["warning"].values())
        
        cursor.execute("""
            INSERT INTO run_log (
                run_id, object_type, process_type, records_processed,
                records_success, records_failed, records_skipped,
                records_warnings, runtime_seconds, remarks
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            self.run_id,
            self.object_type or "unknown",
            self.run_type or "unknown",
            self.processed, total_success, total_failed,
            total_skipped, total_warnings, runtime_seconds, remarks
        ))
        cursor.connection.commit()
        
def export_logs_to_excel():
    """
    Export the latest run's record-level logs (log_details) to logs.xlsx.
    Always rewrites the file, so it only contains the most recent run.
    """
    conn, cursor = get_conn()
    df = pd.read_sql_query(
        "SELECT * FROM log_details WHERE run_id = (SELECT MAX(run_id) FROM log_details)",
        conn
    )
    conn.close()

    if df.empty:
        print("ℹ️  No logs to export.")
        logging.info("No logs to export.")
        return

    # Parse and flatten context_json into columns
    df['context'] = df['context_json'].apply(lambda x: json.loads(x) if x else {})
    context_df = pd.json_normalize(df['context'])
    df = pd.concat([df.drop(['context', 'context_json'], axis=1), context_df], axis=1)

    with pd.ExcelWriter("logs.xlsx", engine='openpyxl') as writer:
        # All logs
        df.to_excel(writer, sheet_name='All_Logs', index=False)

        # By status (split into tabs)
        for status in ['error', 'warning', 'skipped']:
            subset = df[df['status'] == status]
            if not subset.empty:
                subset.to_excel(writer, sheet_name=status.capitalize(), index=False)

        # Separate tab per msg_id (if present)
        error_groups = df[df['msg_id'].notna()]
        for msg_id in error_groups['msg_id'].unique():
            subset = error_groups[error_groups['msg_id'] == msg_id]
            sheet_name = f'{subset["status"].iloc[0]}_{msg_id}'[:31]  # Excel sheet name limit
            subset.to_excel(writer, sheet_name=sheet_name, index=False)

    print("ℹ️  Exported latest run logs to logs.xlsx")
    logging.info("Exported latest run logs to logs.xlsx")


def export_runs_to_excel():
    """
    Export the latest run-level summary (log_runs) to run_log.xlsx.
    Always rewrites the file, so it only contains the most recent run.
    """
    conn, cursor = get_conn()
    df = pd.read_sql_query(
        "SELECT * FROM log_runs WHERE run_id = (SELECT MAX(run_id) FROM log_runs)",
        conn
    )
    conn.close()

    if df.empty:
        print("ℹ️  No run summaries to export.")
        logging.info("No run summaries to export.")
        return

    with pd.ExcelWriter("run_log.xlsx", engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Run_Summary', index=False)

    print("ℹ️  Exported latest run summary to run_log.xlsx")
    logging.info("Exported latest run summary to run_log.xlsx")


def clear_debug_tables(cursor: sqlite3.Cursor, clear_logs: bool = True, clear_runs: bool = False):
    """
    Clear debug tables based on flags.

    - clear_logs=True  → clears log_details table (record-level logs).
    - clear_runs=True  → clears log_runs table (run-level summaries).

    Typical usage:
    - clear only logs between runs, but keep run history:
        clear_debug_tables(cursor, clear_logs=True, clear_runs=False)
    - clear everything (fresh start):
        clear_debug_tables(cursor, clear_logs=True, clear_runs=True)
    """
    try:
        if clear_logs:
            cursor.execute("DELETE FROM log_details")
            logging.info("log_details table cleared.")
            print("ℹ️  log_details table cleared.")

        if clear_runs:
            cursor.execute("DELETE FROM log_runs")
            logging.info("log_runs table cleared.")
            print("ℹ️  log_runs table cleared.")

        cursor.connection.commit()
    except Exception as e:
        logging.error(f"Error clearing tables: {e}")
        print(f"❌ Error clearing tables: {e}")
