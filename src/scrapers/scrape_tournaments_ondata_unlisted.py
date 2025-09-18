# src/scrapers/scrape_tournaments_ondata_unlisted.py

import datetime
from typing import Optional
from bs4 import BeautifulSoup
import requests
from urllib.parse import urljoin
import time
import io
import pdfplumber
import re

from utils import OperationLogger, parse_date
from models.tournament_raw import TournamentRaw


def scrape_tournaments_ondata_unlisted(cursor) -> None:
    """
    Scrape unlisted tournaments by finding gaps in tournament_id_ext and trying those URLs.
    Also re-scrapes existing unlisted tournaments (is_listed = False) for updates.
    Upserts raw data into tournament_raw table.
    """
    logger = OperationLogger(
        verbosity       = 2,
        print_output    = False,
        log_to_db       = True,
        cursor          = cursor,
        object_type     = "tournament",
        process_type    = "scrape_ondata_unlisted"
    )

    # Fetch all tournament_id_ext and is_listed from tournament_raw table
    cursor.execute("SELECT tournament_id_ext, is_listed FROM tournament_raw WHERE data_source_id = 1 AND tournament_id_ext IS NOT NULL")
    rows = cursor.fetchall()
    existing_ids = {int(row[0]) for row in rows if row[0].isdigit()}
    unlisted_ids = [int(row[0]) for row in rows if row[0].isdigit() and row[1] == False]

    if not existing_ids:
        logger.failed({}, "No existing tournament IDs found", to_console=True)
        return

    # Find gaps from the highest ID down to 1
    max_id = max(existing_ids)
    min_id = 1  # Start from 000001
    gap_ids = [i for i in range(max_id, min_id - 1, -1) if i not in existing_ids]

    # Combine unlisted IDs (for updates) and gaps (for new discoveries), sort descending
    ids_to_process = sorted(set(unlisted_ids + gap_ids), reverse=True)

    if not ids_to_process:
        logger.info("No unlisted tournament IDs to process")
        return

    logger.info(f"Processing {len(ids_to_process)} unlisted tournament IDs (updates: {len(unlisted_ids)}, new gaps: {len(gap_ids)}).")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    for process_id in ids_to_process:
        ondata_id = f"{process_id:06d}"  # Pad to 6 digits
        full_url = f"https://resultat.ondata.se/{ondata_id}/"
        
        logger_keys = {
            "ondata_id": ondata_id,
            "full_url": full_url,
            "longname": None
        }

        # Check if the tournament URL exists
        if not _check_tournament_url(ondata_id, headers, logger, logger_keys):
            logger.failed(logger_keys, f"Tournament URL not found", to_console=True)
            continue

        # Fetch longname
        longname = _fetch_tournament_longname(ondata_id, headers, logger, logger_keys)
        
        if not longname:
            logger.failed(logger_keys, f"No valid longname found", to_console=True)
            continue

        logger_keys["longname"] = longname

        # Fetch dates
        start_date, end_date = _fetch_tournament_dates(ondata_id, headers, logger, logger_keys)

        if not start_date:
            logger.failed(logger_keys, f"No valid start_date found", to_console=True)
            continue

        if ondata_id == '000004' or ondata_id == '000002':
            logger.skipped(logger_keys, f"Known bad ID, skipping", to_console=True)
            continue

        # Create TournamentRaw object with minimal fields
        raw = TournamentRaw(
            tournament_id_ext   = ondata_id,
            longname            = longname,
            shortname           = longname,
            startdate           = start_date.isoformat() if start_date else None,
            enddate             = end_date.isoformat() if end_date else None,
            url                 = full_url,
            data_source_id      = 1,
            is_listed           = 0
        )

        # Light validation for tournament_id_ext OR shortname + startdate, otherwise fail
        is_valid, error_message = raw.validate()
        if not is_valid:
            logger.failed(logger_keys, error_message, to_console=True)
            continue

        # Upsert without validation
        action = raw.upsert(cursor)
        if action:
            logger.success(logger_keys, f"Tournament successfully {action}", to_console=True)
        else:
            logger.warning(logger_keys, "No changes made during upsert")

        # Add a short delay to avoid overwhelming the server
        time.sleep(0.5)

    logger.summarize()


def _check_tournament_url(ondata_id: str, headers: dict, logger: OperationLogger, logger_keys: dict) -> bool:
    """
    Check if the tournament URL exists and has a valid result frame.
    Returns True if the URL is valid, False otherwise.
    """
    base_url = f"https://resultat.ondata.se/{ondata_id}/"
    try:
        # Use HEAD request to minimize data transfer
        r = requests.head(base_url, headers=headers, timeout=5)
        if r.status_code != 200:
            return False

        # Follow up with a GET request to check for result frame
        r = requests.get(base_url, headers=headers, timeout=5)
        r.raise_for_status()
        soup = BeautifulSoup(r.content, "html.parser")
        result_frame = soup.find("frame", {"name": "Resultat"})
        return bool(result_frame and result_frame.get("src"))
    except requests.Timeout:
        logger.failed(logger_keys, f"Timeout checking URL")
        return False
    except requests.ConnectionError:
        logger.failed(logger_keys, f"Connection error checking URL")
        return False
    except requests.HTTPError:
        return False  # HTTP errors (e.g., 404) indicate URL doesn't exist
    except Exception as e:
        logger.failed(logger_keys, f"Unexpected error checking URL: {e}")
        return False


def _fetch_tournament_longname(ondata_id: str, headers: dict, logger: OperationLogger, logger_keys: dict) -> Optional[str]:
    """
    Fetch tournament longname from result frame title.
    Returns None on failure.
    """
    base_url = f"https://resultat.ondata.se/{ondata_id}/"
    try:
        r1 = requests.get(base_url, headers=headers, timeout=10)
        r1.raise_for_status()
        soup1 = BeautifulSoup(r1.content, "html.parser")
        result_frame = soup1.find("frame", {"name": "Resultat"})
        if not result_frame or not result_frame.get("src"):
            return None
        result_url = urljoin(base_url, result_frame["src"])

        r2 = requests.get(result_url, headers=headers, timeout=10)
        r2.raise_for_status()
        r2.encoding = "iso-8859-1"
        soup2 = BeautifulSoup(r2.text, "html.parser")
        title_tag = soup2.find("title")
        return title_tag.text.strip() if title_tag else None
    except requests.Timeout:
        logger.failed(logger_keys, f"Timeout fetching longname")
        return None
    except requests.ConnectionError:
        logger.failed(logger_keys, f"Connection error fetching longname")
        return None
    except requests.HTTPError as e:
        logger.failed(logger_keys, f"HTTP error fetching longname: {e}")
        return None
    except Exception as e:
        logger.failed(logger_keys, f"Unexpected error fetching longname: {e}")
        return None


def _fetch_tournament_dates(ondata_id: str, headers: dict, logger: OperationLogger, logger_keys: dict) -> tuple:
    """
    Fetch start and end dates from the first and last participants PDF.
    Returns (start_date, end_date) or (None, None) on failure.
    """
    base_url = f"https://resultat.ondata.se/{ondata_id}/"
    try:
        r1 = requests.get(base_url, headers=headers, timeout=10)
        r1.raise_for_status()
        soup1 = BeautifulSoup(r1.content, "html.parser")
        result_frame = soup1.find("frame", {"name": "Resultat"})
        if not result_frame or not result_frame.get("src"):
            return None, None
        result_url = urljoin(base_url, result_frame["src"])

        r2 = requests.get(result_url, headers=headers, timeout=10)
        r2.raise_for_status()
        r2.encoding = "iso-8859-1"
        soup2 = BeautifulSoup(r2.text, "html.parser")

        table = soup2.find("table", {"width": "100%"})
        if not table:
            # logger.warning(logger_keys, f"No table found for dates extraction")
            return None, None

        rows = table.find_all("tr")[2:]  # Skip headers
        if not rows:
            # logger.warning(logger_keys, f"No rows found for dates extraction")
            return None, None

        # Fetch start date with fallback (first, second, third row)
        start_date = None
        for attempt in range(min(3, len(rows))):
            row = rows[attempt]
            pdf_url = _get_participants_pdf_url(row, result_url)
            if pdf_url:
                start_date = _extract_date_from_pdf(pdf_url, headers, logger, logger_keys)
                if start_date:
                    break
            # logger.warning(logger_keys, f"Start date fallback attempt failed")

        # Fetch end date with fallback (last, second-last, third-last row)
        end_date = None
        for attempt in range(min(3, len(rows))):
            row = rows[-(attempt + 1)]
            pdf_url = _get_participants_pdf_url(row, result_url)
            if pdf_url:
                end_date = _extract_date_from_pdf(pdf_url, headers, logger, logger_keys)
                if end_date:
                    break
            # logger.warning(logger_keys, f"End date fallback attempt failed")

        # If end_date is missing, use start_date
        if not end_date and start_date:
            end_date = start_date

        return start_date, end_date
    except requests.Timeout:
        logger.failed(logger_keys, f"Timeout fetching dates")
        return None, None
    except requests.ConnectionError:
        logger.failed(logger_keys, f"Connection error fetching dates")
        return None, None
    except requests.HTTPError as e:
        logger.failed(logger_keys, f"HTTP error fetching dates: {e}")
        return None, None
    except Exception as e:
        logger.failed(logger_keys, f"Unexpected error fetching dates: {e}")
        return None, None


def _get_participants_pdf_url(row, result_url: str) -> Optional[str]:
    """
    Extract the URL for the participants PDF (first link in the 4th column, assuming stage=1).
    Returns the full URL or None if not found.
    """
    cols = row.find_all("td")
    if len(cols) < 4:
        return None
    a = cols[3].find("a", href=True)
    if not a:
        return None
    return urljoin(result_url, a["href"])


def _extract_date_from_pdf(pdf_url: str, headers: dict, logger: OperationLogger, logger_keys: dict) -> Optional[datetime.date]:
    """
    Fetch and extract the date from the first page of the PDF.
    Looks for YYYY-MM-DD format.
    Returns parsed date or None on failure.
    """
    try:
        r = requests.get(pdf_url, headers=headers, timeout=10)
        r.raise_for_status()
        if 'application/pdf' not in r.headers.get('Content-Type', ''):
            # logger.warning(logger_keys, f"URL is not a PDF")
            return None
        with pdfplumber.open(io.BytesIO(r.content)) as pdf:
            if not pdf.pages:
                return None
            text = pdf.pages[0].extract_text() or ""
            m = re.search(r'(\d{4}-\d{2}-\d{2})', text)
            if m:
                return parse_date(m.group(1), context="extract_date_from_pdf")
            else:
                # logger.warning(logger_keys, f"No date found in PDF")
                return None
    except Exception as e:
        # logger.warning(logger_keys, f"Error extracting date from PDF: {e}")
        return None