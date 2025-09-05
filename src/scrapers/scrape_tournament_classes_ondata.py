# src/scrapers/scrape_tournament_classes_ondata.py
import logging
import re
import datetime
import sqlite3
import time
from bs4 import BeautifulSoup
from typing import List, Optional, Dict, Any, Set
from urllib.parse import urljoin, urlparse, parse_qs
import requests
from pathlib import Path
import json
from utils import parse_date, OperationLogger
from config import SCRAPE_CLASSES_MAX_TOURNAMENTS, SCRAPE_CLASSES_TOURNAMENT_ID_EXTS, SCRAPE_TOURNAMENTS_ORDER, SCRAPE_TOURNAMENTS_CUTOFF_DATE, PDF_CACHE_DIR
from models.tournament_class_raw import TournamentClassRaw
from models.tournament import Tournament
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Constants for PDF downloading
PDF_BASE = "https://resultat.ondata.se/ViewClassPDF.php"
CACHE_DIR = Path(PDF_CACHE_DIR)

def scrape_tournament_classes_ondata(cursor) -> None:
    """
    Scrape raw tournament classes from ondata for tournaments after cutoff or specific ext_ids,
    perform light validation, and insert into tournament_class_raw table.
    """
    logger = OperationLogger(
        verbosity=2,
        print_output=False,
        log_to_db=True,
        cursor=cursor
    )

    cutoff_date = parse_date(SCRAPE_TOURNAMENTS_CUTOFF_DATE)

    start_time = time.time()
    classes_processed = 0
    bytes_downloaded = 0
    PDFs_downloaded = 0

    # Fetch tournaments by status and filter by cutoff date or ext_ids
    if SCRAPE_CLASSES_TOURNAMENT_ID_EXTS:  # treat empty list or None as False
        filtered_tournaments = Tournament.get_by_ext_ids(
            cursor, SCRAPE_CLASSES_TOURNAMENT_ID_EXTS
        )
        limit = SCRAPE_CLASSES_MAX_TOURNAMENTS or len(filtered_tournaments)
        logger.info(
            f"Filtered to {len(filtered_tournaments)} specific tournaments "
            f"via SCRAPE_CLASSES_TOURNAMENT_ID_EXT (overriding cutoff)."
        )
    else:
        tournaments = Tournament.get_valid_ongoing_ended(cursor)
        if not tournaments:
            logger.failed({}, "No tournaments found in database.")
            return

        filtered_tournaments = [
            t for t in tournaments
            if t.startdate and t.startdate >= cutoff_date
        ]
        if not filtered_tournaments:
            logger.failed({}, "No tournaments after cutoff date.")
            return

        # Sort tournaments by startdate based on SCRAPE_TOURNAMENTS_ORDER
        filtered_tournaments.sort(key=lambda t: t.startdate, reverse=(SCRAPE_TOURNAMENTS_ORDER == "newest"))
        limit = SCRAPE_CLASSES_MAX_TOURNAMENTS or len(filtered_tournaments)

    logger.info(
        f"Found {len(filtered_tournaments)} tournaments after cutoff. "
        f"Scraping classes for up to {limit} tournaments in {SCRAPE_TOURNAMENTS_ORDER} order..."
    )

    # Loop through filtered tournaments
    for i, t in enumerate(filtered_tournaments[:limit], 1):
        logger_keys = {
            'tournament': t.shortname,
            'startdate': str(t.startdate or 'None'),
            'tournament_id': str(t.tournament_id),
            'tournament_id_ext': str(t.tournament_id_ext or 'None'),
            'tournament_class_id_ext': None,
            'tournament_class_shortname': None,
            'tournament_url': t.url or 'None'
        }

        # Validate tournament data
        if not t.url or not t.url.startswith(('http://', 'https://')):
            logger.failed(logger_keys, f"Invalid or missing URL for tournament")
            continue
        if not t.tournament_id_ext:
            logger.failed(logger_keys, f"Missing tournament_id_ext for tournament")
            continue

        try:
            # Scrape raw classes
            t_start = time.time()
            raw_classes, raw_classes_processed = _scrape_raw_classes_for_tournament_ondata(t, logger_keys, logger)
            classes_processed += raw_classes_processed
            t_elapsed = time.time() - t_start 
            if not raw_classes:
                # logger.failed(logger_keys, "Could not find any classes to scrape")
                continue
            logger.info(f"[{i}/{len(filtered_tournaments[:limit])}] Scraped {len(raw_classes)} classes for {t.shortname} (id: {t.tournament_id}, ext_id: {t.tournament_id_ext}) in {t_elapsed:.2f} seconds")

            for raw_data in raw_classes:
                # Create raw object
                tournament_class_raw = TournamentClassRaw.from_dict(raw_data)

                # Light validation
                if not tournament_class_raw.validate():
                    logger.failed(logger_keys, "Missing required fields (shortname, startdate, tournament_id_ext)")
                    continue

                # Upsert to raw table
                action = tournament_class_raw.upsert(cursor)
                if action:
                    logger.success(logger_keys, f"Tournament class successfully {action}", to_console=False)
                    # Download PDFs if not already
                    if tournament_class_raw.tournament_class_id_ext:
                        bytes_downloaded, PDFs_downloaded = _download_class_pdfs(t, tournament_class_raw.tournament_class_id_ext, tournament_class_raw.raw_stage_hrefs, logger, bytes_downloaded, PDFs_downloaded)
                else:
                    logger.failed(logger_keys, f"Tournament class upsert failed (ext_id: {tournament_class_raw.tournament_class_id_ext})")

        except Exception as e:
            logger.failed(logger_keys, f"Exception during scraping: {e}")
            continue

    logger.info(
        f"Processed {classes_processed} tournament classes from {len(filtered_tournaments[:limit])} tournaments "
        f"in {time.time()-start_time:.2f} seconds. Downloaded {PDFs_downloaded} new PDFs "
        f"(total size {_format_size(bytes_downloaded)})."
    )

    logger.summarize()

def _download_class_pdfs(tournament: Tournament, class_id_ext: str, raw_stage_hrefs: str, logger: OperationLogger, bytes_downloaded: int, PDFs_downloaded: int) -> tuple[int, int]:
    """
    Download PDFs for each stage if not already downloaded, using the folder structure
    data/pdfs/tournament_{tournament_id_ext}/class_{class_id_ext}/stage_{stage}.pdf.
    Returns updated bytes_downloaded and PDFs_downloaded.
    """
    if not raw_stage_hrefs:
        logger.info("No stage hrefs for PDF download.")
        return bytes_downloaded, PDFs_downloaded

    try:
        stage_to_href = json.loads(raw_stage_hrefs)
    except json.JSONDecodeError:
        logger.warning("Invalid raw_stage_hrefs format for PDF download.")
        return bytes_downloaded, PDFs_downloaded

    for stage in stage_to_href.keys():
        stage = int(stage)  # Ensure stage is an integer
        logger_keys = {
            'tournament': tournament.shortname,
            'tournament_id_ext': str(tournament.tournament_id_ext),
            'class_id_ext': str(class_id_ext),
            'stage': str(stage)
        }

        # Download the PDF
        pdf_path, was_downloaded = _download_pdf(tournament.tournament_id_ext, class_id_ext, stage, logger)
        if pdf_path:
            size = pdf_path.stat().st_size
            bytes_downloaded += size  # Update total bytes for all processed PDFs
            if was_downloaded:
                PDFs_downloaded += 1  # Increment only for actual downloads
            # logger.success(logger_keys, f"Processed PDF for stage {stage}: {pdf_path} ({_format_size(size)})", to_console=False)
        else:
            logger.failed(logger_keys, f"No PDF available for stage to download")

    return bytes_downloaded, PDFs_downloaded



def _scrape_raw_classes_for_tournament_ondata(
    tournament: Tournament,
    logger_keys: Dict[str, str],
    logger: OperationLogger
) -> List[Dict[str, Any]]:
    """
    Scrape raw class data from tournament page.
    Returns list of raw dicts with parsed row data and number of rows processed.
    """
    raw_classes = []
    raw_classes_processed = 0

    # Set up session with retries
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))

    # Ensure base ends with slash
    base = tournament.url
    if not base.endswith("/"):
        base += "/"

    # 1) fetch outer frameset
    try:
        resp1 = session.get(base, timeout=10)
        resp1.raise_for_status()
        time.sleep(0.5)  # Delay to avoid overwhelming server
    except Exception as e:
        raise ValueError(f"Error fetching outer frame: {e}")

    soup1 = BeautifulSoup(resp1.text, "html.parser")
    frame = soup1.find("frame", {"name": "Resultat"})
    if not frame or not frame.get("src"):
        raise ValueError(f"No Resultat frame found in {base}")

    # 2) fetch inner page
    inner_url = urljoin(base, frame["src"])
    try:
        resp2 = session.get(inner_url, timeout=10)
        resp2.raise_for_status()
        time.sleep(0.5)  # Delay to avoid overwhelming server
    except Exception as e:
        raise ValueError(f"Error fetching inner page: {e}")

    soup2 = BeautifulSoup(resp2.text, "html.parser")

    table = soup2.find("table", attrs={"width": "100%"})
    if not table:
        raise ValueError(f"No class table found in {inner_url}")

    rows = table.find_all("tr")[2:]
    base_date = parse_date(tournament.startdate, context="infer_full_date")
    if not base_date:
        raise ValueError(f"Invalid start date for tournament {tournament.shortname} ({tournament.tournament_id})")

    for row in rows:
        raw = _parse_raw_row(row, tournament.tournament_id_ext, base_date, logger_keys, logger)
        raw_classes_processed += 1
        if raw:
            raw_classes.append(raw)
    return raw_classes, raw_classes_processed

def _parse_raw_row(
    row,
    tournament_id_ext: int,
    base_date: datetime.date,
    logger_keys: Dict[str, str],
    logger: OperationLogger
) -> Optional[Dict[str, Any]]:
    cols = row.find_all("td")
    if len(cols) < 4:
        return None

    day_txt = cols[0].get_text(strip=True)
    m = re.search(r"(\d+)", day_txt)
    if not m:
        return None
    day_num = int(m.group(1))
    try:
        class_date = base_date.replace(day=day_num)
    except ValueError:
        return None

    desc = cols[2].get_text(strip=True)
    a = cols[3].find("a", href=True)
    if not a or not a["href"]:
        return None

    qs = parse_qs(urlparse(a["href"]).query)
    ext_id = int(qs["classID"][0]) if "classID" in qs and qs["classID"][0].isdigit() else None
    short = a.get_text(strip=True)

    # Exclusion list by ext_id (known invalid classes without proper PDF:s)
    EXCLUDED_EXT_IDS = {5345, 5171, 5167}
    if ext_id in EXCLUDED_EXT_IDS:
        logger.info(f"Skipping excluded class ext_id={ext_id} ({short}) due to known invalid PDF")
        return None

    # Collect stages and their hrefs from row links
    stages: Set[int] = set()
    stage_to_href: Dict[int, str] = {}
    for a in row.find_all("a", href=True):
        try:
            qs = parse_qs(urlparse(a["href"]).query)
            st = qs.get("stage", [None])[0]
            if st and str(st).isdigit():
                stage_num = int(st)
                stages.add(stage_num)
                stage_to_href[stage_num] = a["href"]
                logging.debug(f"Found stage {stage_num} in href: {a['href']}")
        except Exception as e:
            logger.failed(logger_keys, f"Error parsing href in row: {e}")
            continue

    if not stages:
        logger.failed(logger_keys, f"No stages found in row HTML")
        return

    raw_stages_str = ",".join(str(s) for s in sorted(stages)) if stages else None
    raw_stage_hrefs_str = json.dumps(stage_to_href) if stage_to_href else None

    logger_keys.update({
        'tournament_class_id_ext': str(ext_id or 'None'),
        'tournament_class_shortname': short or 'None',
        'raw_stages': raw_stages_str
    })

    return {
        "tournament_class_id_ext":  ext_id,
        "tournament_id_ext":        tournament_id_ext,
        "startdate":                class_date,
        "shortname":                short,
        "longname":                 desc,
        "gender":                   None,
        "max_rank":                 None,
        "max_age":                  None,
        "url":                      None,
        "raw_stages":               raw_stages_str,
        "raw_stage_hrefs":          raw_stage_hrefs_str,
        "data_source_id":           1
    }

# Helper functions for PDF downloading
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

def _download_pdf(tournament_id_ext: str, class_id_ext: str, stage: int, logger: OperationLogger, force: bool = False) -> tuple[Path | None, bool]:
    """Download a PDF if available. Returns (path, downloaded) where downloaded is True if newly downloaded, False if cached or skipped."""
    pdf_path = _get_pdf_path(tournament_id_ext, class_id_ext, stage)

    # If file exists but is invalid, remove it
    if pdf_path.exists() and not _is_valid_pdf(pdf_path):
        pdf_path.unlink()

    if pdf_path.exists() and not force:
        # logger.info({'tournament_id_ext': str(tournament_id_ext), 'class_id_ext': str(class_id_ext), 'stage': str(stage)}, f"Cached PDF used: {pdf_path}")
        return pdf_path, False

    url = f"{PDF_BASE}?tournamentID={tournament_id_ext}&classID={class_id_ext}&stage={stage}"
    pdf_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        resp = requests.get(url, timeout=20)
        if resp.status_code != 200 or not resp.content.startswith(b"%PDF-"):
            return None, False

        with open(pdf_path, "wb") as f:
            f.write(resp.content)
        # logger.info({'tournament_id_ext': str(tournament_id_ext), 'class_id_ext': str(class_id_ext), 'stage': str(stage)}, f"Downloaded PDF: {pdf_path}")
        return pdf_path, True
    except Exception:
        return None, False