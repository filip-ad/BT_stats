# src/scrapers/scrape_tournament_classes_ondata.py
import logging
import re
import datetime
import time
from bs4 import BeautifulSoup
from typing import List, Optional, Dict, Any, Set
from urllib.parse import urljoin, urlparse, parse_qs
import requests
from pathlib import Path
import json
from utils import parse_date, OperationLogger, _format_size, _download_pdf_ondata_by_tournament_class_and_stage
from config import SCRAPE_CLASSES_MAX_TOURNAMENTS, SCRAPE_CLASSES_TOURNAMENT_ID_EXTS, SCRAPE_TOURNAMENTS_ORDER, SCRAPE_TOURNAMENTS_CUTOFF_DATE, PDF_CACHE_DIR
from models.tournament_class_raw import TournamentClassRaw
from models.tournament import Tournament
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Constants for PDF downloading
PDF_BASE = "https://resultat.ondata.se/ViewClassPDF.php"
CACHE_DIR = Path(PDF_CACHE_DIR)

# SCRAPE_CLASSES_TOURNAMENT_ID_EXTS = ['000660'] # Ungdoms Top 12, 2020 structure 4 but Danish


def scrape_tournament_classes_ondata(cursor, run_id=None) -> None:
    """
    Scrape raw tournament classes from ondata for tournaments after cutoff or specific ext_ids,
    perform light validation, and insert into tournament_class_raw table.
    """
    logger = OperationLogger(
        verbosity       = 2,
        print_output    = False,
        log_to_db       = True,
        cursor          = cursor,
        object_type     = "tournament_class",
        run_type        = "scrape_ondata",
        run_id          = run_id
    )

    cutoff_date         = parse_date(SCRAPE_TOURNAMENTS_CUTOFF_DATE)

    start_time          = time.time()
    classes_processed   = 0
    bytes_downloaded    = 0
    PDFs_downloaded     = 0
    
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

    logger.set_run_remark(
        f"Cutoff: {cutoff_date}, Order: {SCRAPE_TOURNAMENTS_ORDER}, "
        f"Max tournaments: {SCRAPE_CLASSES_MAX_TOURNAMENTS or 'all'}, "
        f"Specific ext_ids: {SCRAPE_CLASSES_TOURNAMENT_ID_EXTS or 'none'}"
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
            raw_classes, raw_classes_processed = _scrape_raw_classes_for_tournament_ondata(t, logger_keys, logger, wait_between_requests=0.1)
            classes_processed += raw_classes_processed
            if not raw_classes:
                logger.failed(logger_keys, "No classes found on tournament page")
                continue

        except Exception as e:
            logger.failed(logger_keys, f"Exception during scraping: {e}")
            continue

        tournament_bytes_downloaded = 0
        tournament_pdfs_downloaded = 0

        for raw_data in raw_classes:
            try: 
                
                logger.inc_processed()

                # Create raw object
                tc_raw = TournamentClassRaw.from_dict(raw_data)

                # Field validation with error messages
                is_valid, error_message = tc_raw.validate()
                if not is_valid:
                    logger.failed(logger_keys, f"Validation failed: {error_message}")
                    continue

                # Upsert to raw table
                action = tc_raw.upsert(cursor)                
                download_pdf = False
                if action == "inserted" or action == "updated":
                    logger.success(logger_keys, f"Raw tournament class successfully {action}")
                    download_pdf = True
                elif action == "unchanged":
                    logger.success(logger_keys, "Raw tournament class unchanged (already up-to-date)")
                    download_pdf = True
                else:
                    logger.failed(logger_keys, "Raw tournament class upsert failed")

                # Download PDFs if not already
                if download_pdf and tc_raw.tournament_class_id_ext:
                    bytes_downloaded, PDFs_downloaded = _download_class_pdfs(t, tc_raw.tournament_class_id_ext, tc_raw.raw_stage_hrefs, logger)
                    tournament_bytes_downloaded += bytes_downloaded
                    tournament_pdfs_downloaded += PDFs_downloaded

            except Exception as e:
                logger.failed(logger_keys, f"Exception processing raw class data: {e}")
                continue

        logger.info(f"[{i}/{len(filtered_tournaments[:limit])}] Scraped {len(raw_classes)} classes for {t.shortname} (id: {t.tournament_id}, ext_id: {t.tournament_id_ext}). PDFs downloaded: {tournament_pdfs_downloaded} ({_format_size(tournament_bytes_downloaded)})", to_console=True)
        PDFs_downloaded += tournament_pdfs_downloaded
        bytes_downloaded += tournament_bytes_downloaded

    logger.info(
        f"Processed {classes_processed} raw rows from {len(filtered_tournaments[:limit])} tournaments "
        f"in {time.time()-start_time:.2f} seconds. "
        f"📄 PDFs downloaded: {PDFs_downloaded} new ({_format_size(bytes_downloaded)})."
    )

    logger.summarize()

def _download_class_pdfs(tournament: Tournament, class_id_ext: str, raw_stage_hrefs: str, logger: OperationLogger) -> tuple[int, int]:
    """
    Download PDFs for each stage if not already downloaded, using the folder structure
    data/pdfs/tournament_{tournament_id_ext}/class_{class_id_ext}/stage_{stage}.pdf.
    Returns updated bytes_downloaded and PDFs_downloaded.
    """

    bytes_downloaded = 0
    PDFs_downloaded = 0

    if not raw_stage_hrefs:
        logger.failed("No stage hrefs for PDF download.")
        return bytes_downloaded, PDFs_downloaded

    try:
        stage_to_href = json.loads(raw_stage_hrefs)
    except json.JSONDecodeError:
        logger.failed("Invalid raw_stage_hrefs format for PDF download.")
        return bytes_downloaded, PDFs_downloaded

    for stage in stage_to_href.keys():
        stage = int(stage)
        logger_keys = {
            'tournament': tournament.shortname,
            'tournament_id_ext': str(tournament.tournament_id_ext),
            'class_id_ext': str(class_id_ext),
            'stage': str(stage)
        }
        try:
            pdf_path, was_downloaded, message = _download_pdf_ondata_by_tournament_class_and_stage(tournament.tournament_id_ext, class_id_ext, stage, force_download=False)
            if message:
                if "Cached" in message:
                    # logger.info(logger_keys, message, to_console=False)
                    pass
                elif "Downloaded" in message:
                    # logger.info(logger_keys, message, to_console=False)
                    pass
                else:
                    logger.failed(logger_keys, message, to_console=False)
                    return bytes_downloaded, PDFs_downloaded
            if pdf_path:
                size = pdf_path.stat().st_size
                if was_downloaded:
                    PDFs_downloaded += 1
                    bytes_downloaded += size   # count only new files in bytes_downloaded
                else:
                    logging.debug(f"Using cached PDF: {pdf_path} ({_format_size(size)})")
                    pass

            else:
                logger.failed(logger_keys, f"No PDF available for stage to download")
        except ValueError as ve:
            logger.failed(logger_keys, f"Unpack error during PDF download: {ve}")
            continue  # Continue to next stage instead of failing entire class

    return bytes_downloaded, PDFs_downloaded

def _scrape_raw_classes_for_tournament_ondata(
    tournament: Tournament,
    logger_keys: Dict[str, str],
    logger: OperationLogger,
    wait_between_requests: float = 0.1
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
        time.sleep(wait_between_requests)  # Delay to avoid overwhelming server
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
        time.sleep(wait_between_requests)  # Delay to avoid overwhelming server
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

    # Determine knockout tree size if stage 5 exists
    ko_tree_size = None
    if 5 in stage_to_href:
        ko_tree_size = _extract_ko_tree_size(stage_to_href[5])

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
        "ko_tree_size":             ko_tree_size, 
        "data_source_id":           1
    }

def _extract_ko_tree_size(stage_5_url: str) -> Optional[int]:
    """
    Fetch the HTML for stage=5 and extract the knockout tree size from 'Schema-<N>'.
    Returns integer N if found, else None.
    """
    try:
        # Ensure full URL
        if stage_5_url.startswith("/"):
            stage_5_url = urljoin("https://resultat.ondata.se/", stage_5_url)

        resp = requests.get(stage_5_url, timeout=10)
        resp.raise_for_status()

        m = re.search(r"Schema-(\d+)", resp.text, re.IGNORECASE)
        if m:
            return int(m.group(1))
    except Exception as e:
        logging.debug(f"Could not extract ko_tree_size from {stage_5_url}: {e}")
    return None
