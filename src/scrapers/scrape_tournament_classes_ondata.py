# src/scrapers/scrape_tournament_classes_ondata.py
import logging
import re
import datetime
import time
from bs4 import BeautifulSoup
from typing import List, Optional, Dict, Any, Set

from urllib.parse import urljoin, urlparse, parse_qs
import requests
from utils import parse_date, OperationLogger
from config import SCRAPE_CLASSES_MAX_TOURNAMENTS, SCRAPE_TOURNAMENTS_CUTOFF_DATE, SCRAPE_CLASSES_TOURNAMENT_ID_EXTS
from models.tournament_class_raw import TournamentClassRaw
from models.tournament import Tournament
import os
import json
import pdfkit

def download_class_pdfs(tournament: Tournament, class_id_ext: str, raw_stage_hrefs: str, logger: OperationLogger) -> None:
    """
    Download PDFs for each stage if not already downloaded, using the same folder structure as download_pdfs.py.
    """
    if not raw_stage_hrefs:
        logger.info("No stage hrefs for PDF download.")
        return

    try:
        stage_to_href = json.loads(raw_stage_hrefs)
    except json.JSONDecodeError:
        logger.warning("Invalid raw_stage_hrefs format for PDF download.")
        return

    root_dir = "PDF"
    tournament_folder = os.path.join(root_dir, f"{tournament.shortname}_{tournament.tournament_id_ext}")
    os.makedirs(tournament_folder, exist_ok=True)

    session = requests.Session()
    base_url = tournament.url if tournament.url.endswith("/") else tournament.url + "/"

    for stage, href in stage_to_href.items():
        # Check if PDF exists using the same pattern as download_pdfs.py
        pdf_url = urljoin(base_url, f"ViewClassPDF.php?classID={class_id_ext}&stage={stage}")
        filename = f"{tournament.tournament_id_ext}_{class_id_ext}_Stage_{stage}.pdf"
        stage_folder = os.path.join(tournament_folder, f"Stage_{stage}")
        pdf_path = os.path.join(stage_folder, filename)
        os.makedirs(stage_folder, exist_ok=True)

        if os.path.exists(pdf_path):
            logger.info(f"PDF already downloaded for stage {stage}: {pdf_path}")
            continue

        try:
            resp = session.get(pdf_url, stream=True)
            resp.raise_for_status()
            if "pdf" in resp.headers.get("Content-Type", "").lower():
                with open(pdf_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        f.write(chunk)
                logger.success(f"Downloaded PDF for stage {stage}: {pdf_path}")
            else:
                logger.warning(f"No PDF content at {pdf_url}")
        except Exception as e:
            logger.failed(f"Failed to download PDF for stage {stage}: {e}")

def scrape_tournament_classes_ondata(cursor, cutoff_date: datetime.date) -> None:
    """
    Scrape raw tournament classes from ondata for tournaments after cutoff or specific ext_ids,
    perform light validation, and insert into tournament_class_raw table.
    """
    logger = OperationLogger(
        verbosity=2,
        print_output=False,
        log_to_db=False,
        cursor=cursor
    )

    start_time = time.time()
    classes_processed = 0

    # Fetch tournaments by status and filter by cutoff date or ext_ids
    if SCRAPE_CLASSES_TOURNAMENT_ID_EXTS:  # treat empty list or None as False
        filtered_tournaments = Tournament.get_by_ext_ids(
            cursor, logger, SCRAPE_CLASSES_TOURNAMENT_ID_EXTS
        )
        limit = SCRAPE_CLASSES_MAX_TOURNAMENTS or len(filtered_tournaments)
        logger.info(
            f"Filtered to {len(filtered_tournaments)} specific tournaments "
            f"via SCRAPE_CLASSES_TOURNAMENT_ID_EXT (overriding cutoff)."
        )
    else:
        tournaments = Tournament.get_by_status(cursor, ["ONGOING", "ENDED"])
        if not tournaments:
            logger.warning("No tournaments found in database.")
            return

        filtered_tournaments = [
            t for t in tournaments
            if t.startdate and t.startdate >= cutoff_date
        ]
        if not filtered_tournaments:
            logger.warning("No tournaments after cutoff date.")
            return

        limit = SCRAPE_CLASSES_MAX_TOURNAMENTS or len(filtered_tournaments)

    logger.info(
        f"Found {len(filtered_tournaments)} tournaments after cutoff. "
        f"Scraping classes for up to {limit} tournaments..."
    )

    # Loop through filtered tournaments
    for i, t in enumerate(filtered_tournaments[:limit], 1):
        logger_keys = {'tournament': t.shortname, 'startdate': str(t.startdate or 'None'), 'tournament_id': str(t.tournament_id), 'tournament_id_ext': str(t.tournament_id_ext or 'None')}
        logger.info(f"Processing tournament [{i}/{len(filtered_tournaments[:limit])}] {t.shortname} (id: {t.tournament_id}, ext_id: {t.tournament_id_ext})", logger_keys)
        classes_processed += 1

        # Validate tournament data
        if not t.url or not t.url.startswith(('http://', 'https://')):
            logger.failed(logger_keys, f"Invalid or missing URL for tournament {t.shortname}: {t.url}")
            continue
        if not t.startdate:
            logger.failed(logger_keys, f"Missing startdate for tournament {t.shortname}")
            continue
        if not t.tournament_id_ext:
            logger.failed(logger_keys, f"Missing tournament_id_ext for tournament {t.shortname}")
            continue

        try:
            # Scrape raw classes
            raw_classes = scrape_raw_classes_for_tournament_ondata(t, logger_keys, logger)
            if not raw_classes:
                logger.skipped(logger_keys, "No classes scraped")
                continue

            for raw_data in raw_classes:
                # Create raw object
                tournament_class_raw = TournamentClassRaw.from_dict(raw_data)

                # Light validation
                if not tournament_class_raw.validate():
                    logger.failed(logger_keys, "Missing required fields (shortname, startdate, tournament_id_ext)")
                    continue

                # Insert to raw table (upsert via unique constraint)
                try:
                    tournament_class_raw.insert(cursor)
                    logger.success(logger_keys, f"Raw tournament class inserted (ext_id: {tournament_class_raw.tournament_class_id_ext})")
                    # Download PDFs if not already
                    if tournament_class_raw.tournament_class_id_ext:
                        download_class_pdfs(t, tournament_class_raw.tournament_class_id_ext, tournament_class_raw.raw_stage_hrefs, logger)
                except sqlite3.IntegrityError:
                    logger.skipped(logger_keys, f"Duplicate raw entry (tournament_id_ext, tournament_class_id_ext, data_source_id)")

        except Exception as e:
            logger.failed(logger_keys, f"Exception during scraping: {e}")
            continue

    logger.info(f"Processed {classes_processed} tournament classes from {len(filtered_tournaments[:limit])} tournaments in {time.time()-start_time:.2f} seconds.")

def scrape_raw_classes_for_tournament_ondata(
    tournament: Tournament,
    logger_keys: Dict[str, str],
    logger: OperationLogger
) -> List[Dict[str, Any]]: 
    """
    Scrape raw class data from tournament page.
    Returns list of raw dicts with parsed row data.
    """
    raw_classes = []

    # Ensure base ends with slash
    base = tournament.url
    if not base.endswith("/"):
        base += "/"

    # 1) fetch outer frameset
    try:
        resp1 = requests.get(base)
        resp1.raise_for_status()
    except Exception as e:
        raise ValueError(f"Error fetching outer frame: {e}")

    soup1 = BeautifulSoup(resp1.text, "html.parser")
    frame = soup1.find("frame", {"name": "Resultat"})
    if not frame or not frame.get("src"):
        raise ValueError(f"No Resultat frame found in {base}")

    # 2) fetch inner page
    inner_url = urljoin(base, frame["src"])
    try:
        resp2 = requests.get(inner_url)
        resp2.raise_for_status()
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
        if raw:
            raw_classes.append(raw)
    return raw_classes

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
            logger.warning(logger_keys, f"Error parsing href in row: {e}")
            continue

    if not stages:
        logger.warning(logger_keys, f"No stages found in row HTML: {row.prettify()}")

    raw_stages_str = ",".join(str(s) for s in sorted(stages)) if stages else None
    raw_stage_hrefs_str = json.dumps(stage_to_href) if stage_to_href else None
        
    return {
        "tournament_class_id_ext":          ext_id,
        "tournament_id_ext":                tournament_id_ext,
        "startdate":                        class_date,
        "shortname":                        short,
        "longname":                         desc,
        "gender":                           None,
        "max_rank":                         None,
        "max_age":                          None,
        "url":                              None,  # Add if scraping URL per class
        "raw_stages":                       raw_stages_str,
        "raw_stage_hrefs":                  raw_stage_hrefs_str,
        "data_source_id":                   1
    }