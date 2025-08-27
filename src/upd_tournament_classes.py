# src/upd_classes.py

import logging
import re
import datetime
import time
from bs4 import BeautifulSoup
from typing import List, Optional, Dict, Any

from urllib.parse import urljoin, urlparse, parse_qs
import requests
from utils import parse_date, OperationLogger
from config import SCRAPE_CLASSES_MAX_TOURNAMENTS, SCRAPE_TOURNAMENTS_CUTOFF_DATE, SCRAPE_CLASSES_TOURNAMENT_ID_EXTS
from db import get_conn

from models.tournament_class import TournamentClass
from models.tournament import Tournament


def upd_tournament_classes():
    """
    Main entry point: scrapes classes for ongoing/ended tournaments on/after cutoff,
    processes through pipeline (scrape -> parse -> validate -> upsert),
    logs via OperationLogger, and summarizes.
    """
    conn, cursor = get_conn()

    # Set up logging
    # =============================================================================
    logger = OperationLogger(
        verbosity       = 2, 
        print_output    = False, 
        log_to_db       = False, 
        cursor          = cursor
        )

    try:
        try:
            cutoff_date = parse_date(SCRAPE_TOURNAMENTS_CUTOFF_DATE)
        except ValueError as ve:
            logging.error(f"Invalid cutoff date format: {ve}")
            print(f"❌ Invalid cutoff date format: {ve}")
            return

        logging.info(f"Starting tournament classes update, cutoff: {cutoff_date}")
        print(f"ℹ️  Starting tournament classes update, cutoff: {cutoff_date}")

        start_time = time.time()
        classes_processed = 0

        # =============================================================================
        # Fetch tournaments by status and filter by cutoff date or ext_ids
        # =============================================================================
        if SCRAPE_CLASSES_TOURNAMENT_ID_EXTS:  # treat empty list or None as False
            filtered_tournaments = Tournament.get_by_ext_ids(
                cursor, logger, SCRAPE_CLASSES_TOURNAMENT_ID_EXTS
            )
            limit = SCRAPE_CLASSES_MAX_TOURNAMENTS or len(filtered_tournaments)
            print(
                f"ℹ️  Filtered to {len(filtered_tournaments)} specific tournaments "
                f"via SCRAPE_CLASSES_TOURNAMENT_ID_EXT (overriding cutoff)."
            )
        else:
            tournaments = Tournament.get_by_status(cursor, ["ONGOING", "ENDED"])
            if not tournaments:
                print("⚠️  No tournaments found in database.")
                return

            filtered_tournaments = [
                t for t in tournaments
                if t.startdate and t.startdate >= cutoff_date
            ]
            if not filtered_tournaments:
                print("⚠️  No tournaments after cutoff date.")
                return

            limit = SCRAPE_CLASSES_MAX_TOURNAMENTS or len(filtered_tournaments)

        print(
            f"ℹ️  Found {len(filtered_tournaments)} tournaments after cutoff. "
            f"Scraping classes for up to {limit} tournaments..."
        )

        # Loop through filtered tournaments
        # =============================================================================
        for i, t in enumerate(filtered_tournaments[:limit], 1):
            item_key = f"{t.shortname} ({t.startdate})"
            print(f"ℹ️  Processing tournament [{i}/{len(filtered_tournaments[:limit])}] {t.shortname} (id: {t.tournament_id}, ext_id: {t.tournament_id_ext})")
            classes_processed += 1

            try:
                # Scrape raw classes
                # =============================================================================
                raw_classes = scrape_raw_classes_for_tournament_ondata(t, item_key, logger)
                if not raw_classes:
                    logger.skipped(item_key, "No classes scraped")
                    continue

                for raw_data in raw_classes:
                    # Parse raw data
                    # =============================================================================
                    parsed_data = parse_raw_class(raw_data, t.tournament_id)
                    if parsed_data is None:
                        continue

                    # Create and validate tournament class object
                    # =============================================================================
                    tournament_class = TournamentClass.from_dict(parsed_data)
                    tournament_class.validate(logger, item_key)

                    # Upsert
                    # =============================================================================
                    tournament_class.upsert(cursor, logger, item_key)
                    

            except Exception as e:
                logger.failed(item_key, f"Exception during processing: {e}")
                continue

        print(f"ℹ️  Processed {classes_processed} tournament classes from {len(filtered_tournaments[:limit])} tournaments in {time.time()-start_time:.2f} seconds.")
        logger.summarize()

    except Exception as e:
        logging.error(f"Error in upd_tournament_classes: {e}")
        print(f"❌ Error in upd_tournament_classes: {e}")

    finally:
        conn.commit()
        conn.close()

def detect_type_id(shortname: str, longname: str) -> int:
    l  = (longname or "").lower()
    up = (shortname or "").upper()
    tokens = [t for t in re.split(r"[^A-ZÅÄÖ]+", up) if t]

    # --- Team (4) ---
    if (re.search(r"\b(herr(?:ar)?|dam(?:er)?)\s+lag\b", l)
        or "herrlag" in l or "damlag" in l):
        return 4
    if any(t in {"HL", "DL", "HLAG", "DLAG", "LAG", "TEAM"} for t in tokens):
        return 4
    if re.search(r"\b[HD]L\d+\b", up) or re.search(r"\b[HD]LAG\d*\b", up):
        return 4

    # --- Doubles (2) ---
    # prefix handles cases like "HDEliteYdr"
    if up.startswith(("HD","DD","WD","MD","MXD","FD")):
        return 2
    if re.search(r"\b(doubles?|dubbel|dubble|dobbel|dobbelt|Familjedubbel)\b", l):
        return 2
    if any(tag in tokens for tag in {"HD","DD","WD","MD","MXD","FD"}):
        return 2
    
    # --- Unknown/garbage starting with XD (9) ---
    if up.startswith(("XB", "XG")):
        return 9

    # --- Default Singles (1) ---
    return 1

def scrape_raw_classes_for_tournament_ondata(
    tournament: Tournament,
    item_key: str,
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
        raw = _parse_raw_row(row, tournament.tournament_id, base_date, item_key, logger)
        if raw:
            raw_classes.append(raw)
    return raw_classes

def _parse_raw_row(
    row, 
    tournament_id: int, 
    base_date: datetime.date, 
    item_key: str, 
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
        logging.info(f"Skipping excluded class ext_id={ext_id} ({short}) due to known invalid PDF")
        return None

    # Infer both ids here without extra HTTP calls
    type_id         = detect_type_id(short, desc)
    structure_id    = _infer_structure_id_from_row(row)

        
    return {
        "tournament_class_id_ext":          ext_id,
        "tournament_id":                    tournament_id,
        "tournament_class_type_id":         type_id,
        "tournament_class_structure_id":    structure_id,
        "date":                             class_date,
        "longname":                         desc,
        "shortname":                        short,
        "gender":                           None,
        "max_rank":                         None,
        "max_age":                          None,
        "url":                              None  # Add if scraping URL per class
    }

def parse_raw_class(raw_data: Dict[str, Any], tournament_id: int) -> Optional[Dict[str, Any]]:
    """
    Parse raw dict to structured data.
    Currently minimal; add parsing logic as needed (e.g., gender from shortname).
    Returns parsed dict or None on failure.
    """
    # Basic validation/parsing (expand as needed)
    if not raw_data.get("shortname") or not raw_data.get("date"):
        return None

    parsed_data = {
        "tournament_class_id_ext":          raw_data.get("tournament_class_id_ext"),
        "tournament_id":                    tournament_id,
        "tournament_class_type_id":         raw_data.get("tournament_class_type_id"),
        "tournament_class_structure_id":    raw_data.get("tournament_class_structure_id"),
        "date":                             raw_data.get("date"),
        "longname":                         raw_data.get("longname"),
        "shortname":                        raw_data.get("shortname"),
        "gender":                           raw_data.get("gender"),
        "max_rank":                         raw_data.get("max_rank"),
        "max_age":                          raw_data.get("max_age"),
        "url":                              raw_data.get("url"),
        "data_source_id":                   1 
    }
    return parsed_data

def _infer_structure_id_from_row(row) -> Optional[int]:
    """
    Inspect all links in this table row. If any link has ?stage=3/4/5/6,
    derive structure from the presence of stages:
      - groups = (3 or 4)
      - ko     = (5)
    Return 1,2,3 or None.
    """
    stages: set[int] = set()
    for a in row.find_all("a", href=True):
        try:
            qs = parse_qs(urlparse(a["href"]).query)
            st = qs.get("stage", [None])[0]
            if st and str(st).isdigit():
                stage_num = int(st)
                stages.add(stage_num)
                logging.debug(f"Found stage {stage_num} in href: {a['href']}")
        except Exception as e:
            logging.warning(f"Error parsing href in row: {e}")
            continue

    if not stages:
        logging.warning(f"No stages found in row HTML: {row.prettify()}")

    has_groups = (3 in stages) or (4 in stages)
    has_ko     = (5 in stages)

    if has_groups and has_ko:
        return 1 # STRUCT_GROUPS_AND_KO
    if has_groups and not has_ko:
        return 2 # STRUCT_GROUPS_ONLY
    if (not has_groups) and has_ko:
        return 3 # STRUCT_KO_ONLY
    return 9