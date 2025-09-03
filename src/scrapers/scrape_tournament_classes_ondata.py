# src/scrapers/scrape_tournament_classes_ondata.py

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
from models.tournament_class_raw import TournamentClassRaw
from models.tournament import Tournament

def scrape_tournament_classes_ondata(cursor, cutoff_date: datetime.date, logger: OperationLogger) -> None:
    """
    Scrape raw tournament classes from ondata for tournaments after cutoff or specific ext_ids,
    perform light validation, and insert into tournament_class_raw table.
    """
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
    for i, t in enumerate(filtered_tournaments[:limit],1):
        item_key = f"{t.shortname} ({t.startdate})"
        logger.info(f"Processing tournament [{i}/{len(filtered_tournaments[:limit])}] {t.shortname} (id: {t.tournament_id}, ext_id: {t.tournament_id_ext})")
        classes_processed += 1

        try:
            # Scrape raw classes
            raw_classes = scrape_raw_classes_for_tournament_ondata(t, item_key, logger)
            if not raw_classes:
                logger.skipped(item_key, "No classes scraped")
                continue

            for raw_data in raw_classes:
                # Create raw object
                tournament_class_raw = TournamentClassRaw.from_dict(raw_data)

                # Light validation
                if not tournament_class_raw.light_validate():
                    logger.failed(item_key, "Missing required fields (shortname, date, tournament_id)")
                    continue

                # Insert to raw table
                tournament_class_raw.insert(cursor)
                logger.success(item_key, f"Raw tournament class inserted (ext_id: {tournament_class_raw.tournament_class_id_ext})")

        except Exception as e:
            logger.failed(item_key, f"Exception during scraping: {e}")
            continue

    logger.info(f"Processed {classes_processed} tournament classes from {len(filtered_tournaments[:limit])} tournaments in {time.time()-start_time:.2f} seconds.")

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
    if (not has_groups)and has_ko:
        return 3 # STRUCT_KO_ONLY
    return 9