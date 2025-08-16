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
from config import SCRAPE_CLASSES_MAX_TOURNAMENTS, SCRAPE_TOURNAMENTS_CUTOFF_DATE
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
        log_to_db       = True, 
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

        # Fetch tournaments by status and filter by cutoff date
        # =============================================================================
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
        print(f"ℹ️  Found {len(filtered_tournaments)} tournaments after cutoff. Scraping classes for up to {limit} tournaments...")

        # Loop through filtered tournaments
        # =============================================================================
        for i, t in enumerate(filtered_tournaments[:limit], 1):
            item_key = f"{t.shortname} ({t.startdate})"
            print(f"ℹ️  Processing tournament [{i}/{len(filtered_tournaments[:limit])}] {t.shortname}")

            try:
                # Scrape raw classes
                # =============================================================================
                raw_classes = scrape_raw_classes_for_tournament_ondata(t)
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
                    val = tournament_class.validate(logger, item_key)
                    if val["status"] != "success":
                        continue

                    # Upsert
                    # =============================================================================
                    tournament_class.upsert(cursor, logger, item_key)
                    classes_processed += 1

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

# ---------- Doubles detection (IDs only) ----------
def detect_type_id(
    shortname: str, 
    longname: str
) -> int:
    
    l = (longname or "").lower()
    if any(k in l for k in {"double", "doubles", "dubbel", "dubble", "dobbel", "dobbelt"}):
        return 2  # Doubles
    up = (shortname or "").upper()
    if any(tag in re.split(r"[^A-Z]+", up) for tag in {"HD", "DD", "WD", "MD", "MXD"}):
        return 2  # Doubles
    return 1  # Singles

def scrape_raw_classes_for_tournament_ondata(
    tournament: Tournament
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
        raw = _parse_raw_row(row, tournament.tournament_id, base_date)
        if raw:
            raw_classes.append(raw)
    return raw_classes

def _parse_raw_row(row, tournament_id: int, base_date: datetime.date) -> Optional[Dict[str, Any]]:
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

    # Infer both ids here without extra HTTP calls
    structure_id    = _infer_structure_id_from_row(row)
    type_id         = detect_type_id(short, desc)

    return {
        "tournament_class_id_ext": ext_id,
        "tournament_id": tournament_id,
        "tournament_class_type_id": type_id,
        "tournament_class_structure_id": structure_id,
        "date": class_date,
        "longname": desc,
        "shortname": short,
        "gender": None,
        "max_rank": None,
        "max_age": None,
        "url": None  # Add if scraping URL per class
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
        "tournament_class_id_ext": raw_data.get("tournament_class_id_ext"),
        "tournament_id": tournament_id,
        "tournament_class_type_id": raw_data.get("tournament_class_type_id"),
        "tournament_class_structure_id": raw_data.get("tournament_class_structure_id"),
        "date": raw_data.get("date"),
        "longname": raw_data.get("longname"),
        "shortname": raw_data.get("shortname"),
        "gender": raw_data.get("gender"),
        "max_rank": raw_data.get("max_rank"),
        "max_age": raw_data.get("max_age"),
        "url": raw_data.get("url"),
        "data_source_id": 1  # Default
    }
    return parsed_data

# Add validate method to TournamentClass (similar to Tournament)
def validate(self, logger: OperationLogger, item_key: str) -> Dict[str, str]:
    """
    Validate TournamentClass fields, log to OperationLogger.
    Returns dict with status and reason.
    """
    if not (self.shortname and self.date and self.tournament_id):
        reason = "Missing required fields (shortname, date, tournament_id)"
        logger.failed(item_key, reason)
        return {"status": "failed", "reason": reason}

    # Warnings (non-fatal)
    if not self.tournament_class_id_ext:
        logger.warning(item_key, "No valid external ID (likely upcoming)")
    if not self.longname:
        logger.warning(item_key, "Missing longname")

    return {"status": "success", "reason": "Validated OK"}

# Bind validate to TournamentClass
TournamentClass.validate = validate

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
                stages.add(int(st))
        except Exception:
            continue

    has_groups = (3 in stages) or (4 in stages)
    has_ko     = (5 in stages)

    if has_groups and has_ko:
        return 1 # STRUCT_GROUPS_AND_KO
    if has_groups and not has_ko:
        return 2 # STRUCT_GROUPS_ONLY
    if (not has_groups) and has_ko:
        return 3 # STRUCT_KO_ONLY
    return None