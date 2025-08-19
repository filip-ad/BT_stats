# src/updaters/tournament_updater.py

import logging
from datetime import date
import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from typing import List, Dict, Any, Optional
from utils import parse_date, OperationLogger
from db import get_conn
from config import SCRAPE_TOURNAMENTS_CUTOFF_DATE, SCRAPE_TOURNAMENTS_URL_ONDATA
from models.tournament import Tournament

def upd_tournaments() -> None:
    """
    Updater entry point: Scrape raw data, process through pipeline, aggregate results.
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

        logging.info(f"Starting tournament update, cutoff: {cutoff_date}")
        print(f"ℹ️  Starting tournament update, cutoff: {cutoff_date}")

        # Scrape all tournaments
        # =============================================================================
        raw_tournaments = scrape_raw_tournaments_ondata(logger)
        if not raw_tournaments:
            logging.warning("No raw data scraped")
            print("⚠️  No raw data scraped")
            return

        # Filter by cutoff date
        # =============================================================================
        filtered_tournaments = [
            t for t in raw_tournaments
            if (start_date := parse_date(t["start_str"])) and start_date >= cutoff_date
        ]

        # Loop through filtered tournaments
        # =============================================================================
        for i, raw_data in enumerate(filtered_tournaments, 1):

            start_d     = parse_date(raw_data["start_str"])
            item_key    = f"{raw_data['shortname']} ({start_d})"
            print(f"ℹ️  Processing tournament [{i}/{len(filtered_tournaments)}] {raw_data['shortname']}")

            # Parse tournaments
            # ============================================================================= 
            parsed_data = parse_raw_tournament(raw_data, logger, item_key)
            if parsed_data is None:
                continue

            # Create and validate tournament object
            # =============================================================================         
            tournament  = Tournament.from_dict(parsed_data)
            val         = tournament.validate(logger, item_key)  
            if val["status"] != "success":
                continue

            tournament.upsert(cursor, logger, item_key)
            
        logger.summarize()


    except Exception as e:
        logging.error(f"Error in upd_tournaments: {e}")
        print(f"❌ Error in upd_tournaments: {e}")

    finally:
        conn.commit()
        conn.close()

def scrape_raw_tournaments_ondata(
        logger: OperationLogger
    ) -> List[Dict[str, Any]]:      
    """
    Scrape raw HTML rows from ondata.se tables.
    Returns list of raw dicts with table row data.
    """

    raw_tournaments = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    try:
        resp = requests.get(SCRAPE_TOURNAMENTS_URL_ONDATA, headers=headers, timeout=30)  # 30s timeout
        resp.raise_for_status()
    except requests.Timeout:
        print("❌ Timeout fetching OnData tournaments. Site slow or network issue—try later or increase timeout.")
        logger.failed("Timeout fetching OnData tournaments")
        return []
    except requests.ConnectionError:
        print("❌ Connection error. Check network/proxy/VPN, or site may be down.")
        logger.failed("Connection error fetching OnData tournaments")
        return []
    except requests.HTTPError as e:
        print(f"❌ HTTP error: {e} (status: {resp.status_code if 'resp' in locals() else 'Unknown'})")
        logger.failed(f"HTTP error fetching OnData tournaments: {e}")
        return []
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        logger.failed(f"Unexpected error fetching OnData tournaments: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    tables = soup.find_all("table", id="listtable")
    if not tables:
        print("❌  No tables found on page—site structure may have changed.")
        logger.failed("No tables found on page—site structure may have changed.")
        return raw_tournaments

    for table in tables:
        for row in table.find_all("tr")[1:]:
            cols = row.find_all("td")
            if len(cols) < 6:
                continue

            raw_data = {
                "shortname":    cols[0].text.strip(),
                "start_str":    cols[1].text.strip(),
                "end_str":      cols[2].text.strip(),
                "city":         cols[3].text.strip(),
                "arena":        cols[4].text.strip(),
                "country_code": cols[5].text.strip(),
                "onclick":      row.get("onclick", "")
            }
            raw_tournaments.append(raw_data)

    return raw_tournaments

def parse_raw_tournament(
        raw_data:   Dict[str, Any], 
        logger:     OperationLogger,
        item_key:   str
    ) -> Optional[Dict[str, Any]]:
    """
    Parse raw dict to structured data (e.g., dates, URL, ID).
    Logs failures and warnings using logger.
    Returns parsed data dict on success, None on failure.
    """

    start_date  = parse_date(raw_data["start_str"])
    end_date    = parse_date(raw_data["end_str"])
    if not start_date or not end_date:
        logger.failed(item_key, "Invalid dates")
        return None

    if not (start_date and end_date):
        logger.warning(item_key, "Invalid dates for status calc")
        # Debug temp
        logging.info(item_key, "Invalid dates for status calc")

    status = 6 if not (start_date and end_date) else 3 if end_date < date.today() else 2 if start_date <= date.today() <= end_date else 1

    _ONCLICK_URL_RE     = re.compile(r"document\.location=(?:'|\")?([^'\"]+)(?:'|\")?")
    _ONDATA_URL_RE      = re.compile(r"https://resultat\.ondata\.se/(\w+)/?$")
    m                   = _ONCLICK_URL_RE.search(raw_data["onclick"])
    full_url            = urljoin(SCRAPE_TOURNAMENTS_URL_ONDATA, m.group(1)) if m else None
    m2                  = _ONDATA_URL_RE.search(full_url) if full_url else None
    ondata_id           = m2.group(1) if m2 else None
    longname            = _fetch_tournament_longname(ondata_id) if ondata_id else None
    status              = 6 if not (start_date and end_date) else 3 if end_date < date.today() else 2 if start_date <= date.today() <= end_date else 1

    parsed_data = {
        "tournament_id_ext":    ondata_id,
        "longname":             longname,
        "shortname":            raw_data["shortname"],
        "startdate":            start_date,
        "enddate":              end_date,
        "city":                 raw_data["city"],
        "arena":                raw_data["arena"],
        "country_code":         raw_data["country_code"],
        "url":                  full_url,
        "tournament_status_id": status,
        "data_source_id":       1
    }

    return parsed_data

def _fetch_tournament_longname(ondata_id: str) -> Optional[str]:
    """
    Fetch tournament longname from result frame title.
    Returns None on failure.
    """
    base_url = f"https://resultat.ondata.se/{ondata_id}/"
    try:
        r1 = requests.get(base_url)
        r1.raise_for_status()
        soup1 = BeautifulSoup(r1.content, "html.parser")
        result_frame = soup1.find("frame", {"name": "Resultat"})
        if not result_frame or not result_frame.get("src"):
            return None
        result_url = urljoin(base_url, result_frame["src"])

        r2 = requests.get(result_url)
        r2.raise_for_status()
        r2.encoding = "iso-8859-1"
        soup2 = BeautifulSoup(r2.text, "html.parser")
        title_tag = soup2.find("title")
        return title_tag.text.strip() if title_tag else None
    except Exception as e:
        logging.error(f"Error fetching longname for {ondata_id}: {e}")
        return None