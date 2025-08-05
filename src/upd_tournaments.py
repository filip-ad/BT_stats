# src/tournament.py

import logging
from datetime import date
import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from utils import parse_date, print_db_insert_results
from db import get_conn
from config import SCRAPE_TOURNAMENTS_ORDER, SCRAPE_TOURNAMENTS_CUTOFF_DATE, SCRAPE_TOURNAMENTS_URL_ONDATA
from models.tournament import Tournament

def upd_tournaments():

    conn, cursor = get_conn()
    
    try:

        logging.info(f"Starting tournament scraping process from ondata, with cutoff date {SCRAPE_TOURNAMENTS_CUTOFF_DATE}...")
        print(f"ℹ️  Starting tournament scraping process from ondata, with cutoff date {SCRAPE_TOURNAMENTS_CUTOFF_DATE}...")

        db_results = []     
        tournaments, db_results = scrape_tournaments_ondata()
        
        if not tournaments:
            logging.warning("No tournaments scraped.")
            print("⚠️  No tournaments scraped.")
            return

        logging.info(f"Successfully scraped {len(tournaments)} tournaments from ondata.")
        print(f"✅ Successfully scraped {len(tournaments)} tournaments from ondata.")

        # No need for batch insert here, save each tournament individually
        for t in tournaments:
            result = t.save_to_db(cursor)
            db_results.append(result)

        print_db_insert_results(db_results)

    except Exception as e:
        logging.error(f"Exception during tournament scraping: {e}")
        print(f"❌ Exception during tournament scraping: {e}")

    finally:
        conn.commit()
        conn.close()

def scrape_tournaments_ondata():
    """
    Returns a list of Tournament instances for all
    rows on the ?viewAll=1 page that meet the 
    date/status criteria (start >= cutoff, etc).
    """
    db_results = []

    _ONDATA_URL_RE = re.compile(r"https://resultat\.ondata\.se/(\w+)/?$")
    _ONCLICK_URL_RE = re.compile(r"document\.location=(?:'|\")?([^'\"]+)(?:'|\")?")
    
    tournaments = []
    today = date.today()
    cutoff_date = parse_date(SCRAPE_TOURNAMENTS_CUTOFF_DATE)

    resp = requests.get(SCRAPE_TOURNAMENTS_URL_ONDATA)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    # Find the Upcoming and Archive tables
    tables = soup.find_all("table", id="listtable")
    if not tables:
        logging.error("Could not find any #listtable on page")
        return tournaments

    # loop both Upcoming and Archive tables
    for table in tables:
        for idx, row in enumerate(table.find_all("tr")[1:], start=1):    
            cols = row.find_all("td")
            if len(cols) < 6:
                logging.debug(f"Skipping row {idx}: only {len(cols)} < 6 columns")
                continue

            # basic cols
            shortname    = cols[0].get_text(strip=True)
            start_str    = cols[1].get_text(strip=True)
            end_str      = cols[2].get_text(strip=True)
            city         = cols[3].get_text(strip=True)
            arena        = cols[4].get_text(strip=True)
            country_code = cols[5].get_text(strip=True)

            # parse dates
            start_date = parse_date(start_str)
            end_date   = parse_date(end_str)
            if not start_date or not end_date:
                logging.warning(f"Skipping {shortname}: invalid dates “{start_str}”–“{end_str}”")
                continue
            if start_date < cutoff_date:
                logging.debug(f"Skipping {shortname}: starts before cutoff ({start_date} < {cutoff_date})")
                continue

            # status
            if end_date < today:
                status = "ENDED"
            elif start_date <= today <= end_date:
                status = "ONGOING"
            else:
                status = "UPCOMING"

            # extract URL from onclick attribute
            onclick = row.get("onclick", "") or ""
            m = _ONCLICK_URL_RE.search(onclick)
            if m:
                full_url = urljoin(SCRAPE_TOURNAMENTS_URL_ONDATA, m.group(1))
            else:
                logging.warning(f"{shortname}: no valid onclick URL")
                db_results.append({
                    "status":   "warning",
                    "key":      shortname,
                    "reason":   "No valid onclick URL"
                })
                full_url = None

            # extract ondata_id ONLY if full_url is non-None
            m2 = _ONDATA_URL_RE.search(full_url) if full_url else None
            if m2:
                ondata_id = m2.group(1)
            else:
                logging.debug(f"{shortname}: invalid ondata_id in URL {full_url}")
                ondata_id = None                

            # build Tournament
            tour = Tournament.from_dict({
                "tournament_id":     None,
                "tournament_id_ext": ondata_id,
                "shortname":         shortname,
                "longname":          shortname,  # use shortname as longname if not available
                "startdate":         start_date,
                "enddate":           end_date,
                "city":              city,
                "arena":             arena,
                "country_code":      country_code,
                "ondata_id":         ondata_id,
                "url":               full_url,
                "status":            status,
                "data_source":       "ondata",
            })
            tournaments.append(tour)

    # sort by start date (earliest first) before inserting into DB
    tournaments.sort(key=lambda t: t.startdate)
    return tournaments, db_results