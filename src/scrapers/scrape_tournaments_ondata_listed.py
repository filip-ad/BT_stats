# src/scrapers/scrape_tournaments_ondata_listed.py

import sqlite3
from typing import Optional
from bs4 import BeautifulSoup
import requests
import re
from urllib.parse import urljoin
import time
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from utils import OperationLogger, parse_date
from models.tournament_raw import TournamentRaw

from config import SCRAPE_TOURNAMENTS_CUTOFF_DATE


def scrape_tournaments_ondata_listed(cursor) -> None:
    """
    Scrape raw HTML rows from ondata.se tables.
    Upserts raw data into tournament_raw table.
    """

    logger = OperationLogger(
        verbosity       = 2,
        print_output    = False,
        log_to_db       = True,
        cursor          = cursor,
        object_type     = "tournament_raw",
        run_type        = "scrape"
    )

    nbr_of_tnmnts_scraped = 0
    start_time = time.time()
    cutoff_date = parse_date(SCRAPE_TOURNAMENTS_CUTOFF_DATE)
    sleep_time = 0.3 # Seconds between requests to avoid overloading server- 0.3 seems fine.
    logger.info(f"Scraping ondata for listed tournaments, cut off date: {cutoff_date}. Using {sleep_time}s sleep time between requests.")

    SCRAPE_TOURNAMENTS_URL_ONDATA = "https://resultat.ondata.se/?viewAll=1"

    # Configure session with retry logic
    session = requests.Session()
    retry_strategy = Retry(
        total=3,  # Maximum number of retries
        backoff_factor=1,  # Wait 1s, 2s, 4s between retries
        status_forcelist=[500, 502, 503, 504, 429],  # Retry on these status codes
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    try:
        resp = session.get(SCRAPE_TOURNAMENTS_URL_ONDATA, headers=headers, timeout=10)
        resp.raise_for_status()
    except requests.Timeout:
        print("❌ Timeout fetching OnData tournaments. Site slow or network issue—try later or increase timeout.")
        logger.failed("Timeout fetching OnData tournaments")
        return
    except requests.ConnectionError:
        print("❌ Connection error. Check network/proxy/VPN, or site may be down.")
        logger.failed("Connection error fetching OnData tournaments")
        return
    except requests.HTTPError as e:
        print(f"❌ HTTP error: {e} (status: {resp.status_code if 'resp' in locals() else 'Unknown'})")
        logger.failed(f"HTTP error fetching OnData tournaments: {e}")
        return
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        logger.failed(f"Unexpected error fetching OnData tournaments: {e}")
        return

    soup = BeautifulSoup(resp.text, "html.parser")
    tables = soup.find_all("table", id="listtable")
    if not tables:
        logger.failed("No tables found on page—site structure may have changed.")
        logger.info("No tables found on page—site structure may have changed.")
        return

    _ONCLICK_URL_RE = re.compile(r"document\.location=(?:'|\")?([^'\"]+)(?:'|\")?")
    _ONDATA_URL_RE = re.compile(r"https://resultat\.ondata\.se/(\w+)/?$")

    for table in tables:
        for row in table.find_all("tr")[1:]:
            cols = row.find_all("td")
            if len(cols) < 6:
                continue

            shortname = cols[0].text.strip()
            start_str = cols[1].text.strip()
            end_str = cols[2].text.strip()
            city = cols[3].text.strip()
            arena = cols[4].text.strip()
            country_code = cols[5].text.strip()
            onclick = row.get("onclick", "")

            logger_keys = {
                "shortname": shortname,
                "longname": None,
                "startdate": start_str,
                "enddate": end_str,
                "ondata_id": None,
                "full_url": None
            }

            start_date = parse_date(start_str, context=f"scrape_raw_tournaments_ondata: Parsing start date for {shortname}")
            end_date = parse_date(end_str, context=f"scrape_raw_tournaments_ondata: Parsing end date for {shortname}")
            if not start_date or not end_date:
                logger.failed(logger_keys.copy(), "Invalid start date or end date")
                continue

            if start_date < cutoff_date:
                logger.skipped(logger_keys.copy(), f"Tournament before cutoff date {cutoff_date}")
                continue

            logger_keys.update({
                "startdate": start_date,
                "enddate": end_date
            })

            m = _ONCLICK_URL_RE.search(onclick)
            if m:
                full_url = urljoin(SCRAPE_TOURNAMENTS_URL_ONDATA, m.group(1))
                m2 = _ONDATA_URL_RE.search(full_url)
                if m2:
                    ondata_id = m2.group(1)
                else:
                    ondata_id = None
            else:
                full_url = None
                ondata_id = None

            if full_url and ondata_id:
                longname = _fetch_tournament_longname(ondata_id, session, headers, logger, logger_keys)
            else:
                longname = None
                logger.warning(logger_keys, "Failed to fetch longname")

            logger_keys.update({
                "longname": longname,
                "full_url": full_url,
                "ondata_id": ondata_id
            })

            # Create TournamentRaw object
            raw = TournamentRaw(
                tournament_id_ext=ondata_id,
                longname=longname,
                shortname=shortname,
                startdate=start_date,
                enddate=end_date,
                city=city or None,
                arena=arena or None,
                country_code=country_code,
                url=full_url,
                data_source_id=1,
                is_listed=True
            )

            nbr_of_tnmnts_scraped += 1
            logger.inc_processed()

            # Light validation for certain missing fields
            is_valid, error_message = raw.validate()
            if not is_valid:
                logger.failed(logger_keys, error_message)
                continue
            try: 
                action = raw.upsert(cursor)
            except sqlite3.Error as e:
                logger.failed(logger_keys, f"Database error during upsert: {e}")
                action = None

            if action == "inserted" or action == "updated":
                logger.success(logger_keys, f"Raw tournament successfully {action}")
            elif action == "unchanged":
                logger.success(logger_keys, "Raw tournament unchanged (already up-to-date)")
            else:
                logger.failed(logger_keys, "Raw tournament upsert failed")

            logger.info(f"Tournament (raw listed) {raw.shortname} on {raw.startdate} successfully {action}", to_console=True, show_key=False)

            time.sleep(sleep_time)

    run_time = time.time() - start_time
    logger.info(f"Completed scraping {nbr_of_tnmnts_scraped} listed tournaments in {run_time:.1f} seconds.")
    logger.summarize()
    logger.commit_run_summary(cursor)

    # run_time and nbr_of_tnmnts_scraped can be used for overhead tracking


def _fetch_tournament_longname(ondata_id: str, session: requests.Session, headers: dict, logger: OperationLogger, logger_keys: dict) -> Optional[str]:
    """
    Fetch tournament longname from result frame title with retry logic.
    Returns None on failure.
    """
    base_url = f"https://resultat.ondata.se/{ondata_id}/"
    try:
        r1 = session.get(base_url, headers=headers, timeout=10)
        r1.raise_for_status()
        soup1 = BeautifulSoup(r1.text, "html.parser")
        result_frame = soup1.find("frame", {"name": "Resultat"})
        if not result_frame or not result_frame.get("src"):
            return None
        result_url = urljoin(base_url, result_frame["src"])

        r2 = session.get(result_url, headers=headers, timeout=10)
        r2.raise_for_status()
        r2.encoding = "iso-8859-1"
        soup2 = BeautifulSoup(r2.text, "html.parser")
        title_tag = soup2.find("title")
        return title_tag.text.strip() if title_tag else None
    except requests.Timeout as e:
        logger.failed(logger_keys, f"Timeout fetching longname for ID {ondata_id}: {e}")
        return None
    except requests.ConnectionError as e:
        logger.failed(logger_keys, f"Connection error fetching longname for ID {ondata_id}: {e}")
        return None
    except requests.HTTPError as e:
        logger.failed(logger_keys, f"HTTP error fetching longname for ID {ondata_id}: {e}")
        return None
    except Exception as e:
        logger.failed(logger_keys, f"Unexpected error fetching longname for ID {ondata_id}: {e}")
        return None