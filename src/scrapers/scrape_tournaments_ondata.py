# src/scrapers/scrape_tournaments_ondata.py

from typing import Any, Dict, List

from bs4 import BeautifulSoup
import requests

from utils import OperationLogger


def scrape_raw_tournaments_ondata(cursor) -> List[Dict[str, Any]]:
    """
    Scrape raw HTML rows from ondata.se tables.
    Returns list of raw dicts with table row data.
    """

    logger = OperationLogger(
        verbosity       = 2, 
        print_output    = False, 
        log_to_db       = True, 
        cursor          = cursor
    )

    SCRAPE_TOURNAMENTS_URL_ONDATA = "https://resultat.ondata.se/?viewAll=1"

    raw_tournaments = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    try:
        resp = requests.get(SCRAPE_TOURNAMENTS_URL_ONDATA, headers=headers, timeout=10)  # 10s timeout
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

            logger_keys = {
                "shortname":    raw_data["shortname"],
                "start_str":    raw_data["start_str"],
                "end_str":      raw_data["end_str"],
                "city":         raw_data["city"],
                "arena":        raw_data["arena"],
                "country_code": raw_data["country_code"],
                "onclick":      raw_data["onclick"]
            }

            raw_tournaments.append(raw_data)
            logger.success(logger_keys.copy(), "Tournament successfully scraped")

    logger.summarize()

    return raw_tournaments