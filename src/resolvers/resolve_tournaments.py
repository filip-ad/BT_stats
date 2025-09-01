from datetime import date
import re
from typing import Any, Dict, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup
import requests

from utils import OperationLogger, parse_date


def resolve_tournaments(
        raw_data:   Dict[str, Any], cursor
    ) -> Optional[Dict[str, Any]]:
    """
    Parse raw dict to structured data (e.g., dates, URL, ID).
    Logs failures and warnings using logger.
    Returns parsed data dict on success, None on failure.
    """

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
            logger.error(logger_keys, f"Error fetching longname for {ondata_id}: {e}")
            return None

    SCRAPE_TOURNAMENTS_URL_ONDATA = "https://resultat.ondata.se/?viewAll=1"

    logger = OperationLogger(
        verbosity       = 2, 
        print_output    = False, 
        log_to_db       = True, 
        cursor          = cursor
    )

    logger_keys = {
        "shortname":    raw_data["shortname"],
        "start_str":    raw_data["start_str"],
        "end_str":      raw_data["end_str"],
        "city":         raw_data["city"],
        "arena":        raw_data["arena"],
        "country_code": raw_data["country_code"],
        "onclick":      raw_data["onclick"]
}

    start_date  = parse_date(raw_data["start_str"])
    end_date    = parse_date(raw_data["end_str"])
    if not start_date or not end_date:
        logger.failed(logger_keys, "Invalid dates")
        return None

    if not (start_date and end_date):
        logger.warning(logger_keys, "Invalid dates for status calc")
        # Debug temp
        logger.info(logger_keys, "Invalid dates for status calc")

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

