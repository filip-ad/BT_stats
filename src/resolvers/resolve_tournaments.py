from datetime import date
from typing import Any, Dict, Optional
from utils import OperationLogger, parse_date


def resolve_tournaments(
        raw_data:   Dict[str, Any], cursor
    ) -> Optional[Dict[str, Any]]:
    """
    Parse raw dict to structured data (e.g., dates, URL, ID).
    Logs failures and warnings using logger.
    Returns parsed data dict on success, None on failure.
    """

    SCRAPE_TOURNAMENTS_URL_ONDATA = "https://resultat.ondata.se/?viewAll=1"

    logger = OperationLogger(
        verbosity       = 2, 
        print_output    = False, 
        log_to_db       = True, 
        cursor          = cursor
    )

    logger_keys = {
        "shortname":    raw_data["shortname"],
        "startdate":    raw_data["startdate"],
        "enddate":      raw_data["enddate"],
        "city":         raw_data["city"],
        "arena":        raw_data["arena"],
        "country_code": raw_data["country_code"],
    }

    start_date  = parse_date(raw_data["startdate"])
    end_date    = parse_date(raw_data["enddate"])
    if not start_date or not end_date:
        logger.failed(logger_keys, "Invalid dates")
        return None

    status = 6 if not (start_date and end_date) else 3 if end_date < date.today() else 2 if start_date <= date.today() <= end_date else 1

    parsed_data = {
        "tournament_id_ext":    raw_data["tournament_id_ext"],
        "longname":             raw_data["longname"],
        "shortname":            raw_data["shortname"],
        "startdate":            start_date,
        "enddate":              end_date,
        "city":                 raw_data["city"],
        "arena":                raw_data["arena"],
        "country_code":         raw_data["country_code"],
        "url":                  raw_data["url"],
        "tournament_status_id": status,
        "data_source_id":       1
    }

    return parsed_data