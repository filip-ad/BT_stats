# src/resolvers/resolve_tournaments.py

from datetime import date
import time
from typing import List
from models.tournament import Tournament
from models.tournament_raw import TournamentRaw
from utils import OperationLogger, parse_date

def resolve_tournaments(cursor) -> List[Tournament]:
    """
    Resolve tournament_raw → tournament.
    Handles parsing, validation, and insert.
    """
    logger = OperationLogger(
        verbosity       = 2,
        print_output    = False,
        log_to_db       = True,
        cursor          = cursor,
        object_type     = "tournament",
        run_type        = "resolve"
    )

    # Fetch all raw tournament records
    raw_objects = TournamentRaw.get_all(cursor)

    # Sort raw tournaments so the oldest gets inserted first (cosmetic)
    raw_objects = sorted(
        raw_objects,
        key=lambda r: (0, parse_date(r.startdate)) if parse_date(r.startdate) else (1, date.max)
    )

    logger.info(f"Resolving {len(raw_objects)} tournaments...", to_console=True)

    if not raw_objects:
        logger.failed({}, "No tournament data found in tournament_raw")
        return []

    tournaments = []
    seen_ext_ids = set()

    for raw in raw_objects:
        logger_keys = {
            "row_id": raw.row_id,
            "tournament_id_ext": str(raw.tournament_id_ext) if raw.tournament_id_ext else 'None',
            "longname": raw.longname or 'None',
            "shortname": raw.shortname or 'None',
            "startdate": str(raw.startdate) if raw.startdate else 'None',
            "enddate": str(raw.enddate) if raw.enddate else 'None',
            "city": raw.city or 'None',
            "arena": raw.arena or 'None',
            "country_code": raw.country_code or 'None',
            "url": raw.url or 'None'
        }

        # Parse dates
        start_date = parse_date(raw.startdate, context=f"row_id: {raw.row_id}")
        end_date = parse_date(raw.enddate, context=f"row_id: {raw.row_id}")

        if not start_date or not end_date:
            logger.warning(logger_keys, "Invalid start date or end date")
            continue

        logger_keys.update({
            "startdate": str(start_date),
            "enddate": str(end_date)
        })

        # Calculate status (default to 6 for incomplete data)
        if start_date and end_date:
            status = 3 if end_date < date.today() else 2 if start_date <= date.today() <= end_date else 1
        else:
            status = 6  # Incomplete data (e.g., unlisted tournaments)

        # Prevent duplicates in same run (only for non-None tournament_id_ext)
        if raw.tournament_id_ext is not None:
            if raw.tournament_id_ext in seen_ext_ids:
                logger.skipped(logger_keys, "Duplicate tournament_id_ext in same batch")
                continue
            seen_ext_ids.add(raw.tournament_id_ext)

        # Validate tournament data
        tournament = Tournament(
            tournament_id_ext=raw.tournament_id_ext,
            longname=raw.longname,
            shortname=raw.shortname,
            startdate=start_date,
            enddate=end_date,
            registration_end_date=None,
            city=raw.city,
            arena=raw.arena,
            country_code=raw.country_code,
            url=raw.url,  # Explicitly preserve URL
            tournament_status_id=status,
            data_source_id=raw.data_source_id
        )

        is_valid, error_message = tournament.validate()
        if not is_valid:
            logger.failed(logger_keys, f"Validation failed: {error_message}")
            continue

        tournaments.append(tournament)

    # Upsert tournaments
    valid_tournaments = []
    for t in tournaments:
        action = t.upsert(cursor)
        if action:
            logger.success({"tournament_id_ext": str(t.tournament_id_ext) if t.tournament_id_ext else 'None'}, f"Tournament successfully {action}")
            valid_tournaments.append(t)
        else:
            logger.warning({"tournament_id_ext": str(t.tournament_id_ext) if t.tournament_id_ext else 'None'}, "No changes made during upsert")

    logger.summarize()

    return valid_tournaments