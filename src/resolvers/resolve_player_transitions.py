# src/resolvers/resolve_player_transitions.py

import time
from typing import List
from models.player_transition import PlayerTransition
from models.player_transition_raw import PlayerTransitionRaw
from models.season import Season
from models.club import Club
from models.player import Player
from models.player_license import PlayerLicense
from utils import OperationLogger, normalize_key, parse_date, sanitize_name

def resolve_player_transitions(cursor, run_id=None) -> List[PlayerTransition]:
    """
    Resolve player_transition_raw → player_transition.
    Handles duplicate detection, parsing, validation, and insert.
    """

    logger = OperationLogger(
        verbosity       = 2, 
        print_output    = False, 
        log_to_db       = True, 
        cursor          = cursor,
        object_type     = "player_transition",
        run_type        = "resolve",
        run_id          = run_id
    )

    logger.info("Resolving player transitions...", to_console=True)

    # Cache mappings
    seasons_map             = Season.cache_by_ext(cursor)
    player_name_year_map    = Player.cache_name_year_map(cursor)
    player_name_map         = Player.cache_name_map_verified(cursor) 
    player_license_map      = PlayerLicense.cache_all(cursor)

    earliest_season_id = min(s.season_id for s in seasons_map.values() if s.season_id is not None)

    # Fetch all raw player transition records
    raw_objects = PlayerTransitionRaw.get_all(cursor)

    if not raw_objects:
        logger.skipped("global", "No player transition data found in player_transition_raw")
        return []

    transitions = []
    seen_final_keys = set()

    for raw in raw_objects:

        logger.inc_processed()

        logger_keys = {
            "row_id":           raw.row_id,
            "season_label":     raw.season_label,
            "firstname":        raw.firstname,
            "lastname":         raw.lastname,
            "year_born":        raw.year_born,
            "club_from":        raw.club_from,
            "club_to":          raw.club_to,
            "transition_date":  raw.transition_date
        }

        # --- Sanitize names ---
        firstname = sanitize_name(raw.firstname)
        lastname = sanitize_name(raw.lastname)

        # --- Parse transition date ---
        if isinstance(raw.transition_date, str):
            transition_date = parse_date(raw.transition_date, context=f"row_id: {raw.row_id}")
            if not transition_date:
                logger.failed(logger_keys.copy(), "Invalid transition date format")
                continue
        else:
            transition_date = raw.transition_date
        logger_keys["transition_date"] = transition_date

        # --- Resolve clubs ---
        club_from_obj, msg_from = Club.resolve(cursor, raw.club_from, allow_prefix=True)
        club_to_obj,   msg_to   = Club.resolve(cursor, raw.club_to,   allow_prefix=True)

        # if msg_from:
        #     logger.warning(logger_keys.copy(), msg_from)
        # if msg_to:
        #     logger.warning(logger_keys.copy(), msg_to)

        if not club_from_obj or not club_to_obj or club_from_obj.club_id == 9999 or club_to_obj.club_id == 9999:
            logger.failed(logger_keys.copy(), "Could not resolve club_from or club_to")
            continue

        club_id_from = club_from_obj.club_id
        club_id_to   = club_to_obj.club_id
        logger_keys["club_id_from"] = club_id_from
        logger_keys["club_id_to"]   = club_id_to

        # --- Resolve season ---
        season = seasons_map.get(raw.season_id_ext)
        if season:
            season_id = season.season_id
        else:
            season = next((s for s in seasons_map.values() if s.contains_date(transition_date)), None)
            if not season:
                season = Season.get_by_date(cursor, transition_date)
            if not season:
                logger.failed(logger_keys.copy(), "No matching season found for transition date")
                continue
            season_id = season.season_id
        logger_keys["season_id"] = season_id

        # --- Resolve player ---
        # player_key = (firstname, lastname, raw.year_born)
        # candidates = player_name_year_map.get(player_key)
        # if not candidates:
        #     candidates = Player.search_by_name_and_year(cursor, firstname, lastname, raw.year_born)
        # if not candidates:
        #     logger.failed(logger_keys.copy(), "No players found matching name and year born")
        #     continue

        player_key = (firstname, lastname, raw.year_born)
        candidates = player_name_year_map.get(player_key)

        if not candidates:
            # fallback to fullname-based lookup (verified only)
            fullname_norm = normalize_key(f"{firstname} {lastname}".strip())
            candidate_ids = player_name_map.get(fullname_norm, [])
            candidates = [Player(player_id=pid) for pid in candidate_ids]

        if not candidates:
            logger.failed(logger_keys.copy(), "No players found matching name/year/fullname")
            continue

        # Filter by license in club_from in previous seasons
        seasons_range = range(earliest_season_id, season_id + 1)
        valid_players = [
            p for p in candidates
            if PlayerLicense.has_license(player_license_map, p.player_id, club_id_from, seasons_range)
        ]

        if not valid_players:
            logger.failed(logger_keys.copy(), "No valid licensed players found in departing club")
            continue

        if len(valid_players) > 1:
            logger.failed(logger_keys.copy(), "Multiple valid players found with licenses in departing club (likely duplicate player_id_ext, run deduplication script)")
            continue

        player_id = valid_players[0].player_id
        logger_keys["player_id"] = player_id

        # --- Prevent duplicates ---
        final_key = (season_id, player_id, club_id_from, club_id_to, transition_date)
        if final_key in seen_final_keys:
            logger.skipped(logger_keys.copy(), "Duplicate transition in same batch")
            continue
        seen_final_keys.add(final_key)

        # --- Final required fields check ---
        if not all([season_id, player_id, club_id_from, club_id_to, transition_date]):
            logger.failed(logger_keys.copy(), "Missing required fields")
            continue

        transitions.append(PlayerTransition(
            season_id=season_id,
            player_id=player_id,
            club_id_from=club_id_from,
            club_id_to=club_id_to,
            transition_date=transition_date
        ))



    # Insert using save_to_db
    results = []
    valid_transitions = []
    for t in transitions:
        result = t.save_to_db(cursor, logger)
        results.append(result)
        if result["status"] == "success":
            valid_transitions.append(t)

    logger.summarize()

    return valid_transitions