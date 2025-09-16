# src/resolvers/resolve_player_licenses.py

import re, time
from typing import List
from models.player_license import PlayerLicense
from models.player_license_raw import PlayerLicenseRaw
from models.season import Season
from models.club import Club
from models.player import Player
from models.license import License
from utils import OperationLogger, parse_date

def resolve_player_licenses(cursor) -> List[PlayerLicense]:
    """
    Resolve player_license_raw → player_license.
    Handles duplicate detection, parsing, validation, and insert.
    """

    logger = OperationLogger(
        verbosity       = 2, 
        print_output    = False, 
        log_to_db       = True, 
        cursor          = cursor
    )

    logger.info("Resolving player licenses...", to_console=True)

    start_time = time.time()
    seen_final_keys = set()

    # Cache mappings
    season_map          = Season.cache_all(cursor) 
    club_name_map       = Club.cache_name_map(cursor)
    club_id_ext_map     = Club.cache_id_ext_map(cursor)
    player_id_ext_map   = Player.cache_id_ext_map(cursor)
    license_map         = License.cache_all(cursor)

    # Cache duplicate licenses
    duplicate_map = PlayerLicenseRaw.get_duplicates(cursor)

    # Fetch all raw player license records
    raw_objects = PlayerLicenseRaw.get_all(cursor)
    '''
    row_id, 
    season_id_ext, 
    season_label, 
    club_name, 
    club_id_ext,
    CAST(player_id_ext AS TEXT) AS player_id_ext,
    firstname, 
    lastname, 
    gender, 
    year_born, 
    license_info_raw
    '''
    if not raw_objects:
        logger.skipped("global", "No player license data found in player_license_raw")
        return []

    # Regex for parsing license strings
    # license_regex = re.compile(
    #     r"(?P<type>(?:[A-D]-licens|48-timmarslicens|Paralicens))(?: (?P<age>\w+))? \((?P<date>\d{4}\.\d{2}\.\d{2})\)"
    # )

    # license_regex = re.compile(
    #     r"(?P<type>(?:[A-D]-licens|48-timmarslicens|Paralicens))(?: (?P<age>\w+))?\s*\((?P<date>\d{4}\.\d{2}\.\d{2})\)"
    # )

    # Allow missing date -- later set to season start and end dates if missing
    license_regex = re.compile(r"(?P<type>(?:[A-D]-licens|48-timmarslicens|Paralicens))(?: (?P<age>\w+))?\s*\((?P<date>\d{4}\.\d{2}\.\d{2})?\)")

    licenses = []
    data_source_id = 3
    player_cache_misses = club_cache_misses = license_cache_misses = 0

    for raw in raw_objects:

        logger_keys = {
            "row_id":           getattr(raw, "row_id", None),
            "season_label":     getattr(raw, "season_label", None),
            "player_id":        None,
            "player_id_ext":    getattr(raw, "player_id_ext", None),
            "firstname":        getattr(raw, "firstname", None),
            "lastname":         getattr(raw, "lastname", None),
            "year_born":        getattr(raw, "year_born", None),
            "gender":           getattr(raw, "gender", None),
            "club_id_ext":      getattr(raw, "club_id_ext", None),
            "club_id":          None,
            "club_name":        getattr(raw, "club_name", None),
            "valid_from":       None,
            "license_info_raw": getattr(raw, "license_info_raw", None) or "".strip()
        }

        item_key = f"{raw.firstname} {raw.lastname} (id ext: {raw.player_id_ext}, club: {raw.club_name}, season: {raw.season_label}, row_id: {raw.row_id})"
        license_info_raw = (raw.license_info_raw or "").strip()

        # Parse license_info_raw
        match = license_regex.search(license_info_raw.strip())
        if not match:
            # logger.failed(item_key, "Invalid license format")
            logger.failed(
                logger_keys.copy(),
                "Invalid license format"
            )
            continue
        type_           = match.group("type").strip().capitalize()
        license_date    = match.group("date")
        age_group       = match.group("age").strip().capitalize() if match.group("age") else None

        # Detect duplicates
        license_key     = f"{type_} {age_group}".strip().lower() if age_group else type_.lower()
        duplicate_key   = (raw.player_id_ext, raw.club_id_ext, raw.season_id_ext, license_key)
        if duplicate_key in duplicate_map and duplicate_map[duplicate_key] != raw.row_id:
            logger.failed(logger_keys.copy(), "Duplicate license detected")
            continue

        # Season lookup
        season = season_map.get(raw.season_label)
        if not season:
            logger_keys["valid_to"] = None
            logger.failed(logger_keys.copy(), "Season not found")
            continue
        season_id                   = season.season_id
        valid_to                    = season.end_date
        logger_keys["season_id"]    = season_id
        logger_keys["valid_to"]     = valid_to

        # Parse valid_from
        if not license_date:
            # Missing date: Use season dates and warn
            logger_keys["valid_from"]   = season.start_date
            logger_keys["valid_to"]     = season.end_date
            logger.warning(logger_keys.copy(), "Missing license date, using season dates")
        else:
            valid_from = parse_date(license_date, context=f"license_info_raw {license_info_raw}")
            if not valid_from:
                logger_keys["valid_from"] = None
                logger.failed(
                    logger_keys.copy(),
                    "Invalid date format"
                )
                continue
            logger_keys["valid_from"] = valid_from

        # Resolve player_id
        player = player_id_ext_map.get((raw.player_id_ext, data_source_id))
        player_id = player.player_id if player else None
        if not player:
            player_cache_misses += 1
            logger.failed(logger_keys.copy(), "Could not resolve player_id")
            continue

        # Resolve club_id
        club = club_id_ext_map.get(raw.club_id_ext) or club_name_map.get(raw.club_name.strip().lower())
        club_id = club.club_id if club else None
        if not club:
            club_cache_misses += 1
            logger.warning(logger_keys.copy(), "No club found in cache")

        # Resolve license_id
        license_obj = license_map.get((type_, age_group))
        license_id = license_obj.license_id if license_obj else None
        if not license_obj:
            license_cache_misses += 1
            logger.warning(logger_keys.copy(), "No license found in cache")

        # Prevent duplicates in same run
        final_key = (player_id, club_id, season_id, license_id)
        if final_key in seen_final_keys:
            logger.skipped(logger_keys.copy(), "Duplicate license in same batch")
            continue
        seen_final_keys.add(final_key)

        # Check required fields
        if not all([player_id, club_id, season_id, license_id, valid_from, valid_to]):
            logger.failed(logger_keys.copy(), "Missing required fields")
            continue

        licenses.append(PlayerLicense(
            player_id=player_id,
            club_id=club_id,
            season_id=season_id,
            license_id=license_id,
            valid_from=valid_from,
            valid_to=valid_to,
            row_id=raw.row_id
        ))

    # Validate + Insert
    validation_results = PlayerLicense.batch_validate(cursor, licenses, logger)
    valid_licenses = [licenses[i] for i, res in enumerate(validation_results) if res["status"] == "success"]
    PlayerLicense.batch_insert(cursor, valid_licenses, logger)

    logger.summarize()

    return valid_licenses