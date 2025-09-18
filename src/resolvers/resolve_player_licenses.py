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

def resolve_player_licenses(cursor, run_id=None) -> List[PlayerLicense]:
    """
    Resolve player_license_raw → player_license.
    Handles duplicate detection, parsing, validation, and insert.
    """

    logger = OperationLogger(
        verbosity       = 2, 
        print_output    = False, 
        log_to_db       = True, 
        cursor          = cursor,
        object_type     = "player_license",
        run_type        = "resolve",
        run_id          = run_id
    )

    logger.info("Resolving player licenses...", to_console=True)

    # For duplication detection
    seen_final_keys = set()

    # Cache mappings
    season_map          = Season.cache_all(cursor)
    club_id_ext_map     = Club.cache_id_ext_to_id(cursor)
    player_id_ext_map   = Player.cache_id_ext_map(cursor)
    license_map         = License.cache_all(cursor)

    # Cache duplicate licenses
    duplicate_map       = PlayerLicenseRaw.get_duplicates(cursor)

    # Fetch all raw player license records
    raw_objects         = PlayerLicenseRaw.get_all(cursor)
    if not raw_objects:
        logger.failed({}, "No player license data found in player_license_raw")
        return []

    # Allow missing date -- later set to season start and end dates if missing
    license_regex = re.compile(r"(?P<type>(?:[A-D]-licens|48-timmarslicens|Paralicens))(?: (?P<age>\w+))?\s*\((?P<date>\d{4}\.\d{2}\.\d{2})?\)")

    for raw in raw_objects:

        logger.inc_processed()
    
        logger_keys = {
                "raw_row_id":           raw.row_id,
                "season_label":         raw.season_label,
                "player_id_ext":        raw.player_id_ext,
                "firstname":            raw.firstname,
                "lastname":             raw.lastname,
                "year_born":            raw.year_born,
                "gender":               raw.gender,
                "club_id_ext":          raw.club_id_ext,
                "club_name":            raw.club_name,
                "license_info_raw":     raw.license_info_raw or "".strip(),
                "player_id":            None,
                "club_id":              None,
                "season_id":            None,
                "license_id":           None,
                "valid_from":           None,
                "valid_to":             None
            }

        # Parse license_info_raw
        license_info_raw    = (raw.license_info_raw or "").strip()
        match               = license_regex.search(license_info_raw.strip())
        if not match:
            logger.failed(logger_keys.copy(), "Invalid license format")
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

        # Parse valid_from
        if not license_date:
            # Missing date: Use season dates and warn
            logger_keys.update({
                "valid_from":       season.start_date,
                "valid_to":         season.end_date
            })
            logger.warning(logger_keys.copy(), "Missing license date, using season dates")
        else:
            valid_from = parse_date(license_date, context=f"license_info_raw {license_info_raw}")
            if not valid_from:
                # logger_keys["valid_from"] = None
                logger.failed(logger_keys.copy(), "Invalid date format")
                continue
            logger_keys["valid_from"] = valid_from

        # Get season ID (internal) and valid_to from season_map
        season = season_map.get(raw.season_label)
        if not season:
            # logger_keys["valid_to"] = None
            logger.failed(logger_keys.copy(), "Season not found")
            continue
        season_id                   = season.season_id
        valid_to                    = season.end_date
        logger_keys.update({
            "season_id":            season_id,
            "valid_to":             valid_to
        })

        # Reassign season if valid_from is outside bounds
        if valid_from and not (season.start_date <= valid_from <= season.end_date):
            new_season = Season.get_by_date(cursor, valid_from)
            if not new_season:
                logger.failed(logger_keys, f"No season found for valid_from date")
                continue
            if new_season.season_id != season_id:
                logger.warning(logger_keys, f"Season reassigned to proper season based on valid_from date")
                season_id = new_season.season_id
                valid_to = new_season.end_date
                logger_keys.update({
                    "season_id": season_id,
                    "valid_to": valid_to
                })

        # If no valid_from, use season start date
        if not valid_from:
            valid_from = season.start_date
            valid_to = season.end_date
            logger_keys.update({
                "valid_from": valid_from,
                "valid_to": valid_to
            })

        # Resolve player_id
        player = player_id_ext_map.get((raw.player_id_ext, 3)) # data_source_id 3 = 'Profixio'. Hardocded for now since only source for player id ext is Profixio. Data source ID for the player license raw is = 1 'ondata', hence cant use that here.
        player_id = player.player_id if player else None
        if not player:
            player_cache_misses += 1
            logger.failed(logger_keys.copy(), "Could not resolve player_id")
            continue

        # Resolve club_id strictly via club_id_ext
        ext = int(str(raw.club_id_ext).strip())
        if ext is None:
            logger.failed(logger_keys.copy(), "Invalid club_id_ext (not numeric)")
            continue
        club_id = club_id_ext_map.get(ext)
        if club_id is None:
            logger.failed(logger_keys.copy(), f"Unknown club_id_ext {ext}")
            continue

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

        lic = PlayerLicense(
            player_id=player_id,
            club_id=club_id,
            season_id=season_id,
            license_id=license_id,
            valid_from=valid_from,
            valid_to=valid_to
        )

        # Validate (pass season_map to avoid DB query)
        is_valid, err = lic.validate(cursor)
        if not is_valid:
            if err == "Season_id reassigned":
                logger.warning(logger_keys, "Season_id reassigned during validation")
            if err == 'License already exists':
                logger.skipped(logger_keys, "License already exists")
                continue
            else:
                logger.failed(logger_keys, err)
                continue

        # Upsert
        result = lic.upsert(cursor)
        if result == "inserted":
            logger.success(logger_keys, "Player license inserted")
        elif result == "updated":
            logger.success(logger_keys, "Player license updated")
        elif result == "unchanged":
            logger.success(logger_keys, "Player license unchanged")
        else:
            logger.failed(logger_keys, "Upsert failed")
            continue

    logger.summarize()

