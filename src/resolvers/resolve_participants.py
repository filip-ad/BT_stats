# src/resolvers/resolve_participants.py
from models.participant import Participant
from models.participant_player import ParticipantPlayer
from models.participant_player_raw_tournament import ParticipantPlayerRawTournament
from models.club import Club
from models.player import Player
from models.player_license import PlayerLicense
from models.tournament_class import TournamentClass
from utils import OperationLogger, name_keys_for_lookup_all_splits, normalize_key
from typing import List, Dict, Optional, Tuple
import sqlite3
from datetime import date
from collections import defaultdict

def resolve_participants(cursor: sqlite3.Cursor, tournament_class_id_ext: Optional[str] = None) -> None:
    """Resolve raw player participants into participant and participant_player tables for a class or all classes."""
    logger = OperationLogger(verbosity=2, print_output=False, log_to_db=True, cursor=cursor)

    # Fetch all raw rows using class method
    raw_rows = ParticipantPlayerRawTournament.get_all(cursor)
    if not raw_rows:
        logger.skipped({}, "No raw participant data to resolve")
        return

    # Group by tournament_class_id_ext and raw_group_id
    groups: Dict[str, Dict[str, List[ParticipantPlayerRawTournament]]] = {}
    for row in raw_rows:
        class_ext = row.tournament_class_id_ext
        if not class_ext:  # Defensive check
            continue
        group_id = row.raw_group_id or f"single_{row.row_id}"  # Fallback for singles
        groups.setdefault(class_ext, {}).setdefault(group_id, []).append(row)

    # Build lookup caches
    club_map = Club.cache_name_map(cursor)
    player_name_map = Player.cache_name_map_verified(cursor)
    player_unverified_name_map = Player.cache_name_map_unverified(cursor)
    unverified_appearance_map = Player.cache_unverified_appearances(cursor)
    license_name_club_map = PlayerLicense.cache_name_club_map(cursor)

    for class_ext, class_groups in groups.items():
        # Initialize logger_keys with detailed fields
        logger_keys = {
            'tournament_class_id_ext': class_ext,
            'tournament_class_shortname': None,
            'fullname_raw': None,
            'clubname_raw': None,
            'player_id': None,
            'club_id': None,
            'match_type': None
        }
        try:
            # Get internal tournament_class_id and start date with defensive check
            tc = TournamentClass.get_by_ext_id(cursor, class_ext)
            if not tc:
                logger.failed(logger_keys, "No matching tournament_class_id found")
                continue
            tournament_class_id = tc.tournament_class_id
            class_date = tc.startdate if tc.startdate else date.today()
            logger_keys.update({'tournament_class_shortname': tc.shortname})

            for group_id, group_rows in class_groups.items():
                if not group_rows:
                    logger.skipped(logger_keys, "Empty group of participants")
                    continue

                # Use first row for participant fields
                first_row = group_rows[0]
                if not first_row.fullname_raw or not first_row.clubname_raw:
                    logger.failed(logger_keys, "Missing required raw data in group")
                    continue
                logger.info(logger_keys, f"Processing group {group_id} with first row: {first_row.fullname_raw}, {first_row.clubname_raw}")
                participant_data = {
                    "tournament_class_id": tournament_class_id,
                    "tournament_class_seed": int(first_row.seed_raw) if first_row.seed_raw and first_row.seed_raw.isdigit() else None,
                    "tournament_class_final_position": int(first_row.final_position_raw) if first_row.final_position_raw and first_row.final_position_raw.isdigit() else None
                }
                participant = Participant.from_dict(participant_data)
                is_valid, error_message = participant.validate()
                if not is_valid:
                    logger_keys.update({'fullname_raw': first_row.fullname_raw, 'clubname_raw': first_row.clubname_raw})
                    logger.failed(logger_keys, f"Participant validation failed: {error_message}")
                    continue

                upsert_res = participant.upsert(cursor)
                if upsert_res["status"] != "success":
                    logger_keys.update({'fullname_raw': first_row.fullname_raw, 'clubname_raw': first_row.clubname_raw})
                    logger.failed(logger_keys, f"Participant upsert failed: {upsert_res['reason']}")
                    continue
                participant_id = participant.participant_id

                for raw_row in group_rows:
                    logger_keys.update({
                        'fullname_raw': raw_row.fullname_raw,
                        'clubname_raw': raw_row.clubname_raw,
                        'player_id': None,
                        'club_id': None,
                        'match_type': None
                    })
                    if not raw_row.fullname_raw or not raw_row.clubname_raw:
                        logger.failed(logger_keys, "Missing required raw data for player")
                        continue
                    fullname_raw = raw_row.fullname_raw
                    clubname_raw = raw_row.clubname_raw
                    t_ptcp_id_ext = raw_row.participant_player_id_ext

                    club = Club.resolve(cursor, clubname_raw, club_map, logger, logger_keys, allow_prefix=True, fallback_to_unknown=True)
                    if not club:
                        logger.warning(logger_keys, "Club not found. Using Unknown (club_id=9999)")
                        club = Club(club_id=9999)
                    club_id = club.club_id
                    logger_keys['club_id'] = club_id

                    player_id, match_type = match_player(
                        cursor,
                        participant,
                        fullname_raw,
                        clubname_raw,
                        class_date,
                        license_name_club_map,
                        player_name_map,
                        player_unverified_name_map,
                        unverified_appearance_map,
                        logger,
                        logger_keys.copy(),
                        class_ext
                    )
                    if player_id is None:
                        logger.failed(logger_keys, "No match for player")
                        continue
                    logger_keys['player_id'] = player_id
                    logger_keys['match_type'] = match_type

                    pp_data = {
                        "participant_player_id_ext": t_ptcp_id_ext,
                        "participant_id": participant_id,
                        "player_id": player_id,
                        "club_id": club_id
                    }
                    participant_player = ParticipantPlayer.from_dict(pp_data)
                    is_valid, error_message = participant_player.validate()
                    if not is_valid:
                        logger.warning(logger_keys, f"ParticipantPlayer validation failed: {error_message}")
                        continue

                    upsert_res = participant_player.upsert(cursor, participant_id, player_id)
                    if upsert_res["status"] != "success":
                        logger.warning(logger_keys, f"ParticipantPlayer upsert failed: {upsert_res['reason']}")
                        continue

                logger.success(logger_keys, f"Resolved group {group_id} ({len(group_rows)} players)")

        except Exception as e:
            logger.failed(logger_keys, f"Exception during resolution: {str(e)}")

    logger.summarize()

def match_player(
    cursor,
    participant: Participant,
    fullname_raw: str,
    clubname_raw: str,
    class_date: date,
    license_name_club_map,
    player_name_map,
    player_unverified_name_map,
    unverified_appearance_map,
    logger: OperationLogger,
    item_keys: Dict,
    tournament_class_id_ext: str,
) -> Tuple[Optional[int], Optional[str]]:
    """Match player using strategies from previous version."""
    strategies = [
        match_by_license_exact,
        match_by_license_substring,
        match_by_any_season_exact,
        match_by_any_season_substring,
        match_by_transition_exact,
        match_by_transition_substring,
        match_by_unverified_with_club
    ]

    for strategy in strategies:
        outcome = strategy(
            cursor,
            fullname_raw,
            clubname_raw,
            class_date,
            license_name_club_map,
            player_name_map,
            participant.club_id,
            logger,
            item_keys.copy(),
            unverified_appearance_map if strategy == match_by_unverified_with_club else None
        )
        if outcome:
            pid, match_type = outcome
            return pid, match_type

    pid = fallback_unverified(cursor, fullname_raw, clubname_raw, player_unverified_name_map, logger, item_keys.copy())
    if pid:
        logger.warning(item_keys.copy(), "Matched with unverified player as fallback")
        return pid, "unverified"
    return None, None

def match_by_license_exact(cursor, fullname_raw: str, clubname_raw: str, class_date: date, license_name_club_map, player_name_map, club_id: int, logger: OperationLogger, item_keys: Dict, _):
    keys = name_keys_for_lookup_all_splits(fullname_raw)
    candidates = set()
    for k in keys:
        key = (k, club_id)
        if key in license_name_club_map:
            for lic in license_name_club_map[key]:
                if lic["valid_from"] <= class_date <= lic["valid_to"]:
                    candidates.add(lic["player_id"])
    if len(candidates) == 1:
        return list(candidates)[0], "license_exact"
    return None

def match_by_license_substring(cursor, fullname_raw: str, clubname_raw: str, class_date: date, license_name_club_map, player_name_map, club_id: int, logger: OperationLogger, item_keys: Dict, _):
    clean = normalize_key(fullname_raw)
    parts = clean.split()
    if len(parts) > 2:
        return None
    first_tok, last_tok = parts[0], parts[-1]
    candidates = set()
    for (full_key, cid), rows in license_name_club_map.items():
        if cid != club_id:
            continue
        if len(full_key.split()) < 3:
            continue
        if first_tok in full_key and last_tok in full_key:
            for row in rows:
                if row["valid_from"] <= class_date <= row["valid_to"]:
                    candidates.add(row["player_id"])
    if len(candidates) == 1:
        return list(candidates)[0], "license_substring"
    return None

def match_by_any_season_exact(cursor, fullname_raw: str, clubname_raw: str, class_date: date, license_name_club_map, player_name_map, club_id: int, logger: OperationLogger, item_keys: Dict, _):
    keys = name_keys_for_lookup_all_splits(fullname_raw)
    candidates = set()
    for k in keys:
        key = (k, club_id)
        if key in license_name_club_map:
            for lic in license_name_club_map[key]:
                candidates.add(lic["player_id"])
    if len(candidates) == 1:
        logger.warning(item_keys.copy(), "Matched by name with license in club, but not necessarily valid on class date")
        return list(candidates)[0], "any_season_exact"
    return None

def match_by_any_season_substring(cursor, fullname_raw: str, clubname_raw: str, class_date: date, license_name_club_map, player_name_map, club_id: int, logger: OperationLogger, item_keys: Dict, _):
    clean = normalize_key(fullname_raw)
    parts = clean.split()
    if len(parts) > 2:
        return None
    first_tok, last_tok = parts[0], parts[-1]
    candidates = set()
    for (full_key, cid), rows in license_name_club_map.items():
        if cid != club_id:
            continue
        if len(full_key.split()) < 3:
            continue
        if first_tok in full_key and last_tok in full_key:
            for row in rows:
                candidates.add(row["player_id"])
    if len(candidates) == 1:
        logger.warning(item_keys.copy(), "Matched by substring with license in club, but not necessarily valid on class date")
        return list(candidates)[0], "any_season_substring"
    return None

def match_by_transition_exact(cursor, fullname_raw: str, clubname_raw: str, class_date: date, license_name_club_map, player_name_map, club_id: int, logger: OperationLogger, item_keys: Dict, _):
    pids = get_name_candidates(fullname_raw, player_name_map)
    if not pids:
        return None
    placeholders = ",".join("?" for _ in pids)
    try:
        cursor.execute(f"SELECT DISTINCT player_id FROM player_transition WHERE (club_id_to = ? OR club_id_from = ?) AND transition_date <= ? AND player_id IN ({placeholders})", [club_id, club_id, class_date] + pids)
    except Exception as e:
        logger.failed(item_keys.copy(), f"Error executing SQL for transition_exact: {e}")
        return None
    trans = [r[0] for r in cursor.fetchall()]
    if len(trans) == 1:
        return trans[0], "transition_exact"
    return None

def match_by_transition_substring(cursor, fullname_raw: str, clubname_raw: str, class_date: date, license_name_club_map, player_name_map, club_id: int, logger: OperationLogger, item_keys: Dict, _):
    clean = normalize_key(fullname_raw)
    parts = clean.split()
    if len(parts) > 2:
        return None
    first_tok, last_tok = parts[0], parts[-1]
    sub_pids = set()
    for (full_key, cid), rows in license_name_club_map.items():
        if cid != club_id:
            continue
        if len(full_key.split()) < 3:
            continue
        if first_tok in full_key and last_tok in full_key:
            for row in rows:
                sub_pids.add(row["player_id"])
    if not sub_pids:
        return None
    placeholders = ",".join("?" for _ in sub_pids)
    cursor.execute(f"SELECT DISTINCT player_id FROM player_transition WHERE (club_id_to = ? OR club_id_from = ?) AND transition_date <= ? AND player_id IN ({placeholders})", [club_id, club_id, class_date] + list(sub_pids))
    trans = [r[0] for r in cursor.fetchall()]
    if len(trans) == 1:
        return trans[0], "transition_substring"
    return None

def match_by_unverified_with_club(cursor, fullname_raw: str, clubname_raw: str, class_date: date, license_name_club_map, player_name_map, club_id: int, logger: OperationLogger, item_keys: Dict, unverified_appearance_map):
    clean = normalize_key(fullname_raw)
    if clean in unverified_appearance_map:
        for entry in unverified_appearance_map[clean]:
            if entry["club_id"] == club_id:
                return entry["player_id"], "unverified player with club"
    return None

def fallback_unverified(cursor, fullname_raw: str, clubname_raw: str, player_unverified_name_map: Dict[str, int], logger: OperationLogger, item_keys: Dict):
    clean = normalize_key(fullname_raw)
    existing = player_unverified_name_map.get(clean)
    if existing is not None:
        return existing

    res = Player.insert_unverified(cursor, fullname_raw)
    if res["status"] in ("created", "reused") and res["player_id"]:
        player_unverified_name_map[clean] = res["player_id"]
        if res["status"] == "created":
            logger.warning(item_keys.copy(), "Created new unverified player")
        else:
            logger.warning(item_keys.copy(), "Reused existing unverified player")
        return res["player_id"]
    logger.failed(item_keys.copy(), "Failed to insert/reuse unverified player")
    return None

def get_name_candidates(fullname_raw: str, player_name_map: Dict[str, List[int]]) -> List[int]:
    keys = name_keys_for_lookup_all_splits(fullname_raw)
    matches = set()
    for k in keys:
        matches.update(player_name_map.get(k, []))
    return list(matches)