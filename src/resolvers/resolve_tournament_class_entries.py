from asyncio.log import logger
from multiprocessing.util import debug
from models.tournament_class import TournamentClass
from models.tournament_class_entry import TournamentClassEntry
from models.tournament_class_player import TournamentClassPlayer
from models.tournament_class_entry_raw import TournamentClassEntryRaw
from models.tournament_class_group import TournamentClassGroup  # Import new
from models.tournament_class_group_member import TournamentClassGroupMember  # Import new
from models.club import Club
from models.player import Player
from models.player_license import PlayerLicense
from utils import OperationLogger, name_keys_for_lookup_all_splits, normalize_key, parse_date
from typing import List, Dict, Optional, Tuple
import sqlite3
from datetime import date
from config import RESOLVE_ENTRIES_CUTOFF_DATE
import re  # For extracting sort_order

def resolve_tournament_class_entries(cursor: sqlite3.Cursor, run_id=None) -> None:
    """Resolve raw entries into tournament_class_entry, tournament_class_player, tournament_class_group, and tournament_class_group_member tables."""

    logger = OperationLogger(
        verbosity       = 2,
        print_output    = False,
        log_to_db       = True,
        cursor          = cursor,
        object_type     = "entry",
        run_type        = "resolve",
        run_id          = run_id
    )

    debug = False

    raw_rows = TournamentClassEntryRaw.get_all(cursor)
    if not raw_rows:
        logger.skipped({}, "No raw entry data to resolve")
        return
    
    cutoff_date: date | None = parse_date(RESOLVE_ENTRIES_CUTOFF_DATE) if RESOLVE_ENTRIES_CUTOFF_DATE else None
    if cutoff_date:
        filtered_classes = TournamentClass.get_filtered_classes(
            cursor,
            cutoff_date       = cutoff_date,
            require_ended     = False,          # set True if you only want ended tournaments
            allowed_type_ids  = [1],            # singles
            order             = "newest"
        )
        allowed_class_exts = {tc.tournament_class_id_ext for tc in filtered_classes if tc.tournament_class_id_ext}
        if allowed_class_exts:
            raw_rows = [r for r in raw_rows if r.tournament_class_id_ext in allowed_class_exts]
        logger.info(f"Resolving tournament class entries for classes since {cutoff_date} ({len(raw_rows)} raw entries to process)...")
    else:
        logger.info("Resolving tournament class entries...", to_console=True)

    # Groups needed to resolve doubles, where 1 entry maps to 2 players (with same group_id)
    groups: Dict[str, Dict[int, List[TournamentClassEntryRaw]]] = {}
    for row in raw_rows:
        class_ext = row.tournament_class_id_ext
        if not class_ext:
            continue
        group_id = row.entry_group_id_int if row.entry_group_id_int is not None else int(f"100000{row.row_id}") 
        groups.setdefault(class_ext, {}).setdefault(group_id, []).append(row)

    logger.info({}, f"Filtered classes after cutoff: {len(filtered_classes)}")
    logger.info({}, f"Classes with raw entry rows: {len(groups)}")

    # Build lookup caches (unchanged from old code)
    player_name_map = Player.cache_name_map_verified(cursor)
    player_unverified_name_map = Player.cache_name_map_unverified(cursor)
    unverified_appearance_map = Player.cache_unverified_appearances(cursor)
    license_name_club_map = PlayerLicense.cache_name_club_map(cursor)

    for idx, (class_ext, class_groups) in enumerate(groups.items(), start=1):

        logger_keys = {
            'tournament_class_id_ext':      class_ext,
            'tournament_class_shortname':   None,
            'fullname_raw':                 None,
            'clubname_raw':                 None,
            'player_id':                    None,
            'club_id':                      None,
            'match_type':                   None
        }
        try:
            # Get internal tournament_class_id and start date
            tc = TournamentClass.get_by_ext_id(cursor, class_ext)
            if not tc:
                logger.failed(logger_keys, "No matching tournament_class_id found")
                continue
            tournament_class_id = tc.tournament_class_id
            class_date = tc.startdate if tc.startdate else date.today()
            logger_keys.update({'tournament_class_shortname': tc.shortname})

            logger.info(f"[{idx}/{len(groups)}] Resolving entries for tournament_class_id_ext={class_ext} (tournament_class_id={tournament_class_id}) with {len(class_groups)} groups...", to_console=True)

            for group_id, group_rows in class_groups.items():   
                if not group_rows:
                    logger.skipped(logger_keys, f"Empty group of entries (group_id={group_id})")
                    continue
                
                # Use first row for entry fields, including new group_id_raw and seed_in_group_raw
                first_row = group_rows[0]        
                if first_row.fullname_raw is None or first_row.clubname_raw is None:
                    logger.failed(logger_keys, f"Missing required raw data in group (row_id={first_row.row_id})")
                    continue
                
                try:
                    entry_data = {
                        "tournament_class_id":                  tournament_class_id,
                        "tournament_class_entry_id_ext":        None,
                        "tournament_class_entry_group_id_int":  group_id,
                        "seed":                                 int(first_row.seed_raw) if first_row.seed_raw and first_row.seed_raw.isdigit() else None,
                        "final_position":                       int(first_row.final_position_raw) if first_row.final_position_raw and first_row.final_position_raw.isdigit() else None
                    }
                    entry = TournamentClassEntry.from_dict(entry_data)
                    is_valid, error_message = entry.validate()
                    if not is_valid:
                        logger.failed(logger_keys, f"Entry validation failed for group {group_id} (row_id={first_row.row_id}): {error_message}")
                        continue

                    action = entry.upsert(cursor)
                    if not action:
                        logger.failed(logger_keys.copy(), f"Entry upsert failed for group {group_id} (row_id={first_row.row_id})")

                        continue
                    # ── CLEAR EXISTING ROWS FOR THIS ENTRY (run ONCE per group) ────────────────
                    # 1) Remove all players linked to this entry
                    TournamentClassPlayer.remove_for_entry(cursor, entry.tournament_class_entry_id)

                    # 2) Remove group-membership for this entry (prevents PK conflicts on rerun)
                    TournamentClassGroupMember.remove_for_entry(cursor, entry.tournament_class_entry_id)
                    # ───────────────────────────────────────────────────────────────────────────

                    logger_keys.update({
                        'fullname_raw': first_row.fullname_raw,
                        'clubname_raw': first_row.clubname_raw
                    })

                    # Handle group assignment if group_id_raw present
                    group_id_raw = first_row.group_id_raw
                    seed_in_group_raw = first_row.seed_in_group_raw
                    tournament_class_group_id = None
                    if group_id_raw:
                        # Extract sort_order from group_id_raw (e.g., "Pool 3" -> 3)
                        sort_order = extract_group_sort_order(group_id_raw)
                        tcg = TournamentClassGroup.get_by_description(cursor, tournament_class_id, group_id_raw)
                        if not tcg:
                            tcg = TournamentClassGroup(
                                tournament_class_id=tournament_class_id,
                                description=group_id_raw,
                                sort_order=sort_order
                            )
                            tcg.upsert(cursor)
                        tournament_class_group_id = tcg.tournament_class_group_id

                        # Assign member with seed_in_group
                        seed_in_group = int(seed_in_group_raw) if seed_in_group_raw and seed_in_group_raw.isdigit() else None
                        member = TournamentClassGroupMember(
                            tournament_class_group_id=tournament_class_group_id,
                            tournament_class_entry_id=entry.tournament_class_entry_id,
                            seed_in_group=seed_in_group
                        )
                        is_valid, error_message = member.validate()
                        if is_valid:
                            member.insert(cursor)
                        else:
                            logger.warning(logger_keys, f"Group member validation failed: {error_message}")

                except Exception as e:
                    logger.failed(logger_keys, f"Failed to create entry/group for group {group_id} (row_id={first_row.row_id}): {str(e)}")
                    continue

                for raw_row in group_rows:

                    logger_keys.update({
                        'fullname_raw': raw_row.fullname_raw,
                        'clubname_raw': raw_row.clubname_raw,
                        'player_id': None,
                        'club_id': None,
                        'match_type': None
                    })

                    logger.inc_processed()

                    if raw_row.fullname_raw is None or raw_row.clubname_raw is None:
                        logger.failed(logger_keys, f"Missing required raw data for player (row_id={raw_row.row_id})")
                        continue
                    fullname_raw = raw_row.fullname_raw
                    clubname_raw = raw_row.clubname_raw
                    tournament_player_id_ext = raw_row.tournament_player_id_ext

                    club, message = Club.resolve(cursor, clubname_raw, allow_prefix=True, fallback_to_unknown=True)
                    if message:
                        logger.warning(logger_keys.copy(), message)
                    if not club:
                        logger.warning(logger_keys, "Club not found. Using Unknown (club_id=9999)")
                        club = Club(club_id=9999)
                    club_id = club.club_id
                    logger_keys['club_id'] = club_id

                    player_id, match_type = match_player(
                        cursor,
                        fullname_raw,
                        clubname_raw,
                        class_date,
                        license_name_club_map,
                        player_name_map,
                        player_unverified_name_map,
                        unverified_appearance_map,
                        logger,
                        logger_keys.copy(),
                        club_id=club_id
                    )
                    if player_id is None:
                        logger.failed(logger_keys, "No match for player")
                        continue
                    logger_keys['player_id']    = player_id
                    logger_keys['match_type']   = match_type

                    player_data = {
                        "tournament_class_entry_id":    entry.tournament_class_entry_id,
                        "tournament_player_id_ext":     tournament_player_id_ext,
                        "player_id":                    player_id,
                        "club_id":                      club_id
                    }
                    player = TournamentClassPlayer.from_dict(player_data)
                    is_valid, error_message = player.validate()
                    if not is_valid:
                        logger.warning(logger_keys, f"Player validation failed: {error_message}")
                        continue
                    action = player.upsert(cursor)
                    if action:
                        pass
                    else:
                        logger.warning(logger_keys.copy(), "Player upsert failed (invalid or no change)")
                        continue

                    # Link unverified appearance if player is unverified
                    if match_type and match_type.startswith("fallback_unverified"):
                        status = Player.link_unverified_appearance(cursor, player_id, club_id, class_date)
                        if status == "created":
                            logger.info(logger_keys.copy(), "Created new unverified appearance", to_console=False)
                        else:
                            pass


                logger.success(logger_keys.copy(), "Class entry resolved successfully")

        except Exception as e:
            logger.failed(logger_keys.copy(), f"Exception during resolution: {str(e)}")

    logger.summarize()

def match_player(
    cursor,
    fullname_raw: str,
    clubname_raw: str,
    class_date: date,
    license_name_club_map,
    player_name_map,
    player_unverified_name_map,
    unverified_appearance_map,
    logger: OperationLogger,
    item_keys: Dict,
    club_id: Optional[int] = None
) -> Tuple[Optional[int], Optional[str]]:
    """
    Match player using a series of strategies.
    Returns (player_id, match_type) or (None, None).
    """
    strategies = [
        match_by_license_exact,
        match_by_license_substring,
        match_by_any_season_exact,
        match_by_any_season_substring,
        match_by_transition_exact,
        match_by_transition_substring,
        match_by_name_exact,
        match_by_unverified_with_club,
    ]

    for strategy in strategies:
        outcome = strategy(
            cursor,
            fullname_raw,
            clubname_raw,
            class_date,
            license_name_club_map,
            player_name_map,
            club_id,
            logger,
            item_keys.copy(),
            unverified_appearance_map if strategy == match_by_unverified_with_club else None,
        )
        if outcome:
            pid, match_type = outcome
            # unified logging
            log_keys = item_keys.copy()
            log_keys.update({
                "player_id": pid,
                "match_type": match_type,
                "fullname_raw": fullname_raw,
                "clubname_raw": clubname_raw,
                "club_id": club_id,
            })
            if debug:
                logger.info(log_keys, f"Matched player via {strategy.__name__}", to_console=False)
            return pid, match_type

    # fallback
    outcome = fallback_unverified(cursor, fullname_raw, clubname_raw, player_unverified_name_map, logger, item_keys.copy())
    if outcome:
        pid, match_type = outcome
        log_keys = item_keys.copy()
        log_keys.update({
            "player_id": pid,
            "match_type": match_type,
            "fullname_raw": fullname_raw,
            "clubname_raw": clubname_raw,
            "club_id": club_id,
        })
        logger.info(log_keys, "Matched player via fallback_unverified", to_console=False)
        return pid, match_type

    return None, None


def match_by_license_exact(cursor, fullname_raw: str, clubname_raw: str, class_date: date, license_name_club_map, player_name_map, club_id: Optional[int], logger: OperationLogger, item_keys: Dict, _):
    keys = name_keys_for_lookup_all_splits(fullname_raw)
    candidates = set()
    for k in keys:
        key = (k, club_id) if club_id else (k, None)
        if key in license_name_club_map:
            for lic in license_name_club_map[key]:
                if lic["valid_from"] <= class_date <= lic["valid_to"]:
                    candidates.add(lic["player_id"])
    if len(candidates) == 1:
        return list(candidates)[0], "license_exact"
    return None

def match_by_license_substring(cursor, fullname_raw: str, clubname_raw: str, class_date: date, license_name_club_map, player_name_map, club_id: Optional[int], logger: OperationLogger, item_keys: Dict, _):
    clean = normalize_key(fullname_raw)
    parts = clean.split()
    if len(parts) > 2:
        return None
    first_tok, last_tok = parts[0], parts[-1]
    candidates = set()
    for (full_key, cid), rows in license_name_club_map.items():
        if cid != club_id and club_id is not None:
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

def match_by_any_season_exact(cursor, fullname_raw: str, clubname_raw: str, class_date: date, license_name_club_map, player_name_map, club_id: Optional[int], logger: OperationLogger, item_keys: Dict, _):
    keys = name_keys_for_lookup_all_splits(fullname_raw)
    candidates = set()
    for k in keys:
        key = (k, club_id) if club_id else (k, None)
        if key in license_name_club_map:
            for lic in license_name_club_map[key]:
                candidates.add(lic["player_id"])
    if len(candidates) == 1:
        logger.warning(item_keys.copy(), "Matched by name with license in club, but not necessarily valid on class date")
        return list(candidates)[0], "any_season_exact"
    return None

def match_by_any_season_substring(cursor, fullname_raw: str, clubname_raw: str, class_date: date, license_name_club_map, player_name_map, club_id: Optional[int], logger: OperationLogger, item_keys: Dict, _):
    clean = normalize_key(fullname_raw)
    parts = clean.split()
    if len(parts) > 2:
        return None
    first_tok, last_tok = parts[0], parts[-1]
    candidates = set()
    for (full_key, cid), rows in license_name_club_map.items():
        if cid != club_id and club_id is not None:
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

def match_by_transition_exact(cursor, fullname_raw: str, clubname_raw: str, class_date: date, license_name_club_map, player_name_map, club_id: Optional[int], logger: OperationLogger, item_keys: Dict, _):
    pids = get_name_candidates(fullname_raw, player_name_map)
    if not pids:
        return None
    placeholders = ",".join("?" for _ in pids)
    try:
        cursor.execute(f"SELECT DISTINCT player_id FROM player_transition WHERE (club_id_to = ? OR club_id_from = ?) AND transition_date <= ? AND player_id IN ({placeholders})", [club_id, club_id, class_date] + pids if club_id else [None, None, class_date] + pids)
    except Exception as e:
        logger.failed(item_keys.copy(), f"Error executing SQL for transition_exact: {e}")
        return None
    trans = [r[0] for r in cursor.fetchall()]
    if len(trans) == 1:
        return trans[0], "transition_exact"
    return None

def match_by_transition_substring(cursor, fullname_raw: str, clubname_raw: str, class_date: date, license_name_club_map, player_name_map, club_id: Optional[int], logger: OperationLogger, item_keys: Dict, _):
    clean = normalize_key(fullname_raw)
    parts = clean.split()
    if len(parts) > 2:
        return None
    first_tok, last_tok = parts[0], parts[-1]
    candidates = set()
    for (full_key, cid), rows in license_name_club_map.items():
        if cid != club_id and club_id is not None:
            continue
        if len(full_key.split()) < 3:
            continue
        if first_tok in full_key and last_tok in full_key:
            for row in rows:
                candidates.add(row["player_id"])
    if not candidates:
        return None
    placeholders = ",".join("?" for _ in candidates)
    cursor.execute(f"SELECT DISTINCT player_id FROM player_transition WHERE (club_id_to = ? OR club_id_from = ?) AND transition_date <= ? AND player_id IN ({placeholders})", [club_id, club_id, class_date] + list(candidates) if club_id else [None, None, class_date] + list(candidates))
    trans = [r[0] for r in cursor.fetchall()]
    if len(trans) == 1:
        return trans[0], "transition_substring"
    return None

def match_by_unverified_with_club(cursor, fullname_raw: str, clubname_raw: str, class_date: date, license_name_club_map, player_name_map, club_id: Optional[int], logger: OperationLogger, item_keys: Dict, unverified_appearance_map):
    clean = normalize_key(fullname_raw)
    if clean in unverified_appearance_map:
        for entry in unverified_appearance_map[clean]:
            if entry["club_id"] == club_id or club_id is None:
                # logger.warning(item_keys.copy(), "Matched unverified player with prior apperance for same club")
                return entry["player_id"], "unverified_with_club"
    return None

def match_by_name_exact(cursor, fullname_raw: str, clubname_raw: str, class_date: date, license_name_club_map, player_name_map, club_id: Optional[int], logger: OperationLogger, item_keys: Dict, _):
    """Match player by exact name in player_name_map, ignoring club_id."""
    # print(f"Matching by exact name: {fullname_raw}")
    keys = name_keys_for_lookup_all_splits(fullname_raw)
    candidates = set()
    for k in keys:
        if k in player_name_map:
            candidates.update(player_name_map[k])
    # logger.info(item_keys.copy(), f"match_by_name_exact: found {len(candidates)} candidates: {candidates}")
    if len(candidates) == 1:
        pid = list(candidates)[0]
        # logger.info(item_keys.copy(), f"match_by_name_exact: matched player_id={pid}")
        return pid, "name_exact_verified"
    return None

def fallback_unverified(
    cursor,
    fullname_raw: str,
    clubname_raw: str,
    player_unverified_name_map: Dict[str, int],
    logger: OperationLogger,
    item_keys: Dict
):
    """
    Insert or reuse an unverified player when no verified match is found.
    
    Outcomes:
      • (player_id, "fallback_unverified_new") 
          → A brand-new unverified player was created in DB.
      • (player_id, "fallback_unverified_existing") 
          → Player already seen in this run (from in-memory map).
    
    Note:
      "reused" is not a possible outcome in this setup, since:
        - No other process touches the DB concurrently.
        - Every time we insert/reuse, we also update the in-memory map.
      Therefore, once a player exists in DB, they will always be picked up
      from the cache map in subsequent calls.
    """
    clean = normalize_key(fullname_raw)

    # 1. Check in-memory map first
    existing = player_unverified_name_map.get(clean)
    if existing is not None:
        return existing, "fallback_unverified_existing"

    # 2. Otherwise create new
    res = Player.insert_unverified(cursor, fullname_raw)
    if res["status"] == "created" and res["player_id"]:
        player_unverified_name_map[clean] = res["player_id"]
        logger.warning(item_keys.copy(), "Created new unverified player")
        return res["player_id"], "fallback_unverified_new"

    logger.failed(item_keys.copy(), "Failed to insert new unverified player")
    return None


def get_name_candidates(fullname_raw: str, player_name_map: Dict[str, List[int]]) -> List[int]:
    keys = name_keys_for_lookup_all_splits(fullname_raw)
    matches = set()
    for k in keys:
        matches.update(player_name_map.get(k, []))
    return list(matches)

def extract_group_sort_order(group_id_raw: str) -> Optional[int]:
    match = re.search(r'\d+', group_id_raw)
    return int(match.group()) if match else None