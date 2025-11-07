import logging
from typing import Dict, Tuple, List, Set

from utils import sanitize_name, OperationLogger
from models.player import Player

# ────────────────────────────────────────────────────────────────────────────
# Config
# ────────────────────────────────────────────────────────────────────────────

DUPLICATE_EXT_GROUPS: List[Set[int]] = [
    {70599, 72096},                     # Mark Simpson, 1990
    {12033, 39961},                     # Nicklas Forsling, 1987
    {12546, 63530},                     # Magnus Oskarsson, 1970
    {400241, 579767},                   # Maxim Stevens, 2003
    {15987, 58542},                     # Davis Bui, 1995
    {40187, 588796},                    # Terje Herting, 1978
    {253796, 336669, 354740, 379720}    # Peter Svenningsen, 2001
]

DEPENDENT_TABLES = [
    ("player_license",              "player_id"),
    ("player_transition",           "player_id"),
    ("tournament_class_player",     "player_id"),
    ("match_player",                "player_id"),
    ("player_ranking_group",        "player_id"),
    ("player_unverified_appearance","player_id"),
]

DATA_SOURCE_ID = 3

# ────────────────────────────────────────────────────────────────────────────
# Orchestrator
# ────────────────────────────────────────────────────────────────────────────

def upd_players_verified(cursor, run_id=None):
    """
    Orchestrates the verified-player update:
      1) Load raw player candidates.
      2) Merge manual duplicate groups (ID-centric).
      3) Insert remaining non-duplicates.
      4) Purge unverified orphans.
      5) Commit & summarize.
    """
    
    logger = OperationLogger(
        verbosity       = 2 , 
        print_output    = False, 
        log_to_db       = True,
        cursor          = cursor, 
        object_type     = "player", 
        run_type        = "update", 
        run_id          = run_id
    )

    metrics = dict()  # one object to gather all step metrics

    try:
        logger.info("Updating player table...")

        # 🧩 Ensure 'Unknown Player' exists
        _ensure_unknown_player(cursor, logger)

        # 1) load raw
        player_data = _load_raw_player_data(cursor)
        logger.info(f"Found {len(player_data):,} unique external players in license and ranking tables")

        # 2) merge manual groups
        m_groups = _merge_manual_groups(cursor, logger, player_data)

        # 3) insert remaining
        nondup_count = _insert_non_duplicates(cursor, logger, player_data)

        # 4) purge unverified orphans
        purged = _purge_unverified_orphans(cursor, logger)

        # 5) commit
        cursor.connection.commit()

        # summary
        metrics.update(m_groups)
        metrics["non_duplicate_processed"] = nondup_count
        metrics["purged_unverified_orphans"] = purged

        _print_player_summary(metrics)
        logger.summarize()
        logging.info("Done updating players")

    except Exception as e:
        logging.error(f"Error in upd_players_verified: {e}")
        print(f"❌ Error updating players: {e}")
        cursor.connection.rollback()


# ────────────────────────────────────────────────────────────────────────────
# Low-level helpers (small, testable)
# ────────────────────────────────────────────────────────────────────────────

def _pick_survivor(player_ids: Set[int]) -> int:
    """Deterministic survivor policy."""
    return min(player_ids)

def _repoint_children(cursor, loser_id: int, survivor_id: int) -> int:
    """Repoint all FK children from loser → survivor. Returns rows updated."""
    total = 0
    for table, col in DEPENDENT_TABLES:
        cursor.execute(f"UPDATE {table} SET {col} = ? WHERE {col} = ?", (survivor_id, loser_id))
        total += cursor.rowcount
    return total

def _print_player_summary(metrics: Dict[str, int]):
    print("\n📊 Operation Summary:")
    print(f"   👥 Manual groups total:          {metrics.get('groups_total', 0)}")
    print(f"      • With existing players:      {metrics.get('groups_with_existing', 0)}")
    print(f"      • Created new survivors:      {metrics.get('groups_created_survivor', 0)}")
    print(f"      • Groups merged successfully: {metrics.get('groups_merged', 0)}")
    print(f"      • Ext aliases added:          {metrics.get('ext_aliases_added', 0)}")
    print(f"      • Ext repointed:              {metrics.get('ext_repointed', 0)}")
    print(f"      • Losers total:               {metrics.get('losers_total', 0)}")
    print(f"      • Losers deleted:             {metrics.get('losers_deleted', 0)}")
    print(f"      • Losers kept (with refs):    {metrics.get('losers_kept_with_refs', 0)}")
    print()
    print(f"   🆕 Non-duplicate processed:      {metrics.get('non_duplicate_processed', 0)}")
    print(f"   🗑️  Purged unverified players:   {metrics.get('purged_players ', 0)}")
    print(f"      • Appearances deleted:        {metrics.get('purged_appearances', 0)}")
    print()

def _delete_player_if_orphan(cursor, player_id: int) -> bool:
    """
    Delete a player if:
      - no ext rows
      - no deps in the "real" dependent tables
    If only unverified appearances exist, delete them together with the player.
    Returns True if the player was deleted.
    """
    # Any ext rows? Then keep the player
    cursor.execute("SELECT COUNT(*) FROM player_id_ext WHERE player_id = ?", (player_id,))
    if cursor.fetchone()[0] > 0:
        return False

    # Check other dependent tables EXCEPT unverified appearances
    for table, col in [t for t in DEPENDENT_TABLES if t[0] != "player_unverified_appearance"]:
        cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {col} = ?", (player_id,))
        if cursor.fetchone()[0] > 0:
            return False

    # At this point, only appearances may exist → delete them as well
    cursor.execute("DELETE FROM player_unverified_appearance WHERE player_id = ?", (player_id,))
    cursor.execute("DELETE FROM player WHERE player_id = ?", (player_id,))
    return cursor.rowcount > 0


# ────────────────────────────────────────────────────────────────────────────
# Mid-level helpers (orchestrated steps)
# ────────────────────────────────────────────────────────────────────────────

def _load_raw_player_data(cursor) -> Dict[int, Tuple[str, str, int]]:
    """Build map ext_id → (firstname, lastname, year_born); license > ranking."""
    cursor.execute("""
        SELECT player_id_ext, firstname, lastname, year_born
        FROM player_license_raw
        WHERE player_id_ext IS NOT NULL
          AND TRIM(firstname) <> '' AND TRIM(lastname) <> '' AND year_born IS NOT NULL
    """)
    license_rows = cursor.fetchall()

    cursor.execute("""
        SELECT player_id_ext, firstname, lastname, year_born
        FROM player_ranking_raw
        WHERE player_id_ext IS NOT NULL
          AND TRIM(firstname) <> '' AND TRIM(lastname) <> '' AND year_born IS NOT NULL
    """)
    ranking_rows = cursor.fetchall()

    player_data: Dict[int, Tuple[str, str, int]] = {}
    for ext, fn, ln, yb in license_rows:
        ext = int(ext)
        if ext not in player_data:
            player_data[ext] = (sanitize_name(fn), sanitize_name(ln), int(yb))
    for ext, fn, ln, yb in ranking_rows:
        ext = int(ext)
        if ext not in player_data:
            player_data[ext] = (sanitize_name(fn), sanitize_name(ln), int(yb))
    return player_data

def _groups_from_manual() -> Dict[int, Set[int]]:
    """Return canonical_ext → full set of group ext_ids."""
    groups: Dict[int, Set[int]] = {}
    for grp in DUPLICATE_EXT_GROUPS:
        can = min(grp)
        groups.setdefault(can, set()).update(grp)
    return groups

def _merge_manual_groups(cursor, logger: OperationLogger, player_data: Dict[int, Tuple[str, str, int]]) -> Dict[str, int]:
    """Process manual groups with per-group savepoints. Returns metrics."""
    m = dict(
        groups_total=0, groups_skipped_no_data=0, groups_with_existing=0, groups_created_survivor=0,
        groups_merged=0, ext_aliases_added=0, ext_repointed=0, losers_total=0, losers_deleted=0, losers_kept_with_refs=0
    )

    groups = _groups_from_manual()
    m["groups_total"] = len(groups)
    logger.info(f"Processing {m['groups_total']} manual duplicate group(s)")

    for can_ext, all_exts in sorted(groups.items()):
        cursor.execute("SAVEPOINT merge_group")

        # Pick canonical tuple (prefer can_ext, else any ext in group)
        can_tuple = player_data.get(can_ext)
        if not can_tuple:
            for e in sorted(all_exts):
                if e in player_data:
                    can_tuple = player_data[e]
                    break
        if not can_tuple:
            m["groups_skipped_no_data"] += 1
            logger.failed({"canonical_ext": can_ext, "exts": sorted(all_exts)}, "No source data for group; skipping")
            cursor.execute("RELEASE merge_group")
            continue

        fn, ln, yb = can_tuple

        # Collect existing player_ids linked to this group
        linked_ids: Set[int] = set()
        for ext in all_exts:
            cursor.execute(
                "SELECT player_id FROM player_id_ext WHERE player_id_ext = ? AND data_source_id = ?",
                (str(ext), DATA_SOURCE_ID)
            )
            row = cursor.fetchone()
            if row:
                linked_ids.add(row[0])

        # Decide/insert survivor
        if linked_ids:
            survivor_id = _pick_survivor(linked_ids)
            m["groups_with_existing"] += 1
            if len(linked_ids) > 1:
                logger.warning(
                    {"canonical_ext": can_ext, "linked_player_ids": sorted(linked_ids), "survivor": survivor_id},
                    "Manual group spans multiple player_ids; merging into survivor"
                )
        else:
            p = Player(firstname=fn, lastname=ln, year_born=yb, is_verified=True)
            res = p.save_to_db(cursor, player_id_ext=str(can_ext), data_source_id=DATA_SOURCE_ID)
            if res["status"] == "success":
                survivor_id = res["player_id"]
                logger.success(res["player"], res["reason"])
                m["groups_created_survivor"] += 1
            elif res["status"] == "skipped":
                survivor_id = res.get("player_id")
                if not survivor_id:
                    m["groups_skipped_no_data"] += 1
                    logger.failed({"player": f"{fn} {ln}"}, "Insert skipped without player_id; skipping group")
                    cursor.execute("RELEASE merge_group")
                    continue
            else:
                m["groups_skipped_no_data"] += 1
                logger.failed(res["player"], res["reason"])
                cursor.execute("ROLLBACK TO merge_group")
                cursor.execute("RELEASE merge_group")
                continue

        # Repoint/add ext_ids
        loser_ids: Set[int] = set()
        for ext in all_exts:
            cursor.execute(
                "SELECT player_id FROM player_id_ext WHERE player_id_ext = ? AND data_source_id = ?",
                (str(ext), DATA_SOURCE_ID)
            )
            row = cursor.fetchone()
            if row:
                old_id = row[0]
                if old_id != survivor_id:
                    loser_ids.add(old_id)
                    cursor.execute("""
                        UPDATE player_id_ext SET player_id = ?
                        WHERE player_id_ext = ? AND data_source_id = ?
                    """, (survivor_id, str(ext), DATA_SOURCE_ID))
                    m["ext_repointed"] += 1
            else:
                cursor.execute("""
                    INSERT INTO player_id_ext (player_id, player_id_ext, data_source_id)
                    VALUES (?, ?, ?)
                """, (survivor_id, str(ext), DATA_SOURCE_ID))
                m["ext_aliases_added"] += 1
                logger.success({"ext": ext, "survivor": survivor_id}, "Added player_id_ext alias")

        # Repoint children and try deleting losers
        if loser_ids:
            m["losers_total"] += len(loser_ids)
            for loser in sorted(loser_ids):
                _repoint_children(cursor, loser, survivor_id)
                if _delete_player_if_orphan(cursor, loser):
                    m["losers_deleted"] += 1
                    logger.info({"loser": loser, "survivor": survivor_id}, "Deleted merged loser")
                else:
                    m["losers_kept_with_refs"] += 1
                    logger.warning({"loser": loser}, "Loser still has references; not deleting")

        m["groups_merged"] += 1
        cursor.execute("RELEASE merge_group")

    return m

def _insert_non_duplicates(cursor, logger: OperationLogger, player_data: Dict[int, Tuple[str, str, int]]) -> int:
    """Insert players for ext_ids not in any manual group. Returns count attempted."""
    processed_exts = set().union(*DUPLICATE_EXT_GROUPS) if DUPLICATE_EXT_GROUPS else set()
    remaining = [ext for ext in player_data if ext not in processed_exts]
    logger.info(f"Processing {len(remaining):,} non-duplicate externals")


    for ext in sorted(remaining):
        fn, ln, yb = player_data[ext]
        p = Player(firstname=fn, lastname=ln, year_born=yb, is_verified=True)
        res = p.save_to_db(cursor, player_id_ext=str(ext), data_source_id=DATA_SOURCE_ID)
        keys = {"player_id_ext": ext, "firstname": fn, "lastname": ln, "year_born": yb, "source": "non_duplicate"}
        if res["status"] == "success":
            logger.success(keys, res["reason"])
        elif res["status"] == "failed":
            logger.failed(keys, res["reason"])
        else:
            logger.skipped(keys, res["reason"])
    return len(remaining)

def _purge_unverified_orphans(cursor, logger: OperationLogger) -> int:
    """
    Delete all unverified players that have no ext rows and no real deps.
    Also deletes any unverified_appearance rows tied to them.
    Returns the number of players deleted.
    """
    cursor.execute("SELECT player_id FROM player WHERE is_verified = 0")
    candidate_ids = [row[0] for row in cursor.fetchall()]

    purged_players = 0
    purged_appearances = 0

    for pid in candidate_ids:
        # Count appearances before attempting delete
        cursor.execute("SELECT COUNT(*) FROM player_unverified_appearance WHERE player_id = ?", (pid,))
        app_count = cursor.fetchone()[0]

        if _delete_player_if_orphan(cursor, pid):
            purged_players += 1
            purged_appearances += app_count

    if purged_players:
        logger.info(
            {"purged_players": purged_players, "purged_appearances": purged_appearances},
            "Purged unverified orphan players and their appearances"
        )
    return purged_players

def _ensure_unknown_player(cursor, logger: OperationLogger) -> int:
    """
    Ensure a placeholder player exists with player_id=99999 and name='Unknown Player'.
    Returns the player_id (creates if missing).
    """
    player_id = 99999
    cursor.execute("SELECT 1 FROM player WHERE player_id = ?", (player_id,))
    if cursor.fetchone():
        logger.info({"player_id": player_id}, "Unknown Player already exists")
        return player_id

    # Create it manually, bypassing Player.save_to_db to avoid duplicate logic
    cursor.execute("""
        INSERT INTO player (player_id, firstname, lastname, year_born, is_verified)
        VALUES (?, ?, ?, NULL, 1)
    """, (player_id, "Unknown", "Player"))
    logger.success({"player_id": player_id}, "Inserted Unknown Player placeholder")
    return player_id
