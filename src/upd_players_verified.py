import logging
from typing import Dict, Tuple, List, Set

from db import get_conn
from utils import sanitize_name, OperationLogger
from models.player import Player

# ── MANUAL DUPLICATE GROUPS ────────────────────────────────────────────────
# Any ext-IDs in the same set are the same real person.
DUPLICATE_EXT_GROUPS: List[Set[int]] = [
    {70599, 72096},     # Mark Simpson, born 1990
    {12033, 39961},     # Nicklas Forsling, born 1987
    {12546, 63530},     # Magnus Oskarsson, born 1970
    {400241, 579767},   # Maxim Stevens, born 2003
    {15987, 58542},     # Davis Bui (b. 1995) 
    {40187, 588796},     # Terje Herting (b. 1978) 
    {253796, 336669, 354740, 379720}   # Peter Svenningsen (b. 2001)

    # add more as you discover them...
]

# Build a flat map: any ext → its chosen “canonical” ext (smallest wins)
CANONICAL_EXT: Dict[int, int] = {}
for grp in DUPLICATE_EXT_GROUPS:
    keep = min(grp)
    for alias_ext in grp:
        CANONICAL_EXT[alias_ext] = keep

def upd_players_verified(cursor, run_id = None):

    logger = OperationLogger(
        verbosity           = 2,
        print_output        = False,
        log_to_db           = True,
        cursor              = cursor,
        object_type         = "player",
        run_type            = "update",
        run_id              = run_id
    )

    logger.info("Updating player table...")

    try:
        # ── Load raw license rows ────────────────────────────────────────
        cursor.execute("""
            SELECT 
                player_id_ext, 
                firstname, 
                lastname, 
                year_born
            FROM player_license_raw
            WHERE player_id_ext IS NOT NULL
              AND TRIM(firstname) <> ''
              AND TRIM(lastname)  <> ''
              AND year_born IS NOT NULL
        """)
        license_rows = cursor.fetchall()

        # ── Load raw ranking rows ────────────────────────────────────────
        cursor.execute("""
            SELECT 
                player_id_ext, 
                firstname, 
                lastname, 
                year_born
            FROM player_ranking_raw
            WHERE player_id_ext IS NOT NULL
              AND TRIM(firstname) <> ''
              AND TRIM(lastname)  <> ''
              AND year_born IS NOT NULL
        """)
        ranking_rows = cursor.fetchall()

        # ── Merge into a single map, giving license priority ─────────────
        player_data: Dict[int, Tuple[str, str, int]] = {}

        # License first
        for ext, fn, ln, yb in license_rows:
            ext_int = int(ext)
            if ext_int not in player_data:
                player_data[ext_int] = (
                    sanitize_name(fn),
                    sanitize_name(ln),
                    int(yb)
                )

        # Then ranking only if missing
        for ext, fn, ln, yb in ranking_rows:
            ext_int = int(ext)
            if ext_int not in player_data:
                player_data[ext_int] = (
                    sanitize_name(fn),
                    sanitize_name(ln),
                    int(yb)
                )

        logger.info(f"Found {len(player_data):,} unique external players in license and ranking tables")

        # ── Process manual duplicate groups first ────────────────────────
        groups: Dict[int, Set[int]] = {}
        for grp in DUPLICATE_EXT_GROUPS:
            can = min(grp)
            groups.setdefault(can, set()).update(grp)

        print(f"ℹ️  Processing {len(groups)} manual duplicate group(s)…")
        logger.info(f"Processing {len(groups)} non-duplicate externals")

        data_source_id = 3

        for i, (can_ext, all_exts) in enumerate(sorted(groups.items()), start=1):
            # Find data for canonical or first available
            can_tuple = player_data.get(can_ext)
            if not can_tuple:
                for e in sorted(all_exts):
                    if e in player_data:
                        can_tuple = player_data[e]
                        break

            if not can_tuple:
                msg = f"No source data for group with canonical ext; skipping"
                logger.failed(f"Group {can_ext}", msg)
                continue

            can_fn, can_ln, can_yb = can_tuple
            player_name = f"{can_fn} {can_ln}"

            # Check if any ext in group already exists
            existing_player_id = None
            for ext_id in all_exts:
                cursor.execute(
                    "SELECT player_id FROM player_id_ext WHERE player_id_ext = ? AND data_source_id = ?",
                    (str(ext_id), data_source_id)
                )
                row = cursor.fetchone()
                if row:
                    if existing_player_id is None:
                        existing_player_id = row[0]
                    elif existing_player_id != row[0]:
                        conflict_msg = f"Conflict: ext points to different player_id than"
                        logger.warning(player_name, conflict_msg)
                    # Continue to collect, but use first found

            if existing_player_id:
                # Use existing player_id, add missing ext_ids
                player = Player.get_by_id(cursor, existing_player_id)
                if not player:
                    error_msg = f"Existing player_id not found"
                    logger.failed(player_name, error_msg)
                    continue
                logger_keys = {
                    "canonical_ext":   can_ext,
                    "all_exts":        list(all_exts),
                    "player_id":       existing_player_id,
                    "firstname":       can_fn,
                    "lastname":        can_ln,
                    "year_born":       can_yb,
                    "source":          "manual_duplicate_group"
                }
                logger.warning(logger_keys, "Manual duplicate group → already merged to existing player")
            else:
                # Insert new player with canonical ext
                p = Player(firstname=can_fn, lastname=can_ln, year_born=can_yb, is_verified=True)
                res = p.save_to_db(cursor, player_id_ext=str(can_ext), data_source_id=data_source_id)
                if res["status"] == "success":
                    logger.success(res["player"], res["reason"])
                    existing_player_id = res["player_id"]
                elif res["status"] == "failed":
                    logger.failed(res["player"], res["reason"])
                    continue
                elif res["status"] == "skipped":
                    logger.skipped(res["player"], res["reason"])
                    existing_player_id = res.get("player_id")
                    if not existing_player_id:
                        continue

            # # Add other ext_ids if not already present
            # for ext_id in all_exts:
            #     if ext_id == can_ext and existing_player_id:
            #         continue  # Already added if new, or existing
            #     cursor.execute(
            #         "SELECT 1 FROM player_id_ext WHERE player_id_ext = ? AND data_source_id = ?",
            #         (str(ext_id), data_source_id)
            #     )
            #     if not cursor.fetchone():
            #         cursor.execute("""
            #             INSERT INTO player_id_ext (player_id, player_id_ext, data_source_id)
            #             VALUES (?, ?, ?)
            #         """, (existing_player_id, str(ext_id), data_source_id))
            #         logger.success(player_name, f"Added additional player_id_ext for existing player")
            #     else:
            #         logger_keys = {
            #             "player_id":       existing_player_id,
            #             "ext_id":          ext_id,
            #             "canonical_ext":   can_ext,
            #             "firstname":       can_fn,
            #             "lastname":        can_ln,
            #             "year_born":       can_yb,
            #             "source":          "alias_check"
            #         }
            #         logger.warning(logger_keys, "Ext_id already linked to player (no new alias added)")

            # Add or repoint other ext_ids if not already present
            for ext_id in all_exts:
                if ext_id == can_ext and existing_player_id:
                    continue  # canonical ext already handled

                cursor.execute(
                    "SELECT player_id FROM player_id_ext WHERE player_id_ext = ? AND data_source_id = ?",
                    (str(ext_id), data_source_id)
                )
                row = cursor.fetchone()
                if row:
                    old_player_id = row[0]
                    if old_player_id != existing_player_id:
                        # Repoint this ext_id to canonical player_id
                        cursor.execute("""
                            UPDATE player_id_ext
                            SET player_id = ?
                            WHERE player_id_ext = ? AND data_source_id = ?
                        """, (existing_player_id, str(ext_id), data_source_id))

                        # Repoint dependent tables from old_player_id → existing_player_id
                        for table, col in [
                            ("player_license", "player_id"),
                            ("player_transition", "player_id"),
                            ("tournament_class_player", "player_id"), 
                            ("match_player", "player_id"),
                            ("player_ranking_group", "player_id"),
                            ("participant_player", "player_id"),
                            ("player_unverified_appearance", "player_id")
                        ]:
                            cursor.execute(f"""
                                UPDATE {table}
                                SET {col} = ?
                                WHERE {col} = ?
                            """, (existing_player_id, old_player_id))

                        # Delete unverified appearances tied to the old player
                        cursor.execute("""
                            DELETE FROM player_unverified_appearance
                            WHERE player_id = ?
                        """, (old_player_id,))

                        logger.warning({
                            "player_id": existing_player_id,
                            "ext_id": ext_id,
                            "canonical_ext": can_ext,
                            "firstname": can_fn,
                            "lastname": can_ln,
                            "year_born": can_yb,
                            "source": "repoint"
                        }, f"Repointed ext_id {ext_id} from old player_id {old_player_id} → {existing_player_id}")
                    else:
                        logger.info({
                            "player_id": existing_player_id,
                            "ext_id": ext_id,
                            "canonical_ext": can_ext,
                            "firstname": can_fn,
                            "lastname": can_ln,
                            "year_born": can_yb,
                            "source": "alias_check"
                        }, "Ext_id already linked correctly")
                else:
                    # Normal insert
                    cursor.execute("""
                        INSERT INTO player_id_ext (player_id, player_id_ext, data_source_id)
                        VALUES (?, ?, ?)
                    """, (existing_player_id, str(ext_id), data_source_id))
                    logger.success(player_name, f"Added additional player_id_ext for existing player")

            # --- Cleanup non-canonical players for this group ---
            for ext_id in all_exts:
                cursor.execute(
                    "SELECT player_id FROM player_id_ext WHERE player_id_ext = ? AND data_source_id = ?",
                    (str(ext_id), data_source_id)
                )
                row = cursor.fetchone()
                if row:
                    pid = row[0]
                    if pid != existing_player_id:
                        cursor.execute("SELECT COUNT(*) FROM player_id_ext WHERE player_id = ?", (pid,))
                        if cursor.fetchone()[0] == 0:
                            cursor.execute("DELETE FROM player WHERE player_id = ?", (pid,))
                            logger.info(
                                {"old_player_id": pid, "canonical_player_id": existing_player_id},
                                f"Deleted orphan player {pid} (merged into {existing_player_id})"
                            )
            
                
                # else:
                #     # Normal insert if this ext_id wasn’t seen before
                #     cursor.execute("""
                #         INSERT INTO player_id_ext (player_id, player_id_ext, data_source_id)
                #         VALUES (?, ?, ?)
                #     """, (existing_player_id, str(ext_id), data_source_id))
                #     logger.success(logger_keys, f"Added new alias ext_id {ext_id} for player {existing_player_id}")


        # ── Handle remaining non-duplicate externals ─────────────────────
        processed_exts = set()
        for grp in DUPLICATE_EXT_GROUPS:
            processed_exts.update(grp)
        remaining = [k for k in player_data if k not in processed_exts]
        logger.info("Processing %d non-duplicate externals", len(remaining))

        logger.inc_processed(len(remaining))

        for ext_id in sorted(remaining):
            fn, ln, yb = player_data[ext_id]
            p = Player(firstname=fn, lastname=ln, year_born=yb, is_verified=True)
            res = p.save_to_db(cursor, player_id_ext=str(ext_id), data_source_id=data_source_id)
            
            logger_keys = {
                "player_id_ext":   ext_id,
                "firstname":       fn,
                "lastname":        ln,
                "year_born":       yb,
                "source":          "non_duplicate"
            }
                             
            if res["status"] == "success":
                logger.success(logger_keys, res["reason"])
            elif res["status"] == "failed":
                logger.failed(logger_keys, res["reason"])
            elif res["status"] == "skipped":
                logger.skipped(logger_keys, res["reason"])

        # ── Commit & report ──────────────────────────────────────────────
        cursor.connection.commit()
        logger.summarize()
        logging.info("Done updating players")

    except Exception as e:
        logging.error(f"Error in upd_players: {e}")
        print(f"❌ Error updating players: {e}")
        cursor.connection.rollback()