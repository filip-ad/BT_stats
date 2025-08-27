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
    # add more as you discover them...
]

# Build a flat map: any ext → its chosen “canonical” ext (smallest wins)
CANONICAL_EXT: Dict[int, int] = {}
for grp in DUPLICATE_EXT_GROUPS:
    keep = min(grp)
    for alias_ext in grp:
        CANONICAL_EXT[alias_ext] = keep

def upd_players_verified():
    conn, cursor = get_conn()
    logging.info("Updating player table...")
    print("ℹ️  Updating player table...")

    logger = OperationLogger(
        verbosity=2,
        print_output=False, 
        log_to_db=False)

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

        logging.info(f"Found {len(player_data):,} unique external players in license and ranking tables")
        print(f"ℹ️  Found {len(player_data):,} unique external players in license and ranking tables")

        # ── Process manual duplicate groups first ────────────────────────
        groups: Dict[int, Set[int]] = {}
        for grp in DUPLICATE_EXT_GROUPS:
            can = min(grp)
            groups.setdefault(can, set()).update(grp)

        print(f"ℹ️  Processing {len(groups)} manual duplicate group(s)…")
        logging.info("Processing %d manual duplicate groups", len(groups))

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
                # logging.warning(f"No source data for group with canonical ext {can_ext}; skipping {sorted(all_exts)}")
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
                        # logging.warning(f"Conflict: ext {ext_id} points to different player_id {row[0]} than {existing_player_id}")
                        logger.warning(player_name, conflict_msg)
                    # Continue to collect, but use first found

            if existing_player_id:
                # Use existing player_id, add missing ext_ids
                player = Player.get_by_id(cursor, existing_player_id)
                if not player:
                    error_msg = f"Existing player_id not found"
                    # logging.error(f"Existing player_id {existing_player_id} not found")
                    logger.failed(player_name, error_msg)
                    continue
                logger.skipped(player_name, "Existing player for group")
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

            # Add other ext_ids if not already present
            for ext_id in all_exts:
                if ext_id == can_ext and existing_player_id:
                    continue  # Already added if new, or existing
                cursor.execute(
                    "SELECT 1 FROM player_id_ext WHERE player_id_ext = ? AND data_source_id = ?",
                    (str(ext_id), data_source_id)
                )
                if not cursor.fetchone():
                    cursor.execute("""
                        INSERT INTO player_id_ext (player_id, player_id_ext, data_source_id)
                        VALUES (?, ?, ?)
                    """, (existing_player_id, str(ext_id), data_source_id))
                    logger.success(player_name, f"Added additional player_id_ext for existing player")
                else:
                    logger.skipped(player_name, f"Ext_id already exists")

        # ── Handle remaining non-duplicate externals ─────────────────────
        processed_exts = set()
        for grp in DUPLICATE_EXT_GROUPS:
            processed_exts.update(grp)
        remaining = [k for k in player_data if k not in processed_exts]
        print(f"ℹ️  Processing {len(remaining):,} non-duplicate external players…")
        logging.info("Processing %d non-duplicate externals", len(remaining))

        for ext_id in sorted(remaining):
            fn, ln, yb = player_data[ext_id]
            p = Player(firstname=fn, lastname=ln, year_born=yb, is_verified=True)
            res = p.save_to_db(cursor, player_id_ext=str(ext_id), data_source_id=data_source_id)
            if res["status"] == "success":
                logger.success(res["player"], res["reason"])
            elif res["status"] == "failed":
                logger.failed(res["player"], res["reason"])
            elif res["status"] == "skipped":
                logger.skipped(res["player"], res["reason"])

        # ── Commit & report ──────────────────────────────────────────────
        conn.commit()
        logger.summarize()
        logging.info("Done updating players")

    except Exception as e:
        logging.error(f"Error in upd_players: {e}")
        print(f"❌ Error updating players: {e}")
        conn.rollback()

    finally:
        conn.close()

if __name__ == "__main__":
    upd_players_verified()