# # src/upd_players.py

# src/upd_players.py

import logging
from typing import Dict, Tuple, List, Set

from db import get_conn
from utils import sanitize_name, print_db_insert_results
from models.player import Player

# ── 1) MANUAL DUPLICATES GROUPS ────────────────────────────────────────────────
# Any ext-IDs in the same set are the same real person.
DUPLICATE_EXT_GROUPS: List[Set[int]] = [
    
    {70599, 72096},     # Mark Simpson, born 1990
    {12033, 39961},     # Nicklas Forsling, born 1987
    {12546, 63530},     # Magnus Oskarsson, born 1970
    {400241, 579767}    # Maxim Stevens, born 2003

    # add more as you discover them...
]

# Build a flat map: any ext → its chosen “canonical” ext
CANONICAL_EXT: Dict[int, int] = {}
for grp in DUPLICATE_EXT_GROUPS:
    keep = min(grp)                          # pick the smallest as canonical
    for alias_ext in grp:
        CANONICAL_EXT[alias_ext] = keep

def upd_players():
    conn, cursor = get_conn()
    logging.info("Updating player table...")
    print("ℹ️  Updating player table...")

    try:
        # ── 2) Load raw license rows ────────────────────────────────────────
        cursor.execute("""
            SELECT player_id_ext, firstname, lastname, year_born
            FROM player_license_raw
            WHERE player_id_ext IS NOT NULL
              AND TRIM(firstname) <> ''
              AND TRIM(lastname)  <> ''
              AND year_born IS NOT NULL
        """)
        license_rows = cursor.fetchall()

        # ── 3) Load raw ranking rows ────────────────────────────────────────
        cursor.execute("""
            SELECT player_id_ext, firstname, lastname, year_born
            FROM player_ranking_raw
            WHERE player_id_ext IS NOT NULL
              AND TRIM(firstname) <> ''
              AND TRIM(lastname)  <> ''
              AND year_born IS NOT NULL
        """)
        ranking_rows = cursor.fetchall()

        # ── 4) Merge into a single map, giving license priority ─────────────
        # ext_id → (firstname, lastname, year_born)
        player_data: Dict[int, Tuple[str,str,int]] = {}

        # 4a) License first
        for ext, fn, ln, yb in license_rows:
            if ext not in player_data:
                player_data[ext] = (
                    sanitize_name(fn),
                    sanitize_name(ln),
                    int(yb)
                )

        # 4b) Then ranking only if missing
        for ext, fn, ln, yb in ranking_rows:
            if ext not in player_data:
                player_data[ext] = (
                    sanitize_name(fn),
                    sanitize_name(ln),
                    int(yb)
                )

        logging.info(f"Found {len(player_data):,} unique external players in license and ranking tables")
        print(f"ℹ️  Found {len(player_data):,} unique external players in license and ranking tables")

        # ── 5) Insert/upsert each canonical player & aliases ────────────────
        results = []
        for ext_id, (fn, ln, yb) in sorted(player_data.items()):
            canonical_ext = CANONICAL_EXT.get(ext_id, ext_id)

            # 5a) Upsert the canonical player row
            if ext_id == canonical_ext:
                p = Player(firstname=fn, lastname=ln, year_born=yb)
                res = p.save_to_db(cursor, ext_id)
                results.append(res)


            # 5b) If this ext was an alias, register it
            else:
                canon = Player.get_by_id_ext(cursor, canonical_ext)
                if not canon:
                    logging.error(f"Could not load canonical player for ext {canonical_ext}")
                    results.append({
                        "status":           "failed",
                        "player_id_ext":    ext_id,
                        "reason":           "Canonical player not found"
                    })
                    continue

                aliased = canon.add_alias(
                    cursor,
                    player_id_ext=ext_id,
                    firstname=fn,
                    lastname=ln,
                    year_born=yb
                )

                results.append(aliased)

        # ── 6) Commit & report ──────────────────────────────────────────────
        conn.commit()
        print_db_insert_results(results)
        logging.info("Done updating players")

    except Exception as e:
        logging.error(f"Error in upd_players: {e}")
        print(f"❌ Error updating players: {e}")
        conn.rollback()

    finally:
        conn.close()
