# # src/upd_players.py

# import logging
# from typing import Dict, Tuple, List, Set

# from db import get_conn
# from utils import sanitize_name, print_db_insert_results
# from models.player import Player

# # ── 1) MANUAL DUPLICATES GROUPS ────────────────────────────────────────────────

# # Any ext-IDs in the same set are the same real person.
# DUPLICATE_EXT_GROUPS: List[Set[int]] = [
    
#     {70599, 72096},     # Mark Simpson, born 1990
#     {12033, 39961},     # Nicklas Forsling, born 1987
#     {12546, 63530},     # Magnus Oskarsson, born 1970
#     {400241, 579767}    # Maxim Stevens, born 2003

#     # add more as you discover them...
# ]

# # Build a flat map: any ext → its chosen “canonical” ext
# CANONICAL_EXT: Dict[int, int] = {}
# for grp in DUPLICATE_EXT_GROUPS:
#     keep = min(grp)                          # pick the smallest as canonical
#     for alias_ext in grp:
#         CANONICAL_EXT[alias_ext] = keep

# def upd_players_verified():
#     conn, cursor = get_conn()
#     logging.info("Updating player table...")
#     print("ℹ️  Updating player table...")

#     try:
#         # ── 2) Load raw license rows ────────────────────────────────────────
#         cursor.execute("""
#             SELECT 
#                 player_id_ext, 
#                 firstname, 
#                 lastname, 
#                 year_born
#             FROM player_license_raw
#             WHERE player_id_ext IS NOT NULL
#               AND TRIM(firstname) <> ''
#               AND TRIM(lastname)  <> ''
#               AND year_born IS NOT NULL
#         """)
#         license_rows = cursor.fetchall()

#         # ── 3) Load raw ranking rows ────────────────────────────────────────
#         cursor.execute("""
#             SELECT 
#                 player_id_ext, 
#                 firstname, 
#                 lastname, 
#                 year_born
#             FROM player_ranking_raw
#             WHERE player_id_ext IS NOT NULL
#               AND TRIM(firstname) <> ''
#               AND TRIM(lastname)  <> ''
#               AND year_born IS NOT NULL
#         """)
#         ranking_rows = cursor.fetchall()

#         # ── 4) Merge into a single map, giving license priority ─────────────

#         # ext_id → (firstname, lastname, year_born)
#         player_data: Dict[int, Tuple[str,str,int]] = {}

#         # 4a) License first
#         for ext, fn, ln, yb in license_rows:
#             if ext not in player_data:
#                 player_data[ext] = (
#                     sanitize_name(fn),
#                     sanitize_name(ln),
#                     int(yb),
#                     'license' # source_system
#                 )

#         # 4b) Then ranking only if missing
#         for ext, fn, ln, yb in ranking_rows:
#             if ext not in player_data:
#                 player_data[ext] = (
#                     sanitize_name(fn),
#                     sanitize_name(ln),
#                     int(yb),
#                     'ranking' # source_system
#                 )

#         logging.info(f"Found {len(player_data):,} unique external players in license and ranking tables")
#         print(f"ℹ️  Found {len(player_data):,} unique external players in license and ranking tables")

#         # ── 5) Insert/upsert each canonical player & aliases ────────────────
#         results = []
#         for ext_id, (fn, ln, yb, source) in sorted(player_data.items()):
#             canonical_ext = CANONICAL_EXT.get(ext_id, ext_id)

#             # 5a) Upsert the canonical player row
#             if ext_id == canonical_ext:
#                 p = Player(firstname=fn, lastname=ln, year_born=yb, is_verified=True)
#                 res = p.save_to_db(cursor, ext_id, source_system=source)
#                 results.append(res)

#             # 5b) If this ext was an alias, register it
#             else:
#                 canon = Player.get_by_id_ext(cursor, canonical_ext)
#                 if not canon:
#                     logging.error(f"Could not load canonical player for ext {canonical_ext}")
#                     results.append({
#                         "status":           "failed",
#                         "player_id_ext":    ext_id,
#                         "reason":           "Canonical player not found"
#                     })
#                     continue

#                 aliased = canon.add_alias(
#                     cursor,
#                     player_id_ext=ext_id,
#                     firstname=fn,
#                     lastname=ln,
#                     year_born=yb,
#                     fullname_raw=None,  
#                     source_system=source
#                 )

#                 results.append(aliased)

#         # ── 6) Commit & report ──────────────────────────────────────────────
#         conn.commit()
#         print_db_insert_results(results)
#         logging.info("Done updating players")

#     except Exception as e:
#         logging.error(f"Error in upd_players: {e}")
#         print(f"❌ Error updating players: {e}")
#         conn.rollback()

#     finally:
#         conn.close()

# src/upd_players.py

# src/upd_players.py
# src/upd_players.py

import logging
from typing import Dict, Tuple, List, Set, Optional

from db import get_conn
from utils import sanitize_name, print_db_insert_results
from models.player import Player

# ──────────────────────────────────────────────────────────────────────────────
# SAFETY / BEHAVIOR SWITCHES
# ──────────────────────────────────────────────────────────────────────────────
DRY_RUN: bool = False                  # ← flip False to actually write to DB
AUTO_MERGE_DUP_PLAYERS: bool = True  # ← True to merge duplicate player rows

# ──────────────────────────────────────────────────────────────────────────────
# 1) MANUAL DUPLICATE GROUPS
#    Any ext-IDs in the same set are the same real person.
# ──────────────────────────────────────────────────────────────────────────────
DUPLICATE_EXT_GROUPS: List[Set[int]] = [
    {70599, 72096},     # Mark Simpson, born 1990
    {12033, 39961},     # Nicklas Forsling, born 1987
    {12546, 63530},     # Magnus Oskarsson, born 1970
    {400241, 579767},   # Maxim Stevens, born 2003
    {15987, 58542},     # Davis Bui (b. 1995) 
    {40187, 588796},     # Terje Herting (b. 1978) 
    # add more as you discover them...
]

# Build a flat map: any ext → its chosen “canonical” ext (smallest wins by default)
CANONICAL_EXT: Dict[int, int] = {}
for grp in DUPLICATE_EXT_GROUPS:
    keep = min(grp)
    for alias_ext in grp:
        CANONICAL_EXT[alias_ext] = keep


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _run(cursor, sql: str, params: tuple, *, dry: bool) -> int:
    """Execute SQL unless DRY; return rowcount (0 on dry-run)."""
    if dry:
        safe_sql = " ".join(sql.split())
        logging.info("[DRY RUN] %s | %s", safe_sql, params)
        return 0
    cursor.execute(sql, params)
    return cursor.rowcount

def _fetchone(cursor, sql: str, params: tuple) -> Optional[tuple]:
    cursor.execute(sql, params)
    return cursor.fetchone()

def _fetchall(cursor, sql: str, params: tuple = ()) -> List[tuple]:
    cursor.execute(sql, params)
    return cursor.fetchall()

def _table_exists(cursor, name: str) -> bool:
    row = _fetchone(cursor,
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                    (name,))
    return bool(row)

def _col_exists(cursor, table: str, col: str) -> bool:
    try:
        cursor.execute(f"PRAGMA table_info({table})")
        return any(r[1] == col for r in cursor.fetchall())
    except Exception:
        return False

def _seasoned_name(fn: str, ln: str, yb: Optional[int]) -> str:
    base = f"{fn} {ln}".strip()
    return f"{base} (b. {yb})" if yb is not None else base

def _as_text_ext(ext_id) -> str:
    # Always store alias externals as TEXT to be consistent
    return str(ext_id) if ext_id is not None else None


# ──────────────────────────────────────────────────────────────────────────────
# MERGE: move all references from drop_id → keep_id, then delete drop_id
# Logs the exact plan; honors DRY_RUN.
# ──────────────────────────────────────────────────────────────────────────────
def merge_players(cursor, keep_id: int, drop_id: int, *, dry_run: bool = True) -> None:
    """
    Move references from drop_id -> keep_id across known tables, then delete drop_id.
    Handles UNIQUE collisions by resolving them before UPDATEs.
    """
    assert keep_id != drop_id, "keep_id and drop_id must differ"

    logging.info("── Merge plan: player_id %s  ←  %s%s",
                 keep_id, drop_id, " (DRY RUN)" if dry_run else "")

    def run(sql, params):
        if dry_run:
            logging.info("[DRY RUN] %s | %s", " ".join(sql.split()), params)
            return 0
        cursor.execute(sql, params)
        return cursor.rowcount

    def table_exists(name: str) -> bool:
        cursor.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,))
        return cursor.fetchone() is not None

    # 0) Move aliases (safe)
    run("UPDATE player_alias SET player_id = ? WHERE player_id = ?", (keep_id, drop_id))

    # 1) player_participant UNIQUE(tournament_class_id, player_id)
    if table_exists("player_participant"):
        # delete would-be duplicates
        run("""
            DELETE FROM player_participant
             WHERE player_id = :drop
               AND EXISTS (
                 SELECT 1 FROM player_participant p2
                  WHERE p2.tournament_class_id = player_participant.tournament_class_id
                    AND p2.player_id = :keep
               )
        """, {"keep": keep_id, "drop": drop_id})
        # move the rest
        run("UPDATE player_participant SET player_id = ? WHERE player_id = ?", (keep_id, drop_id))

    # 2) tournament_class_* tables with UNIQUE(tournament_class_id, player_id)
    for tbl in ("tournament_class_entries", "tournament_class_players", "tournament_class_final_results"):
        if table_exists(tbl):
            run(f"""
                DELETE FROM {tbl}
                 WHERE player_id = :drop
                   AND EXISTS (
                     SELECT 1 FROM {tbl} t2
                      WHERE t2.tournament_class_id = {tbl}.tournament_class_id
                        AND t2.player_id = :keep
                   )
            """, {"keep": keep_id, "drop": drop_id})
            run(f"UPDATE {tbl} SET player_id = ? WHERE player_id = ?", (keep_id, drop_id))

    # 3) player_license UNIQUE(player_id, license_id, season_id, club_id)
    if table_exists("player_license"):
        # 3a) find conflicts between drop and keep
        cursor.execute("""
            SELECT d.license_id, d.season_id, d.club_id,
                   d.valid_from, d.valid_to,
                   k.valid_from, k.valid_to
              FROM player_license d
              JOIN player_license k
                ON k.player_id = :keep
               AND d.player_id = :drop
               AND k.license_id = d.license_id
               AND k.season_id  = d.season_id
               AND k.club_id    = d.club_id
        """, {"keep": keep_id, "drop": drop_id})
        conflicts = cursor.fetchall()

        # 3b) merge date ranges on the keep row, then delete the drop row
        for lic_id, season_id, club_id, d_from, d_to, k_from, k_to in conflicts:
            # expand keep row to cover both ranges
            run("""
                UPDATE player_license
                   SET valid_from = CASE
                                      WHEN date(valid_from) <= date(:d_from) THEN valid_from
                                      ELSE :d_from
                                    END,
                       valid_to   = CASE
                                      WHEN date(valid_to)   >= date(:d_to)   THEN valid_to
                                      ELSE :d_to
                                    END
                 WHERE player_id = :keep
                   AND license_id = :lic
                   AND season_id  = :season
                   AND club_id    = :club
            """, {
                "d_from": d_from, "d_to": d_to,
                "keep": keep_id, "lic": lic_id, "season": season_id, "club": club_id
            })
            # remove drop duplicate
            run("""
                DELETE FROM player_license
                 WHERE player_id = :drop
                   AND license_id = :lic
                   AND season_id  = :season
                   AND club_id    = :club
            """, {"drop": drop_id, "lic": lic_id, "season": season_id, "club": club_id})

        # 3c) move remaining non-conflicting rows
        run("UPDATE player_license SET player_id = ? WHERE player_id = ?", (keep_id, drop_id))

    # 4) player_ranking PRIMARY KEY(player_id, date)
    if table_exists("player_ranking"):
        # delete would-be duplicates (same date already on keep)
        run("""
            DELETE FROM player_ranking
             WHERE player_id = :drop
               AND EXISTS (
                 SELECT 1 FROM player_ranking r2
                  WHERE r2.player_id = :keep
                    AND r2.date = player_ranking.date
               )
        """, {"keep": keep_id, "drop": drop_id})
        run("UPDATE player_ranking SET player_id = ? WHERE player_id = ?", (keep_id, drop_id))

    # 5) player_ranking_group (no UNIQUE, but keep safe order)
    if table_exists("player_ranking_group"):
        run("UPDATE player_ranking_group SET player_id = ? WHERE player_id = ?", (keep_id, drop_id))

    # 6) player_transition (UNIQUE(player_id, club_id_from, club_id_to, transition_date))
    if table_exists("player_transition"):
        run("""
            DELETE FROM player_transition
             WHERE player_id = :drop
               AND EXISTS (
                 SELECT 1 FROM player_transition t2
                  WHERE t2.player_id = :keep
                    AND t2.club_id_from    = player_transition.club_id_from
                    AND t2.club_id_to      = player_transition.club_id_to
                    AND t2.transition_date = player_transition.transition_date
               )
        """, {"keep": keep_id, "drop": drop_id})
        run("UPDATE player_transition SET player_id = ? WHERE player_id = ?", (keep_id, drop_id))

    # # 7) legacy 'match' table columns (if still present)
    # if table_exists("match"):
    #     for col in ("player1_id", "player2_id", "winning_player_id", "losing_player_id"):
    #         # best-effort; no uniques here
    #         run(f"UPDATE match SET {col} = ? WHERE {col} = ?", (keep_id, drop_id))

    # 8) finally delete drop player
    run("DELETE FROM player WHERE player_id = ?", (drop_id,))
    logging.info("✔ Merge %s → %s %s", drop_id, keep_id, "(DRY RUN only)" if dry_run else "")

# ──────────────────────────────────────────────────────────────────────────────
# MAIN: Verified players + aliases + optional merges (with progress prints)
# ──────────────────────────────────────────────────────────────────────────────
def upd_players_verified():
    conn, cursor = get_conn()
    logging.info("Updating player table...")
    print("ℹ️  Updating player table...")

    try:
        # ── 2) Load raw license rows ────────────────────────────────────────
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

        # ── 3) Load raw ranking rows ────────────────────────────────────────
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

        # ── 4) Merge into a single map, giving license priority ─────────────
        player_data: Dict[int, Tuple[str, str, int, str]] = {}

        for ext, fn, ln, yb in license_rows:
            if ext not in player_data:
                player_data[int(ext)] = (
                    sanitize_name(fn),
                    sanitize_name(ln),
                    int(yb),
                    'license'
                )

        for ext, fn, ln, yb in ranking_rows:
            ext = int(ext)
            if ext not in player_data:
                player_data[ext] = (
                    sanitize_name(fn),
                    sanitize_name(ln),
                    int(yb),
                    'ranking'
                )

        # logging.info("Found %,d unique external players in license+ranking", len(player_data))
        # print(f"ℹ️  Found {len(player_data):,} unique external players in license and ranking tables")

        # ── counters for summary ────────────────────────────────────────────
        c_created_canonical = 0
        c_alias_repointed   = 0
        c_alias_added       = 0
        c_merges_planned    = 0
        c_merges_done       = 0
        c_skipped_existing  = 0
        c_inserted_new      = 0

        # ── 5) Process manual duplicate groups first ────────────────────────
        groups: Dict[int, Set[int]] = {}
        for grp in DUPLICATE_EXT_GROUPS:
            can = min(grp)
            groups.setdefault(can, set()).update(grp)

        print(f"ℹ️  Processing {len(groups)} manual duplicate group(s)…")
        logging.info("Processing %d manual duplicate groups", len(groups))

        results = []
        for i, (can_ext, all_exts) in enumerate(sorted(groups.items()), start=1):
            can_tuple = player_data.get(can_ext)
            if not can_tuple and all_exts:
                for e in sorted(all_exts):
                    if e in player_data:
                        can_tuple = player_data[e]
                        break

            if not can_tuple:
                msg = f"No source data for canonical ext {can_ext}; skipping group {sorted(all_exts)}"
                print(f"⚠️  {msg}")
                logging.warning(msg)
                continue

            can_fn, can_ln, can_yb, can_src = can_tuple
            # print(f"   • Group {i}/{len(groups)}: canonical {can_ext} → {_seasoned_name(can_fn, can_ln, can_yb)}")
            logging.info("Group %d: canonical %s (%s)", i, can_ext, _seasoned_name(can_fn, can_ln, can_yb))

            # Ensure canonical exists
            canon_player = Player.get_by_id_ext(cursor, can_ext)
            if not canon_player:
                if DRY_RUN:
                    logging.info("[DRY RUN] Would INSERT canonical player for %s", _seasoned_name(can_fn, can_ln, can_yb))
                    print(f"      [DRY RUN] Would insert canonical player for {_seasoned_name(can_fn, can_ln, can_yb)}")
                else:
                    p = Player(firstname=can_fn, lastname=can_ln, year_born=can_yb, is_verified=True)
                    res = p.save_to_db(cursor, player_id_ext=_as_text_ext(can_ext), source_system=can_src)
                    results.append(res)
                    c_created_canonical += 1
                    logging.info("Inserted canonical player_id=%s for ext=%s", p.player_id, can_ext)
                # re-fetch (only works if inserted or already existed)
                canon_player = Player.get_by_id_ext(cursor, can_ext)

            if not canon_player:
                logging.error("Canonical player still missing for ext %s; skipping group %s", can_ext, all_exts)
                print(f"      ❌ Canonical player missing; skipping group {sorted(all_exts)}")
                continue

            # For each non-canonical ext, repoint alias or add new alias
            for ext_id in sorted(e for e in all_exts if e != can_ext):
                row = _fetchone(cursor,
                                "SELECT player_id FROM player_alias WHERE player_id_ext = ?",
                                (_as_text_ext(ext_id),))
                if row:
                    old_pid = row[0]
                    if old_pid != canon_player.player_id:
                        c_alias_repointed += 1
                        logging.info("Repoint alias %s: %s → %s", ext_id, old_pid, canon_player.player_id)
                        # print(f"      ↪ Repoint alias {ext_id}: {old_pid} → {canon_player.player_id}")
                        _run(cursor,
                             "UPDATE player_alias SET player_id = ? WHERE player_id_ext = ?",
                             (canon_player.player_id, _as_text_ext(ext_id)),
                             dry=DRY_RUN)

                        if AUTO_MERGE_DUP_PLAYERS:
                            c_merges_planned += 1
                            merge_players(cursor, keep_id=canon_player.player_id, drop_id=old_pid, dry_run=DRY_RUN)
                            if not DRY_RUN:
                                c_merges_done += 1
                    else:
                        c_skipped_existing += 1
                        logging.info("Alias %s already on canonical player_id %s", ext_id, canon_player.player_id)
                        # print(f"      = Alias {ext_id} already points to canonical")
                else:
                    # No alias row exists → add one
                    fn, ln, yb, src = player_data.get(ext_id, (can_fn, can_ln, can_yb, "manual"))
                    if DRY_RUN:
                        logging.info("[DRY RUN] Would add alias ext=%s to player_id=%s (%s)",
                                     ext_id, canon_player.player_id, _seasoned_name(fn, ln, yb))
                        print(f"      [DRY RUN] Would add alias ext={ext_id} to player_id={canon_player.player_id}")
                    else:
                        aliased = canon_player.add_alias(
                            cursor,
                            player_id_ext=_as_text_ext(ext_id),
                            firstname=fn,
                            lastname=ln,
                            year_born=yb,
                            fullname_raw=None,
                            source_system=src
                        )
                        results.append(aliased)
                        c_alias_added += 1

        # ── 6) Handle all remaining (non-duplicate) externals normally ─────
        processed_exts = set().union(*groups.values()) if groups else set()
        remaining = [k for k in player_data.keys() if k not in processed_exts]
        print(f"ℹ️  Processing {len(remaining):,} non-duplicate external players…")
        logging.info("Processing %d non-duplicate externals", len(remaining))

        for idx, ext_id in enumerate(sorted(remaining), start=1):
            fn, ln, yb, source = player_data[ext_id]

            # Already aliased anywhere? then skip insert
            row = _fetchone(cursor,
                            "SELECT player_id FROM player_alias WHERE player_id_ext = ?",
                            (_as_text_ext(ext_id),))
            if row:
                c_skipped_existing += 1

            # Insert a new verified player and its alias
            p = Player(firstname=fn, lastname=ln, year_born=yb, is_verified=True)
            res = p.save_to_db(cursor, _as_text_ext(ext_id), source_system=source)
            results.append(res)
            c_inserted_new += 1


        # ── 7) Commit / Report ─────────────────────────────────────────────
        print("\n──────────────── Summary ────────────────")
        print(f"   DRY RUN: {'ON' if DRY_RUN else 'OFF'}")
        print(f"   Duplicate groups:        {len(groups):,}")
        print(f"   Canonical inserted:      {c_created_canonical:,}")
        print(f"   Aliases repointed:       {c_alias_repointed:,}")
        print(f"   Aliases added:           {c_alias_added:,}")
        print(f"   New players inserted:    {c_inserted_new:,}")
        if AUTO_MERGE_DUP_PLAYERS:
            print(f"   Merges planned/done:     {c_merges_planned:,} / {c_merges_done:,}")
        print(f"   Already existed/skipped: {c_skipped_existing:,}")
        print("─────────────────────────────────────────\n")

        if DRY_RUN:
            print("🧪 DRY RUN: No database changes were committed.")
            logging.info("DRY RUN complete; no changes committed.")
        else:
            conn.commit()
            print_db_insert_results(results)
            print("")
            logging.info("Done updating players")

    except Exception as e:
        logging.error(f"Error in upd_players_verified: {e}")
        print(f"❌ Error updating players: {e}")
        if not DRY_RUN:
            conn.rollback()
    finally:
        conn.close()


# Optional: run a single merge with progress logs
def merge_players_oneoff(keep_id: int, drop_id: int, *, dry_run: bool = True):
    conn, cursor = get_conn()
    try:
        print(f"ℹ️  One-off merge: {drop_id} → {keep_id}  (DRY RUN: {'ON' if dry_run else 'OFF'})")
        merge_players(cursor, keep_id=keep_id, drop_id=drop_id, dry_run=dry_run)
        if dry_run:
            logging.info("One-off merge: DRY RUN only.")
            print("🧪 DRY RUN complete (no changes).")
        else:
            conn.commit()
            logging.info("One-off merge committed.")
            print("✅ Merge committed.")
    except Exception as e:
        logging.error(f"Error in merge_players_oneoff: {e}")
        print(f"❌ Error merging players: {e}")
        if not dry_run:
            conn.rollback()
    finally:
        conn.close()


if __name__ == "__main__":
    # Run the verified-updater. Flip DRY_RUN/AUTO_MERGE_DUP_PLAYERS at the top when ready.
    upd_players_verified()
