#!/usr/bin/env python3
import sqlite3

from db import get_conn, DB_NAME
DRY_RUN = True               # <-- set to False to actually delete

def delete_orphan_duplicate_players():
    conn, cursor = get_conn()

    # 1) Find fullname_raw values that occur more than once
    cursor.execute("""
        SELECT fullname_raw
        FROM player
        WHERE fullname_raw IS NOT NULL AND fullname_raw != ''
        GROUP BY fullname_raw
        HAVING COUNT(*) > 1
    """)
    duplicate_fullnames = [row[0] for row in cursor.fetchall()]

    total_deleted = 0

    # 2) Loop through duplicates and select those not in player_participant
    for fullname in duplicate_fullnames:
        cursor.execute("""
            SELECT player_id
            FROM player
            WHERE fullname_raw = ?
              AND player_id NOT IN (
                  SELECT DISTINCT player_id
                  FROM player_participant
                  WHERE player_id IS NOT NULL
              )
        """, (fullname,))
        orphan_ids = [row[0] for row in cursor.fetchall()]

        if orphan_ids:
            if DRY_RUN:
                print(f"[DRY RUN] Would delete {len(orphan_ids)} orphan(s) with fullname_raw = '{fullname}': {orphan_ids}")
            else:
                cursor.executemany(
                    "DELETE FROM player WHERE player_id = ?",
                    [(pid,) for pid in orphan_ids]
                )
                total_deleted += len(orphan_ids)
                print(f"Deleted {len(orphan_ids)} orphan(s) with fullname_raw = '{fullname}'")

    if not DRY_RUN:
        conn.commit()
        print(f"✅ Finished. Total deleted: {total_deleted}")
    else:
        print("💡 Dry run complete — no changes made.")

    conn.close()

if __name__ == "__main__":
    delete_orphan_duplicate_players()
