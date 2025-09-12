import sqlite3
from models.tournament_class_raw import TournamentClassRaw
from utils import get_conn

def backfill_tournament_class_raw_hashes():
    """
    Fill missing content_hash values in tournament_class_raw.
    Safe to rerun: only updates rows where content_hash IS NULL.
    Verifies how many rows remain unfilled after commit.
    """
    conn, cursor = get_conn()

    cursor.execute("""
        SELECT row_id, tournament_id_ext, tournament_class_id_ext, startdate,
               shortname, longname, gender, max_rank, max_age,
               url, raw_stages, raw_stage_hrefs, data_source_id,
               row_created, row_updated
        FROM tournament_class_raw
        WHERE content_hash IS NULL
    """)
    rows = cursor.fetchall()

    total_missing = len(rows)
    print(f"ℹ️ Found {total_missing} rows without content_hash.")

    if not rows:
        conn.close()
        return

    updates = []
    for idx, row in enumerate(rows):
        obj = TournamentClassRaw.from_dict({
            "row_id": row[0],
            "tournament_id_ext": row[1],
            "tournament_class_id_ext": row[2],
            "startdate": row[3],
            "shortname": row[4],
            "longname": row[5],
            "gender": row[6],
            "max_rank": row[7],
            "max_age": row[8],
            "url": row[9],
            "raw_stages": row[10],
            "raw_stage_hrefs": row[11],
            "data_source_id": row[12],
            "row_created": row[13],
            "row_updated": row[14],
        })

        content_hash = obj.compute_content_hash()
        updates.append((content_hash, obj.row_id))

        if idx < 3:  # preview a few
            print(f"   ↳ row_id={obj.row_id}, shortname={obj.shortname}, hash={content_hash}")

    # Bulk update
    cursor.executemany(
        "UPDATE tournament_class_raw SET content_hash = ? WHERE row_id = ?",
        updates
    )
    conn.commit()
    print(f"✅ Attempted to update {len(updates)} rows (cursor.rowcount={cursor.rowcount}).")

    # Verify how many rows remain unfilled
    cursor.execute("SELECT COUNT(*) FROM tournament_class_raw WHERE content_hash IS NULL")
    still_missing = cursor.fetchone()[0]
    print(f"🔎 Verification: {still_missing} rows still missing content_hash.")

    conn.close()


if __name__ == "__main__":
    backfill_tournament_class_raw_hashes()
