#!/usr/bin/env python3
# backfill_player_license_hashes.py
import hashlib
import re
import sqlite3
import time
from config import DB_NAME

# >>> EDIT THIS <<<
DB_PATH = DB_NAME
BATCH_SIZE = 5000

# ---- helpers (mirrors your utils.compute_content_hash rules) ----
_ws_re = re.compile(r"\s+")

def normalize_key(s: str) -> str:
    s = s.strip()
    s = _ws_re.sub(" ", s)
    return s.lower()

def compute_row_hash(row: sqlite3.Row) -> str:
    # Exclude: row_id, data_source_id, row_created, row_updated, last_seen_at, content_hash
    fields_to_hash = [
        "season_label",
        "season_id_ext",
        "club_name",
        "club_id_ext",
        "player_id_ext",
        "firstname",
        "lastname",
        "gender",
        "year_born",
        "license_info_raw",
        "ranking_group_raw",
    ]
    parts = []
    for key in fields_to_hash:
        v = row[key]
        if v is None:
            parts.append("")
        elif isinstance(v, str):
            parts.append(normalize_key(v))
        elif isinstance(v, (int, float)):
            parts.append(str(v))
        else:
            parts.append(str(v))
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()

def backfill(db_path: str, batch_size: int = 5000) -> None:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")

        total_missing = conn.execute(
            "SELECT COUNT(*) FROM player_license_raw WHERE content_hash IS NULL;"
        ).fetchone()[0]

        if total_missing == 0:
            print("Nothing to do: all rows already have content_hash.")
            return

        print(f"Backfilling content_hash for {total_missing} rows...")
        last_id = 0
        processed = 0
        t0 = time.time()

        while True:
            rows = conn.execute(
                """
                SELECT row_id, season_label, season_id_ext, club_name, club_id_ext,
                       player_id_ext, firstname, lastname, gender, year_born,
                       license_info_raw, ranking_group_raw,
                       data_source_id, content_hash, last_seen_at, row_created, row_updated
                FROM player_license_raw
                WHERE content_hash IS NULL AND row_id > ?
                ORDER BY row_id
                LIMIT ?;
                """,
                (last_id, batch_size),
            ).fetchall()

            if not rows:
                break

            with conn:  # one transaction per batch
                for r in rows:
                    h = compute_row_hash(r)
                    conn.execute(
                        """
                        UPDATE player_license_raw
                        SET content_hash = ?
                        WHERE row_id = ? AND content_hash IS NULL;
                        """,
                        (h, r["row_id"]),
                    )
                    last_id = r["row_id"]
                    processed += 1

            if processed % max(1000, batch_size) == 0 or processed == total_missing:
                elapsed = time.time() - t0
                print(f"  processed {processed}/{total_missing} ... ({elapsed:.1f}s)")

        elapsed = time.time() - t0
        print(f"Done. Filled {processed} rows in {elapsed:.1f}s.")
    finally:
        conn.close()

if __name__ == "__main__":
    backfill(DB_PATH, batch_size=BATCH_SIZE)
