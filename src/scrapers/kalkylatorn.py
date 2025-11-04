# pingiskalk_delta.py
from __future__ import annotations
import os, time, json, sqlite3, logging, argparse
from typing import Iterable, List, Optional
import requests

API_BASE   = os.getenv("PINGISKALK_API_BASE", "https://api-pingiskalk-wwr1.onrender.com")
API_TIMEOUT= float(os.getenv("PINGISKALK_TIMEOUT", "15"))
API_SLEEP  = float(os.getenv("PINGISKALK_SLEEP", "0.20"))  # politeness
API_BEARER = os.getenv("PINGISKALK_BEARER", "").strip()

DB_PATH = os.getenv("BTSTATS_DB", "../data/table_tennis.db")

LOCAL_PRESENCE_SQL = """
    SELECT player_id_ext FROM player_id_ext
"""

LOCAL_PRESENCE_SQL = """
    SELECT CAST(player_id_ext AS INTEGER)
    FROM player_id_ext
    WHERE player_id_ext GLOB '[0-9]*'
"""

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Pingiskollen-Bot/1.0 (+https://pingiskollen.se)",
}
if API_BEARER:
    HEADERS["Authorization"] = f"Bearer {API_BEARER}"

session = requests.Session()
session.headers.update(HEADERS)

def _get_json(url: str) -> tuple[int, Optional[object]]:
    try:
        r = session.get(url, timeout=API_TIMEOUT)
        status = r.status_code
        if status == 204:
            return status, None
        if "application/json" in r.headers.get("Content-Type", ""):
            return status, r.json()
        return status, None
    except requests.RequestException as e:
        logging.warning("Request failed %s: %s", url, e)
        return 0, None

def _exists_by_players(id_ext: int) -> Optional[bool]:
    status, _ = _get_json(f"{API_BASE}/players/{id_ext}")
    if status == 200:
        return True
    if status == 404:
        return False
    return None  # unknown

def _exists_by_licenses(id_ext: int) -> bool:
    status, _ = _get_json(f"{API_BASE}/players/{id_ext}/licenses")
    if status == 200:
        return True
    if status == 404:
        return False
    if status in (401, 403):
        logging.warning("Auth required for id %s (status %s). Set PINGISKALK_BEARER.", id_ext, status)
    return False

def api_has_profile(id_ext: int) -> bool:
    probe = _exists_by_players(id_ext)
    if probe is not None:
        return probe
    return _exists_by_licenses(id_ext)

def load_local_ids(db_path: str) -> set[int]:
    con = sqlite3.connect(db_path)
    try:
        cur = con.execute(LOCAL_PRESENCE_SQL)
        return {int(row[0]) for row in cur.fetchall() if row[0] is not None}
    finally:
        con.close()

def compute_delta(candidate_ids: Iterable[int], local_ids: set[int], max_queries: int, progress_every: int) -> List[int]:
    """Return IDs that exist on Pingiskalk but are not present locally.
       Hard cap total API calls via max_queries."""
    delta = []
    processed = 0
    hits = 0
    start_ts = time.time()

    for pid in candidate_ids:
        if max_queries is not None and processed >= max_queries:
            logging.info("Reached max-queries limit (%d). Stopping.", max_queries)
            break

        # Skip if already in local DB
        if pid in local_ids:
            processed += 1
            if processed % progress_every == 0:
                elapsed = time.time() - start_ts
                rate = processed / max(elapsed, 1e-6)
                print(f"[{processed}] skipped-local so far, hits={hits}, elapsed={elapsed:.1f}s, ~{rate:.1f} req/s")
            continue

        exists = api_has_profile(pid)
        if exists:
            hits += 1
            delta.append(pid)
            # Print hit immediately
            print(f"✅ HIT {pid} (exists on Pingiskalkylatorn, not in local DB)")
        else:
            print(f"· miss {pid}")

        processed += 1
        if processed % progress_every == 0:
            elapsed = time.time() - start_ts
            rate = processed / max(elapsed, 1e-6)
            print(f"[{processed}] checked, hits={hits}, elapsed={elapsed:.1f}s, ~{rate:.1f} req/s")

        time.sleep(API_SLEEP)

    elapsed = time.time() - start_ts
    print(f"Done. processed={processed}, hits={hits}, elapsed={elapsed:.1f}s")
    return delta

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Delta-check Pingiskalkylatorn profiles vs local DB.")
    p.add_argument("--db", default=DB_PATH, help=f"Path to SQLite (default: {DB_PATH})")
    p.add_argument("--range-start", type=int, default=int(os.getenv("RANGE_START", "1060000")))
    p.add_argument("--range-end",   type=int, default=int(os.getenv("RANGE_END",   "1060999")))
    p.add_argument("--max-queries", type=int, default=50, help="Max API calls (testing throttle). Use 0 for unlimited.")
    p.add_argument("--progress-every", type=int, default=25, help="Print periodic progress every N checks.")
    # Optional: provide a newline-separated file of candidate ids instead of a range
    p.add_argument("--ids-file", help="Path to file with one player_id_ext per line.")
    p.add_argument("--bearer", help="JWT token for Authorization header")

    return p.parse_args()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = parse_args()

    if args.bearer:
        session.headers["Authorization"] = f"Bearer {args.bearer}"


    db_path = args.db
    local_ids = load_local_ids(db_path)
    logging.info("Local player_id_ext rows: %d (db=%s)", len(local_ids), db_path)

    if args.ids_file:
        with open(args.ids_file, "r", encoding="utf-8") as fh:
            candidate_ids = [int(line.strip()) for line in fh if line.strip()]
        logging.info("Loaded %d candidate ids from %s", len(candidate_ids), args.ids_file)
    else:
        candidate_ids = range(args.range_start, args.range_end + 1)
        logging.info("Candidate range: [%d, %d] (%d ids)",
                     args.range_start, args.range_end, args.range_end - args.range_start + 1)

    max_q = None if args.max_queries == 0 else args.max_queries
    missing_locally = compute_delta(candidate_ids, local_ids, max_q, args.progress_every)

    print("\n=== Delta (exists on Pingiskalkylatorn, NOT in local DB) ===")
    for pid in missing_locally:
        print(pid)
    print(f"Total: {len(missing_locally)}")
