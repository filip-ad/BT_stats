# scrapers/kalkylatorn_scan.py
from __future__ import annotations
import asyncio, aiohttp, time, json, csv, sqlite3, os, math, random
from typing import Optional, Dict, Any, Tuple, Set

# =========================
# CONFIG (edit these only)
# =========================
RANGE_START        = 0
RANGE_END          = 1_200_022

DB_PATH            = "../data/table_tennis.db"
OUTPUT_CSV         = "../data/kalkylatorn_scan.csv"     # main results (resume reads this)
EVENTS_CSV         = "../data/kalkylatorn_events.csv"   # rate-limit/ban/timeout signals

API_BASE           = "https://api-pingiskalk-wwr1.onrender.com"
BEARER_JWT         = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySWQiOiJmZDEyZDE5Zi03YjYzLTRlMjYtOWNjZC1hZmY5ZjhmMzVmYTgiLCJ1c2VybmFtZSI6ImZpbGlwLmFkQGhvdG1haWwuY29tIiwicm9sZSI6InVzZXIiLCJpYXQiOjE3NjIzNTMzMjksImV4cCI6MTc2MjM1NDIyOX0.CoEoXB3e9BbJPn3MccF8Fq6wVgAvMCZ12-208LGov7I"

DEBUG_ENRICH        = True           # print why enrichment didn't take
PREFER_PLAYERS_WHEN_AUTH    = True  # if bearer is present, try /players/{id} first
ENRICH_PLAYER_DETAILS       = True # after existence, try to fetch /players/{id} and store full JSON

# Throughput controls
MAX_CONCURRENCY    = 32     # in-flight requests
QPS_START          = 60.0    # starting QPS
QPS_FLOOR          = 4.0    # min QPS
QPS_CEILING        = 100.0   # max QPS
ADAPT_WINDOW       = 400    # adjust QPS every N responses
ERR_HIGH_CUTOFF    = 0.12   # >12% err → step down
ERR_LOW_CUTOFF     = 0.02   # < 2% err → step up
QPS_STEP_UP        = 1.0
QPS_STEP_DOWN      = 2.0

# Retries/backoff & timeouts
RETRY_MAX          = 4
RETRY_BASE_DELAY   = 0.75
REQUEST_TIMEOUT_S  = 12

# Progress
PROGRESS_EVERY     = 100
HEARTBEAT_EVERY_S  = 5
# =========================

import tempfile

def payload_wrap(endpoint: str, data):
    return {"endpoint": endpoint, "data": data}


def sort_output_csv_inplace(path: str) -> None:
    """
    Sort OUTPUT_CSV by player_id_ext (col 0) ascending, keep the latest row if duplicates exist.
    Uses an atomic temp file replace to avoid corruption on crash.
    No-op if file doesn't exist or is empty.
    """
    if not os.path.exists(path):
        return

    # Read all rows
    rows = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)  # may raise StopIteration if empty
        except StopIteration:
            return

        # build dict to dedupe (keep latest row per id)
        by_id = {}
        for r in reader:
            if not r or not r[0].strip():
                continue
            try:
                pid = int(r[0])
            except Exception:
                # skip malformed id rows
                continue
            by_id[pid] = r  # last one wins

        if not by_id:
            return

        # sort by pid
        sorted_pids = sorted(by_id.keys())
        rows = [by_id[pid] for pid in sorted_pids]

    # Write to temp file, then atomically replace
    dir_name = os.path.dirname(path) or "."
    with tempfile.NamedTemporaryFile("w", delete=False, dir=dir_name, encoding="utf-8", newline="") as tf:
        tmp_path = tf.name
        writer = csv.writer(tf)
        writer.writerow(header)
        writer.writerows(rows)

    os.replace(tmp_path, path)



def load_local_ids(db_path: str) -> Set[int]:
    con = sqlite3.connect(db_path)
    try:
        cur = con.execute("""
            SELECT CAST(player_id_ext AS INTEGER)
            FROM player_id_ext
            WHERE player_id_ext GLOB '[0-9]*'
        """)
        return {int(r[0]) for r in cur.fetchall() if r[0] is not None}
    finally:
        con.close()


def load_processed_ids(path: str) -> Set[int]:
    """
    Read the first column (player_id_ext) from an existing OUTPUT_CSV.
    Ignores header row and any malformed/partial lines.
    """
    done: Set[int] = set()
    if not os.path.exists(path):
        return done
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        first = True
        for row in reader:
            if first:
                first = False
                # optional: verify header
                continue
            if not row:
                continue
            try:
                pid = int(row[0])
            except Exception:
                continue
            done.add(pid)
    return done


class RateLimiter:
    def __init__(self, rate_per_sec: float, capacity: Optional[int] = None):
        self.rate = rate_per_sec
        self.capacity = capacity or max(1, math.ceil(rate_per_sec))
        self.tokens = float(self.capacity)
        self.updated = time.perf_counter()
        self.lock = asyncio.Lock()

    async def acquire(self):
        async with self.lock:
            while self.tokens < 1.0:
                now = time.perf_counter()
                elapsed = now - self.updated
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                self.updated = now
                if self.tokens < 1.0:
                    await asyncio.sleep(max((1.0 - self.tokens) / self.rate, 0.01))
            self.tokens -= 1.0

    def set_rate(self, new_rate: float):
        now = time.perf_counter()
        elapsed = now - self.updated
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
        self.updated = now
        self.rate = max(0.1, new_rate)


def build_headers() -> Dict[str, str]:
    h = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Pingiskollen-Bot/1.0 (+https://pingiskollen.se)"
    }
    if BEARER_JWT:
        h["Authorization"] = f"Bearer {BEARER_JWT}"
    return h


async def fetch_json(session: aiohttp.ClientSession, limiter: RateLimiter, url: str,
                     events_writer: csv.writer, pid: int) -> Tuple[int, Optional[Any], bool]:
    """Return (status, data, counted_error). counted_error flags things for adaptive controller."""
    counted_error = False
    for attempt in range(RETRY_MAX + 1):
        await limiter.acquire()
        try:
            async with session.get(url, timeout=REQUEST_TIMEOUT_S) as resp:
                status = resp.status
                ctype = resp.headers.get("Content-Type", "")
                data = None
                if "application/json" in ctype:
                    try:
                        data = await resp.json(content_type=None)
                    except Exception:
                        data = None

                if status == 429:
                    counted_error = True
                    events_writer.writerow([time.strftime("%Y-%m-%d %H:%M:%S"), pid, "429", url, "rate_limit"])
                elif status == 403:
                    counted_error = True
                    events_writer.writerow([time.strftime("%Y-%m-%d %H:%M:%S"), pid, "403", url, "forbidden_possible_block"])
                elif status >= 500:
                    counted_error = True
                    events_writer.writerow([time.strftime("%Y-%m-%d %H:%M:%S"), pid, str(status), url, "server_error"])
                elif status == 0:
                    counted_error = True
                    events_writer.writerow([time.strftime("%Y-%m-%d %H:%M:%S"), pid, "0", url, "network_error"])

                if status not in (429,) and status < 500:
                    return status, data, counted_error
                # else retry 429/5xx

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            counted_error = True
            events_writer.writerow([time.strftime("%Y-%m-%d %H:%M:%S"), pid, "timeout", url, f"timeout_or_client_error:{type(e).__name__}"])

        delay = (RETRY_BASE_DELAY * (2 ** attempt)) * (0.7 + random.random()*0.6)
        await asyncio.sleep(delay)

    return status if 'status' in locals() else 0, None, counted_error

async def probe_player(session, limiter, pid: int, events_writer) -> tuple[bool, dict, bool]:
    """
    Return (exists, payload, counted_error).

    Strategy:
      0) If a bearer token is present, prefer /players/{id} first (full JSON).
      1) Public /bestplacement (positive if placement>0).
      2) Public /licenses (positive if non-empty).
      3) Fast-fail: if both (1) and (2) are 404, return negative without /players.
      4) Otherwise try /players/{id} (may need bearer).
    """
    counted_err = False

    # 0) Prefer rich details if we have a token
    if BEARER_JWT:
        stp, datp, errp = await fetch_json(
            session, limiter, f"{API_BASE}/players/{pid}", events_writer, pid
        )
        counted_err = counted_err or errp
        if stp == 200 and isinstance(datp, dict):
            return True, payload_wrap("players", datp), counted_err
        # if 401/403/etc, continue to public checks

    # 1) bestplacement (public)
    st, data, err = await fetch_json(
        session, limiter, f"{API_BASE}/players/{pid}/bestplacement", events_writer, pid
    )
    counted_err = counted_err or err
    if st == 200 and isinstance(data, dict) and isinstance(data.get("placement"), int) and data["placement"] > 0:
        return True, payload_wrap("bestplacement", data), counted_err
    bp_404 = (st == 404)

    # 2) licenses (public)
    st2, data2, err2 = await fetch_json(
        session, limiter, f"{API_BASE}/players/{pid}/licenses", events_writer, pid
    )
    counted_err = counted_err or err2
    if st2 == 200 and isinstance(data2, list) and len(data2) > 0:
        return True, payload_wrap("licenses", data2), counted_err
    lic_404 = (st2 == 404)

    # 3) Fast-fail negatives: both public endpoints 404 → very likely no profile
    if bp_404 and lic_404:
        return False, payload_wrap("none", None), counted_err

    # 4) Try full player JSON as last resort (maybe token works / endpoint is public for this id)
    st3, data3, err3 = await fetch_json(
        session, limiter, f"{API_BASE}/players/{pid}", events_writer, pid
    )
    counted_err = counted_err or err3
    if st3 == 200 and isinstance(data3, dict):
        return True, payload_wrap("players", data3), counted_err

    return False, payload_wrap("none", None), counted_err



async def scan_range():

    # Sort output file (if exists) so resume & humans see clean order
    sort_output_csv_inplace(OUTPUT_CSV)

    local_ids = load_local_ids(DB_PATH)
    processed_ids = load_processed_ids(OUTPUT_CSV)
    print(f"Loaded local player_id_ext rows: {len(local_ids)} from {DB_PATH}")
    print(f"Resume: found {len(processed_ids)} rows already in {OUTPUT_CSV}")

    limiter = RateLimiter(QPS_START)
    sem = asyncio.Semaphore(MAX_CONCURRENCY)

    headers = build_headers()
    connector = aiohttp.TCPConnector(limit=None, ttl_dns_cache=300)

    # Ensure directories exist
    out_dir = os.path.dirname(OUTPUT_CSV) or "."
    evt_dir = os.path.dirname(EVENTS_CSV) or "."
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(evt_dir, exist_ok=True)

    # Open files (append for resume)
    out_exists = os.path.exists(OUTPUT_CSV)
    evt_exists = os.path.exists(EVENTS_CSV)

    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f_out, \
         open(EVENTS_CSV, "a", newline="", encoding="utf-8") as f_evt:

        writer = csv.writer(f_out)
        events_writer = csv.writer(f_evt)

        # Write headers only if creating new files
        if not out_exists:
            writer.writerow(["player_id_ext", "in_db", "on_site", "json_payload"])
            f_out.flush()
        if not evt_exists:
            events_writer.writerow(["ts", "player_id_ext", "code", "url", "note"])
            f_evt.flush()

        start_t = time.perf_counter()
        processed_new = 0     # newly processed this run
        skipped_resume = 0    # skipped due to already in CSV
        hits = 0

        # adaptive controller state
        window_total = 0
        window_errors = 0

        async with aiohttp.ClientSession(headers=headers, connector=connector) as session:

            stop_event = asyncio.Event()

            async def heartbeat():
                last_p = -1
                while not stop_event.is_set():
                    await asyncio.sleep(HEARTBEAT_EVERY_S)
                    elapsed = time.perf_counter() - start_t
                    # include skipped in rate to reflect overall forward progress
                    total_progress = processed_new + skipped_resume
                    rate = total_progress / max(elapsed, 1e-6)
                    print(f"[HB] processed_new={processed_new} skipped_resume={skipped_resume} "
                          f"hits={hits} elapsed={elapsed:.1f}s ~{rate:.2f} items/s  qps={limiter.rate:.1f}")

            async def handle(pid: int):
                nonlocal processed_new, skipped_resume, hits, window_total, window_errors
                if pid in processed_ids:
                    skipped_resume += 1
                    return

                async with sem:
                    in_db = pid in local_ids
                    exists, payload, counted_err = await probe_player(session, limiter, pid, events_writer)

                    # ---- enrichment step (fetch /players/{id}) ----
                    if exists and ENRICH_PLAYER_DETAILS and payload.get("endpoint") != "players":
                        st_en, data_en, err_en = await fetch_json(session, limiter, f"{API_BASE}/players/{pid}", events_writer, pid)
                        if st_en == 200 and isinstance(data_en, dict):
                            payload = payload_wrap("players", data_en)
                        else:
                            # keep the earlier payload but annotate why enrichment didn't upgrade
                            meta = {"enrich_status": st_en}
                            if DEBUG_ENRICH:
                                print(f"[ENRICH] pid={pid} players/{pid} -> {st_en} (kept {payload.get('endpoint')})")
                            # attach the meta into payload for visibility
                            if isinstance(payload.get("data"), dict):
                                payload["data"]["_enrich"] = meta
                            else:
                                payload["_enrich"] = meta
                        counted_err = counted_err or err_en  # feed adaptive controller

                    if exists:
                        hits += 1

                    writer.writerow([
                        pid,
                        "Y" if in_db else "N",
                        "Y" if exists else "N",
                        json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
                    ])
                    processed_new += 1

                    # progress prints
                    total_progress = processed_new + skipped_resume
                    if total_progress % PROGRESS_EVERY == 0:
                        elapsed = time.perf_counter() - start_t
                        rate = total_progress / max(elapsed, 1e-6)
                        print(f"[{total_progress}] progress | new={processed_new} skipped={skipped_resume} "
                              f"hits={hits} elapsed={elapsed:.1f}s ~{rate:.2f} items/s  qps={limiter.rate:.1f}")
                        f_out.flush()

                    # adaptive QPS accounting: only for requests we actually made
                    window_total += 1
                    window_errors += int(bool(counted_err))
                    if window_total >= ADAPT_WINDOW:
                        err_rate = window_errors / max(window_total, 1)
                        if err_rate > ERR_HIGH_CUTOFF and limiter.rate > QPS_FLOOR:
                            new_rate = max(QPS_FLOOR, limiter.rate - QPS_STEP_DOWN)
                            limiter.set_rate(new_rate)
                            print(f"[ADAPT] High errors {err_rate:.1%} → lowering QPS to {new_rate:.1f}")
                        elif err_rate < ERR_LOW_CUTOFF and limiter.rate < QPS_CEILING:
                            new_rate = min(QPS_CEILING, limiter.rate + QPS_STEP_UP)
                            limiter.set_rate(new_rate)
                            print(f"[ADAPT] Low errors {err_rate:.1%} → raising QPS to {new_rate:.1f}")
                        window_total = 0
                        window_errors = 0

            # start heartbeat
            hb_task = asyncio.create_task(heartbeat())

            # Launch tasks chunked, skipping already-done IDs
            CHUNK = 10_000
            for chunk_start in range(RANGE_START, RANGE_END + 1, CHUNK):
                chunk_end = min(RANGE_END, chunk_start + CHUNK - 1)
                tasks = []
                # build tasks only for IDs not already in OUTPUT_CSV
                for pid in range(chunk_start, chunk_end + 1):
                    if pid in processed_ids:
                        skipped_resume += 1
                        continue
                    tasks.append(asyncio.create_task(handle(pid)))
                if tasks:
                    await asyncio.gather(*tasks)

            stop_event.set()
            await hb_task

        elapsed = time.perf_counter() - start_t
        total_progress = processed_new + skipped_resume
        print(f"Done. New rows written: {processed_new}, skipped due to resume: {skipped_resume}, hits={hits}, elapsed={elapsed:.1f}s.")
        print(f"CSV → {OUTPUT_CSV}\nEvents → {EVENTS_CSV}")


if __name__ == "__main__":
    asyncio.run(scan_range())
