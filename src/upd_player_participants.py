# src/upd_tournament_participants.py

import logging
import requests
import pdfplumber
import re
import unicodedata
from io import BytesIO
from datetime import datetime, date
import time
from urllib.parse import urljoin
from typing import List, Dict
from db import get_conn
from utils import print_db_insert_results, sanitize_name
from models.player_license import PlayerLicense
from models.club import Club
from models.tournament_class import TournamentClass
from models.tournament import Tournament
from config import SCRAPE_CLASS_PARTICIPANTS_MAX_CLASSES, SCRAPE_CLASS_PARTICIPANTS_CLASS_ID_EXT, SCRAPE_CLASS_PARTICIPANTS_ORDER
from models.player_participant import PlayerParticipant
from models.player import Player

PDF_BASE = "https://resultat.ondata.se/ViewClassPDF.php"



def upd_player_participants():
    """
    Populate tournament_class_participant by downloading each class's 'stage=1' PDF.
    """
    overall_start = time.perf_counter()

    conn, cursor = get_conn()
    logging.info("Starting participant update…")
    print("ℹ️  Updating tournament class participants…")

    # 1a) Load all classes
    classes = TournamentClass.cache_all(cursor)

    # 1b) If config says “only this one external ID”, filter to it
    if SCRAPE_CLASS_PARTICIPANTS_CLASS_ID_EXT is not None:
        wanted = SCRAPE_CLASS_PARTICIPANTS_CLASS_ID_EXT
        classes = [tc for tc in classes if tc.tournament_class_id_ext == wanted]
        if not classes:
            logging.error(f"No class found with external ID {wanted}")
            print(f"❌ No tournament class with external ID {wanted}")
            conn.close()
            return

    # 1c) Otherwise, sort by date (newest/oldest) if configured
    else:
        order = (SCRAPE_CLASS_PARTICIPANTS_ORDER or "").lower()
        if order == "newest":
            # most recent dates first
            classes.sort(key=lambda tc: tc.date or date.min, reverse=True)
        elif order == "oldest":
            # earliest dates first
            classes.sort(key=lambda tc: tc.date or date.min)
        elif order:
            logging.warning(f"Unknown SCRAPE_CLASS_PARTICIPANTS_ORDER='{order}', skipping sort")

        # then optionally limit to the first N (0 or negative → no limit)
        max_n = SCRAPE_CLASS_PARTICIPANTS_MAX_CLASSES or 0
        if max_n > 0:
            classes = classes[:max_n]

    # 2) Build lookup caches exactly once
    club_map                = Club.cache_name_map(cursor)
    license_name_club_map   = PlayerLicense.cache_name_club_map(cursor)
    player_name_map         = Player.cache_name_map(cursor)
    cache_raw_name_map      = Player.cache_raw_name_map(cursor)

    results = []
    total_parsed   = 0
    total_expected = 0

    for tc in classes:
        if not tc.tournament_class_id_ext:
            logging.warning(f"Skipping class {tc.tournament_class_id}: no external class ID")
            continue

        # — Download phase —
        t1 = time.perf_counter()
        try: 
            pdf_url    = f"{PDF_BASE}?classID={tc.tournament_class_id_ext}&stage=1"
            pdf_bytes  = download_pdf(pdf_url)
        except Exception as e:
            logging.error(f"❌ PDF download failed for class_ext={tc.tournament_class_id_ext}: {e}")
            results.append({
                "status": "failed",
                "key":    tc.tournament_class_id_ext,
                "reason": f"PDF download failed: {e}"
            })
            continue
        t2 = time.perf_counter()

        if not pdf_bytes:
            logging.error(f"❌ PDF download failed for class_ext={tc.tournament_class_id_ext}")
            results.append({
                "status": "failed",
                "key":    tc.tournament_class_id_ext,
                "reason": "PDF download failed"
            })
            continue

        # — Parse phase —
        t3 = time.perf_counter()
        try: 
            participants, expected_count = parse_players_pdf(pdf_bytes)
            total_parsed += len(participants)
            if expected_count is not None:
                total_expected += expected_count
        except Exception as e:
            logging.error(f"❌ PDF parsing failed for class_ext={tc.tournament_class_id_ext}: {e}")
            results.append({
                "status": "failed",
                "key":    tc.tournament_class_id_ext,
                "reason": f"PDF parsing failed: {e}"
            })
            continue
        t4 = time.perf_counter()

        # Wipe out any old participants for this class:
        deleted = PlayerParticipant.remove_for_class(cursor, tc.tournament_class_id)
        logging.info(f"Deleted {deleted} old participants for class (ext_id: {tc.tournament_class_id_ext}, id: {tc.tournament_class_id})")
        print(f"ℹ️  Deleted {deleted} old participants for class (ext_id: {tc.tournament_class_id_ext}, id: {tc.tournament_class_id})")

        # If we didn’t get exactly the expected number, save for later review
        if expected_count is not None and len(participants) != expected_count:
            missing = expected_count - len(participants)
            cursor.execute("""
                INSERT OR IGNORE INTO player_participant_missing
                  (tournament_class_id,
                   tournament_class_id_ext,
                   participant_url,
                   nbr_of_missing_players)
                VALUES (?, ?, ?, ?)
            """, (
                tc.tournament_class_id,
                tc.tournament_class_id_ext,
                pdf_url,
                missing
            ))
            logging.warning(
                f"{tc.shortname}: parsed {len(participants)}/{expected_count}, "
                "recording in player_participant_missing"
            )

        # normalize class_date
        class_date = (
            tc.date if isinstance(tc.date, date)
            else datetime.fromisoformat(tc.date).date()
        )

        # — DB insert & collect results —
        t5 = time.perf_counter()
        class_results = []
        for idx, raw in enumerate(participants, start=1):
            raw_name  = raw["raw_name"]
            club_name = raw["club_name"].strip()

            pp = PlayerParticipant.from_dict({
                "tournament_class_id": tc.tournament_class_id,
                "fullname_raw":        raw_name,
                "club_name_raw":       club_name,
            })
            result = pp.save_to_db(
                cursor,
                class_date,
                club_map,
                license_name_club_map,
                player_name_map,
                cache_raw_name_map
            )
            # attach idx & raw/club for later logging
            result.update({"idx": idx, "raw_name": raw_name, "club_name": club_name})
            class_results.append(result)
            results.append(result)  # only here, not again later

            # — only extract IDs for success/skipped rows, never for failures —
            if result["status"] != "failed":
                # stash the club_id
                result["club_id"] = pp.club_id

                # for canonical participants
                if result.get("category") == "canonical":
                    result["player_id"] = pp.player_id

                # for raw fallbacks
                elif result.get("category") == "raw":
                    # key is "raw_<raw_id>"
                    raw_part = result["key"].split("_", 1)[1]
                    result["raw_player_id"] = int(raw_part)

        t6 = time.perf_counter()

        # ── Config: True to log only FAILED main lines, False to include all ──
        LOG_ONLY_FAILED = True

        # ── 1) Sort by idx ──
        sorted_results = sorted(class_results, key=lambda r: r["idx"])

        # ── 2) Print parsed vs expected count ──
        parsed  = len(participants)
        exp_cnt = expected_count or parsed
        icon    = "✅" if parsed == exp_cnt else "❌"
        print(f"{icon} Parsed {parsed}/{exp_cnt} participants for class {tc.shortname} "
            f"(class_ext={tc.tournament_class_id_ext} in tournament id {tc.tournament_id})")

        # ── 3) Emit each row ──
        for r in sorted_results:

            # 2a) Build core fields
            idx  = f"{r['idx']:02d}"
            st   = r["status"].upper().ljust(7)
            name = r["raw_name"]
            club = r["club_name"]
            cid  = r.get("club_id","?")

            # pick the right ID tag
            if "player_id" in r:
                idtag = f" pid:{r['player_id']}"
            elif "raw_player_id" in r:
                idtag = f" rpid:{r['raw_player_id']}"
            else:
                idtag = ""

            # 2b) Main line
            line = (
                f"[{idx}] {st} {name}, {club} "
                f"[cid:{cid}{idtag}]     {r['reason']}"
            )

            # 2c) Append ambiguous‐candidate list if this is that case
            if r["status"] == "failed" and "candidates" in r:
                cand_str = ", ".join(
                    f"{c['player_id']}:{c.get('name','<no-name>')}"
                    for c in r["candidates"]
                )
                line += f" [candidates={cand_str}]"

            if not LOG_ONLY_FAILED or r["status"] == "failed":
                # 3) Log & print main line
                if r["status"] == "failed":
                    logging.error(line)
                else:
                    logging.info(line)
                # print(line)

            # 4) Always emit warnings
            for w in r.get("warnings", []):
                warn_line = f"[{idx}] WARNING {st} {name}, {club} [cid:{cid}{idtag}]     {w}"
                logging.warning(warn_line)
                # print(warn_line)

            # ── 4) Per‐class summary ──
        inserted = sum(1 for r in class_results if r["status"] == "success")
        skipped  = sum(1 for r in class_results if r["status"] == "skipped")
        failed   = sum(1 for r in class_results if r["status"] == "failed")
        print(f"   ✅ Inserted: {inserted}   ⏭️  Skipped: {skipped}   ❌ Failed: {failed}")

      # — overall commit & summary —
    conn.commit()
    total = time.perf_counter() - overall_start
    print(f"ℹ️  Participant update complete in {total:.2f}s")
    logging.info(f"Total update took {total:.2f}s")
    # overall parsed vs expected
    if total_expected > 0:
        print(f"ℹ️  Total participants parsed: {total_parsed}/{total_expected}")
    else:
        print(f"ℹ️  Total participants parsed: {total_parsed}")
    print_db_insert_results(results)
    conn.close()

def download_pdf(url: str, retries: int = 3, timeout: int = 30) -> bytes | None:
    """
    Download a PDF with retry logic.
    """
    headers = {"User-Agent": "Mozilla/5.0 (compatible; BTstats/1.0)"}
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            resp.raise_for_status()
            return resp.content
        except requests.exceptions.Timeout:
            logging.warning(f"Timeout fetching {url} (attempt {attempt}/{retries})")
            time.sleep(2)
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed ({url}): {e}")
            break
    return None

# # if you still want to skip pure national–only entries, keep this set
# COUNTRY_CODES = {
#     "SWE","DEN","NOR","FIN","GER","FRA","BEL","IRL","THA","PUR","NED",
#     "USA","ENG","WAL","SMR","SIN","ROU","SUI"
# }

# match a line that begins with a number, then “anything (except comma)” for the name,
# then a comma, then the rest up to end-of-line as the club
# PART_RE = re.compile(
#     r'^\s*\d+\s+([^,]+?)\s*,\s*([^\r\n]+)',
#     re.MULTILINE
# )

PART_RE = re.compile(
    r'^\s*(?:\d+\s+)?([^,]+?)\s*,\s*(\S.*)$',
    re.MULTILINE
)

def extract_columns(page):
    """
    Split a page into left/right halves so we can catch two-column layouts.
    """
    w, h = page.width, page.height
    left   = page.crop((0,    0,   w/2, h)).extract_text() or ""
    right  = page.crop((w/2,  0,   w,   h)).extract_text() or ""
    return left, right

def parse_players_pdf(pdf_bytes: bytes) -> tuple[list[dict], int | None]:
    """
    Extract participant entries from PDF.  Returns list of
    {"raw_name": ..., "club_name": ...}.
    """
    participants = []
    full_text = []

    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            left, right = extract_columns(page)
            # drop those section headers entirely:
            for block in (left, right):
                block = re.sub(r'Directly qualified.*$', "", block, flags=re.M)
                block = re.sub(r'Group stage.*$',        "", block, flags=re.M)
                full_text.append(block)

    text = "\n".join(full_text)

    # extract expected participant count from header, in any of several languages
    # 'entries', 'anmälda', 'påmeldte', 'tilmeldte'
    expected_count = None
    m = re.search(
        r'\b\d{4}-\d{2}-\d{2}(?:\s+\d{2}:\d{2})?\s*\(\s*(\d+)\s+[^\d()]+\)',
        text
    )
    if m:
        expected_count = int(m.group(1))
        logging.info(f"Expected participants: {expected_count}")    

    # # find every “rank name, club” line
    # for m in PART_RE.finditer(text):
    #     name_part = m.group(1).strip()
    #     club_part = m.group(2).strip()

    #     # if the “club” is *exactly* one of the country codes, we skip it
    #     # if club_part.upper() in COUNTRY_CODES:
    #     #     logging.info(f"parse_players_pdf: ❌ Skipping national entry “{name_part}, {club_part}”")
    #     #     continue

    #     # OK, keep it
    #     raw = sanitize_name(name_part)
    #     participants.append({
    #         "raw_name":   raw,
    #         "club_name":  club_part
    #     })

    #     # ── replace the regex loop with this ──
    # for line in text.splitlines():
    #     if ',' not in line:
    #         continue
    #     name_part, club_part = line.split(',', 1)
    #     participants.append({
    #         "raw_name":  sanitize_name(name_part),
    #         "club_name": club_part.strip()
    #     })

    for line in text.splitlines():
        m = PART_RE.match(line)
        if not m:
            continue

        # group(1) is the name (without any leading “123 ” prefix)
        raw_name   = sanitize_name(m.group(1))
        club_name  = m.group(2).strip()

        participants.append({
            "raw_name":  raw_name,
            "club_name": club_name
        })    

    # log every candidate we actually found
    # logging.info(f"parse_players_pdf: ✅ Keeping {len(participants)} entries:")

    # for i, p in enumerate(participants, 1):
    #     logging.info(f"   [{i:02d}] {p['raw_name']}  /  {p['club_name']}")

    return participants, expected_count