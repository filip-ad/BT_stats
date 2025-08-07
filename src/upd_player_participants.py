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
    club_map        = Club.cache_name_map(cursor)
    license_map     = PlayerLicense.cache_name_club_map(cursor)
    player_name_map = Player.cache_name_map(cursor)

    results = []

    for tc in classes:
        if not tc.tournament_class_id_ext:
            logging.warning(f"Skipping class {tc.tournament_class_id}: no external class ID")
            continue

        # — Download phase —
        t1 = time.perf_counter()
        pdf_url    = f"{PDF_BASE}?classID={tc.tournament_class_id_ext}&stage=1"
        pdf_bytes  = download_pdf(pdf_url)
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
        participants, expected_count = parse_players_pdf(pdf_bytes)
        t4 = time.perf_counter()

        # parsed vs expected output
        symbol    = "✅" if expected_count is None or len(participants) == expected_count else "❌"
        count_str = f"{len(participants)}/{expected_count}" if expected_count is not None else str(len(participants))
        print(
            f"{symbol} Parsed {count_str} participants "
            f"for class {tc.shortname} "
            f"(class_ext={tc.tournament_class_id_ext} in tournament id {tc.tournament_id})"
        )
        logging.info(
            f"Parsed {count_str} participants for class {tc.shortname} "
            f"(class_ext={tc.tournament_class_id_ext} in tournament id {tc.tournament_id})"
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
                license_map,
                player_name_map
            )
            # attach idx & raw/club for later logging
            result.update({"idx": idx, "raw_name": raw_name, "club_name": club_name})
            class_results.append(result)
            results.append(result)  # only here, not again later

        t6 = time.perf_counter()

        # — Per‐participant sorted logging —
        status_order = {"success": 0, "raw": 1, "skipped": 2, "failed": 3}
        for r in sorted(
            class_results,
            key=lambda x: (status_order.get(x["status"], 99), x["reason"], x["idx"])
        ):
            prefix  = f"[{r['idx']:02d}] {r['raw_name']}  /  {r['club_name']}"
            padded  = f"{prefix:<50}"
            status  = r["status"].capitalize()
            reason  = r["reason"]
            # logging.info(f"{padded} : {status} - {reason}")
            suffix = f" - Player matched via {r['match_type']}" if r.get("match_type") and r["status"] == "success" else ""
            logging.info(f"{padded} : {status} - {reason}{suffix} ")

        # — Per‐class timing summary —
        logging.info(
            f"class {tc.shortname}: download={(t2-t1):.2f}s "
            f"parse={(t4-t3):.2f}s "
            f"db={(t6-t5):.2f}s"
        )

        # — Per‐class summary print & log —
        inserted = sum(1 for r in class_results if r["status"] == "success")
        skipped  = sum(1 for r in class_results if r["status"] == "skipped")
        failed   = [r for r in class_results if r["status"] == "failed"]
        raw_cnt  = sum(1 for r in class_results if r["status"] == "raw")

        print(f"   ✅ Inserted: {inserted}   ⏭️  Skipped: {skipped}   ❌ Failed: {len(failed)}")
        logging.info(
            f"Class {tc.shortname} ({tc.tournament_class_id_ext}): "
            f"Inserted {inserted}, Skipped {skipped}, Failed {len(failed)}, Raw {raw_cnt}"
        )

    # — overall commit & summary —
    conn.commit()
    total = time.perf_counter() - overall_start
    print(f"ℹ️  Participant update complete in {total:.2f}s")
    logging.info(f"Total update took {total:.2f}s")
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

# if you still want to skip pure national–only entries, keep this set
COUNTRY_CODES = {
    "SWE","DEN","NOR","FIN","GER","FRA","BEL","IRL","THA","PUR","NED",
    "USA","ENG","WAL","SMR","SIN","ROU","SUI"
}

# match a line that begins with a number, then “anything (except comma)” for the name,
# then a comma, then the rest up to end-of-line as the club
PART_RE = re.compile(
    r'^\s*\d+\s+([^,]+?)\s*,\s*([^\r\n]+)',
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

    # find every “rank name, club” line
    for m in PART_RE.finditer(text):
        name_part = m.group(1).strip()
        club_part = m.group(2).strip()

        # if the “club” is *exactly* one of the country codes, we skip it
        # if club_part.upper() in COUNTRY_CODES:
        #     logging.info(f"parse_players_pdf: ❌ Skipping national entry “{name_part}, {club_part}”")
        #     continue

        # OK, keep it
        raw = sanitize_name(name_part)
        participants.append({
            "raw_name":   raw,
            "club_name":  club_part
        })

    # log every candidate we actually found
    # logging.info(f"parse_players_pdf: ✅ Keeping {len(participants)} entries:")

    # for i, p in enumerate(participants, 1):
    #     logging.info(f"   [{i:02d}] {p['raw_name']}  /  {p['club_name']}")

    return participants, expected_count