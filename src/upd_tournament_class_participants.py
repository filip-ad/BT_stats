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
from config import SCRAPE_CLASS_PARTICIPANTS_MAX_CLASSES

PDF_BASE = "https://resultat.ondata.se/ViewClassPDF.php"


def upd_tournament_class_participants():
    """
    Populate tournament_class_participant by downloading each class's 'stage=1' PDF.
    """
    conn, cursor = get_conn()
    logging.info("Starting participant update…")
    print("ℹ️  Updating tournament class participants…")

    # 1) Load all classes (optionally limited)
    classes = TournamentClass.cache_all(cursor)
    if SCRAPE_CLASS_PARTICIPANTS_MAX_CLASSES is not None:
        classes = classes[:SCRAPE_CLASS_PARTICIPANTS_MAX_CLASSES]

    # 2) Prepare caches for club lookup & license lookup
    club_map    = Club.cache_name_map(cursor)
    license_map = PlayerLicense.cache_name_club_map(cursor)

    results = []
    start_ts = time.time()

    for tc in classes:
        # Skip if we don't have the external class ID
        if not tc.tournament_class_id_ext:
            logging.warning(f"Skipping class {tc.tournament_class_id}: no external class ID")
            continue

        # Build URL: always pull stage=1 (the participants list)
        pdf_url = f"{PDF_BASE}?classID={tc.tournament_class_id_ext}&stage=1"

        # Normalize date
        class_date = (
            tc.date 
            if isinstance(tc.date, date) 
            else datetime.fromisoformat(tc.date).date()
        )

        pdf_bytes = download_pdf(pdf_url)
        if not pdf_bytes:
            logging.error(f"❌ PDF download failed for class_ext={tc.tournament_class_id_ext}")
            results.append({
                "status": "failed",
                "key":      tc.tournament_class_id_ext,
                "reason":   "PDF download failed"
            })
            continue

        participants = parse_players_pdf(pdf_bytes)

        # ←—— NEW: log every candidate at INFO level
        logging.info(f"Class {tc.shortname} ({tc.tournament_class_id_ext}): {len(participants)} raw candidates:")
        for idx, part in enumerate(participants, start=1):
            logging.info(f"    [{idx:02d}] {part['raw_name']}  /  {part['club_name']}")


        print(f"✅ Parsed {len(participants)} participants for class {tc.shortname} (class_ext={tc.tournament_class_id_ext} in tournament id {tc.tournament_id})")

        #     # ── INSERT YOUR DEBUG SNIPPET HERE ──
        # expected_count = 81  # or however many you know this class should have
        # if len(participants) != expected_count:
        #     print(f"⚠️  Expected {expected_count} but found {len(participants)}—dumping unmatched lines for inspection:")
        #     from io import BytesIO
        #     import pdfplumber

        #     # Re-open pages and reconstruct lines
        #     for page in pdfplumber.open(BytesIO(pdf_bytes)).pages:
        #         for line in extract_lines(page):
        #             if not ENTRY_RE.search(line):
        #                 print("UNMATCHED LINE:", line)
        #     raise RuntimeError(f"Parsed count mismatch: expected {expected_count}, got {len(participants)}")
        # # ────────────────────────────────────

        for part in participants:

            raw_name  = part["raw_name"]
            club_name = part["club_name"].strip()

            club_key = Club._normalize(club_name)

            # print(f"Processing participant: {raw_name} from normalized club {club_key}")

            club = club_map.get(club_key)
            
            if not club:
                logging.warning(f"Unknown club '{club_name}' (normalized '{club_key}') for player '{raw_name}'")
                results.append({
                    "status": "failed",
                    "key":    f"{tc.tournament_class_id}_{raw_name}",
                    "reason": "Club not found"
                })
                continue

            player_id = PlayerLicense.find_player_id(
                cursor,
                license_map,
                raw_name,
                club.club_id,
                class_date,
                fallback_to_latest=True,
                fuzzy_threshold=0.85
            )
            if not player_id:
                logging.warning(f"No license for player '{raw_name}'")
                results.append({
                    "status": "failed",
                    "key":    f"{tc.tournament_class_id}_{raw_name}",
                    "reason": "No license found for player"
                })
                continue

            cursor.execute(
                "INSERT OR IGNORE INTO tournament_class_participant (tournament_class_id, player_id) VALUES (?, ?)",
                (tc.tournament_class_id, player_id)
            )
            status = "success" if cursor.rowcount else "skipped"
            results.append({
                "status": status,
                "key":    f"{tc.tournament_class_id}_{player_id}",
                "reason": "Added" if status == "success" else "Already exists"
            })

    conn.commit()
    elapsed = time.time() - start_ts
    print(f"✅ Participant update complete in {elapsed:.2f}s")
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

def parse_players_pdf(pdf_bytes: bytes) -> list[dict]:
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

    return participants