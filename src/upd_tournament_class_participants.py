# src/upd_tournament_participants.py

import logging
import requests
import pdfplumber
import re
from io import BytesIO
from datetime import datetime, date
import time
from urllib.parse import urljoin
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
        print(f"✅ Parsed {len(participants)} participants for class {tc.shortname} (class_ext={tc.tournament_class_id_ext} in tournament id {tc.tournament_id})")

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
                    "reason": "License not found"
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


# def parse_players_pdf(pdf_bytes: bytes) -> list[dict]:
#     """
#     Extract (raw_name, club_name) tuples from a participants PDF.
#     """
#     participants = []
#     with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
#         for page in pdf.pages:
#             text = page.extract_text() or ""
#             for line in text.splitlines():
#                 raw = line.strip().replace("\u00A0", " ")
#                 if not raw or raw.startswith("Deltagarlista"):
#                     continue
#                 m = re.match(r'^\s*\d+\s+([A-Za-zÅÄÖåäö\- ]+),\s+(.+)$', raw)
#                 if not m:
#                     continue
#                 fullname, club = m.groups()
#                 participants.append({
#                     "raw_name":  sanitize_name(fullname.strip()),
#                     "club_name": club.strip()
#                 })
#     return participants

# Country codes to ignore as “clubs”
COUNTRY_CODES = {
    "SWE","DEN","NOR","FIN","GER","FRA","BEL","IRL","THA","PUR","NED",
    "USA","ENG","WAL","SMR","SIN","ROU","SUI"
}

# Compile once at module‐scope:
PART_RE = re.compile(
    r'\b\d+\s+'                   # rank
    r'([\wÅÄÖåäö\-\s]+?)'         # name
    r'\s*,\s*'                    # comma
    r'([^\n\r]+?)'                # everything up to line break = club
    r'(?:\r?\n|$)'
)

def extract_columns(page):
    bbox = page.crop((0, 0, page.width/2, page.height))   # left half
    left_text  = bbox.extract_text()
    bbox       = page.crop((page.width/2, 0, page.width, page.height))  # right half
    right_text = bbox.extract_text()
    return (left_text or "", right_text or "")

def parse_players_pdf(pdf_bytes):
    participants = []
    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            left, right = extract_columns(page)
            for text in (left, right):
                # drop section headers
                text = re.sub(r'Directly qualified.*?$', '', text, flags=re.M)
                text = re.sub(r'Group stage.*?$', '', text, flags=re.M)
                for m in PART_RE.finditer(text):
                    fullname, club = m.groups()
                    name = sanitize_name(fullname)
                    club = club.strip()
                    if club.upper() in COUNTRY_CODES:
                        continue
                    participants.append({"raw_name": name, "club_name": club})
    return participants