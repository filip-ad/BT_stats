# src/upd_tournament_participants.py

import logging
import requests
import pdfplumber
import re
from io import BytesIO
from datetime import datetime
from time import sleep
import time
from db import get_conn
from utils import print_db_insert_results, sanitize_name
from models.player_license import PlayerLicense
from models.club import Club
from config import SCRAPE_CLASS_PARTICIPANTS_MAX_CLASSES


def download_pdf(url, retries=3, timeout=30):
    """
    Download a PDF from URL with retry and timeout logic.
    Returns raw PDF bytes or None if all attempts fail.
    """
    headers = {"User-Agent": "Mozilla/5.0 (compatible; BTstats/1.0)"}
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            resp.raise_for_status()
            return resp.content
        except requests.exceptions.Timeout:
            print(f"⚠️ Timeout fetching {url}, attempt {attempt+1}/{retries}")
            sleep(2)
        except requests.exceptions.RequestException as e:
            print(f"❌ Request failed: {e}")
            break
    return None

def parse_players_pdf(pdf_bytes):
    participants = []
    with pdfplumber.open(pdf_bytes) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            for line in text.splitlines():
                raw = line.strip().replace("\u00A0", " ")
                if not raw or raw.startswith("Deltagarlista"):
                    continue
                m = re.match(r'^\s*\d+\s+([A-Za-zÅÄÖåäö\- ]+),\s+(.+)$', raw)
                if not m:
                    continue
                fullname, club = m.groups()
                participants.append({
                    "raw_name": sanitize_name(fullname.strip()),
                    "club_name": club.strip()
                })
    return participants

def upd_tournament_participants():
    conn, cursor = get_conn()
    logging.info("Updating tournament class participating players...")
    print("ℹ️  Updating tournament class participating players...")

    start_time = time.time()

    # Cache for fast lookups
    club_name_map = Club.cache_name_map(cursor)
    licenses_cache = PlayerLicense.cache_name_club_map(cursor)

    cursor.execute("""
        SELECT 
            tc.tournament_class_id,
            tc.class_short,
            tc.players_url,
            tc.date,
            t.name AS tournament_name
        FROM tournament_class tc
        JOIN tournament t 
            ON tc.tournament_id = t.tournament_id
        WHERE tc.players_url IS NOT NULL 
        AND TRIM(tc.players_url) != ''
        ORDER BY tc.tournament_class_id
    """)
    classes = cursor.fetchall()

    if SCRAPE_CLASS_PARTICIPANTS_MAX_CLASSES is not None:
        classes = classes[:SCRAPE_CLASS_PARTICIPANTS_MAX_CLASSES]

    # print(f"ℹ️  Processing {len(classes)} tournament classes (MAX_CLASSES={SCRAPE_CLASS_PARTICIPANTS_MAX_CLASSES})")

    insert_results = []

    for class_id, class_name, players_url, class_date, tournament_name in classes:
        
        tournament_class_date = class_date
        if isinstance(class_date, str):
            class_date = datetime.fromisoformat(class_date).date()

        # print(f"Processing class {class_id} with URL: {players_url}")

        pdf_bytes = download_pdf(players_url)
        if not pdf_bytes:
            logging.error(f"❌ Failed to download PDF for class {class_name} of tournament {tournament_name}: {players_url}")
            continue

        participants = parse_players_pdf(BytesIO(pdf_bytes))
        print(f"✅ Parsed {len(participants)} participants for class {class_name} of tournament {tournament_name} (URL: {players_url})")

        for participant in participants:
            # club_name = participant["club_name"].strip()
            club_obj = club_name_map.get(participant["club_name"].strip())
            if not club_obj:
                logging.warning(f"❗ Unknown club: {participant['club_name']} for class {class_name} of tournament {tournament_name}")
                insert_results.append({
                    "status": "failed",
                    "key": f"{class_id}_{participant['firstname']}_{participant['lastname']}",
                    "reason": f"Could not resolve club"
                })
                continue
            club_id = club_obj.club_id

            player_id = PlayerLicense.find_player_id(
                cursor,
                licenses_cache,
                participant["raw_name"],
                club_id,
                tournament_class_date,
                fallback_to_latest=True,
                fuzzy_threshold=0.85
            )

            if not player_id:
                logging.warning(
                    f"Player not found or no valid license: {participant['firstname']} {participant['lastname']} / {participant['club_name']} in class {class_name} of tournament {tournament_name}"
                )
                insert_results.append({
                    "status": "failed",
                    "key": f"{class_id}_{player_id}",
                    "reason": "No valid license found for player"
                })
                continue

            cursor.execute("""
                INSERT OR IGNORE INTO tournament_class_participant (tournament_class_id, player_id)
                VALUES (?, ?)
            """, (class_id, player_id))

            if cursor.rowcount == 0:
                logging.info(f"Participant {participant['raw_name']} already exists in class {class_name} of tournament {tournament_name}")
                insert_results.append({
                    "status": "skipped",
                    "key": f"{class_id}_{player_id}",
                    "reason": "Participant already exists"
                })
                continue

            insert_results.append({
            "status": "success",
            "key": f"{class_id}_{player_id}",
            "reason": "Participant successfully added"
        })

    conn.commit()
    conn.close()
    time_taken = time.time() - start_time
    print(f"✅ Tournament class participants update complete in {time_taken:.2f} seconds")
    print_db_insert_results(insert_results)
