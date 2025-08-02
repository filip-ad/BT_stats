import sqlite3
import requests
import pdfplumber
import re
from io import BytesIO
from datetime import datetime

DB_PATH = "table_tennis.db"

def download_pdf(url):
    resp = requests.get(url)
    resp.raise_for_status()
    return BytesIO(resp.content)

def parse_players_pdf(pdf_bytes):
    """Extract (firstname, lastname, year_born, club_name) from PDF"""
    participants = []
    with pdfplumber.open(pdf_bytes) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            for line in text.splitlines():
                # Example: '1  Andersson, Erik (2005) Mölndals BTK'
                m = re.match(r'^\s*\d+\s+([\wÅÄÖåäö\-]+),\s*([\wÅÄÖåäö\-]+)\s*\((\d{4})\)\s+(.+)$', line)
                if m:
                    lastname, firstname, year, club = m.groups()
                    participants.append({
                        "firstname": firstname.strip(),
                        "lastname": lastname.strip(),
                        "year_born": int(year),
                        "club_name": club.strip()
                    })
    return participants

def get_or_create_player(cursor, firstname, lastname, year_born):
    """Return player_id, create if not exists"""
    cursor.execute("""
        SELECT player_id FROM player
        WHERE firstname=? AND lastname=? AND year_born=?
    """, (firstname, lastname, year_born))
    row = cursor.fetchone()
    if row:
        return row[0]

    cursor.execute("""
        INSERT INTO player (firstname, lastname, year_born)
        VALUES (?, ?, ?)
    """, (firstname, lastname, year_born))
    return cursor.lastrowid

def populate_tournament_class_players(tournament_class_id, players_url):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    pdf_bytes = download_pdf(players_url)
    participants = parse_players_pdf(pdf_bytes)

    for p in participants:
        player_id = get_or_create_player(cursor, p["firstname"], p["lastname"], p["year_born"])
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO tournament_class_players (tournament_class_id, player_id)
                VALUES (?, ?)
            """, (tournament_class_id, player_id))
        except sqlite3.IntegrityError:
            pass

    conn.commit()
    conn.close()
    print(f"✅ Added {len(participants)} players for tournament_class_id={tournament_class_id}")

# Example usage:
populate_tournament_class_players(
    tournament_class_id=123, 
    players_url="https://resultat.ondata.se/ViewClassPDF.php?classID=29241&stage=1"
)
