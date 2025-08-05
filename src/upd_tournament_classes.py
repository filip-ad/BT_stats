# src/upd_classes.py

import logging
import re
import datetime
from bs4 import BeautifulSoup
from typing import List, Optional

from urllib.parse import urljoin, urlparse, parse_qs
import requests
from utils import parse_date, print_db_insert_results
from config import SCRAPE_CLASSES_MAX_TOURNAMENTS
from db import get_conn

from models.tournament_class import TournamentClass
from models.tournament import Tournament

def upd_tournament_classes():
    """
    Main entry point: scrapes classes for ongoing/ended tournaments,
    saves them to DB, and prints a summary of inserts.
    """
    conn, cursor = get_conn()
    db_results = []

    tournaments = Tournament.get_by_status(cursor, ["ONGOING", "ENDED"])
    if not tournaments:
        print("⚠️  No tournaments found in database.")
        return
    
    limit = SCRAPE_CLASSES_MAX_TOURNAMENTS or len(tournaments)

    print(f"ℹ️  Found {len(tournaments)} tournaments. Scraping classes for up to {limit} tournaments...")
    # all_classes: List[TournamentClass] = []

    for t in tournaments[:limit]:

        # Skip tournaments lacking essential info
        if not t.url:
            logging.warning(f"Skipping tournament missing URL: {t.shortname} (id: {t.tournament_id})")
            db_results.append({
                "status":   "skipped",
                "key":      t.shortname,
                "reason":   "Tournament missing URL"
            })
            continue
    
        try:
            found = scrape_classes_for_tournament_ondata(t)
            print(f"✅ Scraped {len(found)} classes for {t.shortname} (id: {t.tournament_id})")
            logging.info(f"Scraped {len(found)} classes for {t.shortname} (id: {t.tournament_id})")
            for cls in found:
                res = cls.save_to_db(cursor)
                db_results.append(res)

        except Exception as e:
            logging.error(f"Exception during class scraping for {t.shortname} (id: {t.tournament_id}): {e}")
            print(f"❌ Exception scraping {t.shortname} (id: {t.tournament_id}): {e}")
            continue
        
    conn.commit()
    print_db_insert_results(db_results)
    conn.close()

def scrape_classes_for_tournament_ondata(tournament: Tournament) -> List[TournamentClass]:
    # Ensure base ends with slash
    base = tournament.url
    if not base.endswith("/"):
        base += "/"

    # 1) fetch outer frameset
    resp1 = requests.get(base)
    resp1.raise_for_status()
    soup1 = BeautifulSoup(resp1.text, "html.parser")
    frame = soup1.find("frame", {"name": "Resultat"})
    if not frame or not frame.get("src"):
        # logging.warning(f"No Resultat frame in {base}")
        raise ValueError(f"No Resultat frame found in {base}")

    # 2) fetch inner page
    inner_url = urljoin(base, frame["src"])
    resp2 = requests.get(inner_url)
    resp2.raise_for_status()
    soup2 = BeautifulSoup(resp2.text, "html.parser")

    table = soup2.find("table", attrs={"width": "100%"})
    if not table:
        # logging.warning(f"No class table in {inner_url}")
        raise ValueError(f"No class table found in {inner_url}")

    rows = table.find_all("tr")[2:]
    base_date = parse_date(tournament.startdate, context="infer_full_date")
    if not base_date:
        raise ValueError(f"Invalid start date for tournament {tournament.shortname} ({tournament.tournament_id})")

    classes = []
    for row in rows:
        cls = _parse_row(row, tournament.tournament_id, base_date)
        if cls:
            classes.append(cls)
    return classes

def _parse_row(row, tournament_id: int, base_date: datetime.date) -> Optional[TournamentClass]:
    cols = row.find_all("td")
    if len(cols) < 4:
        return None

    day_txt = cols[0].get_text(strip=True)
    m = re.search(r"(\d+)", day_txt)
    if not m:
        return None
    day_num = int(m.group(1))
    try:
        class_date = base_date.replace(day=day_num)
    except ValueError:
        return None

    desc = cols[2].get_text(strip=True)
    a = cols[3].find("a", href=True)
    if not a or not a["href"]:
        return None

    qs = parse_qs(urlparse(a["href"]).query)
    ext_id = int(qs["classID"][0]) if "classID" in qs and qs["classID"][0].isdigit() else None
    short = a.get_text(strip=True)

    return TournamentClass(
        tournament_class_id_ext = ext_id,
        tournament_id           = tournament_id,
        type                    = None,
        date                    = class_date,
        longname                = desc,
        shortname               = short,
        gender                  = None,
        max_rank                = None,
        max_age                 = None,
    )



# def infer_full_date(day_str, tournament_start):
#     """
#     Day_str is like '26', tournament_start may be a date or YYYY-MM-DD string.
#     We parse start into a date, then replace day.
#     """
#     dt0 = parse_date(tournament_start, context="infer_full_date")
#     if not dt0:
#         return None

#     m = re.search(r"\d+", day_str)
#     if not m:
#         return None
#     day = int(m.group())

#     try:
#         return dt0.replace(day=day)
#     except ValueError:
#         logging.warning(f"Invalid day {day} for month {dt0.month}")
#         return None