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

    for t in tournaments[:limit]:
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
            for cls in found:
                res = cls.upsert(cursor)
                db_results.append(res)

            print(f"✅ Processed {len(found)} classes for {t.shortname} (id: {t.tournament_id})")
            logging.info(f"Processed {len(found)} classes for {t.shortname} (id: {t.tournament_id})")

        except Exception as e:
            logging.error(f"Exception during class scraping for {t.shortname} (id: {t.tournament_id}): {e}")
            print(f"❌ Exception scraping {t.shortname} (id: {t.tournament_id}): {e}")
            continue
        
    conn.commit()
    print_db_insert_results(db_results)
    conn.close()

# ---------- Doubles detection (IDs only) ----------
def detect_type_id(
    shortname: str, 
    longname: str
) -> int:
    
    l = (longname or "").lower()
    if any(k in l for k in {"double", "doubles", "dubbel", "dubble", "dobbel", "dobbelt"}):
        return 2  # Doubles
    up = (shortname or "").upper()
    if any(tag in re.split(r"[^A-Z]+", up) for tag in {"HD", "DD", "WD", "MD", "MXD"}):
        return 2  # Doubles
    return 1  # Singles

def scrape_classes_for_tournament_ondata(
    tournament: Tournament
) -> List[TournamentClass]: 
    
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
        raise ValueError(f"No Resultat frame found in {base}")

    # 2) fetch inner page
    inner_url = urljoin(base, frame["src"])
    resp2 = requests.get(inner_url)
    resp2.raise_for_status()
    soup2 = BeautifulSoup(resp2.text, "html.parser")

    table = soup2.find("table", attrs={"width": "100%"})
    if not table:
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

    # NEW: infer both ids here without extra HTTP calls
    structure_id    = _infer_structure_id_from_row(row)
    type_id         = detect_type_id(short, desc)

    return TournamentClass(
        tournament_class_id_ext = ext_id,
        tournament_id           = tournament_id,
        type_id                 = type_id,
        structure_id            = structure_id,
        date                    = class_date,
        longname                = desc,
        shortname               = short,
        gender                  = None,
        max_rank                = None,
        max_age                 = None,
    )

def _infer_structure_id_from_row(row) -> Optional[int]:
    """
    Inspect all links in this table row. If any link has ?stage=3/4/5/6,
    derive structure from the presence of stages:
      - groups = (3 or 4)
      - ko     = (5)
    Return 1,2,3 or None.
    """
    stages: set[int] = set()
    for a in row.find_all("a", href=True):
        try:
            qs = parse_qs(urlparse(a["href"]).query)
            st = qs.get("stage", [None])[0]
            if st and str(st).isdigit():
                stages.add(int(st))
        except Exception:
            continue

    has_groups = (3 in stages) or (4 in stages)
    has_ko     = (5 in stages)

    if has_groups and has_ko:
        return 1 # STRUCT_GROUPS_AND_KO
    if has_groups and not has_ko:
        return 2 # STRUCT_GROUPS_ONLY
    if (not has_groups) and has_ko:
        return 3 # STRUCT_KO_ONLY
    return None