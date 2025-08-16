# src/download_pdfs.py

import logging
import re
import datetime
from bs4 import BeautifulSoup
from typing import List, Optional
from urllib.parse import urljoin, urlparse, parse_qs
import requests
import os
from utils import parse_date
from config import DOWNLOAD_PDF_NBR_OF_CLASSES, DOWNLOAD_PDF_TOURNAMENT_ID_EXT

from db import get_conn
from models.tournament_class import TournamentClass
from models.tournament import Tournament

# Download filters
DOWNLOAD_PDF_NBR_OF_CLASSES = 1  # Download PDF:s for max this many classes before breaking
DOWNLOAD_PDF_TOURNAMENT_ID_EXT = 678  # Download all PDF:s for this tournament with external ID; Set to None to process all

def _is_pdf_available(base_url: str, pattern: str, cid: int, session: requests.Session, timeout=10) -> bool:
    url = urljoin(base_url if base_url.endswith("/") else (base_url + "/"), pattern.format(cid=cid))
    try:
        r = session.head(url, timeout=timeout, allow_redirects=True)
        if r.status_code >= 400 or "pdf" not in r.headers.get("Content-Type", "").lower():
            r = session.get(url, timeout=timeout, allow_redirects=True, stream=True)
        ok = (200 <= r.status_code < 300) and ("pdf" in r.headers.get("Content-Type", "").lower())
        return ok
    except Exception:
        return False

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
        cols = row.find_all("td")
        if len(cols) < 4:
            continue
        day_txt = cols[0].get_text(strip=True)
        m = re.search(r"(\d+)", day_txt)
        if not m:
            continue
        day_num = int(m.group(1))
        try:
            class_date = base_date.replace(day=day_num)
        except ValueError:
            continue
        longname = cols[2].get_text(strip=True)
        a = cols[3].find("a", href=True)
        if not a or not a["href"]:
            continue
        qs = parse_qs(urlparse(a["href"]).query)
        ext_id = int(qs["classID"][0]) if "classID" in qs and qs["classID"][0].isdigit() else None
        shortname = a.get_text(strip=True)
        cls = TournamentClass(
            tournament_class_id_ext=ext_id,
            tournament_id=tournament.tournament_id,
            type=None,
            date=class_date,
            longname=longname,
            shortname=shortname,
            gender=None,
            max_rank=None,
            max_age=None,
        )
        classes.append(cls)
    return classes

def download_all_pdfs():
    """
    Downloads all available PDFs for stages 1-6 for classes in all tournaments.
    Organizes them in the specified folder structure.
    """
    root_dir = "PDF"
    os.makedirs(root_dir, exist_ok=True)
    
    conn, cursor = get_conn()
    # Assuming Tournament has a method to get all tournaments; adjust if needed
    # For example, if get_by_status is used, pass all possible statuses
    tournaments = Tournament.get_by_status(cursor, ["PLANNED", "ONGOING", "ENDED"])  # Adjust statuses as needed
    
    if not tournaments:
        print("⚠️ No tournaments found in database.")
        return
    
    print(f"ℹ️ Found {len(tournaments)} tournaments. Downloading PDFs...")
    
    for t in tournaments:
        if not t.url:
            logging.warning(f"Skipping tournament missing URL: {t.shortname} (id: {t.tournament_id})")
            continue
        
        base = t.url if t.url.endswith("/") else t.url + "/"
        
        try:
            # Fetch outer frameset to extract tour_id_ext
            resp1 = requests.get(base)
            resp1.raise_for_status()
            soup1 = BeautifulSoup(resp1.text, "html.parser")
            frame = soup1.find("frame", {"name": "Resultat"})
            if not frame or not frame.get("src"):
                logging.warning(f"No Resultat frame in {base}")
                continue
            frame_src = frame["src"]
            qs = parse_qs(urlparse(frame_src).query)
            # Possible keys for tournament external ID; adjust based on actual site
            possible_keys = ["TourID", "tid", "eventID", "tournamentID", "classID"]
            tour_id_ext = None
            for key in possible_keys:
                if key in qs and qs[key][0].isdigit():
                    tour_id_ext = int(qs[key][0])
                    break
            if not tour_id_ext:
                logging.warning(f"Could not extract tour_id_ext for {t.shortname} from {frame_src}")
                continue
            
            # Apply tournament filter
            if DOWNLOAD_PDF_TOURNAMENT_ID_EXT is not None and tour_id_ext != DOWNLOAD_PDF_TOURNAMENT_ID_EXT:
                continue
            
            tournament_folder = os.path.join(root_dir, f"{t.shortname}_{tour_id_ext}")
            os.makedirs(tournament_folder, exist_ok=True)
            
            # Scrape classes
            classes = scrape_classes_for_tournament_ondata(t)
            
            session = requests.Session()
            
            # Limit number of classes
            for cls in classes[:DOWNLOAD_PDF_NBR_OF_CLASSES]:
                cid = cls.tournament_class_id_ext
                if not cid:
                    continue
                
                for stage in range(1, 7):
                    pattern = "ViewClassPDF.php?classID={cid}&stage=" + str(stage)
                    if _is_pdf_available(base, pattern, cid, session):
                        pdf_url = urljoin(base, pattern.format(cid=cid))
                        r = session.get(pdf_url, stream=True)
                        if r.status_code == 200 and "pdf" in r.headers.get("Content-Type", "").lower():
                            stage_folder = os.path.join(tournament_folder, f"Stage_{stage}")
                            os.makedirs(stage_folder, exist_ok=True)
                            pdf_filename = f"{tour_id_ext}_{cid}_Stage_{stage}.pdf"
                            pdf_path = os.path.join(stage_folder, pdf_filename)
                            with open(pdf_path, "wb") as f:
                                for chunk in r.iter_content(chunk_size=8192):
                                    f.write(chunk)
                            print(f"Downloaded {pdf_path}")
            
            print(f"✅ Processed PDFs for {t.shortname} (tour_id_ext: {tour_id_ext})")
        
        except Exception as e:
            logging.error(f"Exception processing {t.shortname} (id: {t.tournament_id}): {e}")
            print(f"❌ Exception for {t.shortname}: {e}")
            continue
    
    conn.close()

if __name__ == "__main__":
    download_all_pdfs()