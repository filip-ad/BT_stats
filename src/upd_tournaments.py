# src/tournament.py

import logging
from datetime import datetime
import re
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import urljoin
from utils import setup_driver, parse_date, print_db_insert_results
from db import get_conn
from config import SCRAPE_TOURNAMENTS_ORDER, SCRAPE_TOURNAMENTS_URL, SCRAPE_TOURNAMENTS_START_DATE
from models.tournament import Tournament

def upd_tournaments():

    conn, cursor = get_conn()
    driver = setup_driver()
    
    try:

        logging.info("Starting tournament scraping process...")
        print("ℹ️  Starting tournament scraping process...")

        tournaments = scrape_tournaments(driver)
        
        if not tournaments:
            logging.warning("No tournaments scraped.")
            print("⚠️  No tournaments scraped.")
            return

        logging.info(f"Successfully scraped {len(tournaments)} tournaments.")
        print(f"✅ Successfully scraped {len(tournaments)} tournaments.")

        db_results = []

        # No need for batch insert here, save each tournament individually
        for t in tournaments:
            result = t.save_to_db(cursor)
            db_results.append(result)

        print_db_insert_results(db_results)

    except Exception as e:
        logging.error(f"Exception during tournament scraping: {e}")
        print(f"❌ Exception during tournament scraping: {e}")

    finally:
        driver.quit()
        conn.commit()
        conn.close()

def scrape_tournaments(driver):
    """
    Returns a list of Tournament instances for all
    rows on the SCRAPE_TOURNAMENTS_URL page that
    meet the date/status criteria.
    """
    tournaments = []

    try:
        driver.get(SCRAPE_TOURNAMENTS_URL)

        WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.ID, "listtable")))  # Wait until the table is present

        rows = driver.find_elements(By.CSS_SELECTOR, "#listtable tr")
        today = datetime.now().date()  # Dynamically set to run date
        cutoff_date = parse_date(SCRAPE_TOURNAMENTS_START_DATE)  # Define the cutoff date

        logging.info(f"Scraping tournaments starting from {cutoff_date} to {today} in {SCRAPE_TOURNAMENTS_ORDER.lower()} order...")
        print(f"ℹ️  Scraping tournaments starting from {cutoff_date} to {today} in {SCRAPE_TOURNAMENTS_ORDER.lower()} order...")

        for i, row in enumerate(rows[1:], 1):
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) < 6:
                logging.debug(f"Skipping row {i} due to insufficient columns (likely a header row like the Archive header)")
                continue

            # Extract tournament details from the row
            name = cols[0].text.strip()
            start_str = cols[1].text.strip()
            end_str = cols[2].text.strip()
            city = cols[3].text.strip()
            arena = cols[4].text.strip()
            country_code = cols[5].text.strip()

            # Parse dates for comparison
            start_date = parse_date(start_str)
            end_date = parse_date(end_str)
            if not start_date or not end_date:
                logging.warning(f"Skipping tournament {name} due to invalid start or end date")
                print(f"⚠️ Skipping tournament {name} due to invalid start or end date")
                continue

            # Skip tournaments that start before the cutoff date
            if start_date < cutoff_date:
                logging.debug(f"Skipping tournament {name} because it starts before the cutoff date ({cutoff_date})")
                continue

            # status
            if end_date < today:
                status = "ENDED"
            elif start_date <= today <= end_date:
                status = "ONGOING"
            else:
                status = "UPCOMING"

            # Extract tournament URL from onclick
            onclick = row.get_attribute("onclick") or ""
            full_url = _extract_tournament_url(onclick)
            if not full_url:
                logging.warning(f"Skipping tournament {name} (Status: {status}) due to invalid URL format")
                print(f"⚠️  Skipping tournament {name} (Status: {status}) due to invalid URL format")
                continue

            # Extract ondata_id from URL
            ondata_id = _extract_ondata_id(full_url)
            if not ondata_id:
                logging.warning(f"Skipping tournament {name} (Status: {status}) due to missing ondata_id")
                continue

            tournament = Tournament.from_dict({
                "tournament_id": None,
                "name": name,
                "startdate": start_date,
                "enddate": end_date,
                "city": city,
                "arena": arena,
                "country_code": country_code,
                "ondata_id": ondata_id,
                "url": full_url,
                "status": status
            })

            tournaments.append(tournament)

        # Sort tournaments by start_date based on SCRAPE_TOURNAMENTS_ORDER
        reverse = SCRAPE_TOURNAMENTS_ORDER.lower() != "oldest"
        tournaments.sort(
        key=lambda t: t.startdate,  # <-- use the attribute, not a dict lookup
        reverse=reverse
        )

        return tournaments

    except Exception as e:
        logging.error(f"Exception during scraping: {e}")
        print(f"❌ Exception during scraping: {e}")
        return []

def _extract_ondata_id(url):
    match = re.search(r"https://resultat\.ondata\.se/(\w+)/?$", url)
    if match:
        return match.group(1)
    else:
        logging.error("Failed to extract ondata_id from URL: %s", url)
        return None

def _extract_tournament_url(onclick):
    if not onclick or "document.location=" not in onclick:
        return None
    match = re.search(r"document\.location=(?:'|\"|)([^'\"]+)(?:'|\"|)", onclick)
    if match:
        return urljoin("https://resultat.ondata.se", match.group(1))
    return None