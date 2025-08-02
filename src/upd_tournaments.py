# src/tournament.py

import logging
from datetime import datetime
import re
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import urljoin
from utils import setup_driver, parse_date
from db import get_conn, save_to_db_tournaments
from config import SCRAPE_TOURNAMENTS_ORDER, SCRAPE_TOURNAMENTS_URL, SCRAPE_TOURNAMENTS_START_DATE

def upd_tournaments():

    conn, cursor = get_conn()
    driver = setup_driver()
    
    try:
        logging.info("Starting tournament scraping process...")
        print("ℹ️  Starting tournament scraping process...")
        tournaments = scrape_tournaments(driver)
        
        if tournaments:
            logging.info(f"Successfully scraped {len(tournaments)} tournaments.")
            print(f"✅ Successfully scraped {len(tournaments)} tournaments.")
            print_details_tournament(tournaments)

            try: 
                db_results = save_to_db_tournaments(cursor, tournaments)
                
                if db_results:
                    print_db_insert_results(db_results)

                else: 
                    logging.warning("No tournaments saved to database.")
                    print("⚠️ No tournaments saved to database.")

            except Exception as e:
                logging.error(f"Error during database insertion: {e}") 
                print(f"❌ Error during database insertion: {e}")

        else:
            logging.warning("No tournaments scraped.")
            print("⚠️ No tournaments scraped.")

    except Exception as e:
        logging.error(f"Exception during tournament scraping: {e}")
        print(f"❌ Exception during tournament scraping: {e}")

    finally:
        driver.quit()
        conn.commit()
        conn.close()

def scrape_tournaments(driver):

    tournaments = []

    try:
        driver.get(SCRAPE_TOURNAMENTS_URL)

        WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.ID, "listtable")))  # Wait until the table is present

        rows = driver.find_elements(By.CSS_SELECTOR, "#listtable tr")
        current_date = datetime.now().date()  # Dynamically set to run date
        start_cutoff = parse_date(SCRAPE_TOURNAMENTS_START_DATE)  # Define the cutoff date

        logging.info(f"Scraping tournaments starting from {start_cutoff} to {current_date} in {SCRAPE_TOURNAMENTS_ORDER.lower()} order...")
        print(f"ℹ️  Scraping tournaments starting from {start_cutoff} to {current_date} in {SCRAPE_TOURNAMENTS_ORDER.lower()} order...")

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
            start_dt = parse_date(start_str)
            end_dt = parse_date(end_str)
            if not start_dt or not end_dt:
                logging.warning(f"Skipping tournament {name} due to invalid start or end date")
                print(f"⚠️   Skipping tournament {name} due to invalid start or end date")
                continue

            # Skip tournaments that start before the cutoff date
            if start_dt < start_cutoff:
                logging.debug(f"Skipping tournament {name} because it starts before the cutoff date ({start_cutoff})")
                continue

            # Determine the status of the tournament (ONGOING, UPCOMING, ENDED)
            status = determine_tournament_status(current_date, start_dt, end_dt)
            if status is None:
                continue

            # Extract tournament URL from onclick
            onclick = row.get_attribute("onclick")
            full_url = extract_tournament_url(onclick)
            if not full_url:
                logging.warning(f"Skipping tournament {name} (Status: {status}) due to invalid URL format")
                print(f"⚠️   Skipping tournament {name} (Status: {status}) due to invalid URL format")
                continue

            # Extract ondata_id from URL
            ondata_id = extract_ondata_id(full_url)
            if not ondata_id:
                logging.warning(f"Skipping tournament {name} (Status: {status}) due to missing ondata_id")
                continue

            match = re.search(r"https://resultat\.ondata\.se/(\w+)/?$", full_url)    
            if match:
                ondata_id = match.group(1)
            else:
                logging.warning("Failed to extract ondata_id from URL: %s", full_url)
                print(f"⚠️ Failed to extract ondata_id from URL: {full_url}")
                continue

            # Construct the tournament dictionary
            tournaments.append({
                "name": name,                   # Tournament name   
                "start_date": start_str,        # The string format of the start date
                "end_date": end_str,            # The string format of the end date
                "city": city,                   # City name
                "arena": arena,                 # Arena name
                "country_code": country_code,   # Country code 3 char (e.g., 'SWE' for Sweden)
                "ondata_id": ondata_id,         # Extracted ondata_id
                "url": full_url,                # Full tournament URL
                "status": status,               # Tournament status: 'ONGOING', 'UPCOMING', or 'ENDED'
                "start_dt": start_dt,           # The datetime object for start date (used for comparisons)
                "end_dt": end_dt,               # The datetime object for end date (used for comparisons)
            })

        # Sort tournaments by start_dt based on SCRAPE_TOURNAMENTS_ORDER
        if SCRAPE_TOURNAMENTS_ORDER.lower() == 'oldest':
            tournaments = sorted(tournaments, key=lambda x: x['start_dt'])
        else:
            tournaments = sorted(tournaments, key=lambda x: x['start_dt'], reverse=True)

        return tournaments

    except Exception as e:
        logging.error(f"Exception during scraping: {e}")
        print(f"❌ Exception during scraping: {e}")
        return []


def determine_tournament_status(current_date, start_dt, end_dt):
    if end_dt < current_date:
        return "ENDED"
    elif start_dt <= current_date <= end_dt:
        return "ONGOING"
    elif start_dt > current_date:
        return "UPCOMING"
    else:
        return None

def extract_ondata_id(url):
    match = re.search(r"https://resultat\.ondata\.se/(\w+)/?$", url)
    if match:
        return match.group(1)
    else:
        logging.error("Failed to extract ondata_id from URL: %s", url)
        return None

def extract_tournament_url(onclick):
    if not onclick or "document.location=" not in onclick:
        return None
    match = re.search(r"document\.location=(?:'|\"|)([^'\"]+)(?:'|\"|)", onclick)
    if match:
        return urljoin("https://resultat.ondata.se", match.group(1))
    return None

def print_details_tournament(tournaments):
    for tournament in tournaments:
        logging.debug(f"Tournament: {tournament['name']}")
        logging.debug(f"Start Date: {tournament['start_date']} | End Date: {tournament['end_date']}")
        logging.debug(f"City: {tournament['city']} | Arena: {tournament['arena']}")
        logging.debug(f"Country: {tournament['country_code']} | OnData ID: {tournament['ondata_id']}")
        logging.debug(f"URL: {tournament['url']} | Status: {tournament['status']}")

def print_db_insert_results(status_list):
    success_count = sum(1 for status in status_list if status["status"] == "success")
    failed_count = sum(1 for status in status_list if status["status"] == "failed")
    skipped_count = sum(1 for status in status_list if status["status"] == "skipped")

    logging.info(f"Database summary: {success_count} tournaments inserted, {failed_count} failed, {skipped_count} skipped")
    print(f"ℹ️  Database summary: {success_count} tournaments inserted, {failed_count} failed, {skipped_count} skipped")
