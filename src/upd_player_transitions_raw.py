# src/upd_player_transitions_raw.py

import logging
from collections import defaultdict
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from utils import setup_driver, parse_date
from config import LICENSES_URL, SCRAPE_TRANSITIONS_NBR_OF_SEASONS, SCRAPE_TRANSITIONS_RUN_ORDER
from db import get_conn


def upd_player_transitions_raw():
    conn, cursor = get_conn()
    driver = setup_driver()
    try:
        logging.info("Scraping player transition raw data...")
        print("ℹ️  Scraping player transition raw data...")

        scrape_transitions(driver, cursor)

    except Exception as e:
        logging.error(f"An error occurred while scraping player transition raw data: {e}")
    finally:
        logging.info("-------------------------------------------------------------------")
        driver.quit()
        conn.commit()
        conn.close()

def scrape_transitions(driver, cursor):
    
    driver.get(LICENSES_URL)

    # Navigate to Transitions section
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.LINK_TEXT, "Spelklarlistor"))).click()
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.LINK_TEXT, "Övergångar"))).click()
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "periode")))

    period_dropdown = Select(driver.find_element(By.ID, "periode"))
    all_seasons = [opt.get_attribute("value") for opt in period_dropdown.options if opt.get_attribute("value").isdigit() and int(opt.get_attribute("value")) != 0]
    if SCRAPE_TRANSITIONS_RUN_ORDER.lower() == 'oldest':
        logging.info("Sorting seasons from oldest to newest.")
        print("ℹ️ Sorting seasons from oldest to newest.")
        reverse = False
    else:
        logging.info("Sorting seasons from newest to oldest.")
        print("ℹ️ Sorting seasons from newest to oldest.")
        reverse = True
    all_seasons = sorted(all_seasons, key=int, reverse=reverse)

    # Ensure seasons_to_process is valid; if SCRAPE_TRANSITIONS_NBR_OF_SEASONS <= 0, use all seasons
    seasons_to_process = all_seasons[:SCRAPE_TRANSITIONS_NBR_OF_SEASONS] if SCRAPE_TRANSITIONS_NBR_OF_SEASONS > 0 else all_seasons
    if not seasons_to_process:
        logging.warning("No valid seasons found in dropdown to process.")
        print("⚠️ No valid seasons found in dropdown to process.")

    # In player_licenses_raw.py (within the scraping function)
    logging.info(f"Scraping {len(seasons_to_process)} season(s) in {SCRAPE_TRANSITIONS_RUN_ORDER.lower()} order.")
    print(f"ℹ️  Scraping {len(seasons_to_process)} season(s) in {SCRAPE_TRANSITIONS_RUN_ORDER.lower()} order.")
    
    total_scraped = 0
    total_scraped_skipped = 0
    total_inserted = 0
    total_inserted_skipped = 0

    for season_value in seasons_to_process:

        try: 
        
            # Select the season (this reloads the DOM)
            Select(driver.find_element(By.NAME, "periode")).select_by_value(season_value)

            # Re-fetch the dropdown to avoid stale reference
            period_dropdown = Select(driver.find_element(By.NAME, "periode"))
            selected_option = period_dropdown.first_selected_option
            season_label = selected_option.text.strip()
            season_id_ext = int(selected_option.get_attribute("value"))        

            logging.info(f"Scraping transitions for season {season_label}")
            print(f"ℹ️  Scraping transitions for season {season_label}")

            season_scraped = 0
            season_scraped_skipped = 0
            season_inserted = 0
            season_inserted_skipped = 0

            # Find the "Uppdaterad" marker to locate the transitions table
            html = driver.page_source
            soup = BeautifulSoup(html, "html.parser")
            update_text = soup.find(string=lambda text: text and "Uppdaterad" in text)
            
            if not update_text:
                logging.warning(f"Could not find 'Uppdaterad' marker for season {season_label}")
                print(f"⚠️ Could not find 'Uppdaterad' marker for season {season_label}")
                return []
            
            table = update_text.find_next("table")
            
            if not table:
                logging.warning(f"Could not find transitions table after 'Uppdaterad' for season {season_label}")
                print(f"⚠️ Could not find transitions table after 'Uppdaterad' for season {season_label}")
                return []
            
            rows = table.find_all("tr")
            logging.debug(f"Number of rows found in transitions table: {len(rows)}")        

            # transitions = []

            for i, row in enumerate(rows):
                if "tabellhode" in row.get("class", []):
                    logging.debug(f"ℹ️ Skipping header row at index {i}")
                    continue

                cols = row.find_all("td")
                if len(cols) < 6:
                    logging.debug(f"Skipping row {i} due to insufficient columns ({len(cols)})")
                    continue 

                lastname = cols[0].get_text(strip=True)
                firstname = cols[1].get_text(strip=True)
                date_born_str = cols[2].get_text(strip=True)
                date_born = parse_date(date_born_str, context="upd_player_transitions_raw: Parsing date of birth")
                year_born = str(date_born.year) if date_born else None
                club_from = cols[3].get_text(strip=True)
                club_to = cols[4].get_text(strip=True)
                transition_date_str = cols[5].get_text(strip=True)
                transition_date = parse_date(transition_date_str, context="upd_player_transitions_raw: Parsing transition date")

                if not transition_date:
                    logging.warning(f"Skipping transition due to invalid date: {transition_date_str} - {season_label}/{season_id_ext} {firstname} {lastname} {date_born}, from {club_from} to {club_to} on {transition_date}")
                    # print(f"⚠️  Skipping transition due to invalid date: {transition_date_str} - {season_label}/{season_id_ext} {firstname} {lastname} {date_born}, from {club_from} to {club_to} on {transition_date}")
                    season_scraped_skipped += 1
                    continue
                
                if not year_born:
                    logging.warning(f"Skipping transition due to invalid year of birth: {date_born_str} - {season_label}/{season_id_ext} {firstname} {lastname} {date_born}, from {club_from} to {club_to} on {transition_date}")
                    # print(f"⚠️  Skipping transition due to invalid year of birth: {date_born_str} - {season_label}/{season_id_ext} {firstname} {lastname} {date_born}, from {club_from} to {club_to} on {transition_date}")
                    season_scraped_skipped += 1
                    continue

                # Debug log to inspect the transition data
                logging.debug({
                    "season_id_ext": season_id_ext,
                    "season_label": season_label,
                    "firstname": firstname,
                    "lastname": lastname,
                    "date_born": date_born,
                    "year_born": year_born,
                    "club_from": club_from,
                    "club_to": club_to,
                    "transition_date": transition_date
                })

                # transitions.append({
                #     "season_id_ext": season_id_ext,
                #     "season_label": season_label,
                #     "firstname": firstname,
                #     "lastname": lastname,
                #     "date_born": date_born,
                #     "year_born": year_born,
                #     "club_from": club_from,
                #     "club_to": club_to,
                #     "transition_date": transition_date,
                # })
                season_scraped += 1

                try:
                    cursor.execute("""
                        INSERT OR IGNORE INTO player_transition_raw (
                        season_id_ext, 
                        season_label, 
                        firstname, 
                        lastname, 
                        date_born, 
                        year_born, 
                        club_from, 
                        club_to, 
                        transition_date)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (season_id_ext, 
                        season_label, 
                        firstname, 
                        lastname, 
                        date_born, 
                        year_born, 
                        club_from, 
                        club_to, 
                        transition_date
                    ))

                    if cursor.rowcount == 0:
                        logging.warning(f"Transition for already exists, skipping insert - {season_label}/{season_id_ext} {firstname} {lastname} {date_born}, from {club_from} to {club_to} on {transition_date}")
                        # print(f"⚠️  Transition for already exists, skipping insert - {season_label}/{season_id_ext} {firstname} {lastname} {date_born}, from {club_from} to {club_to} on {transition_date}")
                        season_inserted_skipped += 1

                    else: 
                        season_inserted += 1
                
                except Exception as e:
                    logging.error(f"Failed to insert transition for {firstname} {lastname}: {e}")
                    # print(f"⚠️ Failed to insert transition for {firstname} {lastname}: {e}")
                    season_inserted_skipped += 1
                    total_inserted_skipped += 1

        except Exception as e:
            logging.error(f"Error processing season {season_label} ({season_value}): {e}")
            print(f"❌ Error processing season {season_label} ({season_value}): {e}")
            continue

        total_scraped += season_scraped
        total_scraped_skipped += season_scraped_skipped
        total_inserted += season_inserted
        total_inserted_skipped += season_inserted_skipped

        logging.info(f"Scraped {season_scraped} transitions ({season_scraped_skipped} skipped) for season {season_label} and inserted {season_inserted} transitions ({season_inserted_skipped} skipped).")
        print(f"✅ Scraped {season_scraped} transitions ({season_scraped_skipped} skipped) for season {season_label} and inserted {season_inserted} transitions ({season_inserted_skipped} skipped).")

    logging.info(f"Scraping completed - Total transitions scraped: {total_scraped} ({total_scraped_skipped} skipped). Total transitions inserted: {total_inserted} ({total_inserted_skipped} skipped).")
    print(f"✅ Scraping completed - Total transitions scraped: {total_scraped} ({total_scraped_skipped} skipped). Total transitions inserted: {total_inserted} ({total_inserted_skipped} skipped).")