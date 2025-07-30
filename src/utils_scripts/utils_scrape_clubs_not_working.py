# src/upd_club_data.py

import logging
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from collections import defaultdict
import time
from urllib.parse import urlencode
from utils import setup_driver, print_db_insert_results
from config import LICENSES_URL
from db import get_conn
from models.club import Club
from models.district import District

#
# Scrapes the player rankings lists for clubs. 
# Currently not working because the club class has been updated to handle club aliases.
# This script should be rewritten to only scrape and log clubs (club_id_ext) not in the club table.
#

def upd_clubs():

    conn, cursor = get_conn()
    driver = setup_driver()

    try:
        # Scrape clubs
        logging.info("Starting club scraping process...")
        print("ℹ️  Starting club scraping process...")
        clubs = scrape_clubs(driver, cursor)

        if clubs:
            logging.info(f"Successfully scraped {len(clubs)} clubs.")
            print(f"✅ Successfully scraped {len(clubs)} clubs.")

            # # Fetch and update club names using run_id and club_id_ext
            # clubs = fetch_club_names_by_runs(driver, cursor, clubs)

            # try: 
            #     # Insert clubs into the database
            #     db_results = [club.save_to_db(cursor) for club in clubs]

            #     if db_results:
            #         print_db_insert_results(db_results)

            #     else: 
            #         logging.warning("No clubs saved to database.")
            #         print("⚠️ No clubs saved to database.")

            # except Exception as e:
            #     logging.error(f"Error during database insertion: {e}") 
            #     print(f"❌ Error during database insertion: {e}")

        else:
            logging.warning("No clubs scraped.")
            print("⚠️  No clubs scraped.")

    finally:
        logging.info("-------------------------------------------------------------------")
        driver.quit()
        conn.commit()
        conn.close()


def scrape_clubs(driver, cursor):

    clubs = []
    try:
        driver.get("https://www.profixio.com/fx/ranking_sbtf/ranking_sbtf_list.php?gender=m")
        wait = WebDriverWait(driver, 10)

        # Find the select element for Distrikt (distr) and get all non-empty values
        select_element = wait.until(EC.presence_of_element_located((By.NAME, "distr")))
        select = Select(select_element)
        districts = [(opt.get_attribute("value"), opt.text.strip()) for opt in select.options if opt.get_attribute("value")]
        print(f"ℹ️  Found {len(districts)} districts in the dropdown.")

        for district in districts:
            district_id_ext_str, district_name = district
            try:
                district_id_ext = int(district_id_ext_str)
            except ValueError:
                logging.warning(f"Invalid district_id_ext value: '{district_id_ext_str}'. Skipping this district.")
                print(f"⚠️  Invalid district_id_ext value: '{district_id_ext_str}'. Skipping this district.")
                continue

            # Check if district_id_ext is in the District table using District class
            district = District.get_by_id_ext(cursor, district_id_ext)
            if not district:
                logging.warning(f"District ID {district_id_ext} not found in District table. Skipping.")
                print(f"⚠️  District ID {district_id_ext} not found in District table. Skipping.")
                continue

            # If valid, use district.district_id for club insertion
            district_id = district.district_id      

            # Log the district being processed
            logging.info(f"Processing district: {district_name} (ID: {district_id_ext})")
            print(f"ℹ️  Processing district: {district_name} (ID: {district_id_ext})")  

            # Re-locate the district select element and clear selection
            district_select_element = wait.until(EC.presence_of_element_located((By.NAME, "distr")))
            district_select = Select(district_select_element)
            district_select.select_by_value("")  # Reset to empty to avoid stale state

            # Use JS to set the value and trigger the change event
            driver.execute_script("arguments[0].value = '" + district_id_ext_str + "'; arguments[0].dispatchEvent(new Event('change'));", district_select_element)

            # Optional short sleep to allow JS to populate
            time.sleep(0.5)

            # Wait for the club dropdown to update with at least one non-empty option
            try:
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'select[name="club"] option[value]:not([value="0"])')))
            except TimeoutException:
                logging.warning(f"Timeout waiting for club dropdown update for district {district_name} (ID: {district_id_ext}). Skipping.")
                print(f"⚠️ Timeout waiting for club dropdown update for district {district_name} (ID: {district_id_ext}).")
                continue

            # Re-locate the club dropdown after update to avoid stale reference
            club_dropdown_element = wait.until(EC.presence_of_element_located((By.NAME, "club")))
            club_dropdown = Select(club_dropdown_element)

            for option in club_dropdown.options:
                club_id_ext_str = option.get_attribute("value").strip()
                if not club_id_ext_str or club_id_ext_str == "0":  # Skip empty or "0" value options
                    continue
                club_long_name = option.text.strip()
                if not club_long_name:  # Skip if name is empty
                    continue
                try:
                    club_id_ext = int(club_id_ext_str)
                    if club_id_ext <= 0:
                        continue
                except ValueError:
                    logging.warning(f"Invalid club_id_ext '{club_id_ext_str}' for club {club_long_name} in district {district_name}. Skipping.")
                    print(f"⚠️  Invalid club_id_ext '{club_id_ext_str}' for club {club_long_name} in district {district_name}. Skipping.")
                    continue

                clubs.append(Club(
                    club_id_ext=club_id_ext,
                    name=None, # Name will be set later 
                    long_name=club_long_name,
                    city=None,
                    country_code=None,
                    district_id=district_id
                ))

        return clubs

    except Exception as e:
        logging.error(f"An error occurred while scraping player rankings: {e}")
        print(f"❌ An error occurred while scraping player rankings: {e}")

def fetch_club_names_by_runs(driver, cursor, clubs, max_clubs=None):
    wait = WebDriverWait(driver, 10)

    if max_clubs is None:
        max_clubs = len(clubs)

    print(f"ℹ️  Fetching club names for a maximum of {max_clubs} clubs.")
    logging.info(f"Fetching club names for a maximum of {max_clubs} clubs.")
   
    try:
        # Get distinct run_id from player_ranking_raw
        cursor.execute("SELECT DISTINCT run_id FROM player_ranking_raw WHERE run_id IS NOT NULL")
        run_ids = [row[0] for row in cursor.fetchall()]

        if not run_ids:
            logging.warning("No run IDs found in player_ranking_raw. Skipping name updates.")
            print("⚠️ No run IDs found in player_ranking_raw.")
            return clubs

        # Create a lookup for clubs by club_id_ext
        club_lookup = {club.club_id_ext: club for club in clubs if club.club_id_ext is not None}

        if not club_lookup:
            logging.warning("No clubs with valid club_id_ext found. Skipping name updates.")
            print("⚠️ No clubs with valid club_id_ext found.")
            return clubs

        # Track number of clubs processed
        clubs_processed = 0

        # Construct URLs
        base_url = "https://www.profixio.com/fx/ranking_sbtf/ranking_sbtf_list.php"
        for club_id_ext, club in club_lookup.items():

            print(f"ℹ️  Fetching name for club ID {club_id_ext} ({club.long_name}). {clubs_processed + 1} of {max_clubs} clubs processed.")
            logging.info(f"Fetching name for club ID {club_id_ext} ({club.long_name}). {clubs_processed + 1} of {max_clubs} clubs processed.")

            if max_clubs is not None and clubs_processed >= max_clubs:
                logging.warning(f"Reached max_clubs limit ({max_clubs}). Stopping name updates.")
                print(f"ℹ️  Reached max_clubs limit ({max_clubs}). Stopping name updates.")
                break

            club_updated = False
            for run_id in run_ids:
                params = {
                    "searching": "1",
                    "rid": str(run_id),
                    "club": str(club_id_ext),
                    "gender": "m",
                    "distr": "",
                    "licencesubtype": "",
                    "age": "",
                    "ln": "",
                    "fn": ""
                }
                url = f"{base_url}?{urlencode(params)}"

                # Visit URL and fetch club name
                try:
                    driver.get(url)
                    table = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'table.table.table-condensed.table-hover.table-striped')))
                    rows = table.find_elements(By.CSS_SELECTOR, 'tbody tr')
                    if len(rows) < 2:
                        logging.debug(f"No data rows for club ID {club_id_ext} (run {run_id}). Trying next run.")
                        continue
                    club_name_cell = rows[1].find_element(By.CSS_SELECTOR, 'td:nth-child(5)')
                    table_club_name = club_name_cell.text.strip().rstrip('*')

                    # Update the club's name
                    club.name = table_club_name
                    logging.info(f"Updated club name to '{table_club_name}' for club ID {club_id_ext} (run {run_id}).")
                    print(f"✅ Updated club name to '{table_club_name}' for club ID {club_id_ext} (run {run_id}).")
                    club_updated = True
                    break  # Stop checking further runs for this club

                except (TimeoutException, NoSuchElementException, StaleElementReferenceException) as e:
                    logging.debug(f"Failed to retrieve club name for club ID {club_id_ext} (run {run_id}): {e}. Trying next run.")

            clubs_processed += 1  # Increment for all processed clubs, successful or not
            if not club_updated:
                logging.warning(f"No valid club name found for club ID {club_id_ext} across all runs. Keeping name as None.")
                print(f"⚠️  No valid club name found for club ID {club_id_ext} across all runs. Keeping name as None.")
        return clubs

    except Exception as e:
        logging.error(f"An error occurred while fetching club names: {e}")
        print(f"❌ An error occurred while fetching club names: {e}")
        return clubs

def fetch_club_names(driver, cursor, clubs):
    try:
        wait = WebDriverWait(driver, 10)
        # Group clubs by district_id to minimize district switches
        clubs_by_district = defaultdict(list)
        for club in clubs:
            clubs_by_district[club.district_id].append(club)

        for district_id, district_clubs in clubs_by_district.items():
            # Fetch district to get ext ID and name
            cursor.execute("SELECT district_id_ext, name FROM district WHERE district_id = ?", (district_id,))
            row = cursor.fetchone()
            if not row:
                logging.warning(f"District ID {district_id} not found in database. Skipping clubs in this district.")
                continue
            district_id_ext, district_name = row
            district_id_ext_str = str(district_id_ext)

            # Navigate to the page
            driver.get("https://www.profixio.com/fx/ranking_sbtf/ranking_sbtf_list.php?gender=m")

            # Re-locate and select district
            district_select = Select(wait.until(EC.presence_of_element_located((By.NAME, "distr"))))
            district_select.select_by_value("")  # Reset to avoid stale state
            driver.execute_script("arguments[0].value = '" + district_id_ext_str + "'; arguments[0].dispatchEvent(new Event('change'));", district_select._el)
            time.sleep(0.5)  # Allow dropdown to populate

            # Wait for club dropdown to update
            try:
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'select[name="club"] option[value]:not([value="0"])')))
            except TimeoutException:
                logging.warning(f"Timeout waiting for club dropdown for district {district_name} (ID: {district_id_ext}). Skipping clubs in this district.")
                continue

            # Re-locate club dropdown
            club_dropdown = Select(wait.until(EC.presence_of_element_located((By.NAME, "club"))))
            # Log available club IDs for debugging
            available_club_ids = [opt.get_attribute("value") for opt in club_dropdown.options if opt.get_attribute("value") and opt.get_attribute("value") != "0"]
            logging.debug(f"Available club IDs in district {district_name}: {available_club_ids}")

            for club in district_clubs:
                club_id_ext_str = str(club.club_id_ext)  # Ensure string
                if club_id_ext_str not in available_club_ids:
                    logging.warning(f"Club ID {club_id_ext_str} not found in dropdown for district {district_name}. Skipping.")
                    continue

                # Retry selection up to 2 times
                for attempt in range(2):
                    try:
                        club_dropdown = Select(wait.until(EC.presence_of_element_located((By.NAME, "club"))))
                        club_dropdown.select_by_value(club_id_ext_str)
                        break
                    except (StaleElementReferenceException, NoSuchElementException):
                        if attempt == 1:
                            logging.warning(f"Failed to select club ID {club_id_ext_str} in district {district_name} after retries. Skipping.")
                            break
                        time.sleep(0.5)  # Brief pause before retry

                else:  # Skip if selection failed
                    continue

                # Click "Sök"
                try:
                    submit_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'input[type="submit"][value="Sök"]')))
                    submit_button.click()
                except TimeoutException:
                    logging.warning(f"Timeout waiting for submit button for club ID {club_id_ext_str} in district {district_name}. Skipping name update.")
                    continue

                # Fetch club name from table (second row)
                try:
                    table = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'table.table.table-condensed.table-hover.table-striped')))
                    rows = table.find_elements(By.CSS_SELECTOR, 'tbody tr')
                    if len(rows) < 2:
                        logging.warning(f"No data rows for club ID {club_id_ext_str} in district {district_name}. Skipping name update.")
                        continue
                    club_name_cell = rows[1].find_element(By.CSS_SELECTOR, 'td:nth-child(5)')
                    club.name = club_name_cell.text.strip().rstrip('*')
                    logging.info(f"Updated club name to '{club.name}' for club ID {club_id_ext_str} in district {district_name}.")
                except (TimeoutException, NoSuchElementException, StaleElementReferenceException):
                    logging.warning(f"Failed to retrieve club name from table for club ID {club_id_ext_str} in district {district_name}. Skipping name update.")

                # Reset form state
                driver.get("https://www.profixio.com/fx/ranking_sbtf/ranking_sbtf_list.php?gender=m")
                wait.until(EC.presence_of_element_located((By.NAME, "distr")))

    except Exception as e:
        logging.error(f"An error occurred while fetching club names: {e}")
        print(f"❌ An error occurred while fetching club names: {e}")

    return clubs