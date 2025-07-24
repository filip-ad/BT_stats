# src/upd_player_licenses_raw.py

import logging
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from bs4 import BeautifulSoup
from collections import defaultdict
from utils import setup_driver
from db import get_conn
from config import SCRAPE_MAX_CLUBS, SCRAPE_SEASONS, LICENSES_URL

def upd_player_licenses_raw():
    conn, cursor = get_conn()
    driver = setup_driver()
    try:
        logging.info("Scraping player license raw data...")
        print("ℹ️  Scraping player license raw data...")
        scrape_player_licenses(driver, cursor)

    except Exception as e:
        logging.error(f"An error occurred while scraping player license raw data: {e}")
    finally:
        logging.info("-------------------------------------------------------------------")
        driver.quit()
        conn.commit()
        conn.close()

def scrape_player_licenses(driver, cursor):

    driver.get(LICENSES_URL)

    WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.LINK_TEXT, "Spelklarlistor"))).click()
    WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.NAME, "periode")))

    period_dropdown = Select(driver.find_element(By.NAME, "periode"))
    all_seasons = sorted(
            [opt.get_attribute("value") for opt in period_dropdown.options if opt.get_attribute("value").isdigit()],
            key=int,
            reverse=True  # Sort in descending order (True = newest to oldest), set to False for oldest to newest
        )
    seasons_to_process = all_seasons[:SCRAPE_SEASONS] if SCRAPE_SEASONS > 0 else all_seasons

    club_dropdown = Select(driver.find_element(By.NAME, "klubbid"))
    club_map = [
        {"club_name": opt.text.strip(), "club_id_ext": int(opt.get_attribute("value"))}
        for opt in club_dropdown.options if opt.text.strip() and opt.get_attribute("value").isdigit()
    ]
    clubs = club_map[:SCRAPE_MAX_CLUBS] if SCRAPE_MAX_CLUBS > 0 else club_map

    total_inserted = 0
    total_skipped = 0
    updated_ranking_groups = 0
    unchanged_ranking_groups = 0

    for season_value in seasons_to_process:

        # Select the season (this reloads the DOM)
        Select(driver.find_element(By.NAME, "periode")).select_by_value(season_value)

        # Re-fetch the dropdown to avoid stale reference
        period_dropdown = Select(driver.find_element(By.NAME, "periode"))
        selected_option = period_dropdown.first_selected_option
        season_label = selected_option.text.strip()
        season_id_ext = int(selected_option.get_attribute("value"))

        logging.info(f"Scraping raw license data for season {season_label}...")
        print(f"ℹ️  Scraping raw license data for season {season_label}...")

        season_inserted = 0
        season_skipped = 0

        for club in clubs:
            club_name = club["club_name"]
            club_id_ext = club["club_id_ext"]

            club_season_inserted = 0
            club_season_skipped = 0

            Select(driver.find_element(By.NAME, "klubbid")).select_by_visible_text(club_name)

            WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.CLASS_NAME, "table-condensed")))

            soup = BeautifulSoup(driver.page_source, "html.parser")
            table = soup.find("table", class_="table-condensed my-4 shadow-xl")
            if not table:
                logging.warning(f"No table found for club {club_name} in season {season_label}")
                print(f"⚠️ No table found for club {club_name} in season {season_label}")
                continue

            for row in table.select("tbody tr"):
                cols = row.find_all("td")
                if len(cols) < 9:
                    continue

                input_el = row.find("input", {"type": "checkbox"})
                player_id_ext = int(input_el["id"]) if input_el and input_el.has_attr("id") else None

                lastname = cols[1].get_text(strip=True)
                firstname = cols[2].get_text(strip=True)
                gender = cols[3].get_text(strip=True)
                year_born = cols[4].get_text(strip=True)
                license_info_raw = cols[5].get_text(strip=True)
                ranking_group_raw = cols[6].get_text(strip=True)

                # Validate and convert year_born
                year_born = int(year_born) if year_born.isdigit() else None
                if year_born is None:
                    logging.warning(f"Invalid year_born for {firstname} {lastname}: {year_born}")
                    continue

                # Debug log to inspect data
                logging.debug({
                    "season_label": season_label,
                    "season_id_ext": season_id_ext,
                    "club_name": club_name,
                    "club_id_ext": club_id_ext,
                    "player_id_ext": player_id_ext,
                    "firstname": firstname,
                    "lastname": lastname,
                    "gender": gender,
                    "year_born": year_born,
                    "license_info_raw": license_info_raw,
                    "ranking_group_raw": ranking_group_raw
                })

                inserted_count = 0
                skipped_count = 0

                try:
                    cursor.execute('''
                        INSERT OR IGNORE INTO player_license_raw (
                            season_label, 
                            season_id_ext, 
                            club_name, 
                            club_id_ext, 
                            player_id_ext,
                            firstname, 
                            lastname, 
                            gender, 
                            year_born,
                            license_info_raw, 
                            ranking_group_raw
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        season_label,
                        season_id_ext,
                        club_name,
                        club_id_ext,
                        player_id_ext,
                        firstname,
                        lastname,
                        gender,
                        year_born,
                        license_info_raw,
                        ranking_group_raw
                    ))

                    if cursor.rowcount == 0:
                        # logging.warning(f"Insert ignored for {firstname} {lastname} (season: {season_label}, club: {club_name}): Possible duplicate or NULL violation")
                        # 🔄 Try to update ranking_group_raw if the data has changed
                        cursor.execute('''
                            SELECT ranking_group_raw FROM player_license_raw
                            WHERE player_id_ext = ? AND season_id_ext = ? AND club_id_ext = ?
                        ''', (player_id_ext, season_id_ext, club_id_ext))

                        row = cursor.fetchone()
                        if row:
                            existing_ranking_group = row[0]
                            if existing_ranking_group != ranking_group_raw:
                                cursor.execute('''
                                    UPDATE player_license_raw
                                    SET ranking_group_raw = ?
                                    WHERE player_id_ext = ? AND season_id_ext = ? AND club_id_ext = ?
                                ''', (ranking_group_raw, player_id_ext, season_id_ext, club_id_ext))
                                logging.info(f"Updated ranking_group_raw for {firstname} {lastname} (season: {season_label}, club: {club_name}) to {ranking_group_raw} (was: {existing_ranking_group})")
                                print(f"✅ Updated ranking_group_raw for {firstname} {lastname} (season: {season_label}, club: {club_name}) to {ranking_group_raw} (was: {existing_ranking_group})")
                                updated_ranking_groups += 1
                            else:
                                logging.debug(f"No ranking group change for {firstname} {lastname}, current: {existing_ranking_group}, new: {ranking_group_raw}")
                                unchanged_ranking_groups += 1
                        else:
                            logging.warning(f"Row skipped but not found for update (shouldn’t happen): {firstname} {lastname}")   
                            print(f"⛔ Row skipped but not found for update (shouldn’t happen): {firstname} {lastname}")                    
                        skipped_count += 1
                        club_season_skipped += 1
                    else:
                        inserted_count += 1
                        club_season_inserted += 1

                    season_inserted += inserted_count
                    season_skipped += skipped_count

                except Exception as e:
                    logging.error(f"Failed to insert row for {firstname} {lastname}: {e}")
                    print(f"❌ Failed to insert row for {firstname} {lastname}: {e}")

            logging.info(f"Finished club {club_name} in season {season_label}, added {club_season_inserted} rows, skipped {club_season_skipped} rows")
            print(f"✅ Finished club {club_name} in season {season_label}, added {club_season_inserted} rows, skipped {club_season_skipped} rows")

        # Commit after each season
        cursor.connection.commit()
        logging.info(f"Committed changes for season {season_label} (Season Inserted: {season_inserted}, Season Skipped: {season_skipped})")
        print(f"✅ Committed changes for season {season_label} (Season Inserted: {season_inserted}, Season Skipped: {season_skipped})")
        total_inserted += season_inserted
        total_skipped += season_skipped

    logging.info(f"Scraping completed — Total inserted: {total_inserted}, Total skipped: {total_skipped}")
    print(f"✅ Scraping completed — Total inserted: {total_inserted}, Total skipped: {total_skipped}")
    logging.info(f"Ranking group updates: {updated_ranking_groups} updated, {unchanged_ranking_groups} unchanged")
    print(f"📊 Ranking group updates: {updated_ranking_groups} updated, {unchanged_ranking_groups} unchanged")

