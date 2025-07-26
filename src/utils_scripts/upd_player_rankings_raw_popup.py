# src/upd_player_rankings_raw_popup.py

import logging
from datetime import date
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from utils import setup_driver, print_db_insert_results, parse_date
from db import get_conn
from bs4 import BeautifulSoup

SCRAPE_RANKING_RUNS = 0  # Not used in popup method, but kept for compatibility
SCRAPE_NBR_OF_PLAYERS = 10  # Set to 0 for all players, or a positive integer for the first N players (for testing)

def upd_player_rankings_raw_popup():

    conn, cursor = get_conn()
    driver = setup_driver()

    try:
        logging.info("Starting player ranking scraping process...")
        print("ℹ️  Starting player ranking scraping process...")
        scrape_player_rankings_popup(driver, cursor, 'https://www.profixio.com/fx/ranking_sbtf/ranking_sbtf_list.php?gender=m', 'Men')
        scrape_player_rankings_popup(driver, cursor, 'https://www.profixio.com/fx/ranking_sbtf/ranking_sbtf_list.php?gender=k', 'Women')

    finally:
        logging.info("-------------------------------------------------------------------")
        driver.quit()
        conn.commit()
        conn.close()

def scrape_player_rankings_popup(driver, cursor, url, gender):
    total_skipped = 0
    try:
        driver.get(url)

        wait = WebDriverWait(driver, 10)

        # Select the first (latest) run and submit to load all players
        select_element = wait.until(EC.presence_of_element_located((By.NAME, "rid")))
        select = Select(select_element)
        select.select_by_index(0)  # Select the first option (latest run)

        submit_button = driver.find_element(By.CSS_SELECTOR, 'input[type="submit"][value="Sök"]')
        submit_button.click()

        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.table.table-condensed.table-hover.table-striped")))

        soup = BeautifulSoup(driver.page_source, "html.parser")
        table = soup.find("table", class_="table table-condensed table-hover table-striped")

        if not table:
            logging.warning(f"No table found for {gender}.")
            print(f"⚠️ No table found for {gender}.")
            return

        rows = table.select("tbody tr")
        if SCRAPE_NBR_OF_PLAYERS > 0:
            rows = rows[:SCRAPE_NBR_OF_PLAYERS]

        for row in rows:
            cols = row.find_all("td")
            if len(cols) != 7:
                continue

            # Extract common data from main row
            name_span = cols[2].find("span", class_="rml_poeng")
            if not name_span or not name_span.has_attr("id"):
                continue

            id_parts = name_span["id"].split(":")
            if len(id_parts) < 3:
                continue

            player_id_ext = int(id_parts[1]) if id_parts[1].isdigit() else None
            if player_id_ext is None:
                continue

            fullname = name_span.get_text(strip=True)
            if "," in fullname:
                lastname, firstname = [part.strip() for part in fullname.split(",", 1)]
            else:
                firstname = None
                lastname = fullname

            year_born_str = cols[3].get_text(strip=True)
            year_born = int(year_born_str) if year_born_str.isdigit() and len(year_born_str) == 4 else None

            club_name_raw = cols[4].get_text(strip=True)
            club_name = club_name_raw.rstrip('*')

            position_world_str = cols[0].get_text(strip=True)
            try:
                position_world = int(position_world_str.split()[-1])
            except (ValueError, IndexError):
                position_world = 0

            # Click the name span to open popup
            name_element = driver.find_element(By.ID, name_span["id"])
            name_element.click()

            # Wait for popup to appear
            try:
                wait.until(EC.visibility_of_element_located((By.ID, "multipurpose")))
            except TimeoutException:
                logging.warning(f"Timeout waiting for popup for player {player_id_ext}. Skipping.")
                continue

            # Parse the popup
            popup_soup = BeautifulSoup(driver.page_source, "html.parser")
            popup_div = popup_soup.find("div", id="multipurpose")
            if not popup_div:
                driver.execute_script("hideMultipurpose();")  # Close if open
                continue

            popup_table = popup_div.find("table")
            if not popup_table:
                driver.execute_script("hideMultipurpose();")
                continue

            popup_rows = popup_table.find_all("tr")[1:]  # Skip header
            for popup_row in popup_rows:
                popup_cols = popup_row.find_all("td")
                if len(popup_cols) != 4:
                    continue

                run_date_str = popup_cols[0].get_text(strip=True)
                run_date = parse_date(run_date_str.replace('-', '.'), context="Player history date")  # Adjust format if needed

                points_span = popup_cols[1].find("span", class_="rmld_poeng")
                points_str = points_span.get_text(strip=True) if points_span else popup_cols[1].get_text(strip=True)
                try:
                    points = int(points_str)
                except ValueError:
                    points = 0

                position_str = popup_cols[2].get_text(strip=True)
                try:
                    position = int(position_str)
                except ValueError:
                    position = 0

                points_change_str = popup_cols[3].get_text(strip=True).strip("()")
                try:
                    points_change_since_last = int(points_change_str) if points_change_str else 0
                except ValueError:
                    points_change_since_last = 0

                run_id = None
                if points_span and points_span.has_attr("id"):
                    popup_id_parts = points_span["id"].split(":")
                    if len(popup_id_parts) > 2:
                        run_id = int(popup_id_parts[2]) if popup_id_parts[2].isdigit() else None

                logging.info(f"Values: "
                    f"player_id_ext={player_id_ext}, "
                    f"firstname={firstname}, lastname={lastname}, "
                    f"year_born={year_born}, club_name={club_name}, "
                    f"points={points}, points_change_since_last={points_change_since_last}, "
                    f"position_world={position_world}, position={position}, "
                    f"run_date={run_date}, run_id={run_id}"
                )
                
                # # Insert into DB
                # try:
                #     cursor.execute("""
                #         INSERT OR IGNORE INTO player_transition_raw (
                #             run_id,
                #             run_date,
                #             player_id_ext,
                #             firstname,
                #             lastname,
                #             year_born,
                #             club_name,
                #             points,
                #             points_change_since_last,
                #             position_world,
                #             position)
                #         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                #     """, (
                #         run_id,
                #         run_date,
                #         player_id_ext,
                #         firstname,
                #         lastname,
                #         year_born,
                #         club_name,
                #         points,
                #         points_change_since_last,
                #         position_world if run_date == date.today() else 0,  # Assume position_world only for current
                #         position
                #     ))

                #     if cursor.rowcount > 0:
                #         db_results.append((run_id, run_date, player_id_ext, firstname, lastname, year_born, club_name, points, points_change_since_last, position_world, position))
                #     else:
                #         total_skipped += 1

                # except Exception as e:
                #     total_skipped += 1
                #     logging.error(f"Failed to insert for player {player_id_ext} on run_date {run_date}: {e}")

            # Close the popup
            driver.execute_script("hideMultipurpose();")
            wait.until(EC.invisibility_of_element_located((By.ID, "multipurpose")))

        if total_skipped > 0:
            print(f"⚠️ Skipped {total_skipped} duplicate or invalid records for {gender}.")

    except Exception as e:
        logging.error(f"An error occurred while scraping player rankings for {gender}: {e}")
        print(f"❌ An error occurred while scraping player rankings for {gender}: {e}")