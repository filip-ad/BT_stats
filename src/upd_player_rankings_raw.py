# src/upd_player_rankings_raw.py

import logging
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from collections import defaultdict
from utils import setup_driver, print_db_insert_results, parse_date
from config import SCRAPE_RANKING_RUNS
from db import get_conn
from bs4 import BeautifulSoup

def upd_player_rankings_raw():

    conn, cursor = get_conn()
    driver = setup_driver()

    try:
        logging.info("Starting player ranking scraping process...")
        print("ℹ️  Starting player ranking scraping process...")
        scrape_player_rankings(driver, cursor, 'https://www.profixio.com/fx/ranking_sbtf/ranking_sbtf_list.php?gender=m', 'Men')
        scrape_player_rankings(driver, cursor, 'https://www.profixio.com/fx/ranking_sbtf/ranking_sbtf_list.php?gender=k', 'Women')

    except Exception as e:
        logging.error(f"An error occurred while scraping player rankings: {e}")
        print(f"❌ An error occurred while scraping player rankings: {e}")


    finally:
        logging.info("-------------------------------------------------------------------")
        driver.quit()
        conn.commit()
        conn.close()

def scrape_player_rankings(driver, cursor, url, gender):

    total_scraped = 0
    total_scraped_skipped = 0
    total_inserted = 0
    total_inserted_skipped = 0
    
    
    try:



        driver.get(url)
        wait = WebDriverWait(driver, 10)

        # Find the select for run and get all non-empty values
        select_element = wait.until(EC.presence_of_element_located((By.NAME, "rid")))
        select = Select(select_element)
        runs = [(opt.get_attribute("value"), opt.text.strip()) for opt in select.options if opt.get_attribute("value")]
        # runs = [(opt.get_attribute("value"), opt.text.strip()) for opt in select.options if opt.get_attribute("value")]

        if SCRAPE_RANKING_RUNS > 0:
            runs = runs[:SCRAPE_RANKING_RUNS]

        for run in runs:

            run_id_str, run_date_str = run
            print(f"ℹ️  Processing ranking run: {run_id_str} (ID: {run_date_str}) for {gender}")
            logging.info(f"Processing ranking run: {run_id_str} (ID: {run_date_str}) for {gender}")

            run_scraped = 0
            run_scraped_skipped = 0
            run_inserted = 0
            run_inserted_skipped = 0

            run_id = None
            if run_id_str.isdigit():
                run_id = int(run_id_str)
            else:
                logging.warning(f"⚠️  Invalid run_id value: '{run_id_str}'")
                continue

            run_date = parse_date(run_date_str, context="upd_player_rankings_raw: Parsing player ranking run date")

            # Click the submit button
            submit_button = driver.find_element(By.CSS_SELECTOR, 'input[type="submit"][value="Sök"]')
            submit_button.click()

            # Wait for the table to be present after submission
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.table.table-condensed.table-hover.table-striped")))

            soup = BeautifulSoup(driver.page_source, "html.parser")
            table = soup.find("table", class_="table table-condensed table-hover table-striped")
            
            if not table:
                logging.warning(f"No table found for ranking run: {run_id_str} (ID: {run_date_str})")
                print(f"⚠️ No table found for ranking run: {run_id_str} (ID: {run_date_str})")
                continue

            tbody = table.find("tbody")
            if not tbody:
                continue

            for row in table.select("tbody tr"):
                cols = row.find_all("td")
                
                if len(cols) != 7:
                    logging.debug("Skipping row with unexpected number of columns.")
                    continue

                # Extract player_id_ext from span in name column
                name_span = cols[2].find("span", class_="rml_poeng")
                player_id_ext = None
                if name_span and name_span.has_attr("id"):
                    id_parts = name_span["id"].split(":")
                    if len(id_parts) > 1:
                        try:
                            player_id_ext = int(id_parts[1])
                        except ValueError:
                            logging.warning(f"Invalid player_id_ext value: {id_parts[1]}, row skipped.")
                            print(f"⚠️  Invalid player_id_ext value: {id_parts[1]}, row skipped.")
                            run_scraped_skipped += 1
                            continue

                if player_id_ext is None:
                    logging.warning("Skipping row without valid player_id_ext.")
                    print("⚠️  Skipping row without valid player_id_ext.")
                    run_scraped_skipped += 1
                    continue

                # Fullname from span or td
                fullname = name_span.get_text(strip=True) if name_span else cols[2].get_text(strip=True)
                if "," in fullname:
                    lastname, firstname = [part.strip() for part in fullname.split(",", 1)]
                else:
                    firstname = None
                    lastname = fullname
                    logging.warning(f"Could not parse fullname: '{fullname}', skipping row.")
                    print(f"⚠️  Could not parse fullname: '{fullname}', skipping row.")
                    run_scraped_skipped += 1
                    continue
                    

                year_born_str = cols[3].get_text(strip=True)
                year_born = None
                if year_born_str.isdigit() and len(year_born_str) == 4:
                    year_born = int(year_born_str)
                else:
                    logging.warning(f"Invalid year_born value: '{year_born_str}', skipping row.")
                    print(f"⚠️  Invalid year_born value: '{year_born_str}', skipping row.")
                    run_scraped_skipped += 1
                    continue

                club_name_raw = cols[4].get_text(strip=True)
                club_name = club_name_raw.rstrip('*')

                position_world_str = cols[0].get_text(strip=True)

                # Extract number after "WR" and ignore the rest
                if position_world_str.startswith("WR"):
                    wr_part = position_world_str.split()[0]  # "WR06 1" -> "WR06"
                    try:
                        position_world = int(wr_part[2:])  # "06" -> 6
                    except ValueError:
                        position_world = 0
                        logging.warning(f"⚠️ Could not parse WR number from: '{position_world_str}', defaulting to 0.")
                else:
                    position_world = 0  # Not a world-ranked player

                position_str = cols[1].get_text(strip=True)
                # Remove parentheses and convert to int
                try:
                    clean_position = position_str.strip("()")
                    position = int(clean_position) if clean_position else 0
                except ValueError:
                    logging.warning(f"Invalid position value: '{position_str}'")
                    print(f"⚠️ Invalid position value: '{position_str}'")
                    run_scraped_skipped += 1
                    continue

                points_str = cols[5].get_text(strip=True)
                try:
                    points = int(points_str)
                except ValueError:
                    logging.warning(f"Invalid points value: '{points_str}'")
                    print(f"⚠️ Invalid points value: '{points_str}'")
                    run_scraped_skipped += 1
                    continue

                points_change_since_last_str = cols[6].get_text(strip=True).strip("()")
                try:
                    points_change_since_last = int(points_change_since_last_str) if points_change_since_last_str else 0
                except ValueError:
                    logging.warning(f"Invalid points_change_since_last value: '{points_change_since_last_str}'")
                    print(f"⚠️ Invalid points_change_since_last value: '{points_change_since_last_str}'")
                    run_scraped_skipped += 1
                    continue

                run_scraped += 1

                # Debug log to inspect data
                logging.info({
                    "run_id": run_id,
                    "run_date": run_date,
                    "player_id_ext": player_id_ext,
                    "firstname": firstname,
                    "lastname": lastname,
                    "year_born": year_born,
                    "club_name": club_name,
                    "position_world": position_world,
                    "position": position,
                    "points": points,
                    "points_change_since_last": points_change_since_last
                })

                try:
                    cursor.execute("""
                        INSERT OR IGNORE INTO player_ranking_raw (
                            run_id,
                            run_date,
                            player_id_ext,
                            firstname,
                            lastname,
                            year_born,
                            club_name,
                            points,
                            points_change_since_last,
                            position_world,
                            position)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        run_id, 
                        run_date,
                        player_id_ext, 
                        firstname, 
                        lastname, 
                        year_born, 
                        club_name, 
                        points, 
                        points_change_since_last, 
                        position_world, 
                        position
                    ))

                    if cursor.rowcount == 0:
                        run_inserted_skipped += 1
                    else:
                        run_inserted += 1

                except Exception as e:
                    run_inserted_skipped += 1
                    logging.error(f"Failed to insert record for player_id_ext: {player_id_ext} on run_date: {run_date}: {e}")

            print(f"ℹ️  Processed ranking run: {run_id_str} (ID: {run_date_str}) for {gender} - "
                f"Scraped: {run_scraped}, scrape skips: {run_scraped_skipped}, "
                f"inserted to db: {run_inserted}, insert skips: {run_inserted_skipped}")
            logging.info(f"Processed ranking run: {run_id_str} (ID: {run_date_str}) for {gender} - "
                        f"Scraped: {run_scraped}, scrape skips: {run_scraped_skipped}, "
                        f"inserted to db: {run_inserted}, insert skips: {run_inserted_skipped}")

            total_scraped += run_scraped
            total_scraped_skipped += run_scraped_skipped
            total_inserted += run_inserted  
            total_inserted_skipped += run_inserted_skipped

        logging.info(f"Total rankings scraped for {gender}: {total_scraped}, scrape skipped: {total_scraped_skipped}, "
                     f"inserted to db: {total_inserted}, insert skips: {total_inserted_skipped}")
        print(f"✅ Total rankings scraped for {gender}: {total_scraped}, scrape skipped: {total_scraped_skipped}, "
              f"inserted to db: {total_inserted}, insert skips: {total_inserted_skipped}")

    except Exception as e:
        logging.error(f"An error occurred while scraping player rankings: {e}")
        print(f"❌ An error occurred while scraping player rankings: {e}")