# src/scrapers/scrape_player_rankings.py

import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
from models.player_ranking_raw import PlayerRankingRaw
from utils import OperationLogger, setup_driver, parse_date
from config import SCRAPE_RANKINGS_NBR_OF_RUNS, SCRAPE_RANKINGS_ORDER
from db import get_conn

def scrape_player_rankings(cursor, run_id=None):
    """
    Scrape player rankings raw data from Profixio, process each row,
    and insert/update into the player_ranking_raw table.
    """
    
    # Initializing logger
    logger = OperationLogger(
        verbosity       = 2,
        print_output    = False,
        log_to_db       = True,
        cursor          = cursor,
        object_type     = "player_ranking",
        run_type        = "scrape",
        run_id          = run_id
    )

    # Setting up Selenium driver
    driver = setup_driver()
    try:
        logger.info("Scraping player rankings...", to_console=True)

        # Processing Men and Women rankings
        for url, gender in [
            ('https://www.profixio.com/fx/ranking_sbtf/ranking_sbtf_list.php?gender=m', 'Men'),
            ('https://www.profixio.com/fx/ranking_sbtf/ranking_sbtf_list.php?gender=k', 'Women')
        ]:
            # Fetching ranking page
            driver.get(url)
            wait = WebDriverWait(driver, 10)

            # Fetching run dropdown
            select_element = wait.until(EC.presence_of_element_located((By.NAME, "rid")))
            select = Select(select_element)
            runs = [(opt.get_attribute("value"), opt.text.strip()) for opt in select.options if opt.get_attribute("value")]

            logger.info(f"Found {len(runs)} ranking runs for {gender}. Limiting to {SCRAPE_RANKINGS_NBR_OF_RUNS} run(s) in {SCRAPE_RANKINGS_ORDER} order.", to_console=True)

            # Sorting runs based on config
            if SCRAPE_RANKINGS_ORDER.lower() == 'oldest':
                logger.info("Reversing runs to process from oldest to newest.", to_console=True)
                runs = runs[::-1]  # Reverse to process oldest first

            # Limiting runs if specified
            if SCRAPE_RANKINGS_NBR_OF_RUNS > 0:
                runs = runs[:SCRAPE_RANKINGS_NBR_OF_RUNS]

            # Logging runs to scrape
            logger.info(f"Runs to be scraped for {gender} in order: {[(run_id, date) for run_id, date in runs]}", to_console=True)

            total_inserted = 0
            total_updated = 0
            total_unchanged = 0

            for run_id_str, run_date_str in runs:
                run_scraped = 0
                run_scraped_skipped = 0
                run_inserted = 0
                run_updated = 0
                run_unchanged = 0

                # Starting run timer
                run_time_start = time.perf_counter()
                # Parsing run date
                run_date = parse_date(run_date_str, context="scrape_player_rankings: Parsing player ranking run date")

                logger.info(f"Scraping ranking run: {run_id_str} (Date: {run_date_str}) for {gender}...", to_console=True)

                # Selecting run from dropdown
                select_element = wait.until(EC.presence_of_element_located((By.NAME, "rid")))
                select = Select(select_element)
                select.select_by_value(run_id_str)

                # Submitting run selection
                submit_button = driver.find_element(By.CSS_SELECTOR, 'input[type="submit"][value="Sök"]')
                submit_button.click()

                # Waiting for table update
                try:
                    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, f'span[id*=":{run_id_str}:"]')))
                except TimeoutException:
                    logger.warning({}, f"Timeout waiting for table update to run_id {run_id_str}. Scraping may use stale data.", to_console=True)

                # Parsing page HTML
                soup = BeautifulSoup(driver.page_source, "html.parser")
                table = soup.find("table", class_="table table-condensed table-hover table-striped")

                if not table:
                    logger.warning({}, f"No table found for ranking run: {run_id_str} (Date: {run_date_str})", to_console=True)
                    continue

                # Fetching table body
                tbody = table.find("tbody")
                if not tbody:
                    logger.warning({}, f"No tbody found for ranking run: {run_id_str} (Date: {run_date_str})", to_console=True)
                    continue

                # Processing table rows
                for row in table.select("tbody tr"):
                    cols = row.find_all("td")
                    if len(cols) != 7:
                        # logger.warning({}, "Skipping row with unexpected number of columns.", to_console=True)
                        # run_scraped_skipped += 1
                        # Possibly header or malformed row, skip
                        continue

                    # Extracting player_id_ext
                    name_span = cols[2].find("span", class_="rml_poeng")
                    player_id_ext = None
                    if name_span and name_span.has_attr("id"):
                        id_parts = name_span["id"].split(":")
                        if len(id_parts) > 1:
                            player_id_ext = id_parts[1]  # Store as TEXT per player_ranking_raw schema

                    logger_keys = {
                        "run_id_ext": run_id_str,
                        "run_date": run_date,
                        "player_id_ext": player_id_ext,
                        "firstname": None,
                        "lastname": None,
                        "year_born": None,
                        "club_name": None,
                        "points": None,
                        "points_change_since_last": None,
                        "position_world": None,
                        "position": None
                    }

                    if not player_id_ext:
                        logger.warning(logger_keys.copy(), "Skipping row without valid player_id_ext.", to_console=True)
                        run_scraped_skipped += 1
                        continue

                    # Parsing fullname
                    fullname = name_span.get_text(strip=True) if name_span else cols[2].get_text(strip=True)
                    if "," in fullname:
                        lastname, firstname = [part.strip() for part in fullname.split(",", 1)]
                    else:
                        firstname = None
                        lastname = fullname
                        logger.warning(logger_keys.copy(), f"Could not parse fullname: '{fullname}', skipping row.", to_console=True)
                        run_scraped_skipped += 1
                        continue

                    # Parsing year_born
                    year_born = cols[3].get_text(strip=True)  # TEXT per schema
                    if not year_born.isdigit() or len(year_born) != 4:
                        logger_keys["year_born"] = year_born
                        logger.warning(logger_keys.copy(), f"Invalid year_born value: '{year_born}', skipping row.", to_console=True)
                        run_scraped_skipped += 1
                        continue

                    # Parsing club_name
                    club_name = cols[4].get_text(strip=True).rstrip('*')
                    logger_keys["club_name"] = club_name

                    # Parsing position_world
                    position_world_str = cols[0].get_text(strip=True)
                    position_world = 0
                    if position_world_str.startswith("WR"):
                        wr_part = position_world_str.split()[0]
                        try:
                            position_world = int(wr_part[2:])
                        except ValueError:
                            logger.warning(logger_keys.copy(), f"Could not parse WR number from: '{position_world_str}', defaulting to 0.", to_console=True)
                    logger_keys["position_world"] = position_world

                    # Parsing position
                    position_str = cols[1].get_text(strip=True)
                    try:
                        clean_position = position_str.strip("()")
                        position = int(clean_position) if clean_position else 0
                    except ValueError:
                        logger_keys["position"] = position_str
                        logger.warning(logger_keys.copy(), f"Invalid position value: '{position_str}'", to_console=True)
                        run_scraped_skipped += 1
                        continue
                    logger_keys["position"] = position

                    # Parsing points
                    points_str = cols[5].get_text(strip=True)
                    try:
                        points = int(points_str)
                    except ValueError:
                        logger_keys["points"] = points_str
                        logger.warning(logger_keys.copy(), f"Invalid points value: '{points_str}'", to_console=True)
                        run_scraped_skipped += 1
                        continue
                    logger_keys["points"] = points

                    # Parsing points_change_since_last
                    points_change_str = cols[6].get_text(strip=True).strip("()")
                    try:
                        points_change_since_last = int(points_change_str) if points_change_str else 0
                    except ValueError:
                        logger_keys["points_change_since_last"] = points_change_str
                        logger.warning(logger_keys.copy(), f"Invalid points_change_since_last value: '{points_change_str}'", to_console=True)
                        run_scraped_skipped += 1
                        continue
                    logger_keys["points_change_since_last"] = points_change_since_last

                    logger_keys.update({
                        "firstname": firstname,
                        "lastname": lastname
                    })

                    run_scraped += 1

                    # Creating PlayerRankingRaw instance
                    raw = PlayerRankingRaw(
                        row_id=None,
                        run_id_ext=run_id_str,
                        run_date=run_date,
                        player_id_ext=player_id_ext,
                        firstname=firstname,
                        lastname=lastname,
                        year_born=year_born,
                        club_name=club_name,
                        points=points,
                        points_change_since_last=points_change_since_last,
                        position_world=position_world,
                        position=position,
                        data_source_id=3
                    )

                    # Incrementing processed count
                    logger.inc_processed()

                    # Validating row
                    is_valid, error_msg = raw.validate()
                    if not is_valid:
                        logger.failed(logger_keys.copy(), error_msg)
                        continue

                    # Upserting row
                    result = raw.upsert(cursor)
                    if result == "inserted":
                        run_inserted += 1
                        total_inserted += 1
                        logger.success(logger_keys.copy(), "Raw ranking inserted")
                    elif result == "updated":
                        run_updated += 1
                        total_updated += 1
                        logger.success(logger_keys.copy(), "Raw ranking updated")
                    elif result == "unchanged":
                        run_unchanged += 1
                        total_unchanged += 1
                        logger.success(logger_keys.copy(), "Raw ranking unchanged")
                    else:
                        logger.failed(logger_keys.copy(), "Upsert failed")

                # Committing changes for run
                cursor.connection.commit()
                run_time = time.perf_counter() - run_time_start

                # Logging run summary
                logger.info(
                    f"Finished run {run_id_str:<10} Date: {run_date_str:<12} for {gender:<6} "
                    f"[Scraped: {run_scraped:<3} Skipped: {run_scraped_skipped:<3} "
                    f"Inserted: {run_inserted:<3} Updated: {run_updated:<3} Unchanged: {run_unchanged:<3}] "
                    f"({run_time:.2f} sec)",
                    to_console=True, emoji="✅"
                )

            # Logging gender summary
            logger.info(
                f"Scraping completed for {gender} — "
                f"Total inserted: {total_inserted}, total updated: {total_updated}, total unchanged: {total_unchanged}",
                to_console=True
            )

        # Summarizing all operations
        logger.summarize()

    except Exception as e:
        # Logging global error
        logger.failed({}, f"An error occurred while scraping player rankings: {e}")
    finally:
        # Closing driver
        driver.quit()

def upd_player_rankings_raw():
    """
    Entry point for scraping player rankings.
    """
    # Opening database connection
    conn, cursor = get_conn()
    try:
        # Running scraper
        scrape_player_rankings(cursor)
    finally:
        # Committing and closing connection
        conn.commit()
        conn.close()