# src/scrapers/scrape_player_transitions.py

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from bs4 import BeautifulSoup
from models.player_transition_raw import PlayerTransitionRaw
from config import (
    SCRAPE_TRANSITIONS_NBR_OF_SEASONS, 
    SCRAPE_TRANSITIONS_ORDER
    )
from utils import OperationLogger, setup_driver, parse_date

LICENSES_URL = "https://www.profixio.com/fx/ranking_sbtf/ranking_sbtf_public.php"

def scrape_player_transitions(cursor):
    """
    Scrape the player transitions raw data, process each row, 
    and insert/update into the player_transition_raw table.
    """

    logger = OperationLogger(
        verbosity       = 2, 
        print_output    = False, 
        log_to_db       = True, 
        cursor          = cursor
    )
    
    driver = setup_driver()
    driver.get(LICENSES_URL)

    logger.info("Scraping player transitions...", to_console=True)

    # Wait for the page elements to load
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.LINK_TEXT, "Spelklarlistor"))).click()
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.LINK_TEXT, "Övergångar"))).click()
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "periode")))

    # Season dropdown
    period_dropdown = Select(driver.find_element(By.ID, "periode"))
    all_seasons = [opt.get_attribute("value") for opt in period_dropdown.options if opt.get_attribute("value").isdigit()]
    if SCRAPE_TRANSITIONS_ORDER.lower() == 'oldest':
        reverse = False
    else:
        reverse = True
    all_seasons = sorted(all_seasons, key=int, reverse=reverse)
    seasons_to_process = all_seasons[:SCRAPE_TRANSITIONS_NBR_OF_SEASONS] if SCRAPE_TRANSITIONS_NBR_OF_SEASONS > 0 else all_seasons

    logger.info(f"Scraping {len(seasons_to_process)} season(s) in {SCRAPE_TRANSITIONS_ORDER.lower()} order.", to_console=True)

    # Counting
    total_inserted = 0
    total_skipped = 0
    current_season_count = 0

    try:
        for season_value in seasons_to_process:

            # Select the season (this reloads the DOM)
            Select(driver.find_element(By.NAME, "periode")).select_by_value(season_value)

            # Re-fetch the dropdown to avoid stale reference
            period_dropdown     = Select(driver.find_element(By.NAME, "periode"))
            selected_option     = period_dropdown.first_selected_option
            season_label        = selected_option.text.strip()
            season_id_ext       = int(selected_option.get_attribute("value"))

            logger.info(f"Scraping raw transition data for season {season_label}...", to_console=True)

            # Define logger_key
            logger_keys = {
                "firstname":            None,
                "lastname":             None,
                "year_born":            None,
                "club_from":            None,
                "club_to":              None,
                "transition_date":      None,
                "season_label":         season_label,
                "season_id_ext":        season_id_ext
            }

            # Adding to season totals
            season_inserted = 0
            season_skipped = 0

            soup = BeautifulSoup(driver.page_source, "html.parser")
            update_text = soup.find(string=lambda text: text and "Uppdaterad" in text)
            if not update_text:
                logger.failed(logger_keys.copy(), "Could not find 'Uppdaterad' marker for season")
                continue
            
            table = update_text.find_next("table")
            if not table:
                logger.failed(logger_keys.copy(), "Could not find transitions table after 'Uppdaterad' for season")
                continue

            for row in table.find_all("tr"):
                cols = row.find_all("td")
                if len(cols) < 6:
                    continue

                lastname                = cols[0].get_text(strip=True)
                firstname               = cols[1].get_text(strip=True)
                date_born_str           = cols[2].get_text(strip=True)
                date_born               = parse_date(date_born_str, context="scrape_player_transitions: Parsing date of birth")
                year_born               = str(date_born.year) if date_born else None
                club_from               = cols[3].get_text(strip=True)
                club_to                 = cols[4].get_text(strip=True)
                transition_date_str     = cols[5].get_text(strip=True)
                transition_date         = parse_date(transition_date_str, context="scrape_player_transitions: Parsing transition date")

                # Update logger_keys
                logger_keys.update({
                    "firstname":            firstname,
                    "lastname":             lastname,
                    "year_born":            year_born,
                    "club_from":            club_from,
                    "club_to":              club_to,
                    "transition_date":      transition_date
                })

                # Create PlayerTransitionRaw object
                raw = PlayerTransitionRaw(
                    season_label        = season_label,
                    season_id_ext       = season_id_ext,
                    firstname           = firstname,
                    lastname            = lastname,
                    date_born           = date_born,
                    year_born           = year_born,
                    club_from           = club_from,
                    club_to             = club_to,
                    transition_date     = transition_date
                )

                # Validate
                is_valid, error_msg = raw.validate()
                if not is_valid:
                    logger.failed(logger_keys.copy(), error_msg)
                    continue

                inserted = raw.upsert_one(cursor, raw)

                if inserted:
                    logger.success(logger_keys.copy(), "Raw player transition record successfully inserted")
                    total_inserted += 1
                    season_inserted += 1
                else:
                    logger.skipped(logger_keys.copy(), "Raw player transition record already exists")
                    total_skipped += 1
                    season_skipped += 1

            # Commit changes after each season
            cursor.connection.commit()

            current_season_count += 1
            logger.info(
                f"Finished season {season_label}, "
                f"inserted {season_inserted} rows, skipped {season_skipped} rows "
                f"({len(seasons_to_process) - current_season_count} seasons remaining)", to_console=False
            )
            # Using regular print to use separate icon (for now)
            print(
                f"✅ Finished season {season_label}, "
                f"inserted {season_inserted} rows, skipped {season_skipped} rows "
                f"({len(seasons_to_process) - current_season_count} seasons remaining)"
            )

        logger.info(f"Scraping completed — Total inserted: {total_inserted}, Total skipped: {total_skipped}", to_console=True)

    except Exception as e:
        logger.failed({}, f"Exception during scraping: {str(e)}")
        raise
    finally:
        logger.summarize()
        driver.quit()