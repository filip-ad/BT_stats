# src/scrapers/scrape_player_licenses.py

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from bs4 import BeautifulSoup
from models.player_license_raw import PlayerLicenseRaw
from config import (
    SCRAPE_LICENSES_MAX_CLUBS, 
    SCRAPE_LICENSES_NBR_OF_SEASONS, 
    SCRAPE_LICENSES_ORDER
    )
from utils import OperationLogger, setup_driver

LICENSES_URL = "https://www.profixio.com/fx/ranking_sbtf/ranking_sbtf_public.php"

def scrape_player_licenses(cursor):
    """
    Scrape the player licenses raw data, process each row, 
    and insert/update into the player_license_raw table.
    """

    logger = OperationLogger(
        verbosity       = 2, 
        print_output    = False, 
        log_to_db       = True, 
        cursor          = cursor
    )
    
    driver = setup_driver()
    driver.get(LICENSES_URL)

    logger.info("Scraping player licenses...", to_console=True)

    # Wait for the page elements to load
    WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.LINK_TEXT, "Spelklarlistor"))).click()
    WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.NAME, "periode")))

    # Season dropdown
    period_dropdown = Select(driver.find_element(By.NAME, "periode"))
    all_seasons = [opt.get_attribute("value") for opt in period_dropdown.options if opt.get_attribute("value").isdigit()]
    if SCRAPE_LICENSES_ORDER.lower() == 'oldest':
        reverse = False
    else:
        reverse = True
    all_seasons = sorted(all_seasons, key=int, reverse=reverse)
    seasons_to_process = all_seasons[:SCRAPE_LICENSES_NBR_OF_SEASONS] if SCRAPE_LICENSES_NBR_OF_SEASONS > 0 else all_seasons

    # Club dropdown
    club_dropdown = Select(driver.find_element(By.NAME, "klubbid"))
    club_map = [{
        "club_name":    opt.text.strip(), 
        "club_id_ext":  int(opt.get_attribute("value"))
        } for opt in club_dropdown.options if opt.text.strip() and opt.get_attribute("value").isdigit()
    ]
    clubs = club_map[:SCRAPE_LICENSES_MAX_CLUBS] if SCRAPE_LICENSES_MAX_CLUBS > 0 else club_map

    logger.info(f"Scraping {len(clubs)} clubs for {len(seasons_to_process)} season(s) in {SCRAPE_LICENSES_ORDER.lower()} order.", to_console=True)

    # Counting
    total_inserted = 0
    total_skipped = 0
    current_club_count = 0
    current_season_count = 0

    for season_value in seasons_to_process:

        # Select the season (this reloads the DOM)
        Select(driver.find_element(By.NAME, "periode")).select_by_value(season_value)
        WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.CLASS_NAME, "table-condensed")))

        # Re-fetch the dropdown to avoid stale reference
        period_dropdown     = Select(driver.find_element(By.NAME, "periode"))
        selected_option     = period_dropdown.first_selected_option
        season_label        = selected_option.text.strip()
        season_id_ext       = int(selected_option.get_attribute("value"))

        logger.info(f"Scraping raw license data for season {season_label}...", to_console=True)

        for club in clubs:

            club_name   = club["club_name"]
            club_id_ext = club["club_id_ext"]

            # Define logger_key
            logger_keys = {
                "player_id_ext":        None,
                "firstname":            None,
                "lastname":             None,
                "year_born":            None,
                "license_info_raw":     None,
                "ranking_group_raw":    None,
                "season_label":         season_label,
                "season_id_ext":        season_id_ext,
                "club_name":            club_name,
                "club_id_ext":          club_id_ext
            }

            # Adding to season totals
            club_season_inserted = 0
            club_season_skipped = 0

            # Select club
            Select(driver.find_element(By.NAME, "klubbid")).select_by_visible_text(club_name)
            WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.CLASS_NAME, "table-condensed")))

            soup = BeautifulSoup(driver.page_source, "html.parser")
            table = soup.find("table", class_="table-condensed my-4 shadow-xl")
            if not table:
                logger.failed(logger_keys.copy(), "No table found for club in season")
                continue

            for row in table.select("tbody tr"):
                cols = row.find_all("td")
                if len(cols) < 9:
                    continue

                input_el            = row.find("input", {"type": "checkbox"})
                player_id_ext       = int(input_el["id"]) if input_el and input_el.has_attr("id") else None
                lastname            = cols[1].get_text(strip=True)
                firstname           = cols[2].get_text(strip=True)
                gender              = cols[3].get_text(strip=True)
                year_born           = cols[4].get_text(strip=True)
                license_info_raw    = cols[5].get_text(strip=True)
                ranking_group_raw   = cols[6].get_text(strip=True)

                # Update logger_keys
                logger_keys.update({
                    "player_id_ext":        player_id_ext,
                    "firstname":            firstname,
                    "lastname":             lastname,
                    "year_born":            year_born,
                    "license_info_raw":     license_info_raw,
                    "ranking_group_raw":    ranking_group_raw
                })

                # Validate and convert year_born
                year_born = int(year_born) if year_born.isdigit() else None
                if year_born is None:
                    logger.failed(logger_keys.copy(), "Invalid year_born")
                    continue

                # Create PlayerLicenseRaw object
                raw = PlayerLicenseRaw(
                    row_id              = None,
                    season_label        = season_label,
                    season_id_ext       = season_id_ext,
                    club_name           = club_name,
                    club_id_ext         = club_id_ext,
                    player_id_ext       = player_id_ext,
                    firstname           = firstname,
                    lastname            = lastname,
                    gender              = gender,
                    year_born           = year_born,
                    license_info_raw    = license_info_raw,
                    ranking_group_raw   = ranking_group_raw
                )

                # Validate
                is_valid, error_msg = raw.validate()
                if not is_valid:
                    logger.failed(logger_keys.copy(), error_msg)
                    continue

                inserted = raw.upsert_one(cursor, raw)

                if inserted is not None:
                    logger.success(logger_keys.copy(), "Raw player license record successfully upserted")

                if inserted:
                    total_inserted += 1
                    club_season_inserted += 1
                else:
                    total_skipped += 1
                    club_season_skipped += 1
                    club_season_skipped += 1

            # Commit changes after each club
            cursor.connection.commit()

            current_club_count += 1
            logger.info(
                f"Finished club {club_name} in season {season_label}, "
                f"inserted {club_season_inserted} rows, skipped {club_season_skipped} rows "
                f"({len(clubs) - current_club_count} clubs and {len(seasons_to_process) - current_season_count} seasons remaining)", to_console=False
            )
            # Using regular print to use separate icon (for now)
            print(
                f"✅ Finished club {club_name} in season {season_label}, "
                f"inserted {club_season_inserted} rows, skipped {club_season_skipped} rows "
                f"({len(clubs) - current_club_count} clubs and {len(seasons_to_process) - current_season_count} seasons remaining)"
            )

        current_season_count += 1
        current_club_count = 0

    logger.info(f"Scraping completed — Total inserted: {total_inserted}, Total skipped: {total_skipped}", to_console=True)
    driver.quit()
    logger.summarize()