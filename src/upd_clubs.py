# src/upd_club_data.py

import logging
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from collections import defaultdict
from utils import setup_driver, print_db_insert_results
from config import LICENSES_URL
from db import get_conn
from models.club import Club

def upd_clubs():

    conn, cursor = get_conn()
    driver = setup_driver()

    try:
        # Scrape clubs
        logging.info("Starting club scraping process...")
        print("ℹ️  Starting club scraping process...")
        clubs = scrape_clubs(driver)

        if clubs:
            logging.info(f"Successfully scraped {len(clubs)} clubs.")
            print(f"✅ Successfully scraped {len(clubs)} clubs.")

            try: 
                # Insert clubs into the database
                db_results = [club.save_to_db(cursor) for club in clubs]

                if db_results:
                    print_db_insert_results(db_results)

                else: 
                    logging.warning("No clubs saved to database.")
                    print("⚠️ No clubs saved to database.")

            except Exception as e:
                logging.error(f"Error during database insertion: {e}") 
                print(f"❌ Error during database insertion: {e}")
        else:
            logging.warning("No clubs scraped.")
            print("⚠️ No clubs scraped.")

    finally:
        logging.info("-------------------------------------------------------------------")
        driver.quit()
        conn.commit()
        conn.close()

def scrape_clubs(driver):
    """ 
    Scrape clubs from the specified URL and return a list of Club objects.
    """
    clubs = []
    try:
        driver.get(LICENSES_URL)

        # Click on the "Spelklarlistor" link
        spelklar_link = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.LINK_TEXT, "Spelklarlistor"))
        )
        spelklar_link.click()
    
        WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.NAME, "klubbid"))) # Wait for the page to load and the club dropdown to be present

        club_dropdown = Select(driver.find_element(By.NAME, "klubbid"))
        for option in club_dropdown.options:
            club_name = option.text.strip()
            club_id_ext = option.get_attribute("value").strip()
            if club_name and club_id_ext:
                clubs.append(Club(
                    club_id_ext=int(club_id_ext),
                    name=club_name,
                    city=None,
                    country_code=None
                ))

    except Exception as e:
        logging.error(f"An error occurred while scraping clubs: {e}")
        print(f"❌ An error occurred while scraping clubs: {e}")

    return clubs