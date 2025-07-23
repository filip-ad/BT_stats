import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from config import PLAYERS_URL
from utils import setup_driver
import logging
from collections import Counter

def scrape_unique_licenses(season_value="126", max_clubs=125):
    driver = setup_driver()
    try:
        driver.get(PLAYERS_URL)
        license_entries = []

        # Click "Spelklarlistor"
        WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.LINK_TEXT, "Spelklarlistor"))
        ).click()

        # Select season by value
        period_dropdown = Select(WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.NAME, "periode"))
        ))
        period_dropdown.select_by_value(season_value)

        # Get all clubs and limit by max_clubs
        club_dropdown = Select(WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.NAME, "klubbid"))
        ))
        club_options = [opt.text.strip() for opt in club_dropdown.options if opt.text.strip()]
        clubs_to_scrape = club_options[:max_clubs]

        for club in clubs_to_scrape:
            print(f"🔍 Scraping club: {club} (season {season_value})")

            # Select club
            club_dropdown = Select(driver.find_element(By.NAME, "klubbid"))
            club_dropdown.select_by_visible_text(club)

            # Select "[Alla]" in class dropdown
            WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.NAME, "klasse")))
            klasse_dropdown = Select(driver.find_element(By.NAME, "klasse"))
            klasse_dropdown.select_by_visible_text("[Alla]")

            # Wait for table and parse
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "table-condensed"))
            )
            soup = BeautifulSoup(driver.page_source, "html.parser")
            rows = soup.select("table.table-condensed tbody tr")

            for row in rows:
                cols = row.find_all("td")
                if len(cols) >= 6:
                    raw_text = cols[5].get_text(strip=True)
                    
                    # Regex: Match license type, optional age, and date
                    match = re.match(r"(?P<type>[A-D]-licens)(?: (?P<age>\w+))? \((?P<date>\d{4}\.\d{2}\.\d{2})\)", raw_text)
                    if match:
                        license_entries.append({
                            "license_type": match.group("type"),
                            "license_age": match.group("age") or None,
                            "license_date": match.group("date")
                        })

    finally:
        driver.quit()

    # Count unique (type, age) combinations
    combo_counter = Counter(
        (entry["license_type"], entry["license_age"]) for entry in license_entries
    )

    logging.info("Summary of unique license_type & license_age combinations:")
    for (license_type, license_age), count in combo_counter.items():
        logging.info(f" - {license_type} / {license_age or 'N/A'}: {count}")