import logging
from datetime import datetime, timedelta
from collections import defaultdict
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from utils import setup_driver, parse_date
from config import LICENSES_URL, SCRAPE_SEASONS
from db import get_conn, get_from_db_season

def save_to_db_transitions(cursor, transitions):
    status_list = []
    for t in transitions:
        try:
            # Check if the transition already exists
            cursor.execute("""
                SELECT COUNT(*) FROM player_transition
                WHERE lastname = ? AND firstname = ? AND year_born = ? 
                AND club_from = ? AND club_to = ? AND transition_date = ? AND season = ?
            """, (t['lastname'], t['firstname'], t['year_born'], t['club_from'], t['club_to'], t['transition_date'], t['season']))
            exists = cursor.fetchone()[0] > 0

            if exists:
                status_list.append({
                    "status": "skipped",
                    "player": f"{t['firstname']} {t['lastname']}",
                    "reason": "duplicate"
                })
                continue

            # Insert the new transition
            cursor.execute("""
                INSERT INTO player_transitions (lastname, firstname, year_born, club_from, club_to, transition_date, season)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (t['lastname'], t['firstname'], t['year_born'], t['club_from'], t['club_to'], t['transition_date'], t['season']))

            status_list.append({
                "status": "inserted",
                "player": f"{t['firstname']} {t['lastname']}",
                "reason": "success"
            })
        except Exception as e:
            status_list.append({
                "status": "failed",
                "player": f"{t['firstname']} {t['lastname']}",
                "reason": str(e)
            })

    return status_list

def scrape_transitions(driver, season_value, season_label):
    # Select season in dropdown
    period_dropdown = Select(driver.find_element(By.ID, "periode"))
    period_dropdown.select_by_value(season_value)

    logging.debug(f"Selected season {season_label} (value: {season_value})")

    # Wait until the page loads table rows inside any <table>
    WebDriverWait(driver, 10).until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table tbody tr"))
    )

    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")

    update_text = soup.find(string=lambda text: text and "Uppdaterad" in text)
    if not update_text:
        logging.warning(f"⚠️ Could not find 'Uppdaterad' marker for season {season_label}")
        return []

    table = update_text.find_next("table")

    if not table:
        logging.warning(f"⚠️ Could not find transitions table after 'Uppdaterad' for season {season_label}")
        return []

    rows = table.find_all("tr")
    logging.debug(f"Number of rows found in transitions table: {len(rows)}")

    transitions = []

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
        date_born = parse_date(date_born_str)
        year_born = str(date_born.year) if date_born else None
        club_from = cols[3].get_text(strip=True)
        club_to = cols[4].get_text(strip=True)
        transition_date_str = cols[5].get_text(strip=True)
        transition_date = parse_date(transition_date_str)

        if not transition_date:
            logging.warning(f"Skipping transition due to invalid date: {transition_date_str}")
            continue

        if not year_born:
            logging.warning(f"Skipping transition due to invalid year of birth: {date_born_str}")
            continue

        transitions.append({
            "lastname": lastname,
            "firstname": firstname,
            "year_born": year_born,
            "club_from": club_from,
            "club_to": club_to,
            "transition_date": transition_date,
            "season": season_value,
        })

    return transitions

def upd_player_transitions():
    conn, cursor = get_conn()
    driver = setup_driver()

    try:
        logging.info("Scraping transition data...")
        print("ℹ️  Scraping transition data...")

        driver.get(LICENSES_URL)

        # Click the "Spelklarlistor" link
        spelklar_link = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.LINK_TEXT, "Spelklarlistor"))
        )
        spelklar_link.click()        

        # Click the "Övergångar" link
        overgang_link = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.LINK_TEXT, "Övergångar"))
        )
        overgang_link.click()

        # Wait for season dropdown
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "periode"))
        )

        period_dropdown = Select(driver.find_element(By.ID, "periode"))
        all_seasons = sorted(
            [opt.get_attribute("value") for opt in period_dropdown.options if opt.get_attribute("value").isdigit()],
            key=int,
            reverse=True
        )

        season_map = {
            opt.get_attribute("value"): opt.text.strip()
            for opt in period_dropdown.options if opt.get_attribute("value").isdigit()
        }

        seasons_to_process = all_seasons[:SCRAPE_SEASONS] if SCRAPE_SEASONS > 0 else all_seasons

        for season_value in seasons_to_process:
            season_label = season_map.get(season_value, season_value)
            logging.info(f"Scraping transitions for season {season_label}")
            print(f"ℹ️  Scraping transitions for season {season_label}")

            season = get_from_db_season(cursor, season_id_ext=int(season_value))
            if not season:
                logging.warning(f"Skipping season {season_value} - not found in DB.")
                continue                  

            try:
                transitions = scrape_transitions(driver, season_value, season_label)

                if transitions:
                    logging.info(f"Found {len(transitions)} transitions for season {season_label}")
                    print(f"✅ Found {len(transitions)} transitions for season {season_label}") 
                    print_details_transition(transitions)  # Print details for debugging
                    db_results = save_to_db_transitions(cursor, transitions)

                    if db_results:
                        print_db_insert_results(db_results)
                        log_failed_or_skipped_entries(db_results)  # log detailed failure info
                    else:
                        logging.warning(f"No transitions saved to database.")
                        print(f"⚠️ No transitions saved to database.")
                else:
                    logging.warning(f"No transitions scraped.")
                    print(f"⚠️ No transitions scraped.") 

            except Exception as e:
                logging.error(f"Error processing season {season_label}: {e}")
                print(f"❌ Error processing season {season_label}: {e}")

    finally:
        logging.info("-------------------------------------------------------------------")
        print("-------------------------------------------------------------------")
        driver.quit()
        conn.commit()
        conn.close()

def print_details_transition(transitions):
    for t in transitions:
        logging.debug(f"Lastname: {t.get('lastname', 'N/A')}")
        logging.debug(f"Firstname: {t.get('firstname', 'N/A')}")
        logging.debug(f"Year born: {t.get('year_born', 'N/A')}")
        logging.debug(f"From club: {t.get('club_from', 'N/A')}")
        logging.debug(f"To club: {t.get('club_to', 'N/A')}")
        logging.debug(f"Transition date: {t.get('transition_date', 'N/A')}")

def print_db_insert_results(status_list):
    summary = defaultdict(lambda: defaultdict(int))  # summary[status][reason] = count

    for entry in status_list:
        status = entry.get("status", "unknown")
        reason = entry.get("reason", "unspecified")
        summary[status][reason] += 1

    logging.info("Transition insert summary:")
    print("ℹ️  Transition insert summary:")

    for status in summary:
        status_total = sum(summary[status].values())
        logging.info(f"  {status.title()}: {status_total}")
        print(f"  {status.title()}: {status_total}")

        for reason, count in summary[status].items():
            logging.info(f"    - {reason}: {count}")
            print(f"    - {reason}: {count}")

def log_failed_or_skipped_entries(status_list):
    for entry in status_list:
        if entry.get("status") in ("skipped", "failed"):
            player = entry.get("player", "Unknown player")
            reason = entry.get("reason", "No reason given")
            logging.debug(f"{entry['status'].title()} transition for {player}: {reason}")