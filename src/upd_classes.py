import logging
import time
import re
import datetime
from datetime import timedelta
from utils import setup_driver, parse_date
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from config import SCRAPE_CLASSES_MAX_TOURNAMENTS
from db import get_conn, get_from_db_tournaments
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from utils import print_db_insert_results

def upd_classes():

    conn, cursor = get_conn()
    driver = setup_driver()

    try:
        logging.info("Starting classes scraping process...")
        print("ℹ️  Starting classes scraping process...")
        tournaments = get_from_db_tournaments(cursor)
        
        if not tournaments:
            logging.warning("No tournaments found in database.")
            print("⚠️ No tournaments found in database.")
            return

        logging.info(f"Found {len(tournaments)} tournaments in database.")
        print(f"ℹ️  Found {len(tournaments)} tournaments in database.")
        
        classes = scrape_classes(tournaments, driver)
        
        if classes:
            unique_tournaments = len(set(class_data['tournament_id'] for class_data in classes))
            logging.info(f"Scraped {len(classes)} classes from {unique_tournaments} tournament(s).")
            print(f"✅ Scraped {len(classes)} classes from {unique_tournaments} tournament(s).")
            print_details_class(classes)
            
            try:
                status_list = save_classes_to_db(cursor, classes)
                if status_list:
                    print_db_insert_results(status_list)
                else:
                    logging.warning("No classes were saved to the database.")
                    print("⚠️ No classes were saved to the database.")
            
            except Exception as e:
                logging.error(f"Error during database insertion: {e}")
                print(f"❌ Error during database insertion: {e}")

        else:
            logging.warning("No classes found to scrape.")

    except Exception as e:
        logging.error(f"Error during class scraping: {e}")
        print(f"❌ Error during class scraping: {e}")
    
    finally:
        logging.info("-------------------------------------------------------------------")
        driver.quit()
        conn.commit()
        conn.close()



def scrape_classes(tournaments, driver):
    
    all_classes = []

    try:
        for tournament in tournaments:
            tournament_id = tournament["tournament_id"]
            tournament_name = tournament["name"]
            tournament_url = tournament["url"]
            tournament_startdate = tournament["start_date"]
            
            classes = scrape_classes_for_tournament(driver, tournament_id, tournament_name, tournament_url, tournament_startdate)
            
            if classes:
                logging.debug(f"Found {len(classes)} classes for tournament '{tournament_name}' (ID: {tournament_id})")
                all_classes.extend(classes)
            else:
                logging.warning(f"No classes found for tournament '{tournament_name}' (ID: {tournament_id})")

        return all_classes

    except Exception as e:
        logging.error(f"Exception during class scraping: {e}")
        print(f"❌ Exception during class scraping: {e}")
        return []

def scrape_classes_for_tournament(driver, tournament_id, tournament_name, tournament_url, tournament_startdate):
    try:
        logging.debug(f"Fetching classes for tournament {tournament_name} at {tournament_url}")
        driver.get(tournament_url)

        WebDriverWait(driver, 10).until(
            EC.frame_to_be_available_and_switch_to_it((By.NAME, "Resultat"))
        )
        logging.debug(f"Switched to 'Resultat' frame.")

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "table"))
        )

        soup = BeautifulSoup(driver.page_source, "html.parser")

        table = parse_table(soup)
        if not table:
            driver.switch_to.default_content()
            return []

        rows = parse_rows(table)
        if not rows:
            driver.switch_to.default_content()
            return []

        classes_found = []
        for row in rows:
            class_data = extract_class_data_from_row(row, tournament_id, tournament_url, tournament_startdate)
            if class_data:
                classes_found.append(class_data)
                logging.debug(f"Found class: {class_data['class_description']} on {class_data['date']}")

        driver.switch_to.default_content()
        return classes_found

    except Exception as e:
        logging.error(f"Error fetching classes from {tournament_url}: {e}")
        return []

    
def save_classes_to_db(cursor, classes):
    """Insert classes into the database, checking for existing entries."""
    status_list = []

    for class_data in classes:
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO tournament_class (
                    tournament_id, date, class_description, class_short, gender, max_rank, 
                    max_age, players_url, groups_url, group_games_url, group_results_url, 
                    knockout_url, final_results_url, row_created
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                class_data["tournament_id"],
                class_data["date"],
                class_data["class_description"],
                class_data["class_short"],
                class_data["gender"], 
                class_data["max_rank"], 
                class_data["max_age"], 
                class_data["players_url"], 
                class_data["groups_url"], 
                class_data["group_games_url"], 
                class_data["group_results_url"], 
                class_data["knockout_url"], 
                class_data["final_results_url"],
                class_data["row_created"]
            ))

            if cursor.rowcount == 0:
                # Row was ignored => class already exists (unique constraint)
                status_list.append({
                    "status": "skipped",
                    "key": class_data["class_short"],
                    "reason": "Tournament class already exists",
                    "class": class_data
                })

            else:
                status_list.append({
                    "status": "success", 
                    "reason": "Tournament class inserted successfully",
                    "class": class_data})

        except Exception as e:
            logging.error(f"Error inserting class {class_data['class_short']} into the database: {e}")
            status_list.append({"status": "failed", "class": class_data})

    return status_list

def parse_table(soup):
    """Extract the main table element or return None."""
    table = soup.find("table")
    if not table:
        logging.warning("No table found in HTML.")
        return None
    return table

def parse_rows(table):
    """Extract rows from the table, skipping headers."""
    rows = table.find_all("tr")
    if len(rows) < 3:
        logging.warning("Not enough rows found in table.")
        return []
    return rows[2:]  # Skip header rows


def extract_url_from_cell(cols, idx, base_url):
    if len(cols) > idx:
        a_tag = cols[idx].find("a", href=True)
        if a_tag and "ViewClassPDF.php" in a_tag["href"]:
            return urljoin(base_url, a_tag["href"])
    return ""

def extract_class_data_from_row(row, tournament_id, tournament_url, tournament_startdate):
    """Parse a single row to extract class details."""
    cols = row.find_all("td")
    if len(cols) < 7:
        logging.debug(f"Skipping row with insufficient columns ({len(cols)})")
        return None

    day = cols[0].get_text(strip=True) or "N/A"
    class_desc = cols[2].get_text(strip=True) or "N/A"
    class_short = class_desc[:50] if class_desc else "N/A"

    def extract_url(idx):
        if len(cols) > idx:
            a_tag = cols[idx].find("a", href=True)
            if a_tag and "ViewClassPDF.php" in a_tag["href"]:
                return urljoin(tournament_url, a_tag["href"])
        return ""

    players_url = extract_url(3)
    groups_url = extract_url(4)
    group_games_url = extract_url(5)
    group_results_url = extract_url(6)
    knockout_url = extract_url(7)
    final_results_url = extract_url(8)

    full_date = infer_full_date(day, tournament_startdate)
    if not full_date:
        logging.warning(f"Skipping class '{class_desc}' due to invalid date inference.")
        return None

    return {
        "tournament_id": tournament_id,
        "date": full_date,
        "class_description": class_desc,
        "class_short": class_short,
        "gender": None,
        "max_rank": None,
        "max_age": None,
        "players_url": players_url,
        "groups_url": groups_url,
        "group_games_url": group_games_url,
        "group_results_url": group_results_url,
        "knockout_url": knockout_url,
        "final_results_url": final_results_url,
        "row_created": datetime.datetime.now().isoformat(),
    }

def infer_full_date(day_str, tournament_startdate):
    """Infer the full date for a class based on the day string and tournament start date."""
    if not tournament_startdate or not day_str:
        return None
    
    # Ensure tournament_startdate is a string
    if isinstance(tournament_startdate, datetime.date):
        tournament_startdate = tournament_startdate.strftime("%Y-%m-%d")
    
    try:
        # Strip the day_str and extract day number
        day_num = int(re.search(r'\d+', day_str.strip()).group())
        start_date = parse_date(tournament_startdate)
        if start_date:
            return start_date.replace(day=day_num)
        return None
    except (ValueError, AttributeError) as e:
        logging.warning(f"Could not infer date from '{day_str}' and '{tournament_startdate}': {str(e)}")
        return None

def print_details_class(classes):
    for class_data in classes:
        logging.debug(f"Tournament ID: {class_data['tournament_id']}")        
        logging.debug(f"Date: {class_data['date']}")        
        logging.debug(f"Class description: {class_data['class_description']}")
        logging.debug(f"Class Short: {class_data['class_short']}") 
        logging.debug(f"Gender: {class_data['gender']}")
        logging.debug(f"Max Rank: {class_data['max_rank']}")
        logging.debug(f"Max Age: {class_data['max_age']}")
        logging.debug(f"Players URL: {class_data['players_url']}")
        logging.debug(f"Groups URL: {class_data['groups_url']}")
        logging.debug(f"Group Games URL: {class_data['group_games_url']}")
        logging.debug(f"Group Results URL: {class_data['group_results_url']}")
        logging.debug(f"Knockout URL: {class_data['knockout_url']}")
        logging.debug(f"Final Results URL: {class_data['final_results_url']}")


# def print_db_insert_results(status_list):
#     success_count = sum(1 for status in status_list if status["status"] == "success")
#     failed_count = sum(1 for status in status_list if status["status"] == "failed")
#     skipped_count = sum(1 for status in status_list if status["status"] == "skipped")

#     logging.info(f"Database summary: {success_count} classes inserted, {failed_count} failed, {skipped_count} skipped.")
#     print(f"ℹ️  Database summary: {success_count} classes inserted, {failed_count} failed, {skipped_count} skipped.")