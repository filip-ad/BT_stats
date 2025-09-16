# src/scrapers/scrape_player_licenses.py

import time
import requests
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

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from concurrent.futures import ThreadPoolExecutor, as_completed

import random

import threading

thread_local = threading.local()

USE_CONCURRENCY = True   # ← flip to False to go back to sequential
MAX_WORKERS     = 8      # start with 4–6 to be gentle; tune if needed

FX_URL = "https://www.profixio.com/fx/lisens/public_oversikt.php"

COMMON_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
    "Referer": "https://www.profixio.com/fx/lisens/public_oversikt.php",
    "Origin":  "https://www.profixio.com",
}

LICENSES_URL = "https://www.profixio.com/fx/ranking_sbtf/ranking_sbtf_public.php"

def _get_worker_session(cookies_dict):
    s = getattr(thread_local, "session", None)
    if s is None:
        s = requests.Session()
        s.headers.update(COMMON_HEADERS)
        retry = Retry(
            total=3,
            backoff_factor=0.3,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods={"POST"},
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS, max_retries=retry)
        s.mount("https://", adapter)
        # copy Selenium cookies once per worker
        for k, v in cookies_dict.items():
            s.cookies.set(k, v)
        thread_local.session = s
    return s

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
    WebDriverWait(driver, 0.25).until(EC.element_to_be_clickable((By.LINK_TEXT, "Spelklarlistor"))).click()
    WebDriverWait(driver, 0.25).until(EC.presence_of_element_located((By.NAME, "periode")))

    # Also keep a simple cookies dict for thread-local sessions
    selenium_cookies = {c["name"]: c["value"] for c in driver.get_cookies()}

    # Season dropdown + map (build once)
    period_dropdown = Select(driver.find_element(By.NAME, "periode"))
    season_opts = [
        opt for opt in period_dropdown.options
        if opt.get_attribute("value") and opt.get_attribute("value").isdigit()
    ]

    season_value_to_label = {
        opt.get_attribute("value"): opt.text.strip()
        for opt in season_opts
    }

    reverse = SCRAPE_LICENSES_ORDER.lower() != "oldest"
    all_seasons = sorted(
        [opt.get_attribute("value") for opt in season_opts],
        key=int,
        reverse=reverse,
    )
    seasons_to_process = (
        all_seasons[:SCRAPE_LICENSES_NBR_OF_SEASONS]
        if SCRAPE_LICENSES_NBR_OF_SEASONS > 0 else all_seasons
    )
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
    total_updated = 0
    total_unchanged = 0
    current_season_count = 0

    def fetch_club_html(club, season_id_ext, selenium_cookies):
        """
        Thread worker: reuse a per-thread Session (keeps TCP/TLS alive).
        Returns: (club, html, step1_time_seconds)
        """
        # small random jitter to be polite
        time.sleep(random.uniform(0.0, 0.25))

        # ⬇️ Reuse one Session per worker thread
        s = _get_worker_session(selenium_cookies)

        payload = {
            "periode": season_id_ext,
            "hiddenklubbnavn": club["club_name"],
            "klubbid": club["club_id_ext"],
            "kjonn": "a",
            "klasse": "",
            "lisensypeid": "",
            "sort_column": "etternavn",
            "sort_direction": "ASC",
        }

        t0 = time.time()
        r = s.post(FX_URL, data=payload, timeout=(5, 25))  # (connect, read) timeouts
        step1_time = time.time() - t0
        r.raise_for_status()

        return club, r.text, step1_time

    for season_value in seasons_to_process:

        season_time_start = time.perf_counter()
        season_id_ext = int(season_value)
        season_label  = season_value_to_label.get(str(season_value), str(season_value))

        # # Re-fetch the dropdown to avoid stale reference
        # period_dropdown     = Select(driver.find_element(By.NAME, "periode"))
        # selected_option     = period_dropdown.first_selected_option
        # season_label        = selected_option.text.strip()
        # season_id_ext       = int(selected_option.get_attribute("value"))

        

        logger.info(f"Scraping raw license data for season {season_label}...", to_console=True)

        if USE_CONCURRENCY:

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                
                futures = []

                for i, club in enumerate(clubs, start=1):
                    futures.append(ex.submit(fetch_club_html, club, season_id_ext, selenium_cookies))

                processed = 0

                for fut in as_completed(futures):
                    try:
                        club, html, step1_time = fut.result()
                    except Exception as e:
                        logger.failed({"club": None}, f"Fetch failed: {e}")
                        continue

                    club_name   = club["club_name"]
                    club_id_ext = club["club_id_ext"]

                    # Parse
                    step2_start = time.time()
                    soup = BeautifulSoup(html, "html.parser")
                    table = soup.find("table", class_="table-condensed my-4 shadow-xl")
                    step2_time = time.time() - step2_start

                    club_season_inserted = 0
                    club_season_skipped  = 0

                    logger_keys = {
                        "player_id_ext":     None,
                        "firstname":         None,
                        "lastname":          None,
                        "year_born":         None,
                        "license_info_raw":  None,
                        "ranking_group_raw": None,
                        "season_label":      season_label,
                        "season_id_ext":     season_id_ext,
                        "club_name":         club_name,
                        "club_id_ext":       club_id_ext
                    }

                    if not table:
                        logger.failed(logger_keys.copy(), "No table found for club in season")
                        processed += 1
                        remaining = len(clubs) - processed
                        total_time = step1_time + step2_time  # no insert
                        print(
                            f"✅ Finished club {club_name:<25} | Season: {season_label:<12} | "
                            f"Inserted: {club_season_inserted:<3} | Skipped: {club_season_skipped:<3} | "
                            f"Remaining: {remaining} clubs, {len(seasons_to_process) - current_season_count} seasons "
                            f"({total_time:.2f} sec | select: {step1_time:.2f}s, parse: {step2_time:.2f}s, insert: 0.00s)",
                            flush=True
                        )
                        continue

                    # Insert (main thread)
                    step3_start = time.time()
                    for row in table.select("tbody tr"):
                        cols = row.find_all("td")
                        if len(cols) < 9:
                            continue
                        input_el          = row.find("input", {"type": "checkbox"})
                        player_id_ext     = int(input_el["id"]) if input_el and input_el.has_attr("id") else None
                        lastname          = cols[1].get_text(strip=True)
                        firstname         = cols[2].get_text(strip=True)
                        gender            = cols[3].get_text(strip=True)
                        year_born         = cols[4].get_text(strip=True)
                        license_info_raw  = cols[5].get_text(strip=True)
                        ranking_group_raw = cols[6].get_text(strip=True)

                        logger_keys.update({
                            "player_id_ext":        player_id_ext,
                            "firstname":            firstname,
                            "lastname":             lastname,
                            "year_born":            year_born,
                            "license_info_raw":     license_info_raw,
                            "ranking_group_raw":    ranking_group_raw
                        })

                        yb = int(year_born) if year_born.isdigit() else None
                        if yb is None:
                            logger.failed(logger_keys.copy(), "Invalid year_born")
                            club_season_skipped += 1
                            total_skipped += 1
                            continue

                        raw = PlayerLicenseRaw(
                            row_id=None,
                            season_label=season_label,
                            season_id_ext=season_id_ext,
                            club_name=club_name,
                            club_id_ext=club_id_ext,
                            player_id_ext=player_id_ext,
                            firstname=firstname,
                            lastname=lastname,
                            gender=gender,
                            year_born=yb,
                            license_info_raw=license_info_raw,
                            ranking_group_raw=ranking_group_raw
                        )

                        is_valid, error_msg = raw.validate()
                        if not is_valid:
                            logger.failed(logger_keys.copy(), error_msg)
                            club_season_skipped += 1
                            total_skipped += 1
                            continue

                        # inserted = raw.upsert_one(cursor, raw)
                        # if inserted is not None:
                        #     logger.success(logger_keys.copy(), "Raw player license record successfully upserted")

                        # if inserted:
                        #     total_inserted += 1
                        #     club_season_inserted += 1
                        # else:
                        #     total_skipped += 1
                        #     club_season_skipped += 1

                        result = raw.upsert(cursor)

                        if result == "inserted":
                            total_inserted += 1
                            club_season_inserted += 1
                            logger.success(logger_keys.copy(), "Raw license inserted")
                        elif result == "updated":
                            # optional: track updates separately
                            # total_updated += 1
                            # club_season_updated += 1
                            total_updated += 1   # or keep a separate counter; up to you
                            logger.success(logger_keys.copy(), "Raw license updated")
                        elif result == "unchanged":  # "unchanged" or None
                            total_unchanged += 1
                            logger.success(logger_keys.copy(), "Raw license unchanged")
                        else:
                            logger.failed(logger_keys.copy(), "Upsert failed")


                    cursor.connection.commit()
                    step3_time = time.time() - step3_start

                    processed += 1
                    remaining = len(clubs) - processed
                    total_time = step1_time + step2_time + step3_time

                    print(
                        f"✅ Finished club {club_name:<25} | Season: {season_label:<12} | "
                        f"Inserted: {club_season_inserted:<3} | Skipped: {club_season_skipped:<3} | "
                        f"Remaining: {remaining} clubs, {len(seasons_to_process) - current_season_count} seasons "
                        f"({total_time:.2f} sec | select: {step1_time:.2f}s, parse: {step2_time:.2f}s, insert: {step3_time:.2f}s)",
                        flush=True
                    )
        
        season_time = time.perf_counter() - season_time_start
        current_season_count += 1
        logger.info(f"Completed season {season_label} in {season_time:.2f} seconds.", to_console=True)

    logger.info(f"Scraping completed — Total inserted: {total_inserted}, total updated: {total_updated}, total unchanged: {total_unchanged}", to_console=True)
    driver.quit()
    logger.summarize()