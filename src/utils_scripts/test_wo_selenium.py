import logging
import requests
from bs4 import BeautifulSoup

# URL for the public license overview
LICENSES_URL = "https://www.profixio.com/fx/lisens/public_oversikt.php"

# Limits for the test run
NUM_CLUBS = 3
NUM_SEASONS = 2


def scrape_test(num_clubs=NUM_CLUBS, num_seasons=NUM_SEASONS):
    # Configure basic logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s"
    )

    session = requests.Session()
    # Initial GET to obtain dropdowns
    resp = session.get(LICENSES_URL, timeout=10)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    # Extract season values
    period_select = soup.find("select", attrs={"name": "periode"})
    seasons = [opt.get("value") for opt in period_select.find_all("option") if opt.get("value","").isdigit()]
    seasons = seasons[:num_seasons]

    # Extract club list
    club_select = soup.find("select", attrs={"name": "klubbid"})
    clubs = [
        (opt.text.strip(), opt.get("value"))
        for opt in club_select.find_all("option")
        if opt.get("value","").isdigit()
    ]
    clubs = clubs[:num_clubs]

    logging.info(f"Running test scrape for {len(clubs)} clubs and {len(seasons)} seasons")

    for season in seasons:
        for club_name, club_id in clubs:
            params = {"periode": season, "klubbid": club_id}
            resp = session.get(LICENSES_URL, params=params, timeout=10)
            if resp.status_code != 200:
                logging.error(f"HTTP error for {club_name} (season={season}): {resp.status_code}")
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            table = soup.find("table", class_="table-condensed")
            if not table:
                logging.warning(f"No table found for {club_name} in season {season}")
                continue

            rows = table.select("tbody tr")
            logging.info(f"Found {len(rows)} player rows for club '{club_name}' in season {season}")

            # Log the first row's data as an example
            if rows:
                first_cols = [td.get_text(strip=True) for td in rows[0].find_all("td")]
                logging.info(f"First row example for {club_name} season {season}: {first_cols}")

    logging.info("Test scrape complete")


if __name__ == "__main__":
    scrape_test()
