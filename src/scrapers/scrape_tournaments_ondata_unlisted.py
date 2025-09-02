from typing import Optional
from bs4 import BeautifulSoup
import requests
from urllib.parse import urljoin
import time

from utils import OperationLogger
from models.tournament_raw import TournamentRaw


def scrape_tournaments_ondata_unlisted(cursor) -> None:
    """
    Scrape unlisted tournaments by finding gaps in tournament_id_ext from highest ID to 000001.
    Upserts raw data into tournament_raw table.
    """
    logger = OperationLogger(
        verbosity       = 2,
        print_output    = False,
        log_to_db       = True,
        cursor          = cursor
    )

    # Fetch all tournament_id_ext from tournament_raw table
    cursor.execute("SELECT tournament_id_ext FROM tournament_raw WHERE data_source_id = 1 AND tournament_id_ext IS NOT NULL")
    existing_ids = {int(row[0]) for row in cursor.fetchall() if row[0].isdigit()}

    if not existing_ids:
        print("❌ No existing tournament IDs found in tournament_raw table.")
        logger.failed({}, "No existing tournament IDs found")
        return

    # Find gaps from the highest ID down to 1
    max_id = max(existing_ids)
    min_id = 1  # Start from 000001
    missing_ids = [i for i in range(max_id, min_id - 1, -1) if i not in existing_ids]

    if not missing_ids:
        print("✅ No missing tournament IDs found in the range.")
        logger.info("No missing tournament IDs found")
        return

    print(f"🔍 Found {len(missing_ids)} potential unlisted tournament IDs to check.")
    logger.info(f"Found {len(missing_ids)} potential unlisted tournament IDs")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    for missing_id in missing_ids:
        ondata_id = f"{missing_id:06d}"  # Pad to 6 digits
        full_url = f"https://resultat.ondata.se/{ondata_id}/"
        
        logger_keys = {
            "ondata_id": ondata_id,
            "full_url": full_url,
            "longname": None
        }

        # Check if the tournament URL exists
        if not _check_tournament_url(ondata_id, headers, logger, logger_keys):
            print(f"❌ Tournament URL not found for ID {ondata_id}")
            logger.failed(logger_keys, f"Tournament URL not found for ID {ondata_id}")
            continue

        # Fetch longname only if URL exists
        longname = _fetch_tournament_longname(ondata_id, headers, logger, logger_keys)
        
        if not longname:
            print(f"❌ No valid longname found for tournament ID {ondata_id}")
            logger.failed(logger_keys, f"No valid longname found for ID {ondata_id}")
            continue

        logger_keys["longname"] = longname

        # Create TournamentRaw object with minimal fields
        raw = TournamentRaw(
            tournament_id_ext = ondata_id,
            longname = longname,
            url = full_url,
            data_source_id = 1
        )

        # Upsert without validation
        action = raw.upsert(cursor)
        if action:
            print(f"✅ Tournament ID {ondata_id} ({longname}) successfully {action}")
            logger.success(logger_keys, f"Tournament successfully {action}")
        else:
            print(f"⚠️ No changes made for tournament ID {ondata_id}")
            logger.warning(logger_keys, "No changes made during upsert")

        # Add a short delay to avoid overwhelming the server
        time.sleep(0.5)

    logger.summarize()


def _check_tournament_url(ondata_id: str, headers: dict, logger: OperationLogger, logger_keys: dict) -> bool:
    """
    Check if the tournament URL exists and has a valid result frame.
    Returns True if the URL is valid, False otherwise.
    """
    base_url = f"https://resultat.ondata.se/{ondata_id}/"
    try:
        # Use HEAD request to minimize data transfer
        r = requests.head(base_url, headers=headers, timeout=5)
        if r.status_code != 200:
            return False

        # Follow up with a GET request to check for result frame
        r = requests.get(base_url, headers=headers, timeout=5)
        r.raise_for_status()
        soup = BeautifulSoup(r.content, "html.parser")
        result_frame = soup.find("frame", {"name": "Resultat"})
        return bool(result_frame and result_frame.get("src"))
    except requests.Timeout:
        logger.failed(logger_keys, f"Timeout checking URL for ID {ondata_id}")
        return False
    except requests.ConnectionError:
        logger.failed(logger_keys, f"Connection error checking URL for ID {ondata_id}")
        return False
    except requests.HTTPError:
        return False  # HTTP errors (e.g., 404) indicate URL doesn't exist
    except Exception as e:
        logger.failed(logger_keys, f"Unexpected error checking URL for ID {ondata_id}: {e}")
        return False


def _fetch_tournament_longname(ondata_id: str, headers: dict, logger: OperationLogger, logger_keys: dict) -> Optional[str]:
    """
    Fetch tournament longname from result frame title.
    Returns None on failure.
    """
    base_url = f"https://resultat.ondata.se/{ondata_id}/"
    try:
        r1 = requests.get(base_url, headers=headers, timeout=10)
        r1.raise_for_status()
        soup1 = BeautifulSoup(r1.content, "html.parser")
        result_frame = soup1.find("frame", {"name": "Resultat"})
        if not result_frame or not result_frame.get("src"):
            return None
        result_url = urljoin(base_url, result_frame["src"])

        r2 = requests.get(result_url, headers=headers, timeout=10)
        r2.raise_for_status()
        r2.encoding = "iso-8859-1"
        soup2 = BeautifulSoup(r2.text, "html.parser")
        title_tag = soup2.find("title")
        return title_tag.text.strip() if title_tag else None
    except requests.Timeout:
        logger.failed(logger_keys, f"Timeout fetching longname for ID {ondata_id}")
        return None
    except requests.ConnectionError:
        logger.failed(logger_keys, f"Connection error fetching longname for ID {ondata_id}")
        return None
    except requests.HTTPError as e:
        logger.failed(logger_keys, f"HTTP error fetching longname for ID {ondata_id}: {e}")
        return None
    except Exception as e:
        logger.failed(logger_keys, f"Unexpected error fetching longname for ID {ondata_id}: {e}")
        return None