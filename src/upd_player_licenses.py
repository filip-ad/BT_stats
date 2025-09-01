# src/upd_player_licenses.py

import logging
from db import get_conn

from resolvers.resolve_player_licenses import resolve_player_licenses
from resolvers.resolve_player_ranking_groups import resolve_player_ranking_groups
from scrapers.scrape_player_licenses import scrape_player_licenses


def upd_player_licenses(scrape=False, resolve=False, update_ranking_groups=False):
    """
    Update player licenses.
    If scrape_raw_data is True, it will scrape and insert new raw data into player_license_raw.
    If False, it will skip scraping and resolve existing raw data.
    """
    conn, cursor = get_conn()

    try:

        # Step 1: Scrape player license raw data if required
        if scrape:
            scrape_player_licenses(cursor)
       
        # Step 2: Resolve and update player licenses from raw data
        if resolve:
            resolve_player_licenses(cursor)

        # Step 3: Update player ranking groups
        if update_ranking_groups:
            resolve_player_ranking_groups(cursor)

        conn.commit()


    except Exception as e:
        logging.error(f"Error updating player licenses: {e}")
        print(f"❌ Error updating player licenses: {e}")
        conn.rollback()
    finally:
        conn.close()
