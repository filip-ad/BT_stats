# src/upd_player_transitions.py

import logging
from db import get_conn

from resolvers.resolve_player_transitions import resolve_player_transitions
from scrapers.scrape_player_transitions import scrape_player_transitions


def upd_player_transitions(scrape=False, resolve=False):
    """
    Update player transitions.
    If scrape is True, it will scrape and insert new raw data into player_transition_raw.
    If resolve is True, it will resolve existing raw data.
    """
    conn, cursor = get_conn()

    try:

        # Step 1: Scrape player transition raw data if required
        if scrape:
            scrape_player_transitions(cursor)
       
        # Step 2: Resolve and update player transitions from raw data
        if resolve:
            resolve_player_transitions(cursor)

        conn.commit()

    except Exception as e:
        logging.error(f"Error updating player transitions: {e}")
        print(f"❌ Error updating player transitions: {e}")
        conn.rollback()
    finally:
        conn.close()