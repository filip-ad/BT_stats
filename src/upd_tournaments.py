# src/upd_tournaments.py

import logging
from utils import parse_date, OperationLogger
from db import get_conn
from config import SCRAPE_TOURNAMENTS_CUTOFF_DATE
from models.tournament import Tournament
from scrapers.scrape_tournaments_ondata_listed import scrape_tournaments_ondata_listed
from scrapers.scrape_tournaments_ondata_unlisted import scrape_tournaments_ondata_unlisted
from resolvers.resolve_tournaments import resolve_tournaments

def upd_tournaments(scrape_ondata=False, resolve=False):
    """
    Updater entry point: Scrape raw data, process through pipeline, aggregate results.
    """
    conn, cursor = get_conn()

    logger = OperationLogger(
        verbosity       = 2, 
        print_output    = False, 
        log_to_db       = False, 
        cursor          = cursor
        )

    try:

        # Scrape ondata listed tournaments
        # =============================================================================
        if scrape_ondata:
            scrape_tournaments_ondata_listed(cursor)
            # scrape_tournaments_ondata_unlisted(cursor) # Don't use regularly, should be only 2 potentially valid ones


        # Scrape other tournament sources
        # =============================================================================
        # TODO: Implement scraping for other tournament sources

        # Resolve tournaments
        # =============================================================================
        if resolve:
            resolve_tournaments(cursor)

    except Exception as e:
        logging.error(f"Error in upd_tournaments: {e}", stack_info=True, stacklevel=3, exc_info=True)

    finally:
        conn.commit()
        conn.close()