# src/scrape_data.py

import logging
from db import get_conn
from scrapers.scrape_tournaments_ondata_listed import scrape_tournaments_ondata_listed
from scrapers.scrape_tournaments_ondata_unlisted import scrape_tournaments_ondata_unlisted
from scrapers.scrape_tournament_classes_ondata import scrape_tournament_classes_ondata
from scrapers.scrape_player_licenses import scrape_player_licenses
from scrapers.scrape_player_transitions import scrape_player_transitions
# from scrapers.scrape_player_rankings import scrape_player_rankings
from scrapers.scrape_participants_ondata import scrape_participants_ondata

def scrape_data(scrp_tournaments=False, scrp_tournament_classes=False, scrp_player_licenses=False, scrp_player_transitions=False, scrp_participants=False):
    """
    Updater entry point: Scrape raw data, process through pipeline, aggregate results.
    """
    conn, cursor = get_conn()

    try:

        # Scrape ondata listed tournaments
        # =============================================================================
        if scrp_tournaments:
            scrape_tournaments_ondata_listed(cursor)
            # scrape_tournaments_ondata_unlisted(cursor) # Don't use regularly, should be only 2 potentially valid ones


        # Scrape other tournament sources
        # =============================================================================
        # TODO: Implement scraping for other tournament sources

    except Exception as e:
        logging.error(f"Error in upd_tournaments: {e}", stack_info=True, stacklevel=3, exc_info=True)

    finally:
        conn.commit()
        conn.close()