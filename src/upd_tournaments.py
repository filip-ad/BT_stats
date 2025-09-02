# src/upd_tournaments.py

import logging
from utils import parse_date, OperationLogger
from db import get_conn
from config import SCRAPE_TOURNAMENTS_CUTOFF_DATE
from models.tournament import Tournament
from scrapers.scrape_tournaments_ondata_listed import scrape_raw_tournaments_ondata
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
        cutoff_date = parse_date(SCRAPE_TOURNAMENTS_CUTOFF_DATE)
        logger.info(f"Starting tournament update, cutoff: {cutoff_date}")

        # Scrape all tournaments
        # =============================================================================
        if scrape_ondata:
            scrape_raw_tournaments_ondata(cursor)
            

        # Fetch raw from db
        # =============================================================================
        cursor.execute("SELECT * FROM tournament_raw WHERE data_source_id = 1")
        columns = [col[0] for col in cursor.description]
        raw_tournaments = [dict(zip(columns, row)) for row in cursor.fetchall()]
        logger.info(f"Fetched {len(raw_tournaments)} raw tournaments from DB")

        # Filter by cutoff date
        # =============================================================================
        filtered_tournaments = [
            t for t in raw_tournaments
            if (start_date := parse_date(t["start_str"])) and start_date >= cutoff_date
        ]

        # Loop through filtered tournaments
        # =============================================================================
        if resolve:
            for i, raw_data in enumerate(filtered_tournaments, 1):

                start_d     = parse_date(raw_data["start_str"])
                item_key    = f"{raw_data['shortname']} ({start_d})"
                logger.info(f"Processing tournament [{i}/{len(filtered_tournaments)}] {raw_data['shortname']}")

                # Parse tournaments
                # =============================================================================
                parsed_data = resolve_tournaments(raw_data, cursor)
                if parsed_data is None:
                    continue

                # Create and validate tournament object
                # =============================================================================         
                tournament  = Tournament.from_dict(parsed_data)
                val         = tournament.validate(logger, item_key)  
                if val["status"] != "success":
                    continue

                tournament.upsert(cursor, logger, item_key)
            
        logger.summarize()


    except Exception as e:
        logging.error(f"Error in upd_tournaments: {e}", stack_info=True, stacklevel=3, exc_info=True)
        print(f"❌ Error in upd_tournaments: {e}")

    finally:
        conn.commit()
        conn.close()