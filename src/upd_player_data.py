# src/upd_player_data.py

import logging
from db                                         import get_conn
from resolvers.resolve_player_ranking_groups    import resolve_player_ranking_groups
from resolvers.resolve_player_licenses          import resolve_player_licenses
from resolvers.resolve_player_rankings          import resolve_player_rankings
from resolvers.resolve_player_transitions       import resolve_player_transitions
from scrapers.scrape_player_licenses            import scrape_player_licenses
from scrapers.scrape_player_transitions         import scrape_player_transitions
from scrapers.scrape_player_rankings            import scrape_player_rankings
from upd_players_verified                       import upd_players_verified

def upd_player_data (
        do_scrape_player_licenses     = False, 
        do_scrape_player_rankings     = False, 
        do_scrape_player_transitions  = False,
        run_id                        = None
    ):

    conn, cursor = get_conn()

    # Scraping
    try:

        if do_scrape_player_licenses:
            try: 

                scrape_player_licenses(cursor, run_id = run_id)

            except Exception as e:
                logging.error(f"Error in scrape_player_licenses: {e}", stack_info=True, stacklevel=3, exc_info=True)
                print(f"Error in scrape_player_licenses: {e}")
                pass

        if do_scrape_player_rankings:
            try:

                scrape_player_rankings(cursor, run_id=run_id)

            except Exception as e:
                logging.error(f"Error in scrape_player_rankings: {e}", stack_info=True, stacklevel=3, exc_info=True)
                print(f"Error in scrape_player_rankings: {e}")
                pass

        if do_scrape_player_transitions:
            try:
                
                scrape_player_transitions(cursor, run_id=run_id)

            except Exception as e:
                logging.error(f"Error in scrape_player_transitions: {e}", stack_info=True, stacklevel=3, exc_info=True)
                print(f"Error in scrape_player_transitions: {e}")
                pass

        # Resolving
        try:
            upd_players_verified(cursor, run_id=run_id)
            # resolve_player_rankings(cursor, run_id=run_id)
            # resolve_player_ranking_groups(cursor, run_id=run_id)
            # resolve_player_licenses(cursor, run_id=run_id)
            # resolve_player_transitions(cursor, run_id=run_id)
            pass

        except Exception as e:
            logging.error(f"Error in resolving player data: {e}", stack_info=True, stacklevel=3, exc_info=True)
            print(f"Error in resolving player data: {e}")


    except Exception as e:
        logging.error(f"Error in upd_players_verified: {e}", stack_info=True, stacklevel=3, exc_info=True)

    finally:
        conn.commit()
        conn.close()