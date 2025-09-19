# src/upd_tournament_data.py

from db import get_conn

from resolvers.resolve_participants             import resolve_participants
from scrapers.scrape_participants_ondata        import scrape_participants_ondata
from scrapers.scrape_tournament_classes_ondata  import scrape_tournament_classes_ondata
from scrapers.scrape_tournaments_ondata_listed  import scrape_tournaments_ondata_listed

from resolvers.resolve_tournament_classes       import resolve_tournament_classes
from resolvers.resolve_tournaments              import resolve_tournaments


def upd_tournament_data(
        run_id,
        do_scrape_tournaments           = False,
        do_scrape_tournament_classes    = False,
        do_scrape_participants          = False,
    ):

    conn, cursor = get_conn()

    # Scraping
    try: 

        if do_scrape_tournaments:
            try: 

                scrape_tournaments_ondata_listed(cursor, run_id=run_id)

            except Exception as e:
                print(f"Error in do_scrape_tournaments: {e}")
                pass

        if do_scrape_tournament_classes:
            try:

                scrape_tournament_classes_ondata(cursor, run_id=run_id)
                
            except Exception as e:
                print(f"Error in do_scrape_tournament_classes: {e}")
                pass

        if do_scrape_participants:
            try:

                scrape_participants_ondata(cursor, include_positions=True, run_id=run_id)

            except Exception as e:
                print(f"Error in do_scrape_participants: {e}")
                pass
    
    except Exception as e:
        print(f"Error in upd_tournament_data: {e}")

    # Resolving
    try:
        resolve_tournaments(cursor, run_id=run_id)
        resolve_tournament_classes(cursor, run_id=run_id)
        # resolve_participants(cursor, run_id=run_id) 

    except Exception as e:
        print(f"Error in resolve_tournaments: {e}")


