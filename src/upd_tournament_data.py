# src/upd_tournament_data.py

from db import get_conn

from scrapers.scrape_tournaments_ondata_listed                  import scrape_tournaments_ondata_listed
from scrapers.scrape_tournament_classes_ondata                  import scrape_tournament_classes_ondata
from scrapers.scrape_tournament_class_entries_ondata            import scrape_tournament_class_entries_ondata
from scrapers.scrape_tournament_class_group_matches_ondata      import scrape_tournament_class_group_matches_ondata
from scrapers.scrape_tournament_class_knockout_matches_ondata   import scrape_tournament_class_knockout_matches_ondata

from resolvers.resolve_tournaments                              import resolve_tournaments
from resolvers.resolve_tournament_classes                       import resolve_tournament_classes
from resolvers.resolve_tournament_class_entries                 import resolve_tournament_class_entries


def upd_tournament_data(
        run_id,
        do_scrape_tournaments                                   = False,
        do_scrape_tournament_classes                            = False,
        do_scrape_tournament_class_entries                      = False,
        do_scrape_tournament_class_group_matches_ondata         = False,
        do_scrape_tournament_class_knockout_matches_ondata      = False
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

        resolve_tournaments(cursor, run_id=run_id)

        if do_scrape_tournament_classes:
            try:

                scrape_tournament_classes_ondata(cursor, run_id=run_id)
                
            except Exception as e:
                print(f"Error in do_scrape_tournament_classes: {e}")
                pass

        resolve_tournament_classes(cursor, run_id=run_id)

        if do_scrape_tournament_class_entries:
            try:

                scrape_tournament_class_entries_ondata(cursor, include_positions=True, run_id=run_id)

            except Exception as e:
                print(f"Error in scrape_tournament_class_entries_ondata: {e}")
                pass

        resolve_tournament_class_entries(cursor, run_id=run_id)

        if do_scrape_tournament_class_group_matches_ondata:
            try:

               scrape_tournament_class_group_matches_ondata(cursor, run_id=run_id) 
            
            except Exception as e:
                print(f"Error importing scrape_tournament_class_matches_ondata: {e}")
                pass

        if do_scrape_tournament_class_knockout_matches_ondata:
            try:

               scrape_tournament_class_knockout_matches_ondata(cursor, run_id=run_id)

            except Exception as e:
                print(f"Error importing scrape_tournament_knockout_matches_ondata: {e}")
                pass


    
    except Exception as e:
        print(f"Error in upd_tournament_data: {e}")

    # Resolving
    try:

        pass

    except Exception as e:
        print(f"Error in resolve_tournaments: {e}")

    conn.commit()
    conn.close()


