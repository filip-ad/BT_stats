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
from resolvers.resolve_tournament_class_matches                 import resolve_tournament_class_matches


def upd_tournament_data(
        run_id,
        do_scrape_tournaments                                   = False,
        do_scrape_tournament_classes                            = False,
        do_scrape_tournament_class_entries                      = False,
        do_scrape_tournament_class_group_matches_ondata         = False,
        do_scrape_tournament_class_knockout_matches_ondata      = False
    ):
    """
    Run the optional scraping steps (tournaments → classes → entries → matches)
    and their corresponding resolver actions in sequence.
    The boolean flags let callers update only the pieces they care about.
    """

    conn, cursor = get_conn()
    # Open a persistent DB handle for the duration of this update run.

    # Scraping
    try: 

        if do_scrape_tournaments:
            # Scrape and resolve the tournament list before doing any downstream work.
            try:
                # Refresh the tournament catalog from OnData.
                scrape_tournaments_ondata_listed(cursor, run_id=run_id)
            except Exception as e:
                print(f"Error in do_scrape_tournaments: {e}")
                pass

            # Normalize/validate the tournament rows so we have clean IDs later.
            resolve_tournaments(cursor, run_id=run_id)
        
        if do_scrape_tournament_classes:
            # Fetch tournament class metadata tied to the tournaments from the previous step.
            try:

                scrape_tournament_classes_ondata(cursor, run_id=run_id)
                
            except Exception as e:
                print(f"Error in do_scrape_tournament_classes: {e}")
                pass

            resolve_tournament_classes(cursor, run_id=run_id)

        if do_scrape_tournament_class_entries:
            # Load stage 3 (entries/positions) so the resolver knows which players are registered.
            try:

                scrape_tournament_class_entries_ondata(cursor, include_positions=True, run_id=run_id)

            except Exception as e:
                print(f"Error in scrape_tournament_class_entries_ondata: {e}")
                pass

            resolve_tournament_class_entries(cursor, run_id=run_id)

        if do_scrape_tournament_class_group_matches_ondata:
            # Collect group-stage match data so raw rows are ready for resolving.
            try:

               scrape_tournament_class_group_matches_ondata(cursor, run_id=run_id) 
            
            except Exception as e:
                print(f"Error importing scrape_tournament_class_matches_ondata: {e}")
                pass

        if do_scrape_tournament_class_knockout_matches_ondata:
            # Scrape and store KO bracket results (stage 5) for resolver later.
            try:

               scrape_tournament_class_knockout_matches_ondata(cursor, run_id=run_id)

            except Exception as e:
                print(f"Error importing scrape_tournament_knockout_matches_ondata: {e}")
                pass

        # Optional: resolving KO match rows once both group/KO raw data is available.
        # resolve_tournament_class_matches(cursor, run_id=run_id)


    
    except Exception as e:
        print(f"Error in upd_tournament_data: {e}")

    # Resolving
    try:

        # No extra resolver hooks today – keep block for future expansion.
        pass

    except Exception as e:
        print(f"Error in resolve_tournaments: {e}")

    # Persist all changes and release the connection once scraping/resolving is done.
    conn.commit()
    conn.close()


