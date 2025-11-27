# src/upd_league_data.py

from db import get_conn

from scrapers.scrape_leagues_profixio                           import scrape_all_league_data_profixio

def upd_league_data(
        run_id,
        do_scrape_all_league_data_profixio = False
    ):


    conn, cursor = get_conn()
    # Open a persistent DB handle for the duration of this update run.

    # Scraping
    try: 

        if do_scrape_all_league_data_profixio:
            # Scrape and resolve the tournament list before doing any downstream work.
            try:
                # Refresh the tournament catalog from OnData.
                scrape_all_league_data_profixio(cursor, run_id=run_id)
            except Exception as e:
                print(f"Error in do_scrape_leagues: {e}")
                pass


    except Exception as e:
        print(f"Error in resolve_tournaments: {e}")

    # Persist all changes and release the connection once scraping/resolving is done.
    conn.commit()
    conn.close()


