
# src/main.py

import logging
from utils import setup_logging
from upd_clubs import upd_clubs
from upd_players import upd_players
from upd_player_licenses_raw import upd_player_licenses_raw
from upd_player_ranking_groups import upd_player_ranking_groups
from upd_player_licenses import upd_player_licenses
from upd_player_transitions_raw import upd_player_transitions_raw
# from scrape_player_licenses import get_player_license_table_raw
# from upd_players import update_player_table
# from upd_players_licenses import update_player_licenses
from db import get_conn, drop_tables, create_tables, create_and_populate_static_tables
from upd_player_rankings_raw import upd_player_rankings_raw

def main():

    try:
        
        # Set up logging
        setup_logging()

        ### DB stuff
        ################################################################################################        
        
        # Get the connection and cursor
        conn, cursor = get_conn()

        # Drop existing tables to ensure a clean slate
        drop_tables(cursor, [
            # 'club',
            # 'player_license'
            # 'player'
            # 'license'
            # 'season'
            # 'tournament', 
            # 'tournament_class', 
            # 'player_ranking_group', 
            # 'ranking_group'
            # 'player_ranking', 
            # 'player_license'
            # 'player_license_raw',
            # 'player_transition_raw'
            'player_ranking_raw'
        ])

        # # # Create tables if they don't exist
        create_tables(cursor)  

        # Create static tables
        # create_and_populate_static_tables(cursor)

        conn.commit()
        conn.close()

        ################################################################################################

        # 1. Scrape and update club data.
        # upd_clubs()        

        # 2. Scrape and populate player_license_raw table. No dependency.
        # upd_player_licenses_raw()

        # 3. Update player table. Depends on player_license_raw.
        # upd_players()

        # 4. Update player_ranking_group table. Depends on player_license_raw.
        # upd_player_ranking_groups()

        # 5. Update player license table. Depends on player_license_raw, club, player, season, and license tables.
        # upd_player_licenses()

        # 6. Scrape and populate player_transition_raw table. No dependency.
        # upd_player_transitions_raw()

        # Scrape clubs from rankings
        upd_player_rankings_raw()


        ################################################################################################

        # # Get tournaments
        # get_tournaments()
        
        # # # Scrape classes
        # get_classes()

        # # Fetch tournament class entries and process PDFs
        # get_entries()

    except Exception as e:
        logging.error(f"Error: {e}")
        print(f"❌ Error: {e}")

if __name__ == "__main__":#
    main()