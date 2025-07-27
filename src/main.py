
# src/main.py

import logging
from utils import setup_logging
from upd_clubs import upd_clubs
from upd_players import upd_players
from upd_player_licenses_raw import upd_player_licenses_raw
from upd_player_licenses import upd_player_licenses
from upd_player_ranking_groups import upd_player_ranking_groups
from upd_player_rankings_raw import upd_player_rankings_raw
from upd_player_rankings import upd_player_rankings
from upd_player_transitions_raw import upd_player_transitions_raw
from upd_tournaments import upd_tournaments
from db import get_conn, drop_tables, create_tables, create_and_populate_static_tables, create_indexes


def main():

    try:
        
        # Set up logging
        setup_logging()

        ### DB stuff
        ################################################################################################        
        
        # Get the connection and cursor
        conn, cursor = get_conn()

        # # Drop existing tables to ensure a clean slate
        # drop_tables(cursor, [
        #     # 'club',
        #     # 'club_alias',
        #     # 'player',
        #     # 'license'
        #     # 'season'
        #     # 'tournament', 
        #     # 'tournament_class', 
        #     # 'player_ranking_group', 
        #     # 'ranking_group'
        #     # 'player_ranking', 
        #     # 'player_license'
        #     # 'player_license_raw',
        #     # 'player_transition_raw'
        #     # 'player_ranking_raw'
        # ])


        # # Create static tables
        create_and_populate_static_tables(cursor)

        # Create tables if they don't exist
        create_tables(cursor)  

        # Create indexes
        create_indexes(cursor)

        conn.commit()
        conn.close()

        ################################################################################################

        #
        # Describe all functions, what they do, what tables are updated, variables etc etc
        #

        # Scrape and update club data
        #
        #  It seems that player licenses and transitions are only available for clubs that are currently active. 
        #  If a club is no longer active, it won't have any player licenses or transitions listed, not even historical ones.
        #  However, player rankings are available for all clubs, even those that are no longer active.
        #
        #  This means we need to:
        #
        #   1. Scrape active clubs from the license lists.
        #   2. Scrape inactive clubs from the player ranking lists.
        #
        # But it should be enought with 2. !!!!!!!!!!!!!!!!!!!!
        # upd_clubs()
        #### Backup this table as a create statement because it won't be updated often

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

        # 7. Update player transitions. Depends on player_transition_raw, club, player, season, and license tables.
        # upd_player_transitions()

        # 8. Update player rankings raw table. No dependency.
        # upd_player_rankings_raw()

        # 9. 


        ################################################################################################

        # # Get tournaments
        upd_tournaments()
        
        # # # Scrape classes
        # get_classes()

        # # Fetch tournament class entries and process PDFs
        # get_entries()

    except Exception as e:
        logging.error(f"Error: {e}")
        print(f"❌ Error: {e}")

if __name__ == "__main__":#
    main()