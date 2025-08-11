
# src/main.py

import logging
from utils import setup_logging
from upd_clubs import upd_clubs
from upd_players_verified import upd_players_verified
from upd_player_licenses_raw import upd_player_licenses_raw
from upd_player_licenses import upd_player_licenses
from upd_player_ranking_groups import upd_player_ranking_groups
from upd_player_rankings_raw import upd_player_rankings_raw
from upd_player_rankings import upd_player_rankings
from upd_player_transitions_raw import upd_player_transitions_raw
from upd_player_transitions import upd_player_transitions
from upd_tournaments import upd_tournaments
from upd_tournament_classes import upd_tournament_classes
from upd_player_participants import upd_player_participants
from upd_player_positions import upd_player_positions
from db import get_conn, drop_tables, create_tables, create_and_populate_static_tables, create_indexes


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
            # 'club_name_alias'
            # 'club_ext_id',
            # 'club_missing'
            # 'player_participant_missing',
            # 'club_missing',
            # 'club_name_prefix_match'
            # 'club_name_prefix_match'
            # 'player',
            # 'player_alias'
            # 'player_participant'
            # 'license',
            # 'season'
            # 'tournament',
            # 'tournament_class'
            # 'player_participant'
            # 'player_ranking_group'
            # 'ranking_group'
            # 'player_ranking', 
            # 'player_license'
            # 'player_license_raw',
            # 'player_transition_raw'
            # 'player_transition'
            # 'player_ranking_raw'
            # 'game',
            # 'match',
            # 'stage'
        ])


        # # # Create static tables
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

        # upd_clubs()

        # - Scrape and populate player_license_raw table. No dependency.
        # upd_player_licenses_raw()

        # - Update player table. Depends on player_license_raw.
        upd_players_verified()

        # - Update player_ranking_group table. Depends on player_license_raw.
        # upd_player_ranking_groups()

        # - Update player license table. Depends on player_license_raw, club, player, season, and license tables.
        # upd_player_licenses()

        # - Scrape and populate player_transition_raw table. No dependency.
        # upd_player_transitions_raw()

        # - Update player transitions. Depends on player_transition_raw, club, player, season, and license tables.
        # upd_player_transitions()

        # - Update player rankings raw table. No dependency.
        # upd_player_rankings_raw()

        ################################################################################################

        # # Get tournaments
        # upd_tournaments()

        # # # Scrape classes
        # upd_tournament_classes()

        # # Fetch tournament class entries and process PDFs
        # upd_player_participants()

        # # Update player positions
        # upd_player_positions()

    except Exception as e:
        logging.error(f"Error: {e}")
        print(f"❌ Error: {e}")

if __name__ == "__main__":#
    main()