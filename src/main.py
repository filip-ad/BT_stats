
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
from upd_tournament_group_stage import upd_tournament_group_stage
from db import get_conn, drop_tables, create_tables, create_and_populate_static_tables, create_indexes, create_FK_cascades, create_views, drop_old_fk_triggers


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
            # 'player_participant_missing_positions',

            # In order:
            # 'game',
            # 'match_side_participant',
            # 'match',
            # 'tournament_group_member',
            # 'tournament_group'
            # 'player_participant'
        ])

        create_and_populate_static_tables(cursor)
        create_tables(cursor)  
        create_indexes(cursor)
        create_FK_cascades(cursor)
        create_views(cursor)

        conn.commit()
        conn.close()

        ################################################################################################

        #
        # Describe all functions, what they do, what tables are updated, variables etc etc
        #

        # 1 Scrape and populate raw tables.
        # upd_player_licenses_raw()
        # upd_player_transitions_raw()
        # upd_player_rankings_raw()

        # 2 update clubs, verified players and player ranking groups
        # upd_clubs()
        # upd_players_verified()


        # upd_player_ranking_groups()
        # upd_player_licenses()
        # upd_player_transitions()

        ################################################################################################

        # # Get tournaments
        # upd_tournaments()
        # upd_tournament_classes()

        upd_player_participants()
        upd_player_positions()
        upd_tournament_group_stage()

    except Exception as e:
        logging.error(f"Error: {e}")
        print(f"❌ Error: {e}")

if __name__ == "__main__":#
    main()