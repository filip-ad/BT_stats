
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

from upd_participants import upd_participants
from upd_player_positions import upd_player_positions
from upd_tournament_group_stage import upd_tournament_group_stage
from db import get_conn, drop_tables, create_tables, create_and_populate_static_tables, create_indexes, create_triggers, create_views, compact_sqlite, execute_custom_sql


def main():

    try:
        
        # Set up logging
        setup_logging()

        ### DB stuff
        ################################################################################################        

        # compact_sqlite()
        
        # Get the connection and cursor
        conn, cursor = get_conn()

        # Drop existing tables to ensure a clean slate
        drop_tables(cursor, [

           
            # 'player_participant_missing',
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
            # 'player_license',
            # 'player_license_raw',
            # 'player_transition_raw'
            # 'player_transition',
            # 'player_ranking_raw'

            # Debugging tables (no FKs assumed)
            # 'player_participant_missing_positions',
            # 'player_participant_missing',
            # 'debug_invalid_pdf_parse',
            # 'debug_group_parse_missing',

            # Game and match-related (leaves)
            # 'game',
            # 'match_id_ext',
            # 'match_side',
            # 'match_competition',
            # 'match_side_player', # references club

            # Group and standing
            # 'tournament_class_group_member',
            # 'tournament_class_group_standing',

            # Participant-related
            # 'tournament_class_group',

            # Tournament core
            # 'tournament_class'
            # 'tournament',
            # 'participant_player',   # reference to club
            # 'participant',          

            # Fixture (if exists)
            # 'fixture',

            # Player and club extensions (dependents)
            
            #  # 'club_missing'
            # 'club_id_ext',   # References club
            # 'club_name_alias',  # References club
            # 'club',

            # 'player_alias',  # References player
            # 'player',
            
            # Lookup/static tables (no dependents)
            # 'tournament_class_type',
            # 'tournament_class_structure',
            # 'competition_type',
            # 'data_source',
            # 'tournament_class_stage',

            # If district exists and needs dropping (parent of club)
            # 'district'

            'log_events'
        ])

        create_and_populate_static_tables(cursor)
        create_tables(cursor)  
        create_indexes(cursor)
        create_triggers(cursor)
        create_views(cursor)

        execute_custom_sql(cursor)
        

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
        upd_tournament_classes()

        # upd_participants()
        # upd_player_positions()
        # upd_tournament_group_stage()

    except Exception as e:
        logging.error(f"Error: {e}")
        print(f"❌ Error: {e}")

if __name__ == "__main__":#
    main()