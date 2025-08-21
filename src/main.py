
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
    
            # # League / series (not done, nor used yet)
            # 'league',
            # 'series',                             # FK league
            # 'fixture',

            # # Lookup tables
            # 'data_source',
            # 'ranking_group',
            # 'season',
            # 'license',
            # 'district',
            # 'tournament_status',                  # FK tournament
            # 'tournament_class_stage',             # FK tournament_class
            # 'tournament_class_type',              # FK tournament_class
            # 'tournament_class_structure',         # FK tournament_class
            # 'competition_type',                   # FK match
            # 'club_type',                          # FK club


            # # Raw tables
            # 'player_license_raw',
            # 'player_transition_raw',
            # 'player_ranking_raw',

            # # License, ranking, transitions
            # 'player_license',                     # FK player (validated), club, season, license
            # 'player_ranking',                     # FK player (validated)
            # 'player_ranking_group',               # FK player (validated), ranking_group
            # 'player_transition',                  # FK player, club

            # # Game and match-related
            # 'match_competition',                  # match, competition_type, tournament_class, fixture, tournament_class_stage, tournament_class_group 
            # 'match_side',                         # match, participant
            # 'game',                               # match
            # 'match_id_ext',                       # match
            # 'match_side_player',                  # references club
            # 'match'
            
            # # Tournament participants
            # 'participant',                        # FK tournament_class
            # 'participant_player',                 # FK participant, player, club

            # # Group and standing
            # 'tournament_class_group',             # FK tournament_class
            # 'tournament_class_group_member',      # FK tournament_class_group, participant
            # 'tournament_class_group_standing',    # FK tournament_class_group, participant

            # # Tournament core
            # 'tournament_class',                   # FK tournament, tournament_class_type, tournament_class_structure, data_source
            # 'tournament',                         # FK data_source, tournament_status

            # # Club
            # 'club_id_ext',                        # References club
            # 'club_name_alias',                    # References club
            # 'club',

            # # Player
            # 'player_id_ext',                       # References player
            # 'player'

            # # Debugging tables (no FKs assumed)
            # 'club_missing',                       # FK club
            # 'club_name_prefix_match',             # FK club
            # log_tables
        ])

        create_and_populate_static_tables(cursor)
        create_tables(cursor)  
        create_indexes(cursor)
        create_triggers(cursor)
        create_views(cursor)

        # execute_custom_sql(cursor)
        

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



        # upd_player_licenses()
        # upd_player_transitions()
        # upd_player_ranking_groups()

        ################################################################################################

        # # Get tournaments
        # upd_tournaments()
        # upd_tournament_classes()

        upd_participants()
        # upd_player_positions()
        # upd_tournament_group_stage()

    except Exception as e:
        logging.error(f"Error: {e}")
        print(f"❌ Error: {e}")

if __name__ == "__main__":#
    main()