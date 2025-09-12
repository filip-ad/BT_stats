
# src/main.py

import logging
from utils import clear_debug_tables, export_logs_to_excel, export_runs_to_excel, setup_logging, OperationLogger
from scrape_data import scrape_data
from resolve_data import resolve_data

from upd_clubs import upd_clubs
from upd_players_verified import upd_players_verified

from upd_player_licenses import upd_player_licenses

from upd_player_rankings_raw import upd_player_rankings_raw
from upd_player_rankings import upd_player_rankings

from upd_player_transitions import upd_player_transitions

from upd_tournaments import upd_tournaments
from upd_tournament_classes import upd_tournament_classes

from upd_participants import upd_participants

from db import create_raw_tables, get_conn, drop_tables, create_tables, create_and_populate_static_tables, create_indexes, create_triggers, create_views, compact_sqlite, execute_custom_sql


def main():

    try:

        # Get the connection and cursor
        conn, cursor = get_conn()
        
        # Set up logging (set the output format etc)
        setup_logging()
        logger = OperationLogger(
            verbosity=2,
            print_output=False,
            log_to_db=True,
            cursor=cursor
        )


        ### DB stuff
        ################################################################################################        

        # compact_sqlite()
        


        # Drop existing tables to ensure a clean slate
        drop_tables(cursor, logger, 
            [
    
                # # League / series (not done, nor used yet)
                # 'league',
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
                # 'tournament_raw'
                # 'tournament_class_raw',

                # # License, ranking, transitions
                # 'player_license',                     # FK player (verified), club, season, license
                # 'player_ranking',                     # FK player (verified)
                # 'player_ranking_group',               # FK player (verified), ranking_group
                # 'player_transition',                  # FK player, club

                # # Game and match-related
                # 'match_competition',                  # match, competition_type, tournament_class, fixture, tournament_class_stage, tournament_class_group 
                # 'match_side',                         # match, participant
                # 'game',                               # match
                # 'match_id_ext',                       # match
                # 'tournament_class_match',
                # 'tournament_class_group_standing',
                # 'tournament_class_group_member',
                # 'tournament_class_group'
            
                # 'match_side_player'                  # references club
                # 'match'
                
                # # Tournament participants
                # 'participant'                     # FK tournament_class
                # 'participant_player',                 # FK participant, player (verified, unverified), club
                # 'participant_player_raw_tournament'  # FK tournament_class, data_source

                # # Group and standing
                # 'tournament_class_group',             # FK tournament_class
                # 'tournament_class_group_member',      # FK tournament_class_group, participant
                # 'tournament_class_group_standing'    # FK tournament_class_group, participant

                # # Tournament core
                # 'tournament_class'                   # FK tournament, tournament_class_type, tournament_class_structure, data_source
                # 'tournament_raw',                      # FK data_source, tournament_status
                # 'tournament'
                # 'tournament_class_raw'

                # # Club
                # 'club_id_ext',                        # References club
                # 'club_name_alias',                    # References club
                # 'club',

                # # Player
                # 'player_id_ext',                       # References player (verified)
                # 'player'

                # # Debugging tables (no FKs assumed)
                # 'club_missing',                       # FK club
                # 'club_name_prefix_match',             # FK club
                # 'log_run',
                # 'log_details'
            ]      
        )

        create_and_populate_static_tables(cursor, logger)
        create_raw_tables(cursor, logger)
        create_tables(cursor)
        create_indexes(cursor)
        create_triggers(cursor)
        create_views(cursor)
        clear_debug_tables(cursor, clear_logs=True, clear_runs=False)
        # execute_custom_sql(cursor)
        

        conn.commit()
        conn.close()

        ################################################################################################


        ### NEW WORKFLOW
        ################################################################################################
        
        # Populate RAW tables first
        #   

        scrp_tournaments              = True
        scrp_tournament_classes       = False
        scrp_player_licenses          = False
        scrp_player_transitions       = False
        scrp_participants             = False

        rsv_tournaments             = True
        rsv_tournament_classes      = False
        rsv_player_licenses         = False
        rsv_player_ranking_groups   = False
        rsv_player_transitions      = False
        rsv_participants            = False

        scrape_data     (scrp_tournaments,      scrp_tournament_classes)
        resolve_data    (rsv_tournaments,       rsv_tournament_classes,     rsv_player_licenses,    rsv_player_ranking_groups,  rsv_player_transitions,     rsv_participants)

        # upd_players_verified()
        # upd_clubs()

        # resolvers...


        #
        # Describe all functions, what they do, what tables are updated, variables etc etc
        #

        # 1 Scrape and populate raw tables.
        # upd_player_licenses(scrape=True, resolve=False, update_ranking_groups=False)
        # upd_player_rankings_raw()
        # upd_players_verified()
        # upd_player_licenses(scrape=False, resolve=True, update_ranking_groups=True)

        # 2 update clubs, verified players and player ranking groups
        # upd_clubs()
        
        # upd_player_transitions(scrape=True, resolve=True)


        ################################################################################################

        # # Get tournaments
        # upd_tournaments(scrape_ondata=False, resolve=True)
        # upd_tournament_classes(scrape_ondata=True, resolve=True)

        # upd_participants(scrape_ondata=False, resolve=True)
        # upd_player_positions()
        # upd_tournament_group_stage()

        export_runs_to_excel()
        export_logs_to_excel()

    except Exception as e:
        logging.error(f"Error: {e}", stack_info=True, stacklevel=3, exc_info=True)
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()