# src/main.py

import logging
import uuid
from upd_clubs              import upd_clubs
from upd_player_data        import upd_player_data
from upd_tournament_data    import upd_tournament_data
from upd_league_data        import upd_league_data

from utils import (
    clear_debug_tables, 
    export_logs_to_excel, 
    export_runs_to_excel, 
    setup_logging, 
    OperationLogger,
    export_db_dictionary
)

from db import (
    create_raw_tables, 
    get_conn, 
    drop_tables, 
    create_tables, 
    create_and_populate_static_tables, 
    create_indexes, 
    create_triggers, 
    create_views, 
    compact_sqlite, 
    execute_custom_sql
)


def main():

    try:

        pipeline_run_id = str(uuid.uuid4())
        conn, cursor = get_conn()
        
        setup_logging()
        logger = OperationLogger(
            run_id = pipeline_run_id,
            verbosity=2,
            print_output=False,
            log_to_db=True,
            cursor=cursor
        )

        logger.info(f"Starting new run with ID: {pipeline_run_id if pipeline_run_id else 'N/A'}")

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
                # 'player_license_raw'
                # 'player_transition_raw',
                # 'player_ranking_raw',
                # 'tournament_raw'
                # 'tournament_class_raw',

                # # License, ranking, transitions
                # 'player_license'                       # FK player (verified), club, season, license
                # 'player_ranking'                       # FK player (verified)
                # 'player_ranking_group',                 # FK player (verified), ranking_group
                # 'player_transition'                   # FK player, club

                # # Game and match-related
                # 'match_competition',                  # match, competition_type, tournament_class, fixture, tournament_class_stage, tournament_class_group 
                # 'match_side',                         # match, participant
                # 'game',                               # match
                # 'match_id_ext',                       # match
            
                # 'match_side_player'                  # references club
                # 'match'

                # # Group and standing
                # 'tournament_class_group',             # FK tournament_class
                # 'tournament_class_group_member',      # FK tournament_class_group, participant
                # 'tournament_class_player',
                # 'tournament_class_match',
                # 'tournament_class_entry',


                # # Tournament core
                # 'tournament_class'                      # FK tournament, tournament_class_type, tournament_class_structure, data_source
                # 'tournament_raw',                      # FK data_source, tournament_status
                # 'tournament'
                # 'tournament_class_raw'

                # # Club
                # 'club_id_ext',                        # References club
                # 'club_name_alias',                    # References club
                # 'club',

                # # Player
                # 'player_unverified_appearance',         # References player (unverified)
                # 'player_id_ext',                        # References player (verified)
                # 'player'

                # # Matches and related
                # 'match_player',
                # 'game',
                # 'fixture_match',
                # 'match_side',
                # 'tournament_class_match'
                # 'fixture'
                # 'league_raw',
                # 'league_fixture_raw',
                # 'league_fixture_match_raw'

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
        # export_db_dictionary()
        

        conn.commit()
        conn.close()

        ################################################################################################


        ### NEW WORKFLOW
        ################################################################################################
        
        # # # Update club data
        # upd_clubs(dry_run=False)

        # # Update player data
        # upd_player_data(
        #     run_id                            = pipeline_run_id,
        #     do_scrape_player_licenses         = True, 
        #     do_scrape_player_rankings         = True,
        #     do_scrape_player_transitions      = True
        # )

        # Update tournament data
        upd_tournament_data(
            run_id                                                  = pipeline_run_id,
            do_scrape_tournaments                                   = False,
            do_scrape_tournament_classes                            = False,
            do_scrape_tournament_class_entries                      = False,
            do_scrape_tournament_class_group_matches_ondata         = False,
            do_scrape_tournament_class_knockout_matches_ondata      = False
        )

        # upd_league_data(
        #     run_id                                                  = pipeline_run_id,
        #     do_scrape_all_league_data_profixio                      = True
        # )

        export_runs_to_excel()
        export_logs_to_excel()

    except Exception as e:
        logging.error(f"Error: {e}", stack_info=True, stacklevel=3, exc_info=True)
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()