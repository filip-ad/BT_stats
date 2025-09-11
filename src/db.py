
# db.py: 

import sqlite3
from config import DB_NAME
import logging
import datetime

# --- register adapters/converters once (Python 3.12+ friendly) ---
_ADAPTERS_REGISTERED = False

def get_conn():
    
    try:
        _register_sqlite_date_time_adapters()

        # Enable parsing for declared column types (DATE/TIMESTAMP)
        conn = sqlite3.connect(
            DB_NAME,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        )
        logging.debug(f"Connected to database: {DB_NAME}")

        # Recommended pragmas (same as before, plus foreign_keys)
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA temp_store = MEMORY;")
        conn.execute("PRAGMA foreign_keys = ON;")

        return conn, conn.cursor()
    
    except sqlite3.Error as e:
        print(f"❌ Database connection failed: {e}")
        raise

def compact_sqlite():
    print("ℹ️  Compacting SQLite database...")
    try:
        con = sqlite3.connect(DB_NAME)
        con.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        con.execute("VACUUM;")  # rebuilds/shrinks the same file
        con.close()
    except sqlite3.Error as e:
        print(f"❌ Error during database compaction: {e}")
        raise


def _register_sqlite_date_time_adapters() -> None:
    global _ADAPTERS_REGISTERED
    if _ADAPTERS_REGISTERED:
        return

    # Serialize Python date/datetime -> ISO strings
    sqlite3.register_adapter(datetime.date, lambda d: d.isoformat())
    sqlite3.register_adapter(datetime.datetime, lambda dt: dt.isoformat(sep=" "))

    # Parse DB values back into Python objects for columns declared as DATE/TIMESTAMP
    sqlite3.register_converter("DATE", lambda b: datetime.date.fromisoformat(b.decode()))
    sqlite3.register_converter("TIMESTAMP", lambda b: datetime.datetime.fromisoformat(b.decode()))

    _ADAPTERS_REGISTERED = True    

def drop_tables(cursor, logger, tables):

    dropped = []
    for table_name in tables:
        try:
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
                (table_name,)   # single element tuple explains why we use a comma here
            )
            if cursor.fetchone():
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                dropped.append(table_name)
            else:
                logger.warning(f"Table not found, skipping drop: {table_name}", to_console=True)
        except sqlite3.Error as e:
            logger.error(f"Error dropping table {table_name}: {e}", to_console=True)
    if not dropped:
        logger.info("No tables dropped.", to_console=True)
    else:
        logger.info(f"Dropped {len(dropped)} tables: {', '.join(dropped)}", to_console=True)

def create_raw_tables(cursor, logger):

    raw_tables = {
        "tournament_raw": '''
            CREATE TABLE IF NOT EXISTS tournament_raw (
                row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                tournament_id_ext TEXT,
                shortname TEXT,
                longname TEXT,
                startdate DATE,
                enddate DATE,
                registration_end_date DATE,
                city TEXT,
                arena TEXT,
                country_code TEXT,
                url TEXT,
                tournament_level TEXT,
                tournament_type TEXT,
                organiser_name TEXT,
                organiser_email TEXT,
                organiser_phone TEXT,
                data_source_id INTEGER DEFAULT 1,
                is_listed BOOLEAN DEFAULT 1,
                row_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (data_source_id) REFERENCES data_source(data_source_id),
                UNIQUE (tournament_id_ext, data_source_id),
                UNIQUE (shortname, startdate, arena, data_source_id)
            )
        ''',

        "tournament_class_raw": '''
            CREATE TABLE IF NOT EXISTS tournament_class_raw (
                row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                tournament_id_ext TEXT,
                tournament_class_id_ext TEXT,
                startdate DATE,
                shortname TEXT,
                longname TEXT,
                gender TEXT,
                max_rank INTEGER,
                max_age INTEGER,
                url TEXT,
                raw_stages TEXT,
                raw_stage_hrefs TEXT,
                data_source_id INTEGER DEFAULT 1,
                row_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (tournament_id_ext, tournament_class_id_ext, data_source_id)
            )
        ''',

        "player_license_raw": '''
            CREATE TABLE IF NOT EXISTS player_license_raw (
                row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                season_label TEXT NOT NULL,
                season_id_ext INTEGER NOT NULL,
                club_name TEXT NOT NULL,
                club_id_ext INT NOT NULL,
                player_id_ext INTEGER NOT NULL,
                firstname TEXT NOT NULL,
                lastname TEXT NOT NULL,
                gender TEXT NOT NULL,
                year_born INTEGER NOT NULL,
                license_info_raw TEXT NOT NULL,
                ranking_group_raw TEXT,
                row_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(season_id_ext, player_id_ext, club_name, year_born, firstname, lastname, license_info_raw)
            )
        ''',

        "player_ranking_raw": '''
            CREATE TABLE IF NOT EXISTS player_ranking_raw (
                row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER,
                run_date DATE,
                player_id_ext INTEGER,
                firstname TEXT,
                lastname TEXT,
                year_born INTEGER,
                club_name TEXT,
                points INTEGER,
                points_change_since_last INTEGER,
                position_world INTEGER,
                position INTEGER,
                row_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (run_id, player_id_ext)
            )
        ''',

        "player_transition_raw": '''
            CREATE TABLE IF NOT EXISTS player_transition_raw (
                row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                season_id_ext INTEGER NOT NULL,
                season_label TEXT NOT NULL,
                firstname TEXT NOT NULL,
                lastname TEXT NOT NULL,
                date_born DATE NOT NULL,
                year_born INTEGER NOT NULL,
                club_from TEXT NOT NULL,
                club_to TEXT NOT NULL,
                transition_date DATE NOT NULL,
                row_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (firstname, lastname, date_born, transition_date)
            )
        '''
    }

    created, skipped = [], []

    try:
        for name, ddl in raw_tables.items():
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
                (name,)
            )
            if cursor.fetchone():
                skipped.append(name)
            else:
                cursor.execute(ddl)
                created.append(name)

    except Exception as e:
        logger.error(f"Error creating raw tables: {e}")
        print(f"❌ Error creating raw tables: {e}")
        raise

    # Summary printout
    if created:
        logger.info(f"Created raw tables: {', '.join(created)}")
    else:
        logger.info("No raw tables created.")
        


def create_tables(cursor):

    try:

        print("ℹ️  Creating tables if needed...")
        logging.info("Creating tables if needed...")
        logging.info("-------------------------------------------------------------------")

        # Create tournaments table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tournament (
                tournament_id                       INTEGER PRIMARY KEY AUTOINCREMENT,
                tournament_id_ext                   TEXT,
                shortname                           TEXT,
                longname                            TEXT,
                startdate                           DATE,
                enddate                             DATE,
                registration_end_date               DATE,
                city                                TEXT,
                arena                               TEXT,
                country_code                        TEXT,
                url                                 TEXT,
                tournament_level_id                 INTEGER DEFAULT 1,
                tournament_type_id                  INTEGER DEFAULT 1,
                tournament_status_id                INTEGER,
                organiser_name                      TEXT,
                organiser_email                     TEXT,
                organiser_phone                     TEXT,
                is_valid                            BOOLEAN DEFAULT 1,
                data_source_id                      INTEGER DEFAULT 1,
                row_created                         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_updated                         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (data_source_id)        REFERENCES data_source(data_source_id),
                FOREIGN KEY (tournament_level_id)   REFERENCES tournament_level(tournament_level_id),
                FOREIGN KEY (tournament_type_id)    REFERENCES tournament_type(tournament_type_id),
                FOREIGN KEY (tournament_status_id)  REFERENCES tournament_status(tournament_status_id),
                UNIQUE (tournament_id_ext, data_source_id),
                UNIQUE (shortname, startdate, arena)
            )
        ''')

        # Create tournament class table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tournament_class (
                tournament_class_id                         INTEGER PRIMARY KEY AUTOINCREMENT,
                tournament_class_id_ext                     TEXT,
                tournament_id                               INTEGER NOT NULL,
                tournament_class_type_id                    INTEGER,
                tournament_class_structure_id               INTEGER,
                startdate                                   DATE,
                longname                                    TEXT,
                shortname                                   TEXT,
                gender                                      TEXT,
                max_rank                                    INTEGER,
                max_age                                     INTEGER,
                url                                         TEXT,
                data_source_id                              INTEGER DEFAULT 1,
                is_valid                                    BOOLEAN DEFAULT 1,
                row_created                                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_updated                                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (tournament_id)                 REFERENCES tournament(tournament_id)    ON DELETE CASCADE,
                FOREIGN KEY (tournament_class_type_id)      REFERENCES tournament_class_type(tournament_class_type_id),
                FOREIGN KEY (tournament_class_structure_id) REFERENCES tournament_class_structure(tournament_class_structure_id),
                FOREIGN KEY (data_source_id)                REFERENCES data_source(data_source_id),
                UNIQUE      (tournament_class_id_ext, data_source_id),
                UNIQUE      (tournament_id, shortname, startdate)
            )
        ''')



        # Tournament entries (singles or doubles)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tournament_class_entry (
                tournament_class_entry_id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                tournament_class_entry_id_ext               TEXT,
                tournament_class_id                         INTEGER,
                seed                                        INTEGER,
                final_position                              INTEGER,
                row_created                                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_updated                                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (tournament_class_id) REFERENCES tournament_class(tournament_class_id)
            );
        ''')

        # Link players to tournament entries (for singles/doubles)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tournament_class_player (
                tournament_class_entry_id                   INTEGER,
                tournament_class_player_id_ext              TEXT,
                player_id                                   INTEGER,
                club_id                                     INTEGER,
                row_created                                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_updated                                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (tournament_class_entry_id)     REFERENCES tournament_class_entry(tournament_class_entry_id),
                FOREIGN KEY (player_id)                     REFERENCES player(player_id),
                FOREIGN KEY (club_id)                       REFERENCES club(club_id),

                PRIMARY KEY (tournament_class_entry_id, player_id)
            );
        ''')

        # Tournament class match mapping table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS tournament_class_match (
            tournament_class_id                             INTEGER NOT NULL,
            match_id                                        INTEGER NOT NULL,
            tournament_class_match_id_ext                   TEXT,
            tournament_class_stage_id                       INTEGER NOT NULL,
            tournament_class_group_id                       INTEGER,
            stage_round_no                                  INTEGER,  -- 1..N within that stage (RR round or KO progression) ... the playing round within that stage (needed for group RR and for KO ordering).
            draw_pos                                        INTEGER,  -- KO bracket slot (NULL for GROUP/SWISS) ... the KO bracket slot (1..128 etc). This is different from seed and is super handy for bracket reconstruction.
            row_created                                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            row_updated                                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            FOREIGN KEY (tournament_class_id)               REFERENCES tournament_class(tournament_class_id)        ON DELETE CASCADE,
            FOREIGN KEY (match_id)                          REFERENCES match(match_id)                              ON DELETE CASCADE,
            FOREIGN KEY (tournament_class_stage_id)         REFERENCES tournament_class_stage(tournament_class_stage_id),
             
            -- Enforce that the group (if present) belongs to the same class
            --FOREIGN KEY (tournament_class_id, tournament_class_group_id)
            --  REFERENCES tournament_class_group (tournament_class_id, tournament_class_group_id),

            PRIMARY KEY (tournament_class_id, match_id)
        );
    ''')

        # Core Match
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS match (
                match_id                                    INTEGER PRIMARY KEY AUTOINCREMENT,
                best_of                                     INTEGER,
                date                                        DATE,
                status                                      TEXT DEFAULT 'completed',
                winner_side                                 INTEGER CHECK (winner_side IN (1, 2) OR winner_side IS NULL),
                walkover_side                               INTEGER CHECK (walkover_side IN (1, 2) OR walkover_side IS NULL),
                row_created                                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_updated                                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        ''')

        # Match side (which participant played on which side)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS match_side (
                match_id                                        INTEGER,
                side_no                                     INTEGER,
                represented_entry_id                        INTEGER,
                represented_league_team_id                  INTEGER,
                row_created                                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_updated                                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (match_id)                      REFERENCES match(match_id),
                FOREIGN KEY (represented_league_team_id)    REFERENCES league_team(league_team_id),
                FOREIGN KEY (represented_entry_id)          REFERENCES tournament_class_entry(tournament_class_entry_id),

                PRIMARY KEY (match_id, side_no),

                CHECK (side_no IN (1, 2)),
                CHECK (
                    (represented_league_team_id IS NOT NULL AND represented_entry_id IS NULL)
                    OR (represented_league_team_id IS NULL AND represented_entry_id IS NOT NULL)
                    OR (represented_league_team_id IS NULL AND represented_entry_id IS NULL)
                )
            );
        ''')

        # Match player (which player played in which match and on which side)
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS match_player (
            match_id                        INTEGER,
            side_no                         INTEGER,
            player_id                       INTEGER,
            player_order                    INTEGER,
            club_id                         INTEGER,
            row_created                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            row_updated                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            FOREIGN KEY (match_id, side_no) REFERENCES match_side(match_id, side_no),
            FOREIGN KEY (player_id)         REFERENCES player(player_id),
            FOREIGN KEY (club_id)           REFERENCES club(club_id),

            PRIMARY KEY (match_id, side_no, player_id),

            CHECK (player_order IN (1, 2))
        );
    ''')
        
        # Game table (sets within a match)
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS game (
            match_id                        INTEGER,
            game_no                         INTEGER,
            points_side1                    INTEGER,
            points_side2                    INTEGER,
            row_created                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            row_updated                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            FOREIGN KEY (match_id)          REFERENCES match(match_id),

            PRIMARY KEY (match_id, game_no)
        );
    ''')

        # Create tournament class group table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tournament_class_group (
                tournament_class_group_id           INTEGER PRIMARY KEY AUTOINCREMENT,
                tournament_class_id                 INTEGER NOT NULL,
                description                         TEXT,
                sort_order                          INTEGER,
                row_created                         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_updated                         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (tournament_class_id)   REFERENCES tournament_class(tournament_class_id)      ON DELETE CASCADE,
                UNIQUE      (tournament_class_id, description)
            )
        ''')

        # Create tournament group member table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tournament_class_group_member (
                tournament_class_group_id                   INTEGER NOT NULL,
                tournament_class_entry_id                   INTEGER NOT NULL,
                seed_in_group                               INTEGER,
                row_created                                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_updated                                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (tournament_class_group_id)     REFERENCES tournament_class_group(tournament_class_group_id)    ON DELETE CASCADE,
                FOREIGN KEY (tournament_class_entry_id)     REFERENCES tournament_class_entry(tournament_class_entry_id)    ON DELETE CASCADE,
                PRIMARY KEY (tournament_class_group_id, tournament_class_entry_id)
            );
        ''')

        # # Create group standing table
        # cursor.execute('''
        #     CREATE TABLE IF NOT EXISTS tournament_class_group_standing (
        #         tournament_class_group_id                   INTEGER NOT NULL,
        #         participant_id                              INTEGER NOT NULL,
        #         position_in_group                           INTEGER,
        #         nbr_matches_won                             INTEGER NOT NULL DEFAULT 0,
        #         nbr_matches_lost                            INTEGER NOT NULL DEFAULT 0,
        #         nbr_games_won                               INTEGER NOT NULL DEFAULT 0,
        #         nbr_games_lost                              INTEGER NOT NULL DEFAULT 0,
        #         nbr_points_won                              INTEGER NOT NULL DEFAULT 0,
        #         nbr_points_lost                             INTEGER NOT NULL DEFAULT 0,
        #         row_created                                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        #         row_updated                                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        #         FOREIGN KEY (tournament_class_group_id)     REFERENCES tournament_class_group(tournament_class_group_id)    ON DELETE CASCADE,
        #         FOREIGN KEY (participant_id)                REFERENCES participant(participant_id)                          ON DELETE CASCADE,
        #         PRIMARY KEY (tournament_class_group_id, participant_id)
        #     );
        # ''')

        # # Generic participant table
        # cursor.execute('''
        #     CREATE TABLE IF NOT EXISTS participant (
        #         participant_id                      INTEGER PRIMARY KEY AUTOINCREMENT,
                    
        #         -- Polymorphic fields: Exactly one set based on type
        #         tournament_class_id                 INTEGER,
        #         tournament_class_seed               INTEGER,
        #         tournament_class_final_position     INTEGER,
                
        #         -- Add later: league_id             INTEGER,  -- For future leagues
        #         -- fixture_participant_id           INTEGER,  -- If fixtures have separate participants
        #         row_created                         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        #         row_updated                         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        #         FOREIGN KEY (tournament_class_id)   REFERENCES tournament_class(tournament_class_id)         ON DELETE CASCADE,
                    
        #         -- Add FKs for future types here
        #         CHECK (
        #                 (tournament_class_id IS NOT NULL)  -- + (league_id IS NOT NULL) + ... = 1 for exactly one type
        #         )       -- Update CHECK as you add types
        #     );
        # ''')

        # Generic participant raw table (tournament participants only for now)
        # Create similar for leagues/fixtures later
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS participant_player_raw_tournament (
                row_id                          INTEGER PRIMARY KEY AUTOINCREMENT,
                tournament_id_ext               TEXT NOT NULL,   -- external tournament ID from source
                tournament_class_id_ext         TEXT NOT NULL,   -- external class ID from source
                participant_player_id_ext       TEXT,            -- external participant ID from source (if any)
                fullname_raw                    TEXT NOT NULL,   -- raw player name string
                clubname_raw                    TEXT,            -- raw club name string
                seed_raw                        TEXT,            -- raw seed (string, since formats vary)
                final_position_raw              TEXT,            -- raw final position (string/number, as parsed)
                raw_group_id                    TEXT,            -- raw group ID/name if available
                data_source_id                  INTEGER NOT NULL, -- FK to data_source (1=OnData, 2=Profixio, etc.)       
                row_created                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_updated                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (data_source_id)    REFERENCES data_source(data_source_id),
                UNIQUE (tournament_id_ext, tournament_class_id_ext, fullname_raw, clubname_raw, participant_player_id_ext, data_source_id)
            );
        ''')



        # # Create participant player table
        # cursor.execute('''
        #     CREATE TABLE IF NOT EXISTS participant_player (
        #         participant_player_id           INTEGER PRIMARY KEY AUTOINCREMENT,
        #         participant_player_id_ext       TEXT,
        #         participant_id                  INTEGER NOT NULL,
        #         player_id                       INTEGER NOT NULL,
        #         club_id                         INTEGER,
        #         row_created                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        #         row_updated                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        #         FOREIGN KEY (participant_id)    REFERENCES participant(participant_id) ON DELETE CASCADE,
        #         FOREIGN KEY (player_id)         REFERENCES player(player_id),
        #         FOREIGN KEY (club_id)           REFERENCES club(club_id),
        #         UNIQUE (participant_id, player_id)  -- Prevent duplicate players per participant (e.g., in doubles)            
        #     );
        # ''')

        # # Create match table
        # cursor.execute('''
        #     CREATE TABLE IF NOT EXISTS match (
        #         match_id                    INTEGER PRIMARY KEY AUTOINCREMENT,          
        #         best_of                     INTEGER,
        #         date                        DATE,
        #         row_created                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        #         row_updated                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        #         );
        # ''')

        # # Create match id ext table
        # cursor.execute('''
        #     CREATE TABLE IF NOT EXISTS match_id_ext (
        #         match_id                    INTEGER NOT NULL,
        #         match_id_ext                TEXT NOT NULL,
        #         data_source_id              INTEGER NOT NULL,
        #         row_created                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        #         row_updated                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        #         FOREIGN KEY (match_id)      REFERENCES match(match_id)                  ON DELETE CASCADE,
        #         PRIMARY KEY (match_id, match_id_ext, data_source_id)
        #     );
        # ''')

        # # Create match side
        # cursor.execute('''
        #     CREATE TABLE IF NOT EXISTS match_side (
        #         match_side_id                           INTEGER PRIMARY KEY AUTOINCREMENT,
        #         match_id                                INTEGER NOT NULL,
        #         side                                    INTEGER NOT NULL CHECK(side IN (1,2)),
        #         participant_id                          INTEGER NOT NULL,
        #         row_created                             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        #         row_updated                             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        #         FOREIGN KEY (match_id)                  REFERENCES match(match_id) ON DELETE CASCADE,
        #         FOREIGN KEY (participant_id)            REFERENCES participant(participant_id)     ON DELETE CASCADE,
        #         UNIQUE (match_id, side)
        #     );
        # ''')

        # # Create competition match mapping table
        # cursor.execute('''
        #     CREATE TABLE IF NOT EXISTS match_competition (
        #         match_competition_id                    INTEGER PRIMARY KEY AUTOINCREMENT,
        #         match_id                                INTEGER NOT NULL,
        #         competition_type_id                     INTEGER NOT NULL,
                    
        #         -- Only one of the following may be set depending on competition_type_id:
        #         tournament_class_id                     INTEGER,
        #         fixture_id                              INTEGER,
                    
        #         -- Optional tournament context (only valid when competition_type_id=1)
        #         tournament_class_group_id               INTEGER,
        #         tournament_class_stage_id               INTEGER,

        #         -- Optional league context
                    
        #         row_created                             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        #         row_updated                             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        #         FOREIGN KEY (match_id)                  REFERENCES match(match_id)                                      ON DELETE CASCADE,
        #         FOREIGN KEY (competition_type_id)       REFERENCES competition_type(competition_type_id),
        #         FOREIGN KEY (tournament_class_id)       REFERENCES tournament_class(tournament_class_id)                ON DELETE CASCADE,
        #         FOREIGN KEY (fixture_id)                REFERENCES fixture(fixture_id)                                  ON DELETE CASCADE,
        #         FOREIGN KEY (tournament_class_stage_id) REFERENCES tournament_class_stage(tournament_class_stage_id)    ON DELETE SET NULL,
        #         FOREIGN KEY (tournament_class_group_id) REFERENCES tournament_class_group(tournament_class_group_id)    ON DELETE SET NULL,
        #         UNIQUE      (match_id, competition_type_id),

        #         -- Exactly one target depending on competition_type:
        #         CHECK (
        #                 (competition_type_id = 1 AND tournament_class_id    IS NOT NULL AND fixture_id IS NULL)
        #         OR      (competition_type_id = 2 AND fixture_id             IS NOT NULL AND tournament_class_id IS NULL)
        #         OR      (competition_type_id = 3 AND tournament_class_id    IS NULL AND     fixture_id IS NULL)
        #         ),

        #         -- Stage/group only allowed for TournamentClass:
        #         CHECK (
        #                 (competition_type_id = 1)
        #             OR  (tournament_class_stage_id  IS NULL AND tournament_class_group_id IS NULL)
        #         )
        #     );
        # ''')

        # # Create game table (sets)
        # cursor.execute('''
        #     CREATE TABLE IF NOT EXISTS game (
        #         match_id                            INTEGER NOT NULL,
        #         game_nbr                            INTEGER NOT NULL,
        #         side1_points                        INTEGER,
        #         side2_points                        INTEGER,
        #         winning_side                        INTEGER CHECK(winning_side IN (1,2)),
        #         row_created                         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        #         row_updated                         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        #         FOREIGN KEY (match_id)              REFERENCES match(match_id)              ON DELETE CASCADE,
        #         PRIMARY KEY (match_id, game_nbr)
        #     );
        # ''')



        ##############################################
        ########## PLAYER TABLES #####################
        ##############################################

        # Create player table (cannonical player data)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS player (
                player_id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                firstname                   TEXT,
                lastname                    TEXT,
                year_born                   INTEGER,
                date_born                   TEXT CHECK (date_born GLOB '____-__-__'),
                fullname_raw                TEXT,
                is_verified                 BOOLEAN DEFAULT FALSE,
                row_created                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_updated                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (fullname_raw)
            );
        ''')

        # Create player id ext table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS player_id_ext (
                player_id                       INTEGER NOT NULL,
                player_id_ext                   TEXT,
                data_source_id                  INTEGER,
                row_created TIMESTAMP           DEFAULT CURRENT_TIMESTAMP,   
                row_updated TIMESTAMP           DEFAULT CURRENT_TIMESTAMP,   
                FOREIGN KEY (player_id)         REFERENCES player(player_id)            ON DELETE CASCADE,    
                FOREIGN KEY (data_source_id)    REFERENCES data_source(data_source_id),
                UNIQUE      (player_id_ext, data_source_id)      
            );
        ''')

        # Create player unverified appearance
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS player_unverified_appearance (
                player_id                           INTEGER NOT NULL,
                club_id                             INTEGER NOT NULL,
                appearance_date                     DATE NOT NULL,
                row_created                         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_updated                         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (player_id)             REFERENCES player(player_id)                        ON DELETE CASCADE,
                FOREIGN KEY (club_id)               REFERENCES club(club_id)                            ON DELETE CASCADE,
                UNIQUE (player_id, club_id, appearance_date)
            );
        ''')

        ###########################################
        ########## PLAYER LICENSES ################
        ###########################################

 

        # Create player license table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS player_license (
                player_id                       INTEGER NOT NULL,
                club_id                         INTEGER NOT NULL,
                valid_from                      DATE NOT NULL,
                valid_to                        DATE NOT NULL,
                license_id                      INTEGER NOT NULL,
                season_id                       INTEGER NOT NULL,
                row_created                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (player_id) REFERENCES player(player_id),
                FOREIGN KEY (club_id) REFERENCES club(club_id),
                FOREIGN KEY (season_id) REFERENCES season(season_id),
                FOREIGN KEY (license_id) REFERENCES license(license_id),
                UNIQUE (player_id, license_id, season_id, club_id)
            )
        ''')

        ###########################################
        ########## PLAYER TRANSITIONS #############
        ###########################################

          

        # Create player transition table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS player_transition (
                season_id                       INTEGER NOT NULL,
                player_id                       INTEGER NOT NULL,
                club_id_from                    INTEGER NOT NULL,
                club_id_to                      INTEGER NOT NULL,
                transition_date                 DATE NOT NULL,
                row_created                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (player_id)         REFERENCES player(player_id),
                FOREIGN KEY (club_id_from)      REFERENCES club(club_id),
                FOREIGN KEY (club_id_to)        REFERENCES club(club_id),
                UNIQUE (player_id, club_id_from, club_id_to, transition_date)
            )
        ''')


        ###########################################
        #### PLAYER RANKING AND RANKING GROUPS ####
        ###########################################


        # Create player ranking group table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS player_ranking_group (
                player_id                       INTEGER NOT NULL,
                ranking_group_id                INTEGER NOT NULL,
                row_created                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (ranking_group_id)  REFERENCES ranking_group(ranking_group_id),
                FOREIGN KEY (player_id)         REFERENCES player(player_id)
            )
        ''')



        # Create player ranking table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS player_ranking (
                run_id                          INTEGER NOT NULL,
                run_date                        DATE NOT NULL,
                player_id                       INTEGER NOT NULL, 
                points                          INTEGER NOT NULL,
                points_change_since_last        INTEGER NOT NULL,
                position_world                  INTEGER NOT NULL,
                position                        INTEGER NOT NULL,
                       
                PRIMARY KEY (player_id, run_date),
                FOREIGN KEY (player_id) REFERENCES player(player_id)
            )
        ''')

        ##########################################
        ############# LEAGUES ####################
        ##########################################

        # League table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS league (
                league_id                       INTEGER PRIMARY KEY AUTOINCREMENT,
                season_id                       INTEGER,
                league_level_id                 INTEGER,
                name                            TEXT,
                organizer                       TEXT,
                active                          INTEGER DEFAULT 0 CHECK(active IN (0,1)),
                url                             TEXT,
                row_created                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_updated                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                       
                FOREIGN KEY (season_id)         REFERENCES season(season_id),
                FOREIGN KEY (league_level_id)   REFERENCES league_level(league_level_id)
            );
        ''')

        # League teams
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS league_team (
                league_team_id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                league_id                       INTEGER,
                club_id                         INTEGER,
                name                            TEXT,
                row_created                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_updated                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (league_id)         REFERENCES league(league_id),
                FOREIGN KEY (club_id)           REFERENCES club(club_id)
            );
        ''')

        # League team players
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS league_team_player (
                league_team_id                  INTEGER,
                player_id                       INTEGER,
                valid_from                      DATE,
                valid_to                        DATE,
                row_created                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_updated                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (league_team_id)    REFERENCES league_team(league_team_id),
                FOREIGN KEY (player_id)         REFERENCES player(player_id),

                PRIMARY KEY (league_team_id, player_id, valid_from)
            );
        ''')

        # Fixtures (matches in a league)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fixture (
                fixture_id                      INTEGER PRIMARY KEY AUTOINCREMENT,
                league_id                       INTEGER,
                date                            DATE,
                round                           INTEGER,
                home_team_id                    INTEGER,
                away_team_id                    INTEGER,
                home_score                      INTEGER,
                away_score                      INTEGER,
                status                          TEXT DEFAULT 'completed',
                row_created                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_updated                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (league_id)         REFERENCES league(league_id),
                FOREIGN KEY (home_team_id)      REFERENCES league_team(league_team_id),
                FOREIGN KEY (away_team_id)      REFERENCES league_team(league_team_id)
            );
        ''')

        # Fixture matches (individual matches in a fixture)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fixture_match (
                fixture_id                      INTEGER,
                match_id                        INTEGER,
                order_in_fixture                INTEGER,
                row_created                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_updated                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (fixture_id)        REFERENCES fixture(fixture_id),
                FOREIGN KEY (match_id)          REFERENCES match(match_id),

                PRIMARY KEY (fixture_id, match_id),

                UNIQUE (fixture_id, order_in_fixture)
            );
        ''')
    
    except sqlite3.Error as e:
        print(f"Error creating tables: {e}")


def create_and_populate_static_tables(cursor, logger):

    logger.info("Creating static tables if needed...")

    try: 

    
        ############################################
        ########### LOOKUP TABLES ##################
        ############################################

        # Create data source table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS data_source (
                data_source_id      INTEGER PRIMARY KEY,
                name                TEXT NOT NULL,
                url                 TEXT
            );
        ''')

        data_sources = [
            (1, 'ondata',          'https://resultat.ondata.se/'),
            (2, 'PingisArenan',    'https://sbtfott.stupaevents.com/'),
            (3, 'Profixio',        'https://www.profixio.com/fx/ranking_sbtf/ranking_sbtf_public.php')
        ]

        cursor.executemany('''
            INSERT OR IGNORE INTO data_source (data_source_id, name, url)
            VALUES (?, ?, ?)
        ''', data_sources)

        # Create ranking group table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ranking_group (
                ranking_group_id INTEGER PRIMARY KEY AUTOINCREMENT,
                gender TEXT CHECK(gender IN ('H', 'D', 'HD')),
                min_rank INTEGER NOT NULL,
                max_rank INTEGER NOT NULL, 
                class_description TEXT NOT NULL,
                class_short TEXT NOT NULL UNIQUE
            );
        ''')

        classes = [
            ('H', 2250, 100000, 'Elitklass (minst 2250 p)', 'HE'),
            ('H', 2000, 2249, 'Klass 1 (2000-2249 p)', 'H1'),
            ('H', 1750, 1999, 'Klass 2 (1750-1999 p)', 'H2'),
            ('H', 1500, 1749, 'Klass 3 (1500-1749 p)', 'H3'),
            ('H', 1250, 1499, 'Klass 4 (1250-1499 p)', 'H4'),
            ('H', 1000, 1249, 'Klass 5 (1000-1249 p)', 'H5'),
            ('H', 750, 999, 'Klass 6 (750-999 p)', 'H6'),
            ('H', 0, 749, 'Klass 7 (högst 749 p)', 'H7'),
            ('D', 1750, 100000, 'Elitklass (minst 1750 p)', 'DE'),
            ('D', 1500, 1749, 'Klass 1 (1500-1749 p)', 'D1'),
            ('D', 1250, 1499, 'Klass 2 (1250-1499 p)', 'D2'),
            ('D', 1000, 1249, 'Klass 3 (1000-1249 p)', 'D3'),
            ('D', 750, 999, 'Klass 4 (750-999 p)', 'D4'),
            ('D', 0, 749, 'Klass 5 (högst 749 p)', 'D5'),
            ('HD', 0, 299, 'Ungdom u300 (0-299)', 'u300'),
            ('HD', 0, 399, 'Ungdom u400 (0-399)', 'u400'),
            ('HD', 0, 499, 'Ungdom u500 (0-499)', 'u500'),
            ('HD', 0, 599, 'Ungdom u600 (0-599)', 'u600'),
            ('HD', 0, 699, 'Ungdom u700 (0-699)', 'u700'),
            ('HD', 0, 799, 'Ungdom u800 (0-799)', 'u800'),
            ('HD', 0, 899, 'Ungdom u900 (0-899)', 'u900'),
            ('HD', 0, 999, 'Ungdom u1000 (0-999)', 'u1000')
        ]

        cursor.executemany('''
            INSERT OR IGNORE INTO ranking_group 
            (gender, min_rank, max_rank, class_description, class_short)
            VALUES (?, ?, ?, ?, ?)
        ''', classes)

        # Create seasons table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS season (
                season_id INTEGER PRIMARY KEY AUTOINCREMENT,
                season_id_ext INTEGER,
                start_date DATE,
                end_date DATE,
                start_year INTEGER,
                end_year INTEGER,
                description TEXT,
                label TEXT,
                UNIQUE (season_id_ext)
            )
        ''')

        seasons = [
            (39, '2010-07-01', '2011-06-30', 2010, 2011, '2010-11', 'Licens 10/11'),
            (50, '2011-07-01', '2012-06-30', 2011, 2012, '2011-12', 'Licens 11-12'),
            (54, '2012-07-01', '2013-06-30', 2012, 2013, '2012-13', 'Licens 2012-13'),
            (56, '2013-07-01', '2014-06-30', 2013, 2014, '2013-14', 'Licens 2013-14'),
            (68, '2014-07-01', '2015-06-30', 2014, 2015, '2014-15', 'Licens 2014-15'),
            (73, '2015-07-01', '2016-06-30', 2015, 2016, '2015-16', 'Licens 2015-16'),
            (80, '2016-07-01', '2017-06-30', 2016, 2017, '2016-17', 'Licens 2016-17'),
            (99, '2017-07-01', '2018-06-30', 2017, 2018, '2017-18', 'Licens 2017-18'),
            (103, '2018-07-01', '2019-06-30', 2018, 2019, '2018-19', 'Licens 2018-19'),
            (109, '2019-07-01', '2020-06-30', 2019, 2020, '2019-20', 'Licens 2019-20'),
            (114, '2020-07-01', '2021-06-30', 2020, 2021, '2020-21', 'Licens 2020-21'),
            (121, '2021-07-01', '2022-06-30', 2021, 2022, '2021-22', 'Licens 2021-22'),
            (126, '2022-07-01', '2023-06-30', 2022, 2023, '2022-23', 'Licens 2022-23'),
            (135, '2023-07-01', '2024-06-30', 2023, 2024, '2023-24', 'Licens 2023-24'),
            (171, '2024-07-01', '2025-06-30', 2024, 2025, '2024-25', 'Licens 2024-25'),
            (181, '2025-07-01', '2026-06-30', 2025, 2026, '2025-26', 'Licens 2025-26')
        ]

        cursor.executemany('''
            INSERT OR IGNORE INTO season
            (season_id_ext, start_date, end_date, start_year, end_year, description, label)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', seasons)

        # Create license table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS license (
                license_id INTEGER PRIMARY KEY AUTOINCREMENT,
                type TEXT,
                age_group TEXT,
                UNIQUE (type, age_group)
            )
        ''')

        licenses = [
            ('D-licens', ''),
            ('A-licens', 'Barn'),
            ('A-licens', 'Senior'),
            ('A-licens', 'Pensionär'),
            ('A-licens', 'Ungdom'),
            ('48-timmarslicens', ''),
            ('Paralicens', '')
        ]

        cursor.executemany('''
            INSERT OR IGNORE INTO license (type, age_group)
            VALUES (?, ?)
        ''', licenses)  

        # Create district table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS district (
                district_id         INTEGER     PRIMARY KEY AUTOINCREMENT,
                district_id_ext     INTEGER,
                name                TEXT        NOT NULL,
                UNIQUE (name, district_id_ext)
            )
        ''')
    
        districts = [
            ('28', 'Blekinge Bordtennisförbund'),
            ('30', 'Bohuslän-Dals BTF'),
            ('32', 'Dalarnas Bordtennisförbund'),
            ('31', 'Gotlands Bordtennisförbund'),
            ('34', 'Gästriklands Bordtennisförbund'),
            ('33', 'Göteborgs Bordtennisförbund'),
            ('35', 'Hallands Bordtennisförbund'),
            ('36', 'Hälsinglands Bordtennisförbund'),
            ('42', 'Jämtland-Härjedalens Bordtennisförbund'),
            ('37', 'Medelpads Bordtennisförbund'),
            ('181', 'Nordvästra Götalands Bordtennisförbund'),
            ('186', 'Nordöstra Svealands Bordtennisförbund'),
            ('46', 'Norrbottens Bordtennisförbund'),
            ('740', 'Norrlands Bordtennisförbund'),
            ('45', 'Skånes Bordtennisförbund'),
            ('38', 'Smålands Bordtennisförbund'),
            ('47', 'Stockholms Bordtennisförbund'),
            ('739', 'Sydöstra Götalands BTF'),
            ('43', 'Södermanlands Bordtennisförbund'),
            ('48', 'Upplands Bordtennisförbund'),
            ('39', 'Värmlands Bordtennisförbund'),
            ('49', 'Västerbottens Bordtennisförbund'),
            ('40', 'Västergötlands Bordtennisförbund'),
            ('50', 'Västmanlands Bordtennisförbund'),
            ('44', 'Ångermanlands Bordtennisförbund'),
            ('41', 'Örebro Läns Bordtennisförbund'),
            ('51', 'Östergötlands BTF')
        ]

        cursor.executemany('''
            INSERT OR IGNORE INTO district (district_id_ext, name)
            VALUES (?, ?)
        ''', districts) 

        ############### TOURNAMENT LOOKUPS ################

        # tournament_status
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tournament_status (
                tournament_status_id        INTEGER PRIMARY KEY,
                description                 TEXT NOT NULL,
                UNIQUE(description)
            ) WITHOUT ROWID;
        ''')

        tournament_statuses = [
            (1, 'Upcoming'),
            (2, 'Ongoing'),
            (3, 'Ended'),
            (4, 'Cancelled'),
            (5, 'Postponed'),
            (6, 'Unknown')
        ]

        cursor.executemany('''
            INSERT OR IGNORE INTO tournament_status (tournament_status_id, description)
            VALUES (?, ?)
        ''', tournament_statuses)

        # tournament_level
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tournament_level (
                tournament_level_id         INTEGER    PRIMARY KEY,
                description                 TEXT       NOT NULL,
                UNIQUE(description)
            ) WITHOUT ROWID;
        ''')

        tournament_levels = [
            (1, 'National'),
            (2, 'International')
        ]

        cursor.executemany('''
            INSERT OR IGNORE INTO tournament_level (tournament_level_id, description)
            VALUES (?, ?)
        ''', tournament_levels)

        # tournament_type
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tournament_type (
                tournament_type_id          INTEGER    PRIMARY KEY,
                description                 TEXT       NOT NULL,
                UNIQUE(description)
            ) WITHOUT ROWID;
        ''')

        tournament_types = [
            (1, 'National'),
            (2, 'International')
        ]

        cursor.executemany('''
            INSERT OR IGNORE INTO tournament_type (tournament_type_id, description)
            VALUES (?, ?)
        ''', tournament_types)

        ############### TOURNAMENT CLASS LOOKUPS ################

        # tournament_class_stage
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tournament_class_stage (
                tournament_class_stage_id       INTEGER    PRIMARY KEY,
                shortname                       TEXT       NOT NULL,    -- e.g. 'GROUP','R32','R16','QF','SF','F','SWISS'
                description                     TEXT       NOT NULL,
                is_knockout                     INTEGER    NOT NULL CHECK(is_knockout IN (0,1)),
                round_order                     INTEGER,                -- e.g. 32,16,8,... (NULL for GROUP/SWISS)
                UNIQUE (shortname)
            ) WITHOUT ROWID;
        ''') 

        stages = [
            (1, 'GROUP',    'Group',        0,  None),
            (2, 'R128',     'Round of 128', 1,  128),
            (3, 'R64',      'Round of 64',  1,  64),
            (4, 'R32',      'Round of 32',  1,  32),
            (5, 'R16',      'Round of 16',  1,  16),
            (6, 'QF',       'Quarterfinal', 1,  8),
            (7, 'SF',       'Semifinal',    1,  4),
            (8, 'F',        'Final',        1,  2),
            (9,  'SWISS', 'Swiss System',   0,  None)       # A Swiss-system stage pairs players with similar scores across several rounds; nobody is eliminated each round. It’s neither simple group round-robin nor KO.
        ]

        cursor.executemany('''
            INSERT OR IGNORE INTO tournament_class_stage (tournament_class_stage_id, shortname, description, is_knockout, round_order)
            VALUES (?, ?, ?, ?, ?)
        ''', stages)

        # tournament_class_type
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tournament_class_type (
                tournament_class_type_id    INTEGER PRIMARY KEY,
                description                 TEXT NOT NULL,
                UNIQUE(description)
            ) WITHOUT ROWID;
        ''')

        tournament_class_types = [
            (1, 'Singles'),
            (2, 'Doubles'),
            (3, 'Mixed doubles'),
            (4, 'Team'),
            (9, 'Unknown')
        ]

        cursor.executemany('''
            INSERT OR IGNORE INTO tournament_class_type (tournament_class_type_id, description)
            VALUES (?, ?)
        ''', tournament_class_types)

        # tournament_class_structure
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tournament_class_structure (
                tournament_class_structure_id       INTEGER PRIMARY KEY,
                description                         TEXT NOT NULL,
                UNIQUE(description)
            ) WITHOUT ROWID;
        ''')

        tournament_class_structure = [
            (1, 'Groups_and_KO'),
            (2, 'Groups_only'),
            (3, 'KO_only'),
            (9, 'Unknown')
        ]

        cursor.executemany('''
            INSERT OR IGNORE INTO tournament_class_structure (tournament_class_structure_id, description)
            VALUES (?, ?)
        ''', tournament_class_structure)

        # competition_type
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS competition_type (
                competition_type_id         INTEGER PRIMARY KEY,
                description                 TEXT NOT NULL,
                UNIQUE(description)
            ) WITHOUT ROWID;
        ''')

        competition_types = [
            (1, 'Tournament'),
            (2, 'League'),
            (3, 'Other')
        ]

        cursor.executemany('''
            INSERT OR IGNORE INTO competition_type (competition_type_id, description)
            VALUES (?, ?)
        ''', competition_types)


        ######### LEAGUE LOOKUPS ###################
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS league_level (
                league_level_id             INTEGER PRIMARY KEY,
                description                 TEXT NOT NULL,
                UNIQUE(description)
            ) WITHOUT ROWID;
        ''')

        league_levels = [
            (1, 'National'),
            (2, 'Regional'),
            (3, 'District')
        ]

        cursor.executemany('''
            INSERT OR IGNORE INTO league_level (league_level_id, description)
            VALUES (?, ?)
        ''', league_levels)

        ######### CLUBS TABLES ###################

        # Create clubs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS club (
                club_id                     INTEGER     PRIMARY KEY,
                shortname                   TEXT,
                longname                    TEXT,
                club_type                   INTEGER     DEFAULT 1,
                city                        TEXT,
                country_code                TEXT,
                remarks                     TEXT,
                homepage                    TEXT,
                active                      INTEGER     DEFAULT 1 CHECK (active IN (0, 1)),
                district_id                 INTEGER,
                row_created                 TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (district_id)   REFERENCES district(district_id),
                FOREIGN KEY (club_type)     REFERENCES club_type(club_type_id),
                UNIQUE (shortname),
                UNIQUE (longname)
            );
        ''')

        # Create club ext id mappings table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS club_id_ext (
                club_id                     INTEGER,
                club_id_ext                 INTEGER     NOT NULL    PRIMARY KEY,
                data_source                 INTEGER     DEFAULT 3,
                row_created                 TIMESTAMP   DEFAULT     CURRENT_TIMESTAMP,
                FOREIGN KEY (club_id)       REFERENCES club(club_id)        ON DELETE CASCADE,
                FOREIGN KEY (data_source)   REFERENCES data_source(data_source_id),
                UNIQUE      (club_id_ext, data_source)
            )
        ''')
        
        # Create club name alias table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS club_name_alias (
                club_id                 INTEGER     NOT NULL,
                alias                   TEXT        NOT NULL,
                alias_type              TEXT        NOT NULL    CHECK(alias_type IN ('short','long')),
                row_created             TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (club_id)   REFERENCES club(club_id)        ON DELETE CASCADE,
                UNIQUE (club_id, alias, alias_type)

            )
        ''')

        # Create club type table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS club_type (
                club_type_id     INTEGER PRIMARY KEY,
                description      TEXT,
                row_created      TIMESTAMP   DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        club_types = [
            (1, 'Club'),
            (2, 'National Association'),
            (3, 'National Team')
        ]

        cursor.executemany('''
            INSERT OR IGNORE INTO club_type (club_type_id, description)
            VALUES (?, ?)
        ''', club_types)

        ############ DEBUG TABLES ######################


        cursor.execute('''
            CREATE TABLE IF NOT EXISTS log_output (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                function_name TEXT NOT NULL,
                filename TEXT NOT NULL,
                context_json TEXT,
                status TEXT NOT NULL,  -- e.g., 'error', 'warning', 'skipped'
                message TEXT NOT NULL,
                msg_id TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP  -- Auto-adds insert time
            );
        ''')

        # Create missing clubs table to be mapped later
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS club_missing (
                club_name_raw   TEXT        PRIMARY KEY,
                club_name_norm  TEXT        NOT NULL,
                row_created     TIMESTAMP   DEFAULT CURRENT_TIMESTAMP
            );
        ''')

        # Create table for documenting club names with prefixes
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS club_name_prefix_match (
                tournament_class_id INTEGER NOT NULL,
                club_raw_name TEXT NOT NULL,
                matched_club_id INTEGER NOT NULL,
                matched_club_aliases TEXT NOT NULL,
                row_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(club_raw_name)
            );
        ''')

    except sqlite3.Error as e:
        print(f"Error creating tables: {e}")

def create_indexes(cursor):
    print("ℹ️  Creating indexes...")

    indexes = [
        # Tournament table
        "CREATE INDEX IF NOT EXISTS idx_tournament_id_ext ON tournament(tournament_id_ext)",
        "CREATE INDEX IF NOT EXISTS idx_tournament_shortname ON tournament(shortname)",
        "CREATE INDEX IF NOT EXISTS idx_tournament_startdate ON tournament(startdate)",  # Added for date-based queries

        # Tournament class
        "CREATE INDEX IF NOT EXISTS idx_tournament_class_tournament_id ON tournament_class(tournament_id)",
        "CREATE INDEX IF NOT EXISTS idx_tournament_class_type_id ON tournament_class(tournament_class_type_id)",
        "CREATE INDEX IF NOT EXISTS idx_tournament_class_structure_id ON tournament_class(tournament_class_structure_id)",
        "CREATE INDEX IF NOT EXISTS idx_tournament_class_id_ext ON tournament_class(tournament_class_id_ext)",
        "CREATE INDEX IF NOT EXISTS idx_tournament_class_startdate ON tournament_class(startdate)",

        # Participant 
        # "CREATE INDEX IF NOT EXISTS idx_participant_tournament_class_id ON participant(tournament_class_id)",

        # Participant player 
        # "CREATE INDEX IF NOT EXISTS idx_participant_player_player_id ON participant_player(player_id)",
        # "CREATE INDEX IF NOT EXISTS idx_participant_player_club_id ON participant_player(club_id)",
        # "CREATE INDEX IF NOT EXISTS idx_participant_player_participant_id ON participant_player(participant_id)",  # Added for joins to participant

        # Participant raw tournament
        "CREATE INDEX IF NOT EXISTS idx_prt_class ON participant_player_raw_tournament(tournament_class_id_ext)",

        # Tournament class group (replaced tournament_group)
        # "CREATE INDEX IF NOT EXISTS idx_tournament_class_group_class_id ON tournament_class_group(tournament_class_id)",

        # Tournament class group member (replaced tournament_group_member)
        # "CREATE INDEX IF NOT EXISTS idx_tournament_class_group_member_group_id ON tournament_class_group_member(tournament_class_group_id)",
        # "CREATE INDEX IF NOT EXISTS idx_tournament_class_group_member_participant_id ON tournament_class_group_member(participant_id)",  # Updated to participant_id

        # Match
        "CREATE INDEX IF NOT EXISTS idx_match_date ON match(date)",  # Added for date-based queries

        # Match id ext
        # "CREATE INDEX IF NOT EXISTS idx_match_id_ext_match_id ON match_id_ext(match_id)",  # Added for source lookups

        # Match side
        # "CREATE INDEX IF NOT EXISTS idx_match_side_match_id ON match_side(match_id)",
        # "CREATE INDEX IF NOT EXISTS idx_match_side_participant_id ON match_side(participant_id)",  # Added for participant match history

        # Match competition
        # "CREATE INDEX IF NOT EXISTS idx_match_competition_match_id ON match_competition(match_id)",
        # "CREATE INDEX IF NOT EXISTS idx_match_competition_tournament_class_id ON match_competition(tournament_class_id)",
        # "CREATE INDEX IF NOT EXISTS idx_match_competition_competition_type_id ON match_competition(competition_type_id)",  # Added for type filtering

        # Game
        "CREATE INDEX IF NOT EXISTS idx_game_match_id ON game(match_id)",  # Added for match results aggregation

        # Tournament class group standing
        # "CREATE INDEX IF NOT EXISTS idx_tournament_class_group_standing_group_id ON tournament_class_group_standing(tournament_class_group_id)",
        # "CREATE INDEX IF NOT EXISTS idx_tournament_class_group_standing_participant_id ON tournament_class_group_standing(participant_id)"  # Added for standings queries

        # Create unique index for tournament class group
        # "CREATE UNIQUE INDEX IF NOT EXISTS uq_tcg_class_group ON tournament_class_group (tournament_class_id, tournament_class_group_id)"
    ]

    try:
        for stmt in indexes:
            cursor.execute(stmt)

    except sqlite3.Error as e:
        print(f"Error creating indexes: {e}")
        logging.error(f"Error creating indexes: {e}")


def create_triggers(cursor):
    print("ℹ️  Creating database triggers...")

    # # Always enforce FK constraints
    # cursor.execute("PRAGMA foreign_keys = ON;")

    # triggers = [
    #     (
    #         "update_standings_after_game",
    #         """
    #         CREATE TRIGGER IF NOT EXISTS update_standings_after_game
    #         AFTER INSERT ON game
    #         FOR EACH ROW
    #         WHEN (SELECT tournament_class_group_id FROM match_competition WHERE match_id = NEW.match_id AND competition_type_id = 1 AND tournament_class_group_id IS NOT NULL) IS NOT NULL
    #         BEGIN
    #             -- Update games and points for both participants
    #             UPDATE tournament_class_group_standing
    #             SET 
    #                 nbr_games_won = nbr_games_won + 
    #                     CASE WHEN (SELECT side FROM match_side WHERE match_id = NEW.match_id AND participant_id = tournament_class_group_standing.participant_id) = NEW.winning_side THEN 1 ELSE 0 END,
    #                 nbr_games_lost = nbr_games_lost + 
    #                     CASE WHEN (SELECT side FROM match_side WHERE match_id = NEW.match_id AND participant_id = tournament_class_group_standing.participant_id) = NEW.winning_side THEN 0 ELSE 1 END,
    #                 nbr_points_won = nbr_points_won + 
    #                     CASE WHEN (SELECT side FROM match_side WHERE match_id = NEW.match_id AND participant_id = tournament_class_group_standing.participant_id) = 1 THEN NEW.side1_points ELSE NEW.side2_points END,
    #                 nbr_points_lost = nbr_points_lost + 
    #                     CASE WHEN (SELECT side FROM match_side WHERE match_id = NEW.match_id AND participant_id = tournament_class_group_standing.participant_id) = 1 THEN NEW.side2_points ELSE NEW.side1_points END
    #             WHERE tournament_class_group_id = (SELECT tournament_class_group_id FROM match_competition WHERE match_id = NEW.match_id AND competition_type_id = 1)
    #                 AND participant_id IN (SELECT participant_id FROM match_side WHERE match_id = NEW.match_id);

    #             -- Update matches_won for the winner if this game decided the match
    #             UPDATE tournament_class_group_standing
    #             SET nbr_matches_won = nbr_matches_won + 1
    #             WHERE tournament_class_group_id = (SELECT tournament_class_group_id FROM match_competition WHERE match_id = NEW.match_id AND competition_type_id = 1)
    #                 AND participant_id = (
    #                     SELECT participant_id FROM match_side 
    #                     WHERE match_id = NEW.match_id 
    #                     AND side = (
    #                         CASE 
    #                             WHEN (
    #                                 (SELECT COUNT(*) FROM game WHERE match_id = NEW.match_id AND winning_side = 1) 
    #                                 >= ((SELECT best_of FROM match WHERE match_id = NEW.match_id) + 1) / 2
    #                             ) THEN 1
    #                             ELSE 2
    #                         END
    #                     )
    #                 )
    #                 AND (
    #                     -- Was not won before
    #                     (
    #                         (SELECT COUNT(*) FROM game WHERE match_id = NEW.match_id AND winning_side = 1) - (CASE WHEN NEW.winning_side = 1 THEN 1 ELSE 0 END) 
    #                         < ((SELECT best_of FROM match WHERE match_id = NEW.match_id) + 1) / 2
    #                     ) AND (
    #                         (SELECT COUNT(*) FROM game WHERE match_id = NEW.match_id AND winning_side = 2) - (CASE WHEN NEW.winning_side = 2 THEN 1 ELSE 0 END) 
    #                         < ((SELECT best_of FROM match WHERE match_id = NEW.match_id) + 1) / 2
    #                     )
    #                 ) AND (
    #                     -- Is won now
    #                     (
    #                         (SELECT COUNT(*) FROM game WHERE match_id = NEW.match_id AND winning_side = 1) 
    #                         >= ((SELECT best_of FROM match WHERE match_id = NEW.match_id) + 1) / 2
    #                         OR 
    #                         (SELECT COUNT(*) FROM game WHERE match_id = NEW.match_id AND winning_side = 2) 
    #                         >= ((SELECT best_of FROM match WHERE match_id = NEW.match_id) + 1) / 2
    #                     )
    #                 );

    #             -- Update matches_lost for the loser if this game decided the match
    #             UPDATE tournament_class_group_standing
    #             SET nbr_matches_lost = nbr_matches_lost + 1
    #             WHERE tournament_class_group_id = (SELECT tournament_class_group_id FROM match_competition WHERE match_id = NEW.match_id AND competition_type_id = 1)
    #                 AND participant_id = (
    #                     SELECT participant_id FROM match_side 
    #                     WHERE match_id = NEW.match_id 
    #                     AND side = (
    #                         CASE 
    #                             WHEN (
    #                                 (SELECT COUNT(*) FROM game WHERE match_id = NEW.match_id AND winning_side = 1) 
    #                                 >= ((SELECT best_of FROM match WHERE match_id = NEW.match_id) + 1) / 2
    #                             ) THEN 2
    #                             ELSE 1
    #                         END
    #                     )
    #                 )
    #                 AND (
    #                     -- Was not won before
    #                     (
    #                         (SELECT COUNT(*) FROM game WHERE match_id = NEW.match_id AND winning_side = 1) - (CASE WHEN NEW.winning_side = 1 THEN 1 ELSE 0 END) 
    #                         < ((SELECT best_of FROM match WHERE match_id = NEW.match_id) + 1) / 2
    #                     ) AND (
    #                         (SELECT COUNT(*) FROM game WHERE match_id = NEW.match_id AND winning_side = 2) - (CASE WHEN NEW.winning_side = 2 THEN 1 ELSE 0 END) 
    #                         < ((SELECT best_of FROM match WHERE match_id = NEW.match_id) + 1) / 2
    #                     )
    #                 ) AND (
    #                     -- Is won now
    #                     (
    #                         (SELECT COUNT(*) FROM game WHERE match_id = NEW.match_id AND winning_side = 1) 
    #                         >= ((SELECT best_of FROM match WHERE match_id = NEW.match_id) + 1) / 2
    #                         OR 
    #                         (SELECT COUNT(*) FROM game WHERE match_id = NEW.match_id AND winning_side = 2) 
    #                         >= ((SELECT best_of FROM match WHERE match_id = NEW.match_id) + 1) / 2
    #                     )
    #                 );
    #         END;
    #         """
    #     ),
    #     # Add more triggers here as needed
    # ]

    # try:

    #     for name, create_sql in triggers:
    #         cursor.execute(f"DROP TRIGGER IF EXISTS {name};")
    #         cursor.execute(create_sql)

    # except sqlite3.Error as e:
    #     print(f"Error creating triggers: {e}")

def create_views(cursor):
    print("ℹ️  Creating database views...")

    views = [
        (
            "V_match_results",
            """
            CREATE VIEW V_match_results AS
            SELECT 
                m.match_id,
                m.date,
                m.best_of,
                GROUP_CONCAT(DISTINCT p1.name, ' / ') AS side1_players,
                GROUP_CONCAT(DISTINCT c1.name, ' / ') AS side1_clubs,
                GROUP_CONCAT(DISTINCT p2.name, ' / ') AS side2_players,
                GROUP_CONCAT(DISTINCT c2.name, ' / ') AS side2_clubs,
                SUM(CASE WHEN g.winning_side = 1 THEN 1 ELSE 0 END) AS side1_games_won,
                SUM(CASE WHEN g.winning_side = 2 THEN 1 ELSE 0 END) AS side2_games_won,
                CASE 
                    WHEN SUM(CASE WHEN g.winning_side = 1 THEN 1 ELSE 0 END) > SUM(CASE WHEN g.winning_side = 2 THEN 1 ELSE 0 END) THEN 1 
                    ELSE 2 
                END AS winner_side
            FROM match m
            JOIN match_side ms1 ON m.match_id = ms1.match_id AND ms1.side = 1
            JOIN match_side ms2 ON m.match_id = ms2.match_id AND ms2.side = 2
            JOIN participant_player pp1 ON pp1.participant_id = ms1.participant_id
            JOIN player p1 ON p1.player_id = pp1.player_id
            LEFT JOIN club c1 ON c1.club_id = pp1.club_id
            JOIN participant_player pp2 ON pp2.participant_id = ms2.participant_id
            JOIN player p2 ON p2.player_id = pp2.player_id
            LEFT JOIN club c2 ON c2.club_id = pp2.club_id
            LEFT JOIN game g ON g.match_id = m.match_id
            GROUP BY m.match_id;
            """
        ),
        (
            "V_group_standings",
            """
            CREATE VIEW V_group_standings AS
            SELECT 
                s.tournament_class_group_id,
                tc.shortname AS tournament_class_shortname,
                GROUP_CONCAT(p.name, ' / ') AS participant_players,
                s.position_in_group,
                s.nbr_matches_won,
                s.nbr_matches_lost,
                s.nbr_games_won,
                s.nbr_games_lost,
                s.nbr_points_won,
                s.nbr_points_lost
            FROM tournament_class_group_standing s
            JOIN tournament_class_group tcg ON tcg.tournament_class_group_id = s.tournament_class_group_id
            JOIN participant pa ON pa.participant_id = s.participant_id
            JOIN tournament_class tc ON tc.tournament_class_id = pa.tournament_class_id
            JOIN participant_player pp ON pp.participant_id = s.participant_id
            JOIN player p ON p.player_id = pp.player_id
            GROUP BY s.tournament_class_group_id, s.participant_id
            ORDER BY s.tournament_class_group_id, s.position_in_group;
            """
        ),
        (
            "V_tourn_class_overview",
            """
            CREATE VIEW IF NOT EXISTS V_tourn_class_overview AS
            SELECT
                t.shortname  AS tournament_shortname,
                tc.longname  AS class_longname,
                tc.shortname AS class_shortname,
                tct.description AS tournament_class_type,
                tcs.description AS tournament_class_structure,
                ts.description  AS tournament_status,
                tc.date       AS class_date,
                t.country_code,
                t.url         AS tournament_url,
                tc.tournament_class_id,
                tc.tournament_class_id_ext,
                t.tournament_id,
                t.tournament_id_ext,
                tc.is_valid,
                tc.row_created,
                tc.row_updated
            FROM tournament_class tc
            JOIN tournament t
            ON t.tournament_id = tc.tournament_id
            LEFT JOIN tournament_class_type tct
            ON tct.tournament_class_type_id = tc.tournament_class_type_id
            LEFT JOIN tournament_class_structure tcs
            ON tcs.tournament_class_structure_id = tc.tournament_class_structure_id
            LEFT JOIN tournament_status ts
            ON ts.tournament_status_id = t.tournament_status_id
            ORDER BY t.tournament_id, tc.tournament_class_id DESC;
            """
        ),
        (
            "V_tournament_class_participants",
            """
            CREATE VIEW IF NOT EXISTS V_tournament_class_participants AS
            SELECT
                t.tournament_id,
                t.tournament_id_ext,
                tc.tournament_class_id,
                tc.tournament_class_id_ext,
                t.longname AS tournament_name,
                tc.shortname AS class_name,
                COALESCE(pl.firstname || ' ' || pl.lastname, pl.fullname_raw) AS player_name,
                c.shortname as club_name,
                c.club_id as club_id,
                pp.participant_player_id,
                pp.player_id,
                p.participant_id,
				pp.participant_player_id_ext,
                p.tournament_class_seed            AS seed,
                p.tournament_class_final_position  AS final_position,
                tc.date,
                pp.club_id,
                c.shortname AS club_name,
                t.url AS tournament_url
            FROM participant p
            JOIN tournament_class tc ON tc.tournament_class_id = p.tournament_class_id
            JOIN tournament t        ON t.tournament_id        = tc.tournament_id
            JOIN participant_player pp ON pp.participant_id    = p.participant_id
            LEFT JOIN player pl ON pl.player_id = pp.player_id
            LEFT JOIN club   c  ON c.club_id    = pp.club_id
            ORDER BY
                tc.tournament_class_id,
                p.participant_id,
                pp.participant_player_id;        
        """
        ),
        (
            "V_foreign_keys",
            '''
            CREATE VIEW IF NOT EXISTS V_foreign_keys AS
                SELECT m.name AS table_name, p.*
            FROM sqlite_master AS m
            JOIN pragma_foreign_key_list(m.name) AS p
            WHERE m.type = 'table';
            '''
        ),
        (
            "V_clubs_full_agg",
            '''
            CREATE VIEW IF NOT EXISTS V_clubs_full_agg AS
                SELECT 
                    c.club_id,
                    c.shortname        AS club_shortname,
                    c.longname         AS club_longname,
                    c.city,
                    c.country_code,
                    c.active,
                    ct.description     AS club_type,
                    d.district_id,
                    d.name        AS district_shortname,
                    GROUP_CONCAT(DISTINCT ce.club_id_ext || ':' || ce.data_source) AS ext_ids,
                    GROUP_CONCAT(DISTINCT a.alias || ' (' || a.alias_type || ')')  AS aliases
                FROM club c
                LEFT JOIN club_type ct
                    ON ct.club_type_id = c.club_type
                LEFT JOIN district d
                    ON d.district_id = c.district_id
                LEFT JOIN club_id_ext ce
                    ON ce.club_id = c.club_id
                LEFT JOIN club_name_alias a
                    ON a.club_id = c.club_id
                GROUP BY c.club_id
                ORDER BY c.club_id;
            '''
        )
    ]

    try:
        for name, create_sql in views:
            cursor.execute("DROP VIEW IF EXISTS vw_tourn_class_overview")
            cursor.execute("DROP VIEW IF EXISTS vw_match_results")
            cursor.execute("DROP VIEW IF EXISTS vw_tournament_class_participants")
            cursor.execute("DROP VIEW IF EXISTS vw_foreign_keys")
            cursor.execute("DROP VIEW IF EXISTS vw_clubs_full_agg")
            

            cursor.execute(f"DROP VIEW IF EXISTS {name};")
                # cursor.execute(create_sql)
            
    except sqlite3.Error as e:
        print(f"Error creating views: {e}")


def execute_custom_sql(cursor):
    cursor.execute('''
        DELETE FROM TOURNAMENT_CLASS WHERE tournament_class_id_ext = '29604'
    ''')


