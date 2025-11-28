
# db.py: 

import sqlite3
from config import DB_NAME
import logging
import datetime
import time

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
                logger.warning({}, f"Table not found, skipping drop: {table_name}", to_console=True)
        except sqlite3.Error as e:
            logger.failed(f"Error dropping table {table_name}: {e}", to_console=True)
    if not dropped:
        logger.info("No tables dropped.", to_console=True)
    else:
        logger.info(f"Dropped {len(dropped)} tables: {', '.join(dropped)}", to_console=True)

def create_raw_tables(cursor, logger):

    raw_tables = {
        "tournament_raw": 
        '''
            CREATE TABLE IF NOT EXISTS tournament_raw (
                row_id                          INTEGER PRIMARY KEY AUTOINCREMENT,
                tournament_id_ext               TEXT,
                shortname                       TEXT,
                longname                        TEXT,
                startdate                       DATE,
                enddate                         DATE,
                registration_end_date           DATE,
                city                            TEXT,
                arena                           TEXT,
                country_code                    TEXT,
                url                             TEXT,
                tournament_level                TEXT,
                tournament_type                 TEXT,
                organiser_name                  TEXT,
                organiser_email                 TEXT,
                organiser_phone                 TEXT,
                data_source_id                  INTEGER DEFAULT 1,
                is_listed                       BOOLEAN DEFAULT 1,
                content_hash                    TEXT,
                last_seen_at                    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_created                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_updated                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (data_source_id)    REFERENCES data_source(data_source_id),

                UNIQUE (tournament_id_ext, data_source_id),
                UNIQUE (shortname, startdate, arena, data_source_id)
            )
        ''',

        "tournament_class_raw": 
        '''
            CREATE TABLE IF NOT EXISTS tournament_class_raw (
                row_id                          INTEGER PRIMARY KEY AUTOINCREMENT,
                tournament_id_ext               TEXT,
                tournament_class_id_ext         TEXT,
                startdate                       DATE,
                shortname                       TEXT,
                longname                        TEXT,
                gender                          TEXT,
                max_rank                        INTEGER,
                max_age                         INTEGER,
                url                             TEXT,
                raw_stages                      TEXT,
                raw_stage_hrefs                 TEXT,
                ko_tree_size                    INTEGER,
                data_source_id                  INTEGER DEFAULT 1,
                content_hash                    TEXT,
                last_seen_at                    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_created                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_updated                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (data_source_id)    REFERENCES data_source(data_source_id),

                UNIQUE (tournament_id_ext, tournament_class_id_ext, data_source_id)
            )
        ''',

        "player_license_raw": 
        '''
            CREATE TABLE IF NOT EXISTS player_license_raw (
                row_id                          INTEGER PRIMARY KEY AUTOINCREMENT,
                season_label                    TEXT,
                season_id_ext                   TEXT,
                club_name                       TEXT,
                club_id_ext                     TEXT,
                player_id_ext                   TEXT,
                firstname                       TEXT,
                lastname                        TEXT,
                gender                          TEXT,
                year_born                       TEXT,
                license_info_raw                TEXT,
                ranking_group_raw               TEXT,
                data_source_id                  INTEGER DEFAULT 3,
                content_hash                    TEXT,
                last_seen_at                    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_created                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_updated                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (data_source_id)    REFERENCES data_source(data_source_id),

                UNIQUE(season_id_ext, player_id_ext, club_id_ext, license_info_raw)
            )
        ''',

        "player_ranking_raw": 
        '''
            CREATE TABLE IF NOT EXISTS player_ranking_raw (
                row_id                          INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id_ext                      TEXT,
                run_date                        DATE,
                player_id_ext                   TEXT,
                firstname                       TEXT,
                lastname                        TEXT,
                year_born                       TEXT,
                club_name                       TEXT,
                points                          INTEGER,
                points_change_since_last        INTEGER,
                position_world                  INTEGER,
                position                        INTEGER,
                data_source_id                  INTEGER DEFAULT 3,
                content_hash                    TEXT,
                last_seen_at                    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_created                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_updated                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (data_source_id)    REFERENCES data_source(data_source_id),

                UNIQUE (run_id_ext, player_id_ext)
            )
        ''',

        "player_transition_raw": 
        '''
            CREATE TABLE IF NOT EXISTS player_transition_raw (
                row_id                          INTEGER PRIMARY KEY AUTOINCREMENT,
                season_id_ext                   TEXT,
                season_label                    TEXT,
                firstname                       TEXT,
                lastname                        TEXT,
                date_born                       DATE,
                year_born                       TEXT,
                club_from                       TEXT,
                club_to                         TEXT,
                transition_date                 DATE,
                data_source_id                  INTEGER DEFAULT 3,
                content_hash                    TEXT,
                last_seen_at                    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_created                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_updated                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (data_source_id)    REFERENCES data_source(data_source_id),

                UNIQUE (firstname, lastname, date_born, transition_date)
            )
        ''',

        "tournament_class_entry_raw": 
        '''
            CREATE TABLE IF NOT EXISTS tournament_class_entry_raw (
                row_id                          INTEGER PRIMARY KEY AUTOINCREMENT,
                tournament_id_ext               TEXT NOT NULL,   
                tournament_class_id_ext         TEXT NOT NULL,   
                tournament_player_id_ext        TEXT,            
                fullname_raw                    TEXT NOT NULL,   
                clubname_raw                    TEXT,   
                group_id_raw                    TEXT,
                seed_in_group_raw               TEXT,
                seed_raw                        TEXT,        
                final_position_raw              TEXT,            
                entry_group_id_int              INTEGER,            
                data_source_id                  INTEGER NOT NULL, 
                content_hash                    TEXT,
                row_created                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_updated                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (data_source_id)    REFERENCES data_source(data_source_id),

                UNIQUE (tournament_id_ext, tournament_class_id_ext, fullname_raw, clubname_raw, tournament_player_id_ext, data_source_id),
                UNIQUE (tournament_id_ext, tournament_class_id_ext, tournament_player_id_ext)
            );
        ''',

        "tournament_class_match_raw":
        '''
            CREATE TABLE IF NOT EXISTS tournament_class_match_raw (
                row_id                          INTEGER PRIMARY KEY AUTOINCREMENT,
                tournament_id_ext               TEXT NOT NULL,
                tournament_class_id_ext         TEXT NOT NULL,
                group_id_ext                    TEXT,
                match_id_ext                    TEXT,
                s1_player_id_ext                TEXT,
                s2_player_id_ext                TEXT,
                s1_fullname_raw                 TEXT,
                s2_fullname_raw                 TEXT,
                s1_clubname_raw                 TEXT,
                s2_clubname_raw                 TEXT,
                game_point_tokens               TEXT,
                best_of                         INTEGER,
                raw_line_text                   TEXT,
                tournament_class_stage_id       INTEGER,
                data_source_id                  INTEGER NOT NULL,
                content_hash                    TEXT,
                row_created                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_updated                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (data_source_id)    REFERENCES data_source(data_source_id),
                FOREIGN KEY (tournament_class_stage_id) REFERENCES tournament_class_stage(tournament_class_stage_id),

                UNIQUE (tournament_id_ext, tournament_class_id_ext, raw_line_text, data_source_id),
                UNIQUE (tournament_id_ext, tournament_class_id_ext, match_id_ext, data_source_id)
            );
        ''',

        "league_raw":
        '''
            CREATE TABLE IF NOT EXISTS league_raw (
                row_id                          INTEGER PRIMARY KEY AUTOINCREMENT,
                league_id_ext                   TEXT,
                season_id_ext                   TEXT,
                league_level                    TEXT,
                name                            TEXT,
                organiser                       TEXT,
                district_id_ext                 TEXT,
                district_description            TEXT,
                active                          INTEGER DEFAULT 0 CHECK(active IN (0,1)),
                url                             TEXT,
                startdate                       DATE,
                enddate                         DATE,
                data_source_id                  INTEGER DEFAULT 3,
                content_hash                    TEXT,
                last_seen_at                    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_created                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_updated                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (data_source_id)    REFERENCES data_source(data_source_id)
            );
        ''',

        "league_fixture_raw":
        '''
            CREATE TABLE IF NOT EXISTS league_fixture_raw (
                row_id                          INTEGER PRIMARY KEY AUTOINCREMENT,
                league_fixture_id_ext           TEXT,
                league_id_ext                   TEXT,
                startdate                       DATE,
                round                           TEXT,
                home_team_name                  TEXT,
                away_team_name                  TEXT,
                home_score                      INTEGER,
                away_score                      INTEGER,
                status                          TEXT DEFAULT 'completed',
                url                             TEXT,
                data_source_id                  INTEGER DEFAULT 3,
                content_hash                    TEXT,
                last_seen_at                    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_created                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_updated                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (data_source_id)    REFERENCES data_source(data_source_id)
            );
        ''',

        "league_fixture_match_raw":
        '''
            CREATE TABLE IF NOT EXISTS league_fixture_match_raw (
                row_id                          INTEGER PRIMARY KEY AUTOINCREMENT,
                league_fixture_match_id_ext     TEXT,
                league_fixture_id_ext           TEXT,
                home_player_id_ext              TEXT,
                home_player_name                TEXT,
                away_player_id_ext              TEXT,
                away_player_name                TEXT,
                tokens                          TEXT,
                fixture_standing                TEXT,
                data_source_id                  INTEGER DEFAULT 3,
                content_hash                    TEXT,
                last_seen_at                    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_created                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_updated                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (data_source_id)    REFERENCES data_source(data_source_id)
            );
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

        # Unique indexes needed for upserts
        index_statements = [
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_league_raw_ext_source ON league_raw (league_id_ext, data_source_id);",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_league_fixture_raw_ext_source ON league_fixture_raw (league_fixture_id_ext, data_source_id);",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_league_fixture_match_raw_ext_source ON league_fixture_match_raw (league_fixture_id_ext, league_fixture_match_id_ext, data_source_id);",
        ]
        for stmt in index_statements:
            cursor.execute(stmt)

    except Exception as e:
        logger.failed(f"Error creating raw tables: {e}")
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
                tournament_class_id_parent                  INTEGER,
                ko_tree_size                                INTEGER,
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
                FOREIGN KEY (tournament_class_id_parent)    REFERENCES tournament_class(tournament_class_id)   ON DELETE SET NULL,
                FOREIGN KEY (data_source_id)                REFERENCES data_source(data_source_id),
                UNIQUE      (tournament_class_id_ext, data_source_id),
                UNIQUE      (tournament_id, shortname, startdate)
            )
        ''')

        # Tournament entries (singles or doubles)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tournament_class_entry (
                tournament_class_entry_id                   INTEGER     PRIMARY KEY AUTOINCREMENT,
                tournament_class_entry_id_ext               TEXT,
                tournament_class_entry_group_id_int         INTEGER     NOT NULL,
                tournament_class_id                         INTEGER     NOT NULL,
                seed                                        INTEGER,
                final_position                              INTEGER,
                row_created                                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_updated                                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (tournament_class_id)           REFERENCES tournament_class(tournament_class_id)
                       
                UNIQUE (tournament_class_id, tournament_class_entry_id_ext),
                UNIQUE (tournament_class_id, tournament_class_entry_group_id_int)
            );
        ''')

        # Link players to tournament entries (for singles/doubles)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tournament_class_player (
                tournament_class_entry_id                   INTEGER,
                tournament_player_id_ext                    TEXT,
                player_id                                   INTEGER     NOT NULL,
                club_id                                     INTEGER     NOT NULL,
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

            PRIMARY KEY (tournament_class_id, match_id),
                       
            FOREIGN KEY (tournament_class_id)               REFERENCES tournament_class(tournament_class_id)        ON DELETE CASCADE,
            FOREIGN KEY (match_id)                          REFERENCES match(match_id)                              ON DELETE CASCADE,
            FOREIGN KEY (tournament_class_stage_id)         REFERENCES tournament_class_stage(tournament_class_stage_id)
             
            -- Enforce that the group (if present) belongs to the same class
            --FOREIGN KEY (tournament_class_id, tournament_class_group_id)
            --  REFERENCES tournament_class_group (tournament_class_id, tournament_class_group_id),

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
                match_id                                    INTEGER,
                side_no                                     INTEGER,
                represented_entry_id                        INTEGER,
                represented_league_team_id                  INTEGER,
                row_created                                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_updated                                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                PRIMARY KEY (match_id, side_no),            

                FOREIGN KEY (match_id)                      REFERENCES match(match_id)                  ON DELETE CASCADE,
                FOREIGN KEY (represented_league_team_id)    REFERENCES league_team(league_team_id),
                FOREIGN KEY (represented_entry_id)          REFERENCES tournament_class_entry(tournament_class_entry_id),           

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

            PRIMARY KEY (match_id, side_no, player_id),
                       
            FOREIGN KEY (match_id, side_no) REFERENCES match_side(match_id, side_no)        ON DELETE CASCADE,
            FOREIGN KEY (player_id)         REFERENCES player(player_id),
            FOREIGN KEY (club_id)           REFERENCES club(club_id),

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

            PRIMARY KEY (match_id, game_no),
            
            FOREIGN KEY (match_id)          REFERENCES match(match_id)                      ON DELETE CASCADE

            
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

        # # PLACEHOLDER for tournament class group standing table (not used yet)
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





        ##############################################
        ###  PLAYER TABLES 
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
        ### PLAYER LICENSES
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
                row_updated                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                       
                FOREIGN KEY (player_id)         REFERENCES player(player_id),
                FOREIGN KEY (club_id)           REFERENCES club(club_id),
                FOREIGN KEY (season_id)         REFERENCES season(season_id),
                FOREIGN KEY (license_id)        REFERENCES license(license_id),
                       
                UNIQUE (player_id, license_id, season_id, club_id)
            )
        ''')

        ###########################################
        ### PLAYER TRANSITIONS
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
        ### PLAYER RANKING AND RANKING GROUPS 
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
                run_id_ext                      TEXT NOT NULL,
                run_date                        DATE NOT NULL,
                player_id_ext                   TEXT NOT NULL,
                points                          INTEGER DEFAULT 0,
                points_change_since_last        INTEGER DEFAULT 0,
                position_world                  INTEGER DEFAULT 0,
                position                        INTEGER DEFAULT 0,
                data_source_id                  INTEGER DEFAULT 3,
                row_created                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_updated                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (player_id_ext, data_source_id) REFERENCES player_id_ext(player_id_ext, data_source_id),

                PRIMARY KEY (player_id_ext, data_source_id, run_date)
            )
        ''')

        ##########################################
        ### LEAGUES
        ##########################################

        # League table (cannonical league data - season, level, name, organiser, url)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS league (
                league_id                       INTEGER PRIMARY KEY AUTOINCREMENT,
                league_id_ext                   TEXT,
                season_id                       INTEGER,
                league_level_id                 INTEGER,
                name                            TEXT,
                organiser                       TEXT,
                active                          INTEGER DEFAULT 0 CHECK(active IN (0,1)),
                url                             TEXT,
                start_date                      DATE,
                end_date                        DATE,
                row_created                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_updated                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                       
                FOREIGN KEY (season_id)         REFERENCES season(season_id),
                FOREIGN KEY (league_level_id)   REFERENCES league_level(league_level_id)
            );
        ''')

        # League teams (clubs participating in a league)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS league_team (
                league_team_id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                league_team_id_ext              TEXT,
                league_id                       INTEGER,
                club_id                         INTEGER,
                name                            TEXT,
                row_created                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_updated                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (league_id)         REFERENCES league(league_id),
                FOREIGN KEY (club_id)           REFERENCES club(club_id)
            );
        ''')

        # League team players (players registered to a team in a league)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS league_team_player (
                league_team_id                  INTEGER,
                player_id                       INTEGER,
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
                fixture_id_ext                  TEXT,
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

        ##########################################
        ### CLUBS 
        ##########################################

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
    
    except sqlite3.Error as e:
        print(f"Error creating tables: {e}")


def create_and_populate_static_tables(cursor, logger):

    logger.info("Creating static tables if needed...")

    try: 

    
        ############################################
        ### LOOKUP TABLES
        ############################################

        # Data sources
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

        # Ranking groups
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

        # Seasons
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

        # Licenses
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

        # Districts
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

        # Tournament status
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

        # Tournament levels
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

        # Tournament types
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

        # Tournament class stages (e.g. group, R32, R16, QF, SF, F)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tournament_class_stage (
                tournament_class_stage_id       INTEGER         PRIMARY KEY,
                shortname                       TEXT            NOT NULL,    -- e.g. 'GROUP','R32','R16','QF','SF','F','SWISS'
                description                     TEXT            NOT NULL,
                is_knockout                     INTEGER         NOT NULL CHECK(is_knockout IN (0,1)),
                round_order                     INTEGER,        -- e.g. 32,16,8,... (NULL for GROUP/SWISS)
                UNIQUE (shortname)
            ) WITHOUT ROWID;
        ''') 

        stages = [
            (1, 'GROUP',    'Group',                    0,  None),
            (2, 'R128',     'Round of 128',             1,  128),
            (3, 'R64',      'Round of 64',              1,  64),
            (4, 'R32',      'Round of 32',              1,  32),
            (5, 'R16',      'Round of 16',              1,  16),
            (6, 'QF',       'Quarterfinal',             1,  8),
            (7, 'SF',       'Semifinal',                1,  4),
            (8, 'F',        'Final',                    1,  2),
            (9,  'SWISS',   'Swiss System',             0,  None),      # A Swiss-system stage pairs players with similar scores across several rounds; nobody is eliminated each round. It’s neither simple group round-robin nor KO.
            (10, 'KO_QAL',  'Knockout Qualification',   1,  None),       # Qualification matches to enter a knockout stage (e.g. R32) after a group stage
            (11, 'GROUP_STG2', 'Group Stage 2',         0,  None)      # Second group stage (after an initial group stage), e.g. in some international competitions
        ]

        cursor.executemany('''
            INSERT OR IGNORE INTO tournament_class_stage (tournament_class_stage_id, shortname, description, is_knockout, round_order)
            VALUES (?, ?, ?, ?, ?)
        ''', stages)

        # Tournament class types (e.g. singles, doubles, mixed, team)
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

        # Tournament class structures (e.g. groups + KO, groups only, KO only)
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
            (4, 'Groups_and_Groups'),
            (9, 'Unknown')
        ]

        cursor.executemany('''
            INSERT OR IGNORE INTO tournament_class_structure (tournament_class_structure_id, description)
            VALUES (?, ?)
        ''', tournament_class_structure)

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



        ############ DEBUG TABLES ######################

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS log_details (
                id                      INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id                  TEXT NOT NULL,
                run_date                DATETIME DEFAULT CURRENT_TIMESTAMP,
                object_type             TEXT NOT NULL,      -- Same as parent run
                process_type            TEXT NOT NULL,      -- Same as parent run
                function_name           TEXT NOT NULL,
                filename                TEXT NOT NULL,
                context_json            TEXT,
                status                  TEXT NOT NULL,      -- 'error', 'warning', 'skipped', 'success'
                message                 TEXT NOT NULL,
                msg_id                  TEXT
            );
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS log_runs (
                id                      INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id                  TEXT NOT NULL,
                run_date                DATETIME DEFAULT CURRENT_TIMESTAMP,
                object_type             TEXT NOT NULL,          -- e.g., 'tournament_raw', 'player_resolver'
                process_type            TEXT NOT NULL,          -- e.g., 'scrape', 'resolve', 'update'
                records_processed       INTEGER DEFAULT 0,
                records_success         INTEGER DEFAULT 0,
                records_failed          INTEGER DEFAULT 0,
                records_skipped         INTEGER DEFAULT 0,
                records_warnings        INTEGER DEFAULT 0,
                runtime_seconds         REAL,
                remarks                 TEXT
            );
        ''')

        # Create table for documenting club names with prefixes
        # Not implemented anywhere right now I think
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
        # -------------------------------
        # Tournament
        # -------------------------------
        # Lookups by external ID (joins, upserts)
        "CREATE INDEX IF NOT EXISTS idx_tournament_id_ext ON tournament(tournament_id_ext)",
        # Fuzzy searches / filters by shortname
        "CREATE INDEX IF NOT EXISTS idx_tournament_shortname ON tournament(shortname)",
        # Sorting / filtering by tournament date
        "CREATE INDEX IF NOT EXISTS idx_tournament_startdate ON tournament(startdate)",  

        # -------------------------------
        # Tournament Class
        # -------------------------------
        # Joins tournament_class → tournament
        "CREATE INDEX IF NOT EXISTS idx_tournament_class_tournament_id ON tournament_class(tournament_id)",
        # Filtering by class type
        "CREATE INDEX IF NOT EXISTS idx_tournament_class_type_id ON tournament_class(tournament_class_type_id)",
        # Filtering by structure
        "CREATE INDEX IF NOT EXISTS idx_tournament_class_structure_id ON tournament_class(tournament_class_structure_id)",
        # Lookups by external ID
        "CREATE INDEX IF NOT EXISTS idx_tournament_class_id_ext ON tournament_class(tournament_class_id_ext)",
        # Sorting / filtering by class date
        "CREATE INDEX IF NOT EXISTS idx_tournament_class_startdate ON tournament_class(startdate)",

        # -------------------------------
        # Tournament Class Player / Entry
        # -------------------------------
        # Player history lookup (find all classes a player entered)
        "CREATE INDEX IF NOT EXISTS idx_tcp_player ON tournament_class_player(player_id)",
        # Joins entry → class
        "CREATE INDEX IF NOT EXISTS idx_tce_class ON tournament_class_entry(tournament_class_id)",

        # -------------------------------
        # Tournament Class Group
        # -------------------------------
        # Enforce uniqueness per class/group
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_tcg_class_group ON tournament_class_group (tournament_class_id, tournament_class_group_id)",

        # -------------------------------
        # Player ID Ext
        # -------------------------------
        # Joins player → player_id_ext
        "CREATE INDEX IF NOT EXISTS idx_player_id_ext_player_id ON player_id_ext(player_id)",

        # -------------------------------
        # Player License
        # -------------------------------
        # Lookup latest license by player & season
        "CREATE INDEX IF NOT EXISTS idx_player_license_player_season ON player_license(player_id, season_id)",
        # Joins license → club
        "CREATE INDEX IF NOT EXISTS idx_player_license_club ON player_license(club_id)",

        # -------------------------------
        # Player Transition
        # -------------------------------
        # Lookup transitions by player & season (latest club move)
        "CREATE INDEX IF NOT EXISTS idx_player_transition_player_season ON player_transition(player_id, season_id)",

        # -------------------------------
        # Player Ranking Group
        # -------------------------------
        # Lookup ranking groups for player
        "CREATE INDEX IF NOT EXISTS idx_prg_player ON player_ranking_group(player_id)",

        # -------------------------------
        # Player Ranking (3.5M rows, critical)
        # -------------------------------
        # Fast lookup of most recent ranking row per player_id_ext
        "CREATE INDEX IF NOT EXISTS idx_player_ranking_player_date ON player_ranking(player_id_ext, run_date DESC)",
        # Efficient queries when pulling entire ranking snapshot by date
        "CREATE INDEX IF NOT EXISTS idx_player_ranking_date ON player_ranking(run_date)",
    ]

    try:
        for stmt in indexes:
            cursor.execute(stmt)

    except sqlite3.Error as e:
        print(f"Error creating indexes: {e}")
        logging.error(f"Error creating indexes: {e}")


def create_triggers(cursor):
    print("ℹ️  Creating database triggers...")

    # Always enforce FK constraints
    cursor.execute("PRAGMA foreign_keys = ON;")

    triggers = [

    ]

    try:
        for name, create_sql in triggers:
            cursor.execute(f"DROP TRIGGER IF EXISTS {name};")
            cursor.execute(create_sql)

    except sqlite3.Error as e:
        print(f"Error creating triggers: {e}")

def create_views(cursor):
    print("ℹ️  Creating database views...")

    views = [
        (
            "v_tnmt_class",
            """
            CREATE VIEW IF NOT EXISTS v_tnmt_class AS
            SELECT
                tc.tournament_class_id,
                tc.tournament_class_id_ext,
                t.tournament_id,
                t.tournament_id_ext,
                t.shortname                 AS tournament_shortname,
                tc.longname                 AS class_longname,
                tc.shortname                AS class_shortname,
                tct.description             AS tournament_class_type,
                tcs.description             AS tournament_class_structure,
                ts.description              AS tournament_status,
                tc.ko_tree_size,
                tc.startdate                AS class_date,
                t.country_code,
                t.url                       AS tournament_url,
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
            "v_foreign_keys",
            '''
            CREATE VIEW IF NOT EXISTS v_foreign_keys AS
                SELECT m.name AS table_name, p.*
            FROM sqlite_master AS m
            JOIN pragma_foreign_key_list(m.name) AS p
            WHERE m.type = 'table';
            '''
        ),
        (
            "v_clubs",
            '''
            CREATE VIEW IF NOT EXISTS v_clubs AS
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
        ),
        (
        "v_tournament_class_entries",
        '''
        CREATE VIEW IF NOT EXISTS v_tournament_class_entries AS
            SELECT
                -- Tournament context
                t.shortname          AS tournament_shortname,

                -- Class context
                tc.shortname         AS class_shortname,
                tc.longname          AS class_longname,
                tc.startdate         AS class_date,

                -- Player context
                p.firstname,
                p.lastname,
                p.fullname_raw       AS player_fullname_raw,
                p.is_verified,

                -- Club context
                c.shortname          AS club_shortname,

                -- Entry details
                tce.seed,
                tce.final_position,
                tcp.tournament_player_id_ext,

                -- IDs last
                t.tournament_id,
                t.tournament_id_ext,
                tc.tournament_class_id,
                tc.tournament_class_id_ext,
                tce.tournament_class_entry_id,
                tce.tournament_class_entry_group_id_int     AS entry_group_id,
                p.player_id,
                c.club_id

            FROM tournament_class_entry tce
            JOIN tournament_class_player tcp
                ON tcp.tournament_class_entry_id = tce.tournament_class_entry_id
            JOIN tournament_class tc
                ON tc.tournament_class_id = tce.tournament_class_id
            JOIN tournament t
                ON t.tournament_id = tc.tournament_id
            JOIN player p
                ON p.player_id = tcp.player_id
            JOIN club c
                ON c.club_id = tcp.club_id
            ORDER BY t.startdate, tc.startdate, tce.seed;
        '''
    ),
    (
        "v_player_profile",
        '''
        CREATE VIEW IF NOT EXISTS v_player_profile AS
        WITH recent_license AS (
            SELECT pl.player_id,
                c.club_id,
                c.shortname || ' (' || s.label || ')' AS club_with_season,
                pl.season_id,
                ROW_NUMBER() OVER (PARTITION BY pl.player_id ORDER BY s.start_date DESC) AS rn
            FROM player_license pl
            JOIN season s ON pl.season_id = s.season_id
            JOIN club c ON pl.club_id = c.club_id
        ),
        recent_tournament AS (
            SELECT tcp.player_id,
                t.tournament_id,
                t.shortname AS tournament_name,
                tc.shortname AS class_shortname,
                tc.startdate AS class_startdate,
                ROW_NUMBER() OVER (PARTITION BY tcp.player_id ORDER BY tc.startdate DESC) AS rn
            FROM tournament_class_player tcp
            JOIN tournament_class_entry tce
                ON tcp.tournament_class_entry_id = tce.tournament_class_entry_id
            JOIN tournament_class tc
                ON tce.tournament_class_id = tc.tournament_class_id
            JOIN tournament t
                ON tc.tournament_id = t.tournament_id
        ),
        recent_transition AS (
            SELECT pt.player_id,
                cf.shortname || ' → ' || ct.shortname || ' (' || s.label || ')' AS transition_text,
                ROW_NUMBER() OVER (PARTITION BY pt.player_id ORDER BY s.start_date DESC) AS rn
            FROM player_transition pt
            JOIN club cf ON pt.club_id_from = cf.club_id
            JOIN club ct ON pt.club_id_to = ct.club_id
            JOIN season s ON pt.season_id = s.season_id
        ),
        id_exts AS (
            SELECT pie.player_id,
                GROUP_CONCAT(pie.player_id_ext) AS id_ext_list,
                COUNT(*) AS id_ext_count
            FROM player_id_ext pie
            GROUP BY pie.player_id
        ),
        ranking_groups AS (
            SELECT prg.player_id,
                GROUP_CONCAT(rg.class_short, ', ') AS ranking_groups
            FROM player_ranking_group prg
            JOIN ranking_group rg ON prg.ranking_group_id = rg.ranking_group_id
            GROUP BY prg.player_id
        ),
        recent_ranking_points AS (
            SELECT pie.player_id,
                pr.points,
                pr.run_date
            FROM player_id_ext pie
            JOIN (
                SELECT player_id_ext, MAX(run_date) AS max_run_date
                FROM player_ranking
                GROUP BY player_id_ext
            ) latest
            ON pie.player_id_ext = latest.player_id_ext
            JOIN player_ranking pr
            ON pr.player_id_ext = latest.player_id_ext
            AND pr.run_date = latest.max_run_date
        ),
        ranking_points_per_player AS (
            SELECT rrp.player_id,
                rrp.points,
                rrp.run_date
            FROM recent_ranking_points rrp
            JOIN (
                SELECT player_id, MAX(run_date) AS max_date
                FROM recent_ranking_points
                GROUP BY player_id
            ) maxed
            ON rrp.player_id = maxed.player_id
            AND rrp.run_date = maxed.max_date
        )
        SELECT
            -- Player context
            p.player_id,
            CASE 
                WHEN p.is_verified = 1 THEN p.firstname || ' ' || p.lastname
                ELSE p.fullname_raw
            END AS player_name,
            p.year_born,
            p.is_verified,
            COALESCE(id_exts.id_ext_list, '') AS id_exts,
            COALESCE(id_exts.id_ext_count, 0) AS id_ext_count,

            -- Club context
            rl.club_with_season AS recent_club,

            -- Tournament context
            rt.tournament_name || ' - ' || rt.class_shortname || ' (' || rt.class_startdate || ')' AS recent_tournament_class,

            -- Ranking groups (merged list)
            COALESCE(rg.ranking_groups, '') AS ranking_groups,

            -- Ranking points (latest across all player_id_ext)
            CASE 
                WHEN rpp.points IS NOT NULL 
                THEN rpp.points || ' (' || rpp.run_date || ')'
                ELSE ''
            END AS ranking_points,

            -- Transition
            tr.transition_text AS recent_transition

        FROM player p
        LEFT JOIN id_exts
            ON p.player_id = id_exts.player_id
        LEFT JOIN recent_license rl
            ON p.player_id = rl.player_id AND rl.rn = 1
        LEFT JOIN recent_tournament rt
            ON p.player_id = rt.player_id AND rt.rn = 1
        LEFT JOIN ranking_groups rg
            ON p.player_id = rg.player_id
        LEFT JOIN recent_transition tr
            ON p.player_id = tr.player_id AND tr.rn = 1
        LEFT JOIN ranking_points_per_player rpp
            ON p.player_id = rpp.player_id;
        '''
    ),

    (
        "v_trnmt_entry",
        '''
        CREATE VIEW IF NOT EXISTS v_trnmt_entry AS
            SELECT
                -- Tournament context
                t.shortname          AS tournament_shortname,
                --t.startdate          AS tournament_startdate,

                -- Class context
                tc.shortname         AS class_shortname,
                tc.longname          AS class_longname,
                tc.startdate         AS class_date,

                -- Player (single resolved name)
                CASE 
                    WHEN p.is_verified = 1 THEN TRIM(p.firstname || ' ' || p.lastname)
                    ELSE TRIM(
                        CASE 
                            WHEN INSTR(p.fullname_raw, ' ') > 0 
                            THEN SUBSTR(p.fullname_raw, INSTR(p.fullname_raw, ' ') + 1) 
                                || ' ' || SUBSTR(p.fullname_raw, 1, INSTR(p.fullname_raw, ' ') - 1)
                            ELSE p.fullname_raw
                        END
                    )
                END AS player_name,
                
                p.player_id,
                
                

                -- Club context
                c.shortname          AS club_shortname,
                
                c.club_id,

                -- Entry details
                tce.seed                         AS class_seed,
                tce.final_position               AS final_position,
                tcp.tournament_player_id_ext,
                tcg.description                  AS group_name,
                tcgm.seed_in_group               AS group_seed,

                -- IDs last
                t.tournament_id,
                t.tournament_id_ext,
                tc.tournament_class_id,
                tc.tournament_class_id_ext,
                tce.tournament_class_entry_id,
                tce.tournament_class_entry_group_id_int AS entry_group_id,
                tcg.tournament_class_group_id

        

            FROM tournament_class_entry tce
            JOIN tournament_class_player tcp
                ON tcp.tournament_class_entry_id = tce.tournament_class_entry_id
            JOIN tournament_class tc
                ON tc.tournament_class_id = tce.tournament_class_id
            JOIN tournament t
                ON t.tournament_id = tc.tournament_id
            JOIN player p
                ON p.player_id = tcp.player_id
            JOIN club c
                ON c.club_id = tcp.club_id
            LEFT JOIN tournament_class_group_member tcgm
                ON tcgm.tournament_class_entry_id = tce.tournament_class_entry_id
            LEFT JOIN tournament_class_group tcg
                ON tcg.tournament_class_group_id = tcgm.tournament_class_group_id

            ORDER BY t.startdate, tc.startdate, tcg.sort_order, tce.seed, tcgm.seed_in_group;
        '''
),

(
        "v_trnmt_matches",
        '''
            CREATE VIEW IF NOT EXISTS v_trnmt_matches AS
            WITH
                game_summary AS (
                    SELECT
                        g.match_id,
                        GROUP_CONCAT(g.points_side1 || '-' || g.points_side2, ', ') AS games_score,
                        SUM(CASE WHEN g.points_side1 > g.points_side2 THEN 1 ELSE 0 END) AS games_won_side1,
                        SUM(CASE WHEN g.points_side2 > g.points_side1 THEN 1 ELSE 0 END) AS games_won_side2
                    FROM game g
                    GROUP BY g.match_id
                ),
                side1 AS (
                    SELECT 
                        ms.match_id,
                        GROUP_CONCAT(
                            CASE 
                                WHEN p.is_verified = 1 THEN TRIM(p.firstname || ' ' || p.lastname)
                                ELSE TRIM(
                                    CASE 
                                        WHEN INSTR(p.fullname_raw, ' ') > 0 
                                        THEN SUBSTR(p.fullname_raw, INSTR(p.fullname_raw, ' ') + 1) 
                                            || ' ' || SUBSTR(p.fullname_raw, 1, INSTR(p.fullname_raw, ' ') - 1)
                                        ELSE p.fullname_raw
                                    END
                                )
                            END, ', '
                        ) AS side1_player_name,
                        GROUP_CONCAT(p.player_id, ', ')              AS side1_player_id,
                        GROUP_CONCAT(c.shortname, ', ')              AS side1_club_name,
                        GROUP_CONCAT(c.club_id, ', ')                AS side1_club_id,
                        GROUP_CONCAT(p.is_verified, ', ')            AS side1_player_is_verified   -- 🔹 added
                    FROM match_side ms
                    JOIN match_player mp 
                        ON ms.match_id = mp.match_id AND ms.side_no = mp.side_no
                    JOIN player p 
                        ON p.player_id = mp.player_id
                    JOIN club c 
                        ON c.club_id = mp.club_id
                    WHERE ms.side_no = 1
                    GROUP BY ms.match_id
                ),
                side2 AS (
                    SELECT 
                        ms.match_id,
                        GROUP_CONCAT(
                            CASE 
                                WHEN p.is_verified = 1 THEN TRIM(p.firstname || ' ' || p.lastname)
                                ELSE TRIM(
                                    CASE 
                                        WHEN INSTR(p.fullname_raw, ' ') > 0 
                                        THEN SUBSTR(p.fullname_raw, INSTR(p.fullname_raw, ' ') + 1) 
                                            || ' ' || SUBSTR(p.fullname_raw, 1, INSTR(p.fullname_raw, ' ') - 1)
                                        ELSE p.fullname_raw
                                    END
                                )
                            END, ', '
                        ) AS side2_player_name,
                        GROUP_CONCAT(p.player_id, ', ')              AS side2_player_id,
                        GROUP_CONCAT(c.shortname, ', ')              AS side2_club_name,
                        GROUP_CONCAT(c.club_id, ', ')                AS side2_club_id,
                        GROUP_CONCAT(p.is_verified, ', ')            AS side2_player_is_verified   -- 🔹 added
                    FROM match_side ms
                    JOIN match_player mp 
                        ON ms.match_id = mp.match_id AND ms.side_no = mp.side_no
                    JOIN player p 
                        ON p.player_id = mp.player_id
                    JOIN club c 
                        ON c.club_id = mp.club_id
                    WHERE ms.side_no = 2
                    GROUP BY ms.match_id
                )
            SELECT
                -- Tournament context
                t.shortname             AS tournament_shortname,
                t.startdate             AS tournament_startdate,
                t.city                  AS tournament_city,
                t.url                   AS tournament_url,

                -- Class context
                tc.shortname            AS class_shortname,
                tc.longname             AS class_longname,
                tc.startdate            AS class_date,

                -- Stage / group context
                tcs.shortname           AS stage_shortname,
                tcs.description         AS stage_description,
                tcm.stage_round_no      AS stage_round_no,
                tcm.draw_pos            AS draw_position,
                tcg.description         AS group_name,

                -- Match info
                m.match_id,
                m.best_of,
                m.date,
                m.status,
                m.winner_side,
                m.walkover_side,

                -- Player & club details
                s1.side1_player_name,
                s1.side1_player_id,
                s1.side1_club_name,
                s1.side1_club_id,
                s1.side1_player_is_verified,   -- 🔹 new field

                s2.side2_player_name,
                s2.side2_player_id,
                s2.side2_club_name,
                s2.side2_club_id,
                s2.side2_player_is_verified,   -- 🔹 new field

                -- Results
                gs.games_score,
                gs.games_won_side1,
                gs.games_won_side2,
                CASE 
                    WHEN gs.games_won_side1 IS NOT NULL AND gs.games_won_side2 IS NOT NULL
                    THEN gs.games_won_side1 || '-' || gs.games_won_side2
                    ELSE NULL
                END AS match_score_summary,

                CASE 
                    WHEN m.winner_side = 1 THEN s1.side1_player_name
                    WHEN m.winner_side = 2 THEN s2.side2_player_name
                    ELSE NULL
                END AS winner_name,

                -- IDs last
                t.tournament_id,
                t.tournament_id_ext,
                tc.tournament_class_id,
                tc.tournament_class_id_ext,
                tcs.tournament_class_stage_id,
                tcg.tournament_class_group_id,
                m.match_id

            FROM tournament_class_match tcm
            JOIN tournament_class tc
                ON tc.tournament_class_id = tcm.tournament_class_id
            JOIN tournament t
                ON t.tournament_id = tc.tournament_id
            JOIN match m
                ON m.match_id = tcm.match_id
            LEFT JOIN tournament_class_stage tcs
                ON tcs.tournament_class_stage_id = tcm.tournament_class_stage_id
            LEFT JOIN tournament_class_group tcg
                ON tcg.tournament_class_group_id = tcm.tournament_class_group_id
            LEFT JOIN game_summary gs
                ON gs.match_id = m.match_id
            LEFT JOIN side1 s1
                ON s1.match_id = m.match_id
            LEFT JOIN side2 s2
                ON s2.match_id = m.match_id

            ORDER BY t.startdate, tc.startdate, tcs.round_order, tcm.stage_round_no, m.match_id;
        '''
  )

    ]

    try:
        for name, create_sql in views:
            cursor.execute(f"DROP VIEW IF EXISTS {name};")
            cursor.execute(create_sql)
            
    except sqlite3.Error as e:
        print(f"Error creating views: {e}")


def execute_custom_sql(cursor):
    print("ℹ️  Executing custom SQL...")
    cursor.execute('''
        --- Example custom SQL execution
    ''')


