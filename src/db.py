
# db.py: 

import sqlite3
from config import DB_NAME
import logging
from datetime import timedelta
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

def save_to_db_transitions(cursor, transitions):
    db_results = []

    for t in transitions:
        try:
            transition_date = t["transition_date"]
            season_transition = get_from_db_season(cursor, date_object=transition_date)
            season_previous = get_from_db_season(cursor, date_object=transition_date - timedelta(days=365)) # Assuming previous season is one year before
            club_from_name = t.get("club_from")
            club_from = get_from_db_club(cursor, club_name=club_from_name)
            club_to_name = t.get("club_to")
            club_to = get_from_db_club(cursor, club_name=club_to_name)
            firstname = t.get("firstname")
            lastname = t.get("lastname")
            year_born = t.get("year_born")

            # Check if club_from and club_to are valid
            if not club_from or not club_to:
                db_results.append({
                    "status": "failed",
                    "player": f"{firstname} {lastname} ({year_born}) moving from club {club_from_name} to {club_to_name} on transition date {transition_date}",
                    "reason": "Could not resolve club(s) from transition data"
                })
                continue

            # Check if transition date is valid
            if not season_transition or not season_previous:
                db_results.append({
                    "status": "failed",
                    "player": f"{firstname} {lastname} ({year_born}) moving from club {club_from_name} to {club_to_name} on transition date {transition_date}",
                    "reason": "Could not resolve season(s) from transition date"
                })
                continue
            
            # If transition date is equal to or before the season start date, skip
            if transition_date <= season_transition.get("season_start_date"):    
                db_results.append({
                    "status": "skipped",
                    "player": f"{firstname} {lastname} ({year_born}) moving from club {club_from_name} to {club_to_name} on transition date {transition_date}",
                    "reason": "Transition date is equal to or before the season start date"
                })
                continue           

            # Search for player(s) with matching name and year born
            players = search_from_db_players(cursor, firstname, lastname, year_born) # returning tuples
            player_ids = [row[0] for row in players]  # Extract player_id from tuples

            # Check if any players were found
            if not players:
                db_results.append({
                    "status": "failed",
                    "player": f"{firstname} {lastname} ({year_born}) moving from club {club_from_name} to {club_to_name} on transition date {transition_date}",
                    "reason": "No players found with matching name and year born"
                })
                continue
            
            # Find licenses for these players at the old club for the previous season
            license_records = search_from_db_player_licenses(
                cursor,
                player_ids,
                club_id=club_from.get("club_id"),
                season_id=season_previous.get("season_id")            
                )
            
            # If no licenses found from previous season, skip
            if len(license_records) == 0:
                db_results.append({
                    "status": "skipped",
                    "player": f"{firstname} {lastname}, born {year_born} from club {club_from_name} to {club_to_name} with transition date {transition_date} in season {season_transition.get('season_description', 'unknown')}",
                    "reason": "No license found for matching player(s) for relevant season at old club. Might indicate break from competition previous season"
                })
                continue

            # For each case, 1 or multiple previous licenses and/or unknow future license type, each scenario must be handled
            # Make sure 2 licenses are created if player had 2 licenses previous season of the same types
            # If future player license or multiple licenses are know (transition date in the past) - copy that setup for current season gap
            
            # Single license found
            if len(license_records) == 1:
                player_license = license_records[0]
                new_license = {
                    "player_id": player_license.get("player_id"),
                    "club_id": player_license.get("club_id"),
                    "season_id": season_transition.get("season_id"),
                    "license_id": player_license.get("license_id"),
                    "valid_from": season_transition.get("season_start_date"),
                    "valid_to": transition_date - timedelta(days=1)  # Valid until the day before transition,
                }

                # Print log debug info for new_license if single license
                logging.debug(f"New license will be created for player {firstname} {lastname} "
                         f"with player_id {new_license.get('player_id')} at club {club_to_name} "
                         f"({club_to.get('club_id')}) for season {season_transition.get('season_id')} "
                         f"with license details: type={new_license.get('type')}, "
                         f"valid_from={new_license.get('valid_from')}, valid_to={new_license.get('valid_to')}, "
                         f"license_id={new_license.get('license_id')}")
            
            # Multiple licenses found    
            if len(license_records) > 1:
                logging.info(f"Multiple licenses found for player {firstname} {lastname} with {len(license_records)} records")
                for(i, record) in enumerate(license_records):
                    new_license = {
                        "player_id": record.get("player_id"),
                        "club_id": club_to.get("club_id"),
                        "season_id": season_transition.get("season_id"),
                        "license_id": record.get("license_id"),
                        "valid_from": season_transition.get("season_start_date"),
                        "valid_to": transition_date - timedelta(days=1)  # Valid until the day before transition
                    }

                    # Print log debug info for each new license if multiple licenses
                    logging.info(f"New license will be created for player {firstname} {lastname} (player_id: {new_license.get('player_id')}) "
                             f" to club {club_to_name} "
                             f"({club_to.get('club_id')}) for season {season_transition.get('season_id')} "
                             f"valid_from={new_license.get('valid_from')}, valid_to={new_license.get('valid_to')}, "
                             f"license_id={new_license.get('license_id')}")

            #     # insert_license(cursor, new_license)
            #     logging.info(f"✔️ Inserted license for player {firstname} {lastname} with type '{record.get('type')}'")
            #     db_results.append({
            #         "status": "success",
            #         "player": f"{firstname} {lastname}, born {year_born} from club {club_from_name} to {club_to_name} on {transition_date}",
            #         "reason": f"Single license inserted with type {record.get('type')}"
            #     })
            #     continue

            # ✅ Multiple licenses — check validity
            # unique_players = set((rec.get("player_id"), rec.get("lastname"), rec.get("firstname"), rec.get("year_born")) for rec in license_records)
            # unique_clubs = set(rec.get("club_id") for rec in license_records)
            # unique_seasons = set(rec.get("season_id") for rec in license_records)

            # if len(unique_players) == 1 and len(unique_clubs) == 1 and len(unique_seasons) == 1:
            #     player_id = list(unique_players)[0][0]

            #     for record in license_records:
            #         new_license = {
            #             "player_id": player_id,
            #             "club_id": club_to.get("club_id"),
            #             "season_id": season_transition.get("season_id"),
            #             "type": record.get("type"),
            #             "license_id": record.get("license_id"),
            #             "valid_from": transition_date,
            #             "valid_to": season_transition.get("season_end_date"),
            #         }

            #         # insert_license(cursor, new_license)
            #         logging.info(
            #             f"✔️ Inserted license for player {firstname} {lastname} with type '{record.get('type')}'"
            #         )

            #     db_results.append({
            #         "status": "success",
            #         "player": f"{firstname} {lastname}, born {year_born} from club {club_from_name} to {club_to_name} on {transition_date}",
            #         "reason": f"Multiple licenses inserted with types: {', '.join([r.get('type') for r in license_records])}"
            #     })
            #     continue

            # # ❌ Truly ambiguous
            # db_results.append({
            #     "status": "skipped",
            #     "player": f"{firstname} {lastname}, born {year_born} from club {club_from_name} to {club_to_name} on {transition_date}",
            #     "reason": "Ambiguous: multiple license records found for same club and season but different players or clubs"
            # })


        
            # logging.info(
            #     f"New license will be created for player {t.get('firstname', '')} {t.get('lastname', '')} "
            #     f"with player_id {license_record.get('player_id')} at club {club_from} ({club_from.get('club_id')})"
            #     f"for season {season_previous.get('season_id')} with license details: "
            #     f"player_license_id={license_record.get('player_license_id')}, "
            #     f"valid_from={license_record.get('valid_from')}, valid_to={license_record.get('valid_to')}, "
            #     f"license_id={license_record.get('license_id')}"
            # )

            # # Step 2: Insert new license at old club for current season
            # cursor.execute("""
            #     INSERT OR IGNORE INTO player_license (
            #         player_id, 
            #         player_id_ext, 
            #         club_id, 
            #         club_id_ext, 
            #         valid_from, 
            #         valid_to,
            #         license_id,
            #         season_id
            #     )   
            #     VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            # """, (
            #     player_id, 
            #     player_id_ext, 
            #     club_id_from,
            #     club_id_from_ext,
            #     valid_from, 
            #     valid_to,
            #     license_id,
            #     current_season_id
            # ))

            # # Step 3: Insert player transition
            # cursor.execute("""
            #     INSERT INTO player_transition (
            #         player_id,
            #         player_id_ext,
            #         club_id_from,
            #         club_id_from_ext,
            #         club_id_to,
            #         club_id_to_ext,
            #         transition_date
            #     ) VALUES (?, ?, ?, ?, ?, ?, ?)
            # """, (
            #     player_id,
            #     player_id_ext,
            #     club_id_from,
            #     club_id_from_ext,
            #     club_id_to,
            #     club_id_to_ext,
            #     transition_date
            # ))

            db_results.append({
                "status": "success",
                "player": f"{t.get('firstname', '')} {t.get('lastname', '')}",
                "reason": "Inserted transition and license"
            })

        except Exception as e:
            logging.error(f"Error saving transition for {t.get('firstname')} {t.get('lastname')}: {e}")
            db_results.append({
                "status": "failed",
                "player": f"{t.get('firstname', '')} {t.get('lastname', '')}",
                "reason": f"Insertion error: {e}"
            })

    return db_results

def is_duplicate_tournament(cursor, ondata_id):
    cursor.execute("SELECT tournament_id FROM tournament WHERE ondata_id = ?", (ondata_id,))
    return cursor.fetchone() is not None

def drop_tables(cursor, tables):
    dropped = []
    for table_name in tables:
        try:
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
                (table_name,)   # single element tuple explains why we use a comma here
            )
            if cursor.fetchone():
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                logging.debug(f"Dropped table: {table_name}")
                # print(f"🗑️  Dropped table: {table_name}")
                dropped.append(table_name)
            else:
                logging.warning(f"Table not found, skipping drop: {table_name}")
        except sqlite3.Error as e:
            logging.error(f"Error dropping table {table_name}: {e}")
            print(f"❌ Error dropping table {table_name}: {e}")
    if not dropped:
        logging.info("No tables were dropped.")
        print("ℹ️  No tables were dropped.")
    else:
        logging.info(f"Dropped {len(dropped)} tables: {', '.join(dropped)}")
        print(f"🗑️  Dropped {len(dropped)} tables: {', '.join(dropped)}")

def create_tables(cursor):

    print("ℹ️  Creating tables if needed...")
    logging.info("Creating tables if needed...")
    logging.info("-------------------------------------------------------------------")

    # Create tournaments table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tournament (
            tournament_id INTEGER PRIMARY KEY AUTOINCREMENT,
            tournament_id_ext TEXT,
            longname TEXT,
            shortname TEXT,
            startdate DATE,
            enddate DATE,
            city TEXT,
            arena TEXT,
            country_code TEXT,
            url TEXT,
            status TEXT,
            data_source TEXT DEFAULT 'ondata',
            row_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (tournament_id_ext),
            UNIQUE (shortname, startdate)
        )
    ''')

    # Create tournament class table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tournament_class (
            tournament_class_id INTEGER PRIMARY KEY AUTOINCREMENT,
            tournament_class_id_ext INTEGER,
            tournament_id INTEGER NOT NULL,
            type TEXT DEFAULT 'singles',
            date DATE,
            longname TEXT,
            shortname TEXT,
            gender TEXT,
            max_rank INTEGER,
            max_age INTEGER,
            row_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (tournament_id) REFERENCES tournament(tournament_id),
            UNIQUE (tournament_class_id_ext)
        )
    ''')

    # Create tournament group table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tournament_group (
            group_id            INTEGER PRIMARY KEY AUTOINCREMENT,
            tournament_class_id INTEGER NOT NULL,
            name                TEXT,      -- e.g. 'Pool 1'
            sort_order          INTEGER,
            UNIQUE      (tournament_class_id, name),
            FOREIGN KEY (tournament_class_id) REFERENCES tournament_class(tournament_class_id)
        )
    ''')

    # Create tournament group member table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tournament_group_member (
            group_id        INTEGER NOT NULL,
            participant_id  INTEGER NOT NULL,   -- from player_participant
            seed_in_group   INTEGER,            -- optional, if present in PDFs
            PRIMARY KEY (group_id, participant_id),
            FOREIGN KEY (group_id) REFERENCES tournament_group(group_id),
            FOREIGN KEY (participant_id) REFERENCES player_participant(participant_id)
        );
    ''')

    # Create match table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS match (
            match_id            INTEGER PRIMARY KEY AUTOINCREMENT,
            tournament_class_id INTEGER,                 -- NOT NULL for tournaments
            fixture_id          INTEGER,                 -- NOT NULL for league fixtures
            stage_id            INTEGER NOT NULL,        -- 'GROUP','R16',... or 'LEAGUE'
            group_id            INTEGER,                 -- only for stage='GROUP'
            best_of             INTEGER,                 -- e.g. 5
            date                DATE,
            score_summary       TEXT,
            notes               TEXT,
            row_created         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CHECK ( (tournament_class_id IS NOT NULL) != (fixture_id IS NOT NULL) ), -- XOR
            FOREIGN KEY (tournament_class_id) REFERENCES tournament_class(tournament_class_id),
            FOREIGN KEY (fixture_id)          REFERENCES team_fixture(fixture_id),
            FOREIGN KEY (stage_id)            REFERENCES stage(stage_id),
            FOREIGN KEY (group_id)            REFERENCES tournament_group(group_id)
            );
    ''')

    # Create match sides table 
    # A- Tournament: sides reference class participants (guarantees membership)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS match_side_participant (
            match_id            INTEGER NOT NULL,
            side                INTEGER NOT NULL CHECK(side IN (1,2)),
            participant_id      INTEGER NOT NULL,            -- from player_participant
            PRIMARY KEY (match_id, side, participant_id),
            FOREIGN KEY (match_id)       REFERENCES match(match_id),
            FOREIGN KEY (participant_id) REFERENCES player_participant(participant_id)
        );
    ''')

    # B - League: sides reference players (optionally use tag for club home/away)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS match_side_player (
            match_id            INTEGER NOT NULL,
            side                INTEGER NOT NULL CHECK(side IN (1,2)),
            player_id           INTEGER NOT NULL,
            club_id             INTEGER,                           -- optional; usually home/away club
            PRIMARY KEY (match_id, side, player_id),
            FOREIGN KEY (match_id)  REFERENCES match(match_id),
            FOREIGN KEY (player_id) REFERENCES player(player_id),
            FOREIGN KEY (club_id)   REFERENCES club(club_id)
        );
    ''')

    # Create game table (sets)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS game (
            match_id                    INTEGER NOT NULL,
            game_number                 INTEGER NOT NULL,
            side1_points                INTEGER,                      -- NULL if unknown
            side2_points                INTEGER,                      -- NULL if unknown
            winner_side                 INTEGER NOT NULL CHECK(winner_side IN (1,2)),
            row_created                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (match_id, game_number),
            FOREIGN KEY (match_id) REFERENCES match(match_id)
        );
    ''')

    # Create group standing table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS group_standing (
            group_id            INTEGER NOT NULL,
            participant_id      INTEGER NOT NULL,
            position            INTEGER,        -- 1,2,3...
            wins                INTEGER NOT NULL DEFAULT 0,
            losses              INTEGER NOT NULL DEFAULT 0,
            games_won           INTEGER NOT NULL DEFAULT 0,
            games_lost          INTEGER NOT NULL DEFAULT 0,
            points_won          INTEGER NOT NULL DEFAULT 0,
            points_lost         INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (group_id, participant_id),
            FOREIGN KEY (group_id)       REFERENCES tournament_group(group_id),
            FOREIGN KEY (participant_id) REFERENCES player_participant(participant_id)
        );
    ''')       

    # Create player table (cannonical player data)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS player (
            player_id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            firstname                   TEXT,
            lastname                    TEXT,
            year_born                   INTEGER,
            fullname_raw                TEXT,
            is_verified                 BOOLEAN DEFAULT FALSE,
            row_created                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (fullname_raw)
        )
    ''')

    # Create player alias table, currently used only for mapping multiple external ID:s to same player_id
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS player_alias (
            player_alias_id             INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id                   INTEGER NOT NULL,
            player_id_ext               TEXT,
            firstname                   TEXT,
            lastname                    TEXT,
            year_born                   INTEGER,
            fullname_raw                TEXT,
            source_system               TEXT,
            row_created TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,        
            FOREIGN KEY (player_id)     REFERENCES player(player_id),    
            UNIQUE      (player_id_ext)      
        ) 
    ''')

    # Create player participant table (including seed and final position)
    cursor.execute('''
            CREATE TABLE IF NOT EXISTS player_participant (
                participant_id          INTEGER         PRIMARY KEY AUTOINCREMENT,
                tournament_class_id     INTEGER         NOT NULL,
                player_id               INTEGER,
                club_id                 INTEGER,
                seed                    INTEGER,
                final_position          INTEGER,
                row_created             TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
                FOREIGN KEY (tournament_class_id)   REFERENCES tournament_class(tournament_class_id),
                FOREIGN KEY (player_id)             REFERENCES player(player_id),
                FOREIGN KEY (club_id)               REFERENCES club(club_id), 
                UNIQUE      (tournament_class_id, player_id)
            )
    ''')

    # Create player license raw table
    cursor.execute('''
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
    ''')    

    # Create player license table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS player_license (
            player_id INTEGER NOT NULL,
            club_id INTEGER NOT NULL,
            valid_from DATE NOT NULL,
            valid_to DATE NOT NULL,
            license_id INTEGER NOT NULL,
            season_id INTEGER NOT NULL,
            row_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (player_id) REFERENCES player(player_id),
            FOREIGN KEY (club_id) REFERENCES club(club_id),
            FOREIGN KEY (season_id) REFERENCES season(season_id),
            FOREIGN KEY (license_id) REFERENCES license(license_id),
            UNIQUE (player_id, license_id, season_id, club_id)
        )
    ''')

    # Create player transition raw data table
    cursor.execute('''
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
    ''')               

    # Create player transition table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS player_transition (
            season_id INTEGER NOT NULL,
            player_id INTEGER NOT NULL,
            club_id_from INTEGER NOT NULL,
            club_id_to INTEGER NOT NULL,
            transition_date DATE NOT NULL,
            row_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (player_id) REFERENCES player(player_id),
            FOREIGN KEY (club_id_from) REFERENCES club(club_id),
            FOREIGN KEY (club_id_to) REFERENCES club(club_id),
            UNIQUE (player_id, club_id_from, club_id_to, transition_date)
        )
    ''')

    # Create player ranking group table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS player_ranking_group (
            player_id INTEGER NOT NULL,
            ranking_group_id INTEGER NOT NULL,
            row_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (ranking_group_id) REFERENCES ranking_group(ranking_group_id),
            FOREIGN KEY (player_id) REFERENCES player(player_id)
        )
    ''')

    # Create player ranking raw table
    cursor.execute('''
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
    ''')

    # Create player ranking table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS player_ranking (
            run_id INTEGER NOT NULL,
            run_date DATE NOT NULL,
            player_id INTEGER NOT NULL, 
            points INTEGER NOT NULL,
            points_change_since_last INTEGER NOT NULL,
            position_world INTEGER NOT NULL,
            position INTEGER NOT NULL,
            PRIMARY KEY (player_id, run_date),
            FOREIGN KEY (player_id) REFERENCES player(player_id)
        )
    ''')

    ############# Series tables ####################

    #  Groups within a series-season (e.g., 'Norra', 'Södra', or district pools for regional)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS series_season_group (
            group_id         INTEGER PRIMARY KEY AUTOINCREMENT,
            series_season_id INTEGER    NOT NULL,
            name             TEXT       NOT NULL,
            district_id      INTEGER,
            sort_order       INTEGER,
            external_id      TEXT,
            url              TEXT,
            UNIQUE      (series_season_id, name),
            FOREIGN KEY (series_season_id) REFERENCES series_season(series_season_id),
            FOREIGN KEY (district_id)      REFERENCES district(district_id)
        );
    ''')

    #  Teams. Start simple: teams are clubs with a label (A, B, U, etc)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS team (
            team_id         INTEGER PRIMARY KEY AUTOINCREMENT,
            club_id         INTEGER NOT NULL,
            name            TEXT,             -- optional team label, e.g. 'A', 'B', 'U', or full like 'BTK Rekord A'
            gender          TEXT CHECK(gender IN ('H','D','HD')),
            active          INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
            FOREIGN KEY (club_id) REFERENCES club(club_id)
        );
    ''')

    #  Team registration in a given group (what teams participate)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS team_registration (
            group_id        INTEGER NOT NULL,
            team_id         INTEGER NOT NULL,
            seed            INTEGER,
            PRIMARY KEY (group_id, team_id),
            FOREIGN KEY (group_id) REFERENCES series_season_group(group_id),
            FOREIGN KEY (team_id)  REFERENCES team(team_id)
        );
    ''')

    # Rounds (Omgång N) per group
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS series_season_round (
            round_id          INTEGER PRIMARY KEY AUTOINCREMENT,
            series_season_id  INTEGER NOT NULL,
            group_id          INTEGER NOT NULL,         -- optional: some national series don’t need sub-groups, then default to 1??
            round_no          INTEGER NOT NULL,         -- 1..N
            label             TEXT,                     -- 'Omgång 1'
            start_date        DATE,
            end_date          DATE,
            UNIQUE      (series_season_id, group_id, round_no),
            FOREIGN KEY (series_season_id) REFERENCES series_season(series_season_id),
            FOREIGN KEY (group_id)         REFERENCES series_season_group(group_id)
        );
    ''')

    #  Fixtures (team vs team) - tied to a round
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS team_fixture (
            fixture_id      INTEGER PRIMARY KEY AUTOINCREMENT,
            --league_id       TEXT NOT NULL,        -- or FK later
            --season_label    TEXT NOT NULL,        -- '2024/25', etc.
            round_id        INTEGER NOT NULL,
            date            DATETIME,
            home_team_id    INTEGER NOT NULL,
            away_team_id    INTEGER NOT NULL,
            home_score      INTEGER,
            away_score      INTEGER,
            status          TEXT,      -- 'scheduled','live','final'
            notes           TEXT,
            row_created     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (round_id)     REFERENCES series_season_round(round_id),
            FOREIGN KEY (home_team_id) REFERENCES team(team_id),
            FOREIGN KEY (away_team_id) REFERENCES team(team_id)
        );    
    ''')    


def create_and_populate_static_tables(cursor):

    print("ℹ️  Creating static tables if needed...")
    logging.info("Creating static tables if needed...")
    logging.info("-------------------------------------------------------------------")

    

    ########### LOOKUP TABLES #################
    
    
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

    # Define ranking groups
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

    # Insert ranking groups
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

    # Insert seasons
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

    # Define licenses
    licenses = [
        ('D-licens', ''),
        ('A-licens', 'Barn'),
        ('A-licens', 'Senior'),
        ('A-licens', 'Pensionär'),
        ('A-licens', 'Ungdom'),
        ('48-timmarslicens', ''),
        ('Paralicens', '')
    ]

    # Insert licenses 
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

    # Define districts
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

    # Insert districts
    cursor.executemany('''
        INSERT OR IGNORE INTO district (district_id_ext, name)
        VALUES (?, ?)
    ''', districts) 

    # Create stage table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stage (
            stage_id     INTEGER    PRIMARY KEY,
            code         TEXT       NOT NULL,   -- 'GROUP','R64','R32','R16','QF','SF','F'
            label        TEXT       NOT NULL,
            is_knockout  INTEGER    NOT NULL CHECK(is_knockout IN (0,1)),
            round_order  INTEGER,                  -- for ordering in brackets
            UNIQUE (code)
        );
    ''')

    # Define stages
    stages = [
        (1, 'GROUP',    'Group',        0,  None),
        (2, 'R128',     'Round of 128', 1,  128),
        (3, 'R64',      'Round of 64',  1,  64),
        (4, 'R32',      'Round of 32',  1,  32),
        (5, 'R16',      'Round of 16',  1,  16),
        (6, 'QF',       'Quarterfinal', 1,  8),
        (7, 'SF',       'Semifinal',    1,  4),
        (8, 'F',        'Final',        1,  2),
        (10, 'LEAGUE',  'League',       0,  None)
    ]

    # Insert stages
    cursor.executemany('''
        INSERT OR IGNORE INTO stage (stage_id, code, label, is_knockout, round_order)
        VALUES (?, ?, ?, ?, ?)
    ''', stages)


    ############ LEAGUES / SERIES ################

    #  Series catalog (national & regional)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS series (
            series_id     INTEGER PRIMARY KEY AUTOINCREMENT,
            name          TEXT      NOT NULL,
            code          TEXT      UNIQUE,
            scope         TEXT      NOT NULL CHECK(scope IN ('national','regional')),
            tier          INTEGER   NOT NULL,                 -- 1=Pingisligan, 2=Superettan, 3=Div1, ...
            gender        TEXT      CHECK(gender IN ('H','D','HD')),
            organizer     TEXT,
            active        INTEGER   NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
            row_created   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    ''')

    #  Series instance per season (some series repeat every season)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS series_season (
            series_season_id    INTEGER PRIMARY KEY AUTOINCREMENT,
            series_id           INTEGER NOT NULL,
            season_id           INTEGER NOT NULL,
            external_id         TEXT,
            url                 TEXT,
            status              TEXT,   -- 'scheduled','running','final'
            row_created         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE      (series_id, season_id),
            FOREIGN KEY (series_id) REFERENCES series(series_id),
            FOREIGN KEY (season_id) REFERENCES season(season_id)
        );
    ''')

    # Define series'

    # National (Herrar)
    NATIONAL_SERIES_MEN = [
        ('Pingisligan Herrar',  'PINGISLIGAN_H',    'national',     1, 'H', 'SBTF'),
        ('Superettan Herrar',   'SUPERETTAN_H',     'national',     2, 'H', 'SBTF'),
        ('Division 1 Herrar',   'DIV1_H',           'national',     3, 'H', 'SBTF'),
        ('Division 2 Herrar',   'DIV2_H',           'national',     4, 'H', 'SBTF'),
        ('Division 3 Herrar',   'DIV3_H',           'national',     5, 'H', 'SBTF')
    ]

    # National (Damer)
    NATIONAL_SERIES_WOMEN = [
        ('Pingisligan Damer',   'PINGISLIGAN_D',    'national',     1, 'D', 'SBTF'),
        ('Superettan Damer',    'SUPERETTAN_D',     'national',     2, 'D', 'SBTF'),
        ('Division 1 Damer',    'DIV1_D',           'national',     3, 'D', 'SBTF'),
        ('Division 2 Damer',    'DIV2_D',           'national',     4, 'D', 'SBTF'),
        ('Division 3 Damer',    'DIV3_D',           'national',     5, 'D', 'SBTF')
    ]

    # Regional (Div 4–6 shown in the left nav by district)
    REGIONAL_SERIES_MEN = [
        ('Division 4 Herrar',   'DIV4_H',           'regional',     6, 'H', 'SBTF'),
        ('Division 5 Herrar',   'DIV5_H',           'regional',     7, 'H', 'SBTF'),
        ('Division 6 Herrar',   'DIV6_H',           'regional',     8, 'H', 'SBTF')
    ]

    REGIONAL_SERIES_WOMEN = [
        ('Division 4 Damer',    'DIV4_D',           'regional',     6, 'D', 'SBTF'),
        ('Division 5 Damer',    'DIV5_D',           'regional',     7, 'D', 'SBTF'),
        ('Division 6 Damer',    'DIV6_D',           'regional',     8, 'D', 'SBTF')
    ]

    # Insert series
    for series in NATIONAL_SERIES_MEN + NATIONAL_SERIES_WOMEN + REGIONAL_SERIES_MEN + REGIONAL_SERIES_WOMEN:
        cursor.execute('''
            INSERT OR IGNORE INTO series (name, code, scope, tier, gender, organizer)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', series)

    ######### CLUBS TABLES ###################

    # Create clubs table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS club (
            club_id         INTEGER     PRIMARY KEY,
            shortname       TEXT,
            longname        TEXT,
            club_type       TEXT        DEFAULT 'club', -- Might add national team later
            city            TEXT,
            country_code    TEXT,
            remarks         TEXT,
            homepage        TEXT,
            active          INTEGER     DEFAULT 1 CHECK (active IN (0, 1)),
            district_id     INTEGER,
            row_created     TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (shortname),
            UNIQUE (longname),
            FOREIGN KEY (district_id) REFERENCES district(district_id)
        );
    ''')

    # Create club ext id mappings table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS club_ext_id (
            club_id         INTEGER,
            club_id_ext     INTEGER     NOT NULL    PRIMARY KEY,
            row_created     TIMESTAMP   DEFAULT     CURRENT_TIMESTAMP,
            UNIQUE (club_id_ext),
            FOREIGN KEY (club_id) REFERENCES club(club_id)
        )
    ''')
    
    # Create club name alias table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS club_name_alias (
            club_id         INTEGER     NOT NULL,
            alias           TEXT        NOT NULL,
            alias_type      TEXT        NOT NULL       CHECK(alias_type IN ('short','long')),
            row_created     TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (club_id, alias, alias_type),
            FOREIGN KEY (club_id) REFERENCES club(club_id)
        )
    ''')


    ############ DEBUG TABLES ######################

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

    # Create missing participants table to be fixed in parsing later
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS player_participant_missing (
            tournament_class_id         INTEGER     NOT NULL,
            tournament_class_id_ext     INTEGER     NOT NULL,
            participant_url             TEXT        NOT NULL,
            nbr_of_missing_players      INTEGER     NOT NULL,
            row_created                 TIMESTAMP   DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    # Create missing participant positions table to be fixed in parsing later
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS player_participant_missing_positions (
            tournament_class_id         INTEGER NOT NULL,
            tournament_class_id_ext     INTEGER NOT NULL,
            final_results_url           TEXT,
            nbr_of_missing_positions    INTEGER NOT NULL,
            row_created                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE      (tournament_class_id, tournament_class_id_ext),
            FOREIGN KEY (tournament_class_id) REFERENCES tournament_class(tournament_class_id)
    )
    ''')

    # Create table for documenting missing group parsing information
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS group_parse_missing (
            tournament_class_id INTEGER NOT NULL,
            group_name          TEXT,
            raw_text            TEXT NOT NULL,    -- offending line
            reason              TEXT,
            row_created         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    ''')

def create_indexes(cursor):

    print("ℹ️  Creating indexes...")

    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_tournament_class_tournament_id ON tournament_class(tournament_id)",
        "CREATE INDEX IF NOT EXISTS idx_final_results_class_id ON tournament_class_final_results(tournament_class_id)",
        "CREATE INDEX IF NOT EXISTS idx_final_results_player_id ON tournament_class_final_results(player_id)",
        "CREATE INDEX IF NOT EXISTS idx_player_license_player_id ON player_license(player_id)",
        "CREATE INDEX IF NOT EXISTS idx_player_ranking_date ON player_ranking(date)",
        "CREATE INDEX IF NOT EXISTS idx_match_tournament_class_id ON match(tournament_class_id)",
        "CREATE INDEX IF NOT EXISTS idx_player_license_raw_id_ext ON player_license_raw (player_id_ext)",
        "CREATE INDEX IF NOT EXISTS idx_player_ranking_raw_id_ext ON player_ranking_raw (player_id_ext)",
        "CREATE INDEX IF NOT EXISTS idx_player_name_year ON player (firstname, lastname, year_born)",
        "CREATE INDEX IF NOT EXISTS idx_player_license_player_season_club ON player_license (player_id, season_id, club_id)",
        "CREATE INDEX IF NOT EXISTS idx_player_alias_id_ext ON player_alias (player_id_ext)",
        "CREATE INDEX IF NOT EXISTS idx_type_age ON license (type, age_group)",
        "CREATE INDEX IF NOT EXISTS idx_player_license_raw_keys ON player_license_raw (player_id_ext, club_id_ext, season_id_ext, license_info_raw)",
        "CREATE INDEX IF NOT EXISTS idx_player_license_keys ON player_license (player_id, license_id, season_id, club_id)",
        "CREATE INDEX IF NOT EXISTS idx_player_transition_raw_unique ON player_transition_raw (firstname, lastname, date_born, transition_date)",
        "CREATE INDEX IF NOT EXISTS idx_player_alias_name_year ON player_alias (firstname, lastname, year_born)",
        "CREATE INDEX IF NOT EXISTS idx_player_transition_season ON player_transition (season_id)",
        "CREATE INDEX IF NOT EXISTS idx_player_id ON player (player_id)",
        "CREATE INDEX IF NOT EXISTS idx_player_license_player_club_season ON player_license (player_id, club_id, season_id)",
        "CREATE INDEX IF NOT EXISTS idx_player_transition_unique ON player_transition (player_id, club_id_from, club_id_to, transition_date)",
        "CREATE INDEX IF NOT EXISTS idx_match_class ON match(tournament_class_id, stage_id, group_id)"
    ]

    for stmt in indexes:
        cursor.execute(stmt)