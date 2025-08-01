
# db.py: 

import sqlite3
import sys
import os
from config import DB_NAME
import logging
from datetime import timedelta
from clubs_data import CLUBS, CLUB_ALIASES

def get_conn():
    try:
        conn = sqlite3.connect(DB_NAME, detect_types=sqlite3.PARSE_DECLTYPES)
        logging.debug(f"Connected to database: {DB_NAME}")
        return conn, conn.cursor()
    except sqlite3.Error as e:
        print(f"❌ Database connection failed: {e}")
        raise

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

def save_to_db_tournaments(cursor, tournaments):
    db_results = []
    for tournament in tournaments:
        if is_duplicate_tournament(cursor, tournament["ondata_id"]):
            logging.debug(f"Skipping duplicate tournament: {tournament['name']}")
            db_results.append({"status": "skipped", "tournament": tournament["name"]})
            continue
        try:
            cursor.execute('''
                INSERT INTO tournament (name, startdate, enddate, city, arena, country_code, ondata_id, url, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (tournament["name"], tournament["start_date"], tournament["end_date"], 
                  tournament["city"], tournament["arena"], tournament["country_code"], 
                  tournament["ondata_id"], tournament["url"], tournament["status"]))
            logging.debug(f"Inserted tournament into DB: {tournament['name']}")
            db_results.append({"status": "success", "tournament": tournament["name"]})
        except sqlite3.Error as e:
            logging.error(f"Error inserting tournament into DB {tournament['name']}: {e}")
            db_results.append({"status": "failed", "tournament": tournament["name"]})
    return db_results

def is_duplicate_tournament(cursor, ondata_id):
    cursor.execute("SELECT tournament_id FROM tournament WHERE ondata_id = ?", (ondata_id,))
    return cursor.fetchone() is not None

def get_from_db_tournaments(cursor):
    try:
        cursor.execute("""
            SELECT tournament_id, name, startdate, enddate, city, arena, country_code, ondata_id, url, status, row_created
            FROM tournament
            WHERE status IN ('ONGOING', 'ENDED')
        """)
        rows = cursor.fetchall()       
        tournaments = []
        for row in rows:
            tournament = {
                "tournament_id": row[0],
                "name": row[1],
                "start_date": row[2],  # Already in string format (e.g., 'YYYY-MM-DD')
                "end_date": row[3],
                "city": row[4],
                "arena": row[5],
                "country_code": row[6],
                "ondata_id": row[7],
                "url": row[8],
                "status": row[9],
                "row_created": row[10]
            }
            tournaments.append(tournament)
            logging.debug(f"Fetched tournament: {tournament['name']} (ID: {tournament['tournament_id']})")    
        logging.debug(f"Fetched {len(tournaments)} tournaments from database.")
        return tournaments
    except Exception as e:
        logging.error(f"Error fetching tournaments from database: {e}")
        print(f"❌ Error fetching tournaments from database: {e}")
        return []

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
            name TEXT NOT NULL,
            startdate DATE,
            enddate DATE,
            city TEXT,
            arena TEXT,
            country_code TEXT,
            ondata_id TEXT,
            url TEXT,
            status TEXT,
            row_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Create tournament class table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tournament_class (
            tournament_class_id INTEGER PRIMARY KEY AUTOINCREMENT,
            tournament_id INTEGER NOT NULL,
            date DATE,
            class_description TEXT,
            class_short TEXT,
            gender TEXT,
            max_rank INTEGER,
            max_age INTEGER,
            players_url TEXT,
            groups_url TEXT,
            group_games_url TEXT,
            group_results_url TEXT,
            knockout_url TEXT,
            final_results_url TEXT,
            row_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (tournament_id) REFERENCES tournament(tournament_id),
            UNIQUE (tournament_id, class_short)
        )
    ''')

    # Create tournament class players table (starting list of players in a class)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tournament_class_players (
            tournament_class_id INTEGER NOT NULL,
            player_id INTEGER NOT NULL,
            row_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (tournament_class_id) REFERENCES tournament_class(tournament_class_id),
            UNIQUE (tournament_class_id, player_id)
        )
    ''')

    # Create tournament class final results table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tournament_class_final_results (
            tournament_class_id INTEGER NOT NULL,
            player_id INTEGER NOT NULL,
            position INTEGER NOT NULL,
            row_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (tournament_class_id) REFERENCES tournament_class(tournament_class_id),
            UNIQUE (tournament_class_id, player_id)
        )
    ''')

    # Create player table (cannonical player data)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS player (
            player_id INTEGER PRIMARY KEY AUTOINCREMENT,
            firstname TEXT,
            lastname TEXT,
            year_born INTEGER,
            row_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Create player alias table (include firstname, lastname, year_born to cater for different spellings in the future)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS player_alias (
            player_id INTEGER NOT NULL,
            player_id_ext INTEGER,
            firstname TEXT,
            lastname TEXT,
            year_born INTEGER,
            row_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (player_id_ext),
            FOREIGN KEY (player_id) REFERENCES player(player_id)
        ) 
    ''')

    # Create raw data table for player_licenses_raw
    # Rename season to season_label later..
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
    # Player can have two or more licenses of the same license type, in the same season, but for different clubs
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

    # Create match table
    cursor.execute('''
        CREATE TABLE match (
            match_id INTEGER PRIMARY KEY AUTOINCREMENT,
            tournament_class_id INTEGER NOT NULL,
            stage_type TEXT,           -- 'group', 'knockout', 'final'
            round_number INTEGER,       -- optional numeric stage (e.g. 1=R32, 2=R16)
            match_type TEXT NOT NULL,   -- 'singles', 'doubles', 'team'
            date DATE,
            score_summary TEXT,
            winning_team INTEGER,
            row_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(tournament_class_id) REFERENCES tournament_class(tournament_class_id)
        )
    ''')

    # Create game table
    cursor.execute('''
        CREATE TABLE game (
            match_id INTEGER NOT NULL,
            game_number INTEGER NOT NULL,
            team1_score INTEGER NOT NULL,
            team2_score INTEGER NOT NULL,
            row_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (match_id, game_number),
            FOREIGN KEY (match_id) REFERENCES match(match_id)
        )
    ''')

def create_and_populate_static_tables(cursor):

    print("ℹ️  Creating static tables if needed...")
    logging.info("Creating static tables if needed...")
    logging.info("-------------------------------------------------------------------")

    # Create ranking group table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ranking_group (
            ranking_group_id INTEGER PRIMARY KEY AUTOINCREMENT,
            gender TEXT CHECK(gender IN ('H', 'D', 'HD')),
            min_rank INTEGER NOT NULL,
            max_rank INTEGER NOT NULL, 
            class_description TEXT NOT NULL,
            class_short TEXT NOT NULL UNIQUE
        )
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
            season_start_date DATE,
            season_end_date DATE,
            season_start_year INTEGER,
            season_end_year INTEGER,
            season_description TEXT,
            season_label TEXT,
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
        (season_id_ext, season_start_date, season_end_date, season_start_year, season_end_year, season_description, season_label)
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
            district_id INTEGER PRIMARY KEY AUTOINCREMENT,
            district_id_ext INTEGER,
            name TEXT NOT NULL,
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

    # Create clubs table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS club (
            club_id INTEGER PRIMARY KEY,
            name TEXT,
            long_name TEXT,
            city TEXT,
            country_code TEXT,
            remarks TEXT,
            homepage TEXT,
            active INTEGER DEFAULT 1 CHECK (active IN (0, 1)),
            district_id INTEGER,
            UNIQUE (name),
            FOREIGN KEY (district_id) REFERENCES district(district_id)
        )
    ''')

    # Insert clubs
    cursor.executemany('''
        INSERT OR IGNORE INTO club (club_id, name, long_name, city, country_code, remarks, homepage, active, district_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', CLUBS)

    # Create club aliases table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS club_alias (
            club_id INTEGER,
            club_id_ext INTEGER,
            name TEXT,
            long_name TEXT,
            remarks TEXT,
            UNIQUE (club_id_ext),
            UNIQUE (club_id, name, long_name),
            FOREIGN KEY (club_id) REFERENCES club(club_id)
        )
    ''')

    # Insert club aliases
    cursor.executemany('''
        INSERT OR IGNORE INTO club_alias (club_id, club_id_ext, name, long_name, remarks)
        VALUES (?, ?, ?, ?, ?)
    ''', CLUB_ALIASES)

def create_indexes(cursor):

    print("ℹ️  Creating indexes...")

    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_tournament_class_tournament_id ON tournament_class(tournament_id)",
        "CREATE INDEX IF NOT EXISTS idx_final_results_class_id ON tournament_class_final_results(tournament_class_id)",
        "CREATE INDEX IF NOT EXISTS idx_final_results_player_id ON tournament_class_final_results(player_id)",
        "CREATE INDEX IF NOT EXISTS idx_player_license_player_id ON player_license(player_id)",
        "CREATE INDEX IF NOT EXISTS idx_player_ranking_date ON player_ranking(date)",
        "CREATE INDEX IF NOT EXISTS idx_match_tournament_class_id ON match(tournament_class_id)",
        "CREATE INDEX IF NOT EXISTS idx_match_player1_id ON match(player1_id)",
        "CREATE INDEX IF NOT EXISTS idx_match_player2_id ON match(player2_id)",
        "CREATE INDEX IF NOT EXISTS idx_match_winner_id ON match(winning_player_id)",
        "CREATE INDEX IF NOT EXISTS idx_match_loser_id ON match(losing_player_id)",
        "CREATE INDEX IF NOT EXISTS idx_game_match_id ON game(match_id)",
        "CREATE INDEX IF NOT EXISTS idx_club_alias_name ON club_alias (LOWER(name))",
        "CREATE INDEX IF NOT EXISTS idx_club_alias_long_name ON club_alias (LOWER(long_name))",
        "CREATE INDEX IF NOT EXISTS idx_player_license_raw_id_ext ON player_license_raw (player_id_ext)",
        "CREATE INDEX IF NOT EXISTS idx_player_ranking_raw_id_ext ON player_ranking_raw (player_id_ext)",
        "CREATE INDEX IF NOT EXISTS idx_player_name_year ON player (firstname, lastname, year_born)",
        "CREATE INDEX IF NOT EXISTS idx_player_license_player_season_club ON player_license (player_id, season_id, club_id)",
        "CREATE INDEX IF NOT EXISTS idx_club_alias_name ON club_alias (name)",
        "CREATE INDEX IF NOT EXISTS idx_club_alias_long_name ON club_alias (long_name)",
        "CREATE INDEX IF NOT EXISTS idx_season_label ON season (season_label)",
        "CREATE INDEX IF NOT EXISTS idx_player_alias_id_ext ON player_alias (player_id_ext)",
        "CREATE INDEX IF NOT EXISTS idx_club_alias_id_ext ON club_alias (club_id_ext)",
        "CREATE INDEX IF NOT EXISTS idx_season_label ON season (season_label)",
        "CREATE INDEX IF NOT EXISTS idx_type_age ON license (type, age_group)",
        "CREATE INDEX IF NOT EXISTS idx_player_license_raw_keys ON player_license_raw (player_id_ext, club_id_ext, season_id_ext, license_info_raw)",
        "CREATE INDEX IF NOT EXISTS idx_player_license_keys ON player_license (player_id, license_id, season_id, club_id)",
        "CREATE INDEX IF NOT EXISTS idx_player_transition_raw_unique ON player_transition_raw (firstname, lastname, date_born, transition_date)",
        "CREATE INDEX IF NOT EXISTS idx_player_alias_name_year ON player_alias (firstname, lastname, year_born)",
        "CREATE INDEX IF NOT EXISTS idx_player_transition_season ON player_transition (season_id)",
        "CREATE INDEX IF NOT EXISTS idx_player_id ON player (player_id)",
        "CREATE INDEX IF NOT EXISTS idx_club_alias_name ON club_alias (name)",
        "CREATE INDEX IF NOT EXISTS idx_season_dates ON season (season_start_date, season_end_date)",
        "CREATE INDEX IF NOT EXISTS idx_player_license_player_club_season ON player_license (player_id, club_id, season_id)",
        "CREATE INDEX IF NOT EXISTS idx_player_transition_unique ON player_transition (player_id, club_id_from, club_id_to, transition_date)"
    ]

    for stmt in indexes:
        cursor.execute(stmt)