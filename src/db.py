
# db.py: 
# Handles database connections, table creation, and population of static tables (e.g., ranking_group).

import sqlite3
from config import DB_NAME
import logging
from datetime import datetime, timedelta, date
from dateutil import parser  # For flexible date parsing

def get_conn():
    try:
        conn = sqlite3.connect(DB_NAME, detect_types=sqlite3.PARSE_DECLTYPES)
        logging.debug(f"Connected to database: {DB_NAME}")
        return conn, conn.cursor()
    except sqlite3.Error as e:
        print(f"❌ Database connection failed: {e}")
        raise

def is_duplicate_tournament(cursor, ondata_id):
    cursor.execute("SELECT tournament_id FROM tournament WHERE ondata_id = ?", (ondata_id,))
    return cursor.fetchone() is not None

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

def get_from_db_club(cursor, club_id=None, club_id_ext=None, club_name=None):
    if club_id is not None:
        cursor.execute("""
            SELECT club_id, club_id_ext, name, city, country_code
            FROM club
            WHERE club_id = ?
        """, (club_id,))
    elif club_id_ext is not None:
        cursor.execute("""
            SELECT club_id, club_id_ext, name, city, country_code
            FROM club
            WHERE club_id_ext = ?
        """, (club_id_ext,))
    elif club_name is not None:
        cursor.execute("""
            SELECT club_id, club_id_ext, name, city, country_code
            FROM club
            WHERE name = ?
        """, (club_name,))
    else:
        raise ValueError("Must provide either club_id, club_id_ext, or club name")

    row = cursor.fetchone()
    if row:
        keys = ['club_id', 'club_id_ext', 'name', 'city', 'country_code']
        return dict(zip(keys, row))
    return None

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
                         f"with license details: license_type={new_license.get('license_type')}, "
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
            #     logging.info(f"✔️ Inserted license for player {firstname} {lastname} with license_type '{record.get('license_type')}'")
            #     db_results.append({
            #         "status": "success",
            #         "player": f"{firstname} {lastname}, born {year_born} from club {club_from_name} to {club_to_name} on {transition_date}",
            #         "reason": f"Single license inserted with type {record.get('license_type')}"
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
            #             "license_type": record.get("license_type"),
            #             "license_id": record.get("license_id"),
            #             "valid_from": transition_date,
            #             "valid_to": season_transition.get("season_end_date"),
            #         }

            #         # insert_license(cursor, new_license)
            #         logging.info(
            #             f"✔️ Inserted license for player {firstname} {lastname} with license_type '{record.get('license_type')}'"
            #         )

            #     db_results.append({
            #         "status": "success",
            #         "player": f"{firstname} {lastname}, born {year_born} from club {club_from_name} to {club_to_name} on {transition_date}",
            #         "reason": f"Multiple licenses inserted with types: {', '.join([r.get('license_type') for r in license_records])}"
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

def get_from_db_season(cursor, season_id=None, season_id_ext=None, season_label=None, date_object=None):
    if season_id is not None:
        cursor.execute('''
            SELECT season_id, season_id_ext, season_start_date, season_end_date,
                   season_start_year, season_end_year, season_description
            FROM season
            WHERE season_id = ?
        ''', (season_id,))
    elif season_id_ext is not None:
        cursor.execute('''
            SELECT season_id, season_id_ext, season_start_date, season_end_date,
                   season_start_year, season_end_year, season_description
            FROM season
            WHERE season_id_ext = ?
        ''', (season_id_ext,))
    elif date_object is not None:
        cursor.execute('''
            SELECT season_id, season_id_ext, season_start_date, season_end_date,
                   season_start_year, season_end_year, season_description
            FROM season
            WHERE season_start_date <= ? AND season_end_date >= ?
        ''', (date_object, date_object))
    elif season_label is not None:
        cursor.execute('''
            SELECT season_id, season_id_ext, season_start_date, season_end_date,
                   season_start_year, season_end_year, season_description
            FROM season
            WHERE season_label = ?
        ''', (season_label,))
    else:
        raise ValueError("Must provide either season_id, season_id_ext, or date_value")

    row = cursor.fetchone()
    if row:
        keys = ['season_id', 'season_id_ext', 'season_start_date', 'season_end_date',
                'season_start_year', 'season_end_year', 'season_description']
        return dict(zip(keys, row))
    return None

def get_from_db_club(cursor, club_id=None, club_id_ext=None, club_name=None):
    if club_id is not None:
        cursor.execute("""
            SELECT club_id, club_id_ext, name
            FROM club
            WHERE club_id = ?
        """, (club_id,))
    elif club_id_ext is not None:
        cursor.execute("""
            SELECT club_id, club_id_ext, name
            FROM club
            WHERE club_id_ext = ?
        """, (club_id_ext,))
    elif club_name is not None:
        cursor.execute("""
            SELECT club_id, club_id_ext, name
            FROM club
            WHERE name = ?
        """, (club_name,))
    else:
        raise ValueError("Must provide club_id, club_id_ext, or club name")

    row = cursor.fetchone()
    if row:
        keys = ['club_id', 'club_id_ext', 'name']
        return dict(zip(keys, row))
    return None

def get_from_db_player(cursor, player_id=None, player_id_ext=None):
    if player_id is not None:
        cursor.execute('''
            SELECT player_id, player_id_ext, firstname, lastname, year_born
            FROM player
            WHERE player_id = ?
        ''', (player_id,))
    elif player_id_ext is not None:
        cursor.execute('''
            SELECT player_id, player_id_ext, firstname, lastname, year_born
            FROM player
            WHERE player_id_ext = ?
        ''', (player_id_ext,))
    else:
        raise ValueError("Must provide either player_id or player_id_ext")

    row = cursor.fetchone()
    if row:
        keys = ['player_id', 'player_id_ext', 'firstname', 'lastname', 'year_born']
        return dict(zip(keys, row))
    return None

def get_from_db_license(cursor, license_id=None, license_type=None, license_age_group=None):
    if license_id is not None:
        cursor.execute("""
            SELECT license_id, license_type, license_age_group
            FROM license
            WHERE license_id = ?
        """, (license_id,))
    elif license_type is not None and license_age_group is not None:
        cursor.execute("""
            SELECT license_id, license_type, license_age_group
            FROM license
            WHERE license_type = ? AND license_age_group = ?
        """, (license_type, license_age_group))
    else:
        raise ValueError("Must provide either license_id or both license_type and license_age_group")

    row = cursor.fetchone()
    if row:
        keys = ['license_id', 'license_type', 'license_age_group']
        return dict(zip(keys, row))
    return None

def get_from_db_player_license_raw_count(cursor):
    """
    Returns the count of all player_license records in the database.
    """
    cursor.execute("SELECT COUNT(*) FROM player_license_raw")
    row = cursor.fetchone()
    if row:
        return row[0]
    return 0

def get_from_db_player_license(
    cursor,
    player_license_id=None,
    player_id=None,
    license_id=None,
    season_id=None,
    club_id=None
):
    """
    Fetch exactly one player_license row using either:
      - player_license_id
      - OR the unique combination of (player_id, license_id, season_id, club_id)

    Returns:
        - dict if exactly one match is found
        - None if no match is found
    Raises:
        - ValueError if input is invalid or if multiple matches are found
    """
    if player_license_id is not None:
        query = """
            SELECT player_license_id, player_id, player_id_ext, club_id, club_id_ext, 
                   valid_from, valid_to, license_id, season_id
            FROM player_license
            WHERE player_license_id = ?
        """
        cursor.execute(query, (player_license_id,))
    
    elif all(param is not None for param in (player_id, license_id, season_id, club_id)):
        query = """
            SELECT player_license_id, player_id, player_id_ext, club_id, club_id_ext, 
                   valid_from, valid_to, license_id, season_id
            FROM player_license
            WHERE player_id = ? AND license_id = ? AND season_id = ? AND club_id = ?
        """
        cursor.execute(query, (player_id, license_id, season_id, club_id))

    else:
        raise ValueError("Must provide either 'player_license_id' or all of: 'player_id', 'license_id', 'season_id', 'club_id'")

    rows = cursor.fetchall()

    if len(rows) == 0:
        return None
    elif len(rows) == 1:
        keys = ['player_license_id', 'player_id', 'player_id_ext', 'club_id', 'club_id_ext', 
                'valid_from', 'valid_to', 'license_id', 'season_id']
        return dict(zip(keys, rows[0]))
    else:
        raise ValueError(f"Expected 1 result, but found {len(rows)} matching records.")

def get_player_ids_by_name_and_birth(cursor, firstname, lastname, year_born):
    cursor.execute("""
        SELECT player_id
        FROM player
        WHERE firstname = ? AND lastname = ? AND year_born = ?
    """, (firstname, lastname, year_born))
    
    rows = cursor.fetchall()
    return [row[0] for row in rows] if rows else []

def search_from_db_player_licenses(cursor, player_ids, club_id, season_id):
    """
    Returns a list of matching player_license records for one or more player_ids,
    a specific club_id, and a season_id.
    """

    # Normalize to list if single ID is passed
    if isinstance(player_ids, (str, int)):
        player_ids = [player_ids]

    if not player_ids:
        return []

    placeholders = ','.join(['?'] * len(player_ids))
    query = f"""
        SELECT player_license_id, player_id, player_id_ext, club_id, club_id_ext, 
               valid_from, valid_to, license_id, season_id
        FROM player_license
        WHERE player_id IN ({placeholders})
          AND club_id = ?
          AND season_id = ?
    """

    params = player_ids + [club_id, season_id]
    cursor.execute(query, params)

    keys = ['player_license_id', 'player_id', 'player_id_ext', 'club_id', 'club_id_ext',
            'valid_from', 'valid_to', 'license_id', 'season_id']

    return [dict(zip(keys, row)) for row in cursor.fetchall()]

def search_from_db_players(cursor, firstname=None, lastname=None, year_born=None):
    query = """
        SELECT player_id, player_id_ext, firstname, lastname, year_born
        FROM player
        WHERE 1=1
    """
    params = []

    if firstname:
        query += " AND firstname = ?"
        params.append(firstname)

    if lastname:
        query += " AND lastname = ?"
        params.append(lastname)

    if year_born:
        query += " AND year_born = ?"
        params.append(year_born)

    cursor.execute(query, params)
    return cursor.fetchall()


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
                print(f"🗑️  Dropped table: {table_name}")
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

    # Create clubs table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS club (
            club_id INTEGER PRIMARY KEY AUTOINCREMENT,
            club_id_ext INTEGER,
            name TEXT NOT NULL,
            city TEXT,
            country_code TEXT,
            remarks TEXT,
            link TEXT,
            active INTEGER DEFAULT 1 CHECK (active IN (0, 1)),
            UNIQUE (name),
            UNIQUE (club_id_ext)
        )
    ''')

    # Create player table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS player (
            player_id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id_ext INTEGER,
            firstname TEXT,
            lastname TEXT,
            year_born INTEGER,
            row_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (player_id_ext)
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
            player_license_id INTEGER PRIMARY KEY AUTOINCREMENT,
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
            player_transition_id INTEGER PRIMARY KEY AUTOINCREMENT,
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
            PRIMARY KEY (player_id, ranking_group_id),
            FOREIGN KEY (ranking_group_id) REFERENCES ranking_group(ranking_group_id),
            FOREIGN KEY (player_id) REFERENCES player(player_id)
        )
    ''')

    # Create player ranking table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS player_ranking (
            player_id INTEGER NOT NULL,
            date DATE,
            ranking_points INTEGER NOT NULL,
            row_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (player_id, date),
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
        CREATE TABLE IF NOT EXISTS match (
            match_id INTEGER PRIMARY KEY AUTOINCREMENT,
            tournament_class_id INTEGER NOT NULL,
            player1_id INTEGER NOT NULL,
            player2_id INTEGER NOT NULL,
            date DATE,
            score_summary TEXT,
            stage TEXT,
            winning_player_id INTEGER NOT NULL,
            losing_player_id INTEGER NOT NULL,
            row_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (tournament_class_id) REFERENCES tournament_class(tournament_class_id),
            FOREIGN KEY (player1_id) REFERENCES player(player_id),
            FOREIGN KEY (player2_id) REFERENCES player(player_id),
            FOREIGN KEY (winning_player_id) REFERENCES player(player_id),
            FOREIGN KEY (losing_player_id) REFERENCES player(player_id)
        )
    ''')

    # Create game table (child table for matches)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS game (
            match_id INTEGER NOT NULL,
            game_number INTEGER NOT NULL,
            player1_score INTEGER NOT NULL,
            player2_score INTEGER NOT NULL,
            row_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (match_id) REFERENCES match(match_id),
            UNIQUE (match_id, game_number)
        )
    ''')

def create_and_populate_static_tables(cursor):

    print("ℹ️  Creating static tables if needed.")
    logging.info("Creating static tables if needed.")
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

    # Define seasons
    seasons = [
        (181, '2025-07-01', '2026-06-30', 2025, 2026, '2025-26', 'Licens 2025-26'),
        (171, '2024-07-01', '2025-06-30', 2024, 2025, '2024-25', 'Licens 2024-25'),
        (135, '2023-07-01', '2024-06-30', 2023, 2024, '2023-24', 'Licens 2023-24'),
        (126, '2022-07-01', '2023-06-30', 2022, 2023, '2022-23', 'Licens 2022-23'),
        (121, '2021-07-01', '2022-06-30', 2021, 2022, '2021-22', 'Licens 2021-22'),
        (114, '2020-07-01', '2021-06-30', 2020, 2021, '2020-21', 'Licens 2020-21'),
        (109, '2019-07-01', '2020-06-30', 2019, 2020, '2019-20', 'Licens 2019-20'),
        (103, '2018-07-01', '2019-06-30', 2018, 2019, '2018-19', 'Licens 2018-19'),
        (99, '2017-07-01', '2018-06-30', 2017, 2018, '2017-18', 'Licens 2017-18'),
        (80, '2016-07-01', '2017-06-30', 2016, 2017, '2016-17', 'Licens 2016-17'),
        (73, '2015-07-01', '2016-06-30', 2015, 2016, '2015-16', 'Licens 2015-16'),
        (68, '2014-07-01', '2015-06-30', 2014, 2015, '2014-15', 'Licens 2014-15'),
        (56, '2013-07-01', '2014-06-30', 2013, 2014, '2013-14', 'Licens 2013-14'),
        (54, '2012-07-01', '2013-06-30', 2012, 2013, '2012-13', 'Licens 2012-13'),
        (50, '2011-07-01', '2012-06-30', 2011, 2012, '2011-12', 'Licens 11-12'),
        (39, '2010-07-01', '2011-06-30', 2010, 2011, '2010-11', 'Licens 10/11')
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
            license_type TEXT,
            license_age_group TEXT,
            UNIQUE (license_type, license_age_group)
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
        INSERT OR IGNORE INTO license (license_type, license_age_group)
        VALUES (?, ?)
    ''', licenses)  

    # Create player transition club mapping table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS player_transition_club_mapping (
            club_name_raw TEXT NOT NULL,
            club_name_mapped TEXT NOT NULL,
            club_id_mapped INTEGER NOT NULL,
            club_id_ext_mapped INTEGER NOT NULL,
            remarks TEXT,
            link TEXT,
            UNIQUE (club_name_raw),
            FOREIGN KEY (club_id_mapped) REFERENCES club(club_id),
            FOREIGN KEY (club_id_ext_mapped) REFERENCES club(club_id_ext)
        )
    ''')

    # Define club mappings
    club_mappings = [
        ('Allerums Bordtennissällskap', 'Allerums GIF', 2646, 5),
        ('Askims PK', 'Askims BTK', 2659, 13),
        ('Askims BTS', 'Askims BTK', 2659, 13),
        ('Aspuddens BTK', 'No match', None, None),
        ('BTF Warta', 'BTK Warta', 2736, 48),
        ('BTS Warta', 'BTK Warta', 2736, 48),
        ('BTK Kviberg', 'BTK Kviberg', 2716, 40),
        ('BTK Lesseboiterna', 'Lessebo BTK', 3011, 237),
        ('BTK Loket', 'BTK Loket', 2701, 31),
        ('BTK Munken', 'BTK Munken', 2710, 37),
        ('BTK Nissan', 'BTK Nissan', 2714, 38),
        ('BTK Smaragd', 'BTK Smaragd', 2726, 45),
        ('BTK Söder', 'BTK Söder', 2727, 46),
        ('BTK Telje', 'BTK Telje', 2728, 47),
        ('BTK Triss', 'BTK Triss', 2725, 44),
        ('BTS Dalen', 'BTK Dalen', 2701, 31),
        ('BTS Gunhild', 'No match', None, None),
        ('BTsyd BTK', 'No match', None, None),
        ('Bele Barkarby BTF', 'No match', None, None),
        ('Blackebergs SK', 'No match', None, None),
        ('Blåkulla BTK', 'No match', None, None),
        ('Bollmora BTK', 'No match', None, None),
        ('Bordtennisklubben Akvedukt', 'No match', None, None),
        ('Bordtennisklubben Gruvan', 'No match', None, None),
        ('Brännkyrka BTK', 'No match', None, None),
        ('Cafe Norr BTF', 'No match', None, None),
        ('Dala BTK', 'No match', None, None),
        ('Dänningelanda BTS', 'Dänningelanda BTK', 2776, 81),
        ('Eslövs BTK', 'Eslövs AI BTK', 2785, 88),
        ('Eslövs PK', 'Eslövs AI BTK', 2785, 88),
        ('Finspångs AIK Bordtennisklubb', 'Finspångs AIK', 2952, 91),
        ('Fridtuna BTK', 'No match', None, None),
        ('Götene Pingisklubb', 'Götene/Tor BTK', 2844, 125),
        ('Hallsta BTK', 'Hallstahammars BTK', 2849, 129),
        ('Hammarby Boll - och Pucksällskap', 'Hammarby IF BTF', 2783, 131),
        ('Harbo IF', 'No match', None, None),
        ('Heby/Runhällen BTK', 'Heby AIF', 2860, 135),
        ('Helgonabackens Bordtennisklubb', 'No match', None, None),
        ('Herrljunga PK', 'Herrljunga BTK', 2864, 137),
        ('Hovmantorp GoIF', 'No match', None, None),
        ('Hovsta IF', 'No match', None, None),
        ('Hudiksvalls IF', 'Hudiksvalls IF BTK', 3654, 142),
        ('Huvudsta BTK', 'No match', None, None),
        ('Hysingsvik SK', 'No match', None, None),
        ('Höganäs BTK', 'Halmstad BTK', 2850, 130),
        ('Hörnebo SK', 'No match', None, None),
        ('IF Brunskog United', 'No match', None, None),
        ('IF Kil', 'No match', None, None),
        ('IF Laputa PPP', 'No match', None, None),
        ('IFK Hedemora BTK', 'No match', None, None),
        ('IFK Täby', 'IFK Täby BTK', 2928, 174),
        ('IFK Ölme', 'No match', None, None),
        ('IK Studenterna i Umeå', 'No match', None, None),
        ('IK Sturehov', 'No match', None, None),
        ('Innerstans BTK', 'No match', None, None),
        ('Islandstorgets SK', 'Islandstorgets SK', 3018, 241),
        ('Juno BTK', 'IK Juno', 2909, 165),
        ('Järva BTK', 'No match', None, None),
        ('KFUK-KFUM:s IA Säffle', 'KFUM Jönköping IA', 2963, 204),
        ('KFUM Bankeryds BTK', 'KFUM Bankeryd BTK', 2963, 204),
        ('KFUM Kristianstad', 'KFUM Kristianstad BTF', 6204, 204),
        ('KFUM Ljura Bordtennisklubb', 'KFUM Ljura BTK', 2966, 205),
        ('KFUM Norrköping', 'KFUM Norrköping', 3059, 239),
        ('KFUM Norrköping BTK', 'KFUM Norrköping BTK', 3059, 239),
        ('KFUM Urania BTK', 'KFUM Urania BTK', 2966, 205),
        ('Kalix BTK', 'Kalix BTK-Bordtennis', 24234, 197),
        ('Korsbacka BTK', 'No match', None, None),
        ('Kristianstad BTK', 'Kristianstad BTK', 2978, 214),
        ('Kristianstad PK', 'Kristianstad PK', 2955, 195),
        ('Kullens BTK', 'Kullåkra BTK', 2980, 216),
        ('Kvarnby BTK', 'Kvarnby AK', 2982, 218),
        ('Kvibille BTK', 'Kvibille BTK', 2955, 195),
        ('Kärrbacka BTK', 'Kärrtorps BTK', 2985, 221),
        ('Kärrtorps SK', 'Kärrtorps BTK', 2985, 221),
        ('Kävlinge Pingisklubb', 'Kävlinge Pingisklubb', 2996, 232),
        ('Köpingebro IF', 'No match', None, None),
        ('Landeryds PK', 'Landeryds PK', 2963, 204),
        ('Lekstorps BTK', 'Lekstorps IF', 3000, 236),
        ('Lerums BTS', 'Lerums BTS', 3007, 242),
        ('Lidhults GOIF', 'Lidhults GOIF', 2955, 195),
        ('Lidhults PK', 'Lidhults PK', 2955, 195),
        ('Limhamns BTK', 'Limhamns BTK', 3005, 241),
        ('Lindsdals IF', 'Lindsdals IF', 3013, 249),
        ('Linkeboda BTK', 'Linkeboda BTK', 3062, 299),
        ('Linköpings BTK', 'Linköpings Pingisklubb', 3010, 246),
        ('Linköpings BTS', 'Linköpings Pingisklubb', 3010, 246),
        ('Linné IF', 'Linné IF', 3011, 247),
        ('Linné Sydsport BTK', 'Linné Sydsport BTK', 3012, 248),
        ('Ljunghusens MIF', 'Ljunghusens MIF', 3014, 250),
        ('Ljusdals BTK', 'Ljusdals BTK', 3015, 351),
        ('Ljusdalsfritid IK', 'Ljusdalsfritid IK', 3016, 352),
        ('Ljusne Vallviks BTK', 'Ljusne Vallviks BTK', 3017, 353),
        ('Lohärads IF', 'Lohärads IF', 3018, 354),
        ('Lunds BTK', 'Lunds BTK', 3019, 355),
        ('Lunds PK', 'Lunds PK', 3020, 356),
        ('Lyckeby BTS', 'Lyckeby BTK', 3019, 355),
        ('Lyckeby Bordtennisförening', 'Lyckeby BTK', 3019, 355),
        ('Lysekils PK', 'Lysekils PK', 3021, 357),
        ('Läckeby BTK', 'Läckeby BTK', 3022, 358),
        ('Malmö BTK', 'Malmö IF', 3025, 361),
        ('Mariedals BTK', 'Mariedals IK', 3026, 362),
        ('Mattmar SK', 'Mattmar SK', 3077, 407),
        ('Mellösa IF', 'Mellösa IF', 3039, 379),
        ('Memnon IF', 'Memnon IF', 3080, 410),
        ('Midsommarkransens BTK', 'Midsommarkransens BTK', 3081, 411),
        ('Motala BTK', 'Motala BTK', 3032, 368),
        ('Myrstugubergets IF', 'Myrstugubergets IF', 3082, 412),
        ('Mölndals BTS', 'Mölndals BTK', 3034, 370),
        ('Norco BTK', 'Norco BTK', 3039, 379),
        ('Norra Härene GOIF', 'Norra Härene GOIF', 3041, 381),
        ('Norre Port BTK', 'Norre Port BTK', 3042, 382),
        ('Norrtulls BTK', 'Norrtulls SK', 3043, 383),
        ('Norsholms IF', 'Norsholms IF', 3044, 384),
        ('Nybban Pingisklubb', 'Nybban Pingisklubb', 3045, 385),
        ('Nybro Pingisklubb', 'Nybro Pingisklubb', 3046, 386),
        ('Oden BTK', 'Oden BTK', 3047, 387),
        ('Olofströms BTK', 'Olofströms BTK', 3048, 388),
        ('Onsala BTK', 'Onsala BTK', 3049, 389),
        ('Orminge BTK', 'Orminge BTK', 3050, 390),
        ('Oskarshamns PK', 'Oskarshamns PK', 3051, 391),
        ('Ovansiljans Pingisklubb', 'Ovansiljans Sportklubb', 3052, 392),
        ('Oxie SK', 'Oxie SK', 3053, 393),
        ('PK Force', 'PK Force', 3054, 394),
        ('PK GV', 'PK GV', 3055, 395),
        ('PK Södertälje', 'PK Södertälje', 3056, 396),
        ('PK Waldners Vänner', 'PK Waldners Vänner', 3057, 397),
        ('PK Österlen', 'PK Österlen', 3058, 398),
        ('Partille BTK', 'Partille BTK', 3059, 399),
        ('Partille BTS', 'Partille BTS', 3060, 400),
        ('Peng Da Bordtennisklubb', 'Peng Da Bordtennisklubb', 3061, 401),
        ('RK Fisklir', 'RK Fisklir', 3062, 402),
        ('Rantens BTK', 'Rantens BTK', 3063, 403),
        ('Riddaren BTK', 'Riddaren BTK', 3064, 404),
        ('Rimbo BTK', 'Rimbo BTK', 3065, 405),
        ('Roma IF', 'Roma IF', 3066, 406),
        ('Ronneby BTK', 'Ronneby BTK', 3067, 407),
        ('Rotebro PK', 'Rotebro BTK', 3068, 408),
        ('Runhällens BOIS', 'Runhällens BOIS', 3069, 409),
        ('Råcksta SK', 'Råcksta SK', 3070, 410),
        ('Råda-Hindås BTK', 'Råda-Hindås BTK', 3071, 411),
        ('Råda-Hindås BTS', 'Råda-Hindås BTK', 3071, 411),
        ('Råslätts PK', 'Råslätts PK', 3072, 412),
        ('Råå BTK', 'Råå BTK', 3073, 413),
        ('Rönninge SK BTF', 'Rönninge SK BTF', 3074, 414),
        ('SK Iron', 'SK Iron', 3075, 415),
        ('SUIF 2000', 'SUIF 2000', 3076, 416),
        ('Sandåkerns SK', 'Sandåkerns SK', 3077, 417),
        ('Sessmans BTK', 'Sems FF', 3070, 406),
        ('Sickla IF', 'Sickla IF', 3074, 418),
        ('Sjöbo Pingisklubb', 'Sjöbo BTK', 3077, 417),
        ('Skepptuna IK', 'Skepptuna IK', 3078, 418),
        ('Skillingaryds BTK', 'Skillingaryds BTK', 3079, 419),
        ('Skurups PK', 'Skurups BTK', 3082, 418),
        ('Skövde BTK', 'Skövde PK', 3088, 424),
        ('Skövde FK', 'Skövde PK', 3088, 424),
        ('Solsta BTK', 'Solsta BTK', 3089, 425),
        ('Steninge GoIF', 'Steninge GoIF', 3090, 426),
        ('Stenkumla BTK', 'Stenkumla BTK', 3091, 427),
        ('Stocksunds BTK', 'Stocksunds BTK', 3092, 428),
        ('Stratos Enköping IF', 'Stratos Enköping BTK', 3097, 433),
        ('Stratos Enköping PS', 'Stratos Enköping BTK', 3097, 433),
        ('Stratos Enköpings PK', 'Stratos Enköping BTK', 3097, 433),
        ('Strövelstorp BTK', 'Strövelstorp BTK', 3100, 436),
        ('Styrnäs BTK', 'Styrnäs BTK', 3101, 437),
        ('Sunnersta BTK', 'Sunnersta AIF', 3104, 440),
        ('Sunnersta Bordtennisklubb', 'Sunnersta AIF', 3104, 440),
        ('Sunnersta PS', 'Sunnersta AIF', 3104, 440),
        ('Sunnersta Pingissällskap', 'Sunnersta AIF', 3104, 440),
        ('Svanesunds PK', 'Svanesunds GIF', 3105, 441),
        ('Svedala BTK', 'Svedala BTK', 3109, 445),
        ('Söderbrons BTK', 'Söderbrons BTK', 3110, 446),
        ('Söderhamns PK', 'Söderhamns PK', 3111, 447),
        ('Söderkulla BTK', 'Söderkulla BTK', 3112, 448),
        ('Söderkulla IK', 'Söderkulla IK', 3113, 449),
        ('Södertälje PS', 'Södertälje PS', 3114, 450),
        ('Tabergs PK', 'Tabergs PK', 3115, 451),
        ('Telia IoFF', 'Telia IoFF', 3116, 452),
        ('Tierps BTK', 'Tierps BTK', 3117, 453),
        ('Timrå AIF', 'Timrå AIF', 3118, 454),
        ('Tjörns BTK', 'Tjörns BTK', 3119, 455),
        ('Torrbergs Bordtennisklubb', 'Torrbergs Bordtennisklubb', 3120, 456),
        ('Trollbäckens BTK', 'Trollbäckens BTK', 3121, 457),
        ('Tullbrons BTK', 'Tullbrons BTK', 3122, 458),
        ('Tullinge PK', 'Tullinge TP BTK', 3131, 467),
        ('Tuna BTK', 'Tuna BTK', 3132, 468),
        ('Täby BTK', 'Täby BTK', 3133, 469),
        ('Täfteå IK', 'Täfteå IK', 3134, 470),
        ('Tålebo BTK', 'Tålebo Bordtennisklubb', 3133, 469),
        ('Tölö BTK', 'Tölö BTK', 3135, 471),
        ('Tölö IF', 'Tölö IF', 3136, 472),
        ('Ulvsunda IF', 'Ulvsunda IF', 3137, 473),
        ('Umeå BTS', 'Umeå BTS', 3138, 474),
        ('Umeå PK', 'Umeå PK', 3139, 475),
        ('Umeå PS', 'Umeå PS', 3140, 476),
        ('VIK-47', 'VIK-47', 3141, 477),
        ('VMA IK', 'VMA IK', 3142, 478),
        ('Valla BTK', 'Valla BTK', 3143, 479),
        ('Varamons Ping-Pong Pojkar', 'Varamons Ping-Pong Pojkar', 3144, 480),
        ('Vartofta SK', 'Vartofta SK', 3145, 481),
        ('Vasastans Bordtennisklubb', 'Vasastans Bordtennisklubb', 3146, 482),
        ('Vegby SK', 'Vegby SK', 3147, 483),
        ('Vendelsö IK', 'Vendelsö IK', 3148, 484),
        ('Vikingstad BTK', 'Vikingstad BTK', 3149, 485),
        ('Vikingstad SK', 'Vikingstad SK', 3150, 486),
        ('Vinbergs BTK', 'Vinbergs BTK', 3151, 487),
        ('Vindelälvens BTK', 'Vindelälvens BTK', 3152, 488),
        ('Virebergs BTK', 'Virebergs BTK', 3153, 489),
        ('Vällingby SK', 'Vällingby SK', 3154, 490),
        ('Vällingby SK', 'Vällingby SK', 3154, 490)
    ]

    # Insert club mappings
    cursor.executemany('''
        INSERT OR IGNORE INTO player_transition_club_mapping 
        (club_name_raw, club_name_mapped, club_id_mapped, club_id_ext_mapped)
        VALUES (?, ?, ?, ?)
    ''', club_mappings)

def create_indexes(cursor):

    print("Creating indexes.")

    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_tournament_class_tournament_id ON tournament_class(tournament_id)",
        "CREATE INDEX IF NOT EXISTS idx_final_results_class_id ON tournament_class_final_results(tournament_class_id)",
        "CREATE INDEX IF NOT EXISTS idx_final_results_player_id ON tournament_class_final_results(player_id)",
        "CREATE INDEX IF NOT EXISTS idx_class_entries_class_id ON tournament_class_entries(tournament_class_id)",
        "CREATE INDEX IF NOT EXISTS idx_class_entries_player_id ON tournament_class_entries(player_id)",
        "CREATE INDEX IF NOT EXISTS idx_player_license_player_id ON player_license(player_id)",
        "CREATE INDEX IF NOT EXISTS idx_player_ranking_group_id ON player_ranking_group(ranking_group_id)",
        "CREATE INDEX IF NOT EXISTS idx_player_ranking_date ON player_ranking(date)",
        "CREATE INDEX IF NOT EXISTS idx_match_tournament_class_id ON match(tournament_class_id)",
        "CREATE INDEX IF NOT EXISTS idx_match_player1_id ON match(player1_id)",
        "CREATE INDEX IF NOT EXISTS idx_match_player2_id ON match(player2_id)",
        "CREATE INDEX IF NOT EXISTS idx_match_winner_id ON match(winning_player_id)",
        "CREATE INDEX IF NOT EXISTS idx_match_loser_id ON match(losing_player_id)",
        "CREATE INDEX IF NOT EXISTS idx_game_match_id ON game(match_id)"
    ]

    for stmt in indexes:
        cursor.execute(stmt)

    print("Indexes created.")