# src/upd_player_licenses.py

import logging
import time
import re
from db import get_conn
from models.player_license import PlayerLicense
from models.season import Season
from models.club import Club
from models.player import Player
from models.license import License
from utils import parse_date, print_db_insert_results, sanitize_name

def upd_player_licenses():
    conn, cursor = get_conn()
    db_results = []

    try:
        logging.info("Updating player licenses...")
        print("ℹ️  Updating player licenses...")
        start_time = time.time()

        # Cache mappings
        cache_start = time.time()
        club_name_map = Club.cache_name_map(cursor)  # Dict[str, Club]
        # player_name_year_map = Player.cache_name_year_map(cursor)  # Dict[Tuple[str, str, int], Player]
        player_id_ext_map = Player.cache_id_ext_map(cursor)  # Dict[int, Player]
        season_map = Season.cache_all(cursor) # Dict[int, Season]
        license_map = License.cache_all(cursor) # Dict[Tuple[str, Optional[str]], License]
        logging.info(f"Cached mappings in {time.time() - cache_start:.2f} seconds")

        # Cache duplicate licenses
        duplicate_start = time.time()
        cursor.execute("""
            SELECT player_id_ext, club_id_ext, season_id_ext, 
                   LOWER(TRIM(SUBSTR(license_info_raw, 1, INSTR(license_info_raw, '(') - 1))) AS license_key,
                   MIN(row_id) AS min_row_id
            FROM player_license_raw
            GROUP BY player_id_ext, club_id_ext, season_id_ext, license_key
            HAVING COUNT(*) > 1
        """)
        duplicate_map = {(row[0], row[1], row[2], row[3]): row[4] for row in cursor.fetchall()}
        logging.info(f"Cached {len(duplicate_map)} duplicate licenses in {time.time() - duplicate_start:.2f} seconds")

        # Fetch raw data
        fetch_start = time.time()
        cursor.execute('''
            SELECT 
                row_id, season_id_ext, season_label, club_name, club_id_ext,
                player_id_ext, firstname, lastname, year_born, license_info_raw
            FROM player_license_raw
        ''')
        rows = cursor.fetchall()
        count = len(rows)
        print(f"ℹ️  Found {count} player licenses in player_license_raw")
        logging.info(f"Found {count} player licenses in player_license_raw")
        logging.info(f"Fetched {count} rows in {time.time() - fetch_start:.2f} seconds")

        if not rows:
            print("⚠️ No player license data found in player_license_raw.")
            logging.warning("No player license data found in player_license_raw.")
            return []
        
        # # Cache duplicate licenses
        # duplicate_start = time.time()
        # cursor.execute("""
        #     SELECT player_id_ext, club_id_ext, season_id_ext, 
        #            LOWER(TRIM(SUBSTR(license_info_raw, 1, INSTR(license_info_raw, '(') - 1))) AS license_key,
        #            MIN(row_id) AS min_row_id
        #     FROM player_license_raw
        #     GROUP BY player_id_ext, club_id_ext, season_id_ext, license_key
        #     HAVING COUNT(*) > 1
        # """)
        # duplicate_map = {(row[0], row[1], row[2], row[3]): row[4] for row in cursor.fetchall()}
        # logging.info(f"Cached duplicates in {time.time() - duplicate_start:.2f} seconds")     

        # License regex to parse license_info_raw
        license_regex = re.compile(r"(?P<type>(?:[A-D]-licens|48-timmarslicens|Paralicens))(?: (?P<age>\w+))? \((?P<date>\d{4}\.\d{2}\.\d{2})\)")

        for row in rows:
            row_start = time.time()
            (row_id, season_id_ext, season_label, club_name, club_id_ext,
             player_id_ext, firstname, lastname, year_born, license_info_raw) = row

            # Process license_info_raw
            license_part = license_info_raw.strip()
            if not license_part:
                logging.warning(f"Empty license part after splitting: {license_part} for row_id {row_id}")
                print(f"⚠️ Empty license part after splitting: {license_part}")
                db_results.append({
                    "status": "failed",
                    "row_id": row_id,
                    "reason": "Empty license part after splitting"
                })
                continue

            match = license_regex.search(license_part)
            if not match:
                logging.warning(f"Invalid license format: {license_part} for row_id {row_id}")
                db_results.append({
                    "status": "failed",
                    "row_id": row_id,
                    "reason": "Invalid license format"
                })
                continue

            type = match.group("type").strip().capitalize()
            license_date = match.group("date")
            age_group = match.group("age").strip().capitalize() if match.group("age") else None

            # # Detect duplicates in raw data
            # cursor.execute("""
            #     SELECT row_id, SUBSTR(license_info_raw, INSTR(license_info_raw, '(') + 1, 10) AS valid_from_raw
            #     FROM player_license_raw
            #     WHERE player_id_ext = ? AND club_id_ext = ? AND season_id_ext = ?
            #     AND LOWER(TRIM(SUBSTR(license_info_raw, 1, INSTR(license_info_raw, '(') - 1))) = LOWER(?)
            #     ORDER BY valid_from_raw
            # """, (
            #     player_id_ext,
            #     club_id_ext,
            #     season_id_ext,
            #     f"{type} {age_group}".strip() if age_group else type
            # ))
            # duplicate_rows = cursor.fetchall()
            # if len(duplicate_rows) > 1:
            #     current_index = next((i for i, r in enumerate(duplicate_rows) if r[0] == row_id), -1)
            #     if current_index > 0:  # Not the earliest
            #         logging.warning(f"Duplicate license detected for player_id_ext {player_id_ext}: {type} "
            #                       f"(age group: {age_group}) for club_id_ext {club_id_ext}, season_id_ext {season_id_ext}, row_id {row_id}")
            #         db_results.append({
            #             "status": "failed",
            #             "row_id": row_id,
            #             "reason": "Player has duplicate license type and age group for the same club and season"
            #         })
            #         continue

            # Check for duplicates using cached map
            license_key = f"{type} {age_group}".strip().lower() if age_group else type.lower()
            duplicate_key = (player_id_ext, club_id_ext, season_id_ext, license_key)
            if duplicate_key in duplicate_map and duplicate_map[duplicate_key] != row_id:
                logging.warning(f"Duplicate license detected for player_id_ext {player_id_ext}: {type} "
                              f"(age group: {age_group}) for club_id_ext {club_id_ext}, season_id_ext {season_id_ext}, row_id {row_id}")
                db_results.append({
                    "status": "failed",
                    "row_id": row_id,
                    "reason": "Player has duplicate license type and age group for the same club and season"
                })
                continue            

            # Fetch season
            # season = Season.get_by_label(cursor, season_label)
            season = season_map.get(season_label)
            if not season:
                logging.warning(f"No matching season found for season_label '{season_label}' for row_id {row_id}")
                db_results.append({
                    "status": "failed",
                    "row_id": row_id,
                    "reason": "No matching season found for season_label"
                })
                continue
            season_id = season.season_id
            valid_to = season.season_end_date

            # Parse valid_from date
            valid_from = parse_date(license_date, context=f"license_part: {license_part}")
            if valid_from is None:
                db_results.append({
                    "status": "failed",
                    "row_id": row_id,
                    "reason": "Date parsing error (valid_from)"
                })
                continue

            # Check valid_from date against season dates
            if not season.contains_date(valid_from):
                logging.warning(f"Player_id_ext {player_id_ext} - Valid from date {valid_from} for season {season_label} is outside the season range {season.season_start_date} - {season.season_end_date}, row_id {row_id}")
                db_results.append({
                    "status": "failed",
                    "row_id": row_id,
                    "reason": "Valid from date is outside the season range"
                })
                continue

            # Skip if valid_from equals season end date
            if valid_from == season.season_end_date:
                logging.warning(f"Player {firstname} {lastname} (ext_id: {player_id_ext}, id: {player_id}) skipped because valid_from ({valid_from}) equals season end date, row_id {row_id}")
                db_results.append({
                    "status": "skipped",
                    "row_id": row_id,
                    "reason": "Valid from date equals season end date"
                })
                continue

            # # Map player using firstname, lastname, year_born
            # player_key = (sanitize_name(firstname), sanitize_name(lastname), year_born)
            # player = player_name_year_map.get(player_key)
            # if not player:
            #     # Fallback to player_id_ext lookup
            #     logging.warning(f"No player found for name/year {player_key} in cache, trying player_id_ext {player_id_ext} for row_id {row_id}")
            #     player = Player.get_by_id_ext(cursor, player_id_ext)
            #     if not player:
            #         logging.warning(f"Foreign key violation: player_id_ext {player_id_ext} does not exist in player_alias table, row_id {row_id}")
            #         db_results.append({
            #             "status": "failed",
            #             "row_id": row_id,
            #             "reason": "Foreign key violation: player_id_ext does not exist in player_alias table"
            #         })
            #         continue
            # player_id = player.player_id

            # Map player using player_id_ext
            player = player_id_ext_map.get(player_id_ext)
            if not player:
                # Fallback to direct lookup
                logging.warning(f"No player found for player_id_ext {player_id_ext} in cache, trying direct lookup for row_id {row_id}")
                player = Player.get_by_id_ext(cursor, player_id_ext)
                if not player:
                    logging.warning(f"Foreign key violation: player_id_ext {player_id_ext} does not exist in player_alias table, row_id {row_id}")
                    db_results.append({
                        "status": "failed",
                        "row_id": row_id,
                        "reason": "Foreign key violation: player_id_ext does not exist in player_alias table"
                    })
                    continue
            player_id = player.player_id            

            # Map club using club_name
            club = club_name_map.get(club_name)
            if not club:
                # Fallback to club_id_ext lookup
                logging.warning(f"No club found for name {club_name} in cache, trying club_id_ext {club_id_ext} for row_id {row_id}")
                club = Club.get_by_id_ext(cursor, club_id_ext)
                if not club:
                    logging.warning(f"Foreign key violation: club_id_ext {club_id_ext} does not exist in club_alias table, row_id {row_id}")
                    db_results.append({
                        "status": "failed",
                        "row_id": row_id,
                        "reason": "Foreign key violation: club_id_ext does not exist in club_alias table"
                    })
                    continue
            club_id = club.club_id

            # Map type and age_group to license_id
            # license = License.get_by_type_and_age(cursor, type, age_group)
            # if not license:
            #     logging.warning(f"Foreign key violation: type {type} and age group {age_group} not found in license table, row_id {row_id}")
            #     db_results.append({
            #         "status": "failed",
            #         "row_id": row_id,
            #         "reason": "Foreign key violation: type and age group not found in license table"
            #     })
            #     continue
            # license_id = license.license_id  # Extract integer license_id
            license = license_map.get((type, age_group))
            if not license:
                logging.warning(f"Foreign key violation: type {type} and age group {age_group} not found in license table, row_id {row_id}")
                db_results.append({
                    "status": "failed",
                    "row_id": row_id,
                    "reason": "Foreign key violation: type and age group not found in license table"
                })  
                continue
            license_id = license.license_id            

            # Create PlayerLicense object
            player_license = PlayerLicense(
                player_id=player_id,
                club_id=club_id,
                season_id=season_id,
                license_id=license_id,
                valid_from=valid_from,
                valid_to=valid_to
            )

            # Save to database
            result = player_license.save_to_db(cursor)
            db_results.append(result)
            logging.debug(f"Processed row_id {row_id} in {time.time() - row_start:.2f} seconds")

        print_db_insert_results(db_results)
        logging.info(f"Total processing time: {time.time() - start_time:.2f} seconds")

    except Exception as e:
        logging.error(f"Error processing player licenses: {e}")
        print(f"❌ Error processing player licenses: {e}")
        return db_results

    finally:
        conn.commit()
        conn.close()
        logging.info("-------------------------------------------------------------------")

if __name__ == "__main__":
    upd_player_licenses()