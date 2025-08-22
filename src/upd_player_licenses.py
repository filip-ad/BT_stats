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
from utils import parse_date, OperationLogger

def upd_player_licenses():
    conn, cursor = get_conn()
    db_results = []

    # Set up logging
    # =============================================================================
    logger = OperationLogger(
        verbosity       = 2, 
        print_output    = False, 
        log_to_db       = False, 
        cursor          = cursor
        )

    try:
        logging.info("Updating player licenses...")
        print("ℹ️  Updating player licenses...")
        start_time = time.time()
        seen_final_keys = set()

        # Cache mappings   
        cache_start         = time.time()
        season_map          = Season.cache_all(cursor) 
        club_name_map       = Club.cache_name_map(cursor)
        club_id_ext_map     = Club.cache_id_ext_map(cursor)
        player_id_ext_map   = Player.cache_id_ext_map(cursor)
        license_map         = License.cache_all(cursor)
        logging.info(f"Cached mappings in {time.time() - cache_start:.2f} seconds")

        # Cache duplicate licenses
        duplicate_start = time.time()
        cursor.execute("""
            SELECT 
                CAST(player_id_ext AS TEXT) AS player_id_ext,
                CAST(club_id_ext   AS TEXT) AS club_id_ext,
                CAST(season_id_ext AS TEXT) AS season_id_ext,
                LOWER(TRIM(SUBSTR(license_info_raw, 1, INSTR(license_info_raw, '(') - 1))) AS license_key,
                MIN(row_id) AS min_row_id
            FROM player_license_raw
            GROUP BY 1,2,3,4
            HAVING COUNT(*) > 1
        """)
        duplicate_map = {(r[0], r[1], r[2], r[3]): r[4] for r in cursor.fetchall()}
        logging.info(f"Cached {len(duplicate_map)} duplicate licenses in {time.time() - duplicate_start:.2f} seconds")

        # Fetch raw data
        fetch_start = time.time()
        cursor.execute('''
            SELECT 
                row_id, 
                season_id_ext, 
                season_label, 
                club_name, 
                club_id_ext,
                CAST(player_id_ext AS TEXT) AS player_id_ext_str,
                firstname, 
                lastname, 
                year_born, 
                license_info_raw
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

        # License regex to parse license_info_raw
        license_regex = re.compile(r"(?P<type>(?:[A-D]-licens|48-timmarslicens|Paralicens))(?: (?P<age>\w+))? \((?P<date>\d{4}\.\d{2}\.\d{2})\)")

        player_cache_misses     = 0
        club_cache_misses       = 0
        license_cache_misses    = 0
        licenses                = []
        
        data_source_id = 3
        
        for row in rows:
            (row_id, season_id_ext, label, club_name, club_id_ext, player_id_ext, firstname, lastname, year_born, license_info_raw) = row
            item_key = f"{firstname} {lastname} (id ext: {player_id_ext}, club: {club_name}, season: {season_id_ext}, row_id: {row_id})"

            # Process license_info_raw
            license_part = license_info_raw.strip()
            if not license_part:
                logger.failed(item_key, "Empty license part after splitting for row_id")
                continue

            match = license_regex.search(license_part)
            if not match:
                logger.failed(item_key, f"Invalid license format")
                continue

            type            = match.group("type").strip().capitalize()
            license_date    = match.group("date")
            age_group       = match.group("age").strip().capitalize() if match.group("age") else None

            # Check for duplicates using cached map
            license_key     = f"{type} {age_group}".strip().lower() if age_group else type.lower()
            duplicate_key   = (player_id_ext, club_id_ext, season_id_ext, license_key)
            if duplicate_key in duplicate_map and duplicate_map[duplicate_key] != row_id:
                logger.failed(item_key, f"Duplicate license detected for player_id_ext")
                continue  

            # Parse valid_from date
            valid_from = parse_date(license_date, context=f"license_part: {license_part}")
            if valid_from is None:
                logger.failed(item_key, f"Invalid date format in license_info_raw")
                continue

            # Fetch season
            season = season_map.get(label)
            if not season:
                season_id       = None  # Will be validated in batch
            else:
                season_id       = season.season_id
                valid_to        = season.end_date

            # Check valid_from date against season dates
            if not season.contains_date(valid_from):
                logger.failed(item_key, f"Valid from date is outside the season range")
                continue

<<<<<<< HEAD
            # Skip if valid_from equals season end date
            if valid_from == season.end_date:
                logging.warning(f"Player {firstname} {lastname} {club_name} (ext_id: {player_id_ext}, id: {player_id}) skipped because valid_from ({valid_from}) equals season end date, row_id {row_id}")

=======
>>>>>>> dfbb58977579c213753fb2cf6e59bd4e083f1afb
            # Map player using player_id_ext
            player = player_id_ext_map.get((player_id_ext, data_source_id))
            if not player:
                player_cache_misses += 1
                player_id = None 
            else:
                player_id = player.player_id

            # Map club using club_id_ext
            club = club_id_ext_map.get(club_id_ext)
            if not club:
                # Fallback to club_name if club_id_ext is not found
                club = club_name_map.get(club_name.strip().lower())
            if not club:
                club_cache_misses += 1
                club_id = None
                logger.warning(item_key, f"No club in cache for club_id_ext or name")
            else:
                club_id = club.club_id

            # Map type and age_group to license_id
            license = license_map.get((type, age_group))
            if not license:
                license_cache_misses += 1
                license_id = None
            else:
                license_id = license.license_id

            # After you’ve resolved player_id, club_id, season_id, license_id:
            final_key = (player_id, club_id, season_id, license_id)
            if final_key in seen_final_keys:
                logger.skipped(item_key, "Duplicate license in raw data")
                continue
            seen_final_keys.add(final_key)    

            # Create PlayerLicense object
            player_license = PlayerLicense(
                player_id=player_id,
                club_id=club_id,
                season_id=season_id,
                license_id=license_id,
                valid_from=valid_from,
                valid_to=valid_to,
                row_id=row_id
            )
            licenses.append(player_license)

        # Batch validate licenses
        batch_start = time.time()
        validation_results = PlayerLicense.validate_batch(cursor, licenses, logger)

        # Extract valid licenses for saving
        valid_licenses = [licenses[i] for i, result in enumerate(validation_results) if result["status"] == "success"]

        # Batch save valid licenses
        save_results = PlayerLicense.batch_save_to_db(cursor, valid_licenses, logger)

        logging.info(f"Batch validation and insert completed in {time.time() - batch_start:.2f} seconds")
        logging.info(f"Player cache misses: {player_cache_misses}, Club cache misses: {club_cache_misses}, License cache_misses: {license_cache_misses}")
        
        # print_db_insert_results(db_results)
        logging.info(f"Total processing time: {time.time() - start_time:.2f} seconds")
        logger.summarize()

    except Exception as e:
        logging.error(f"Error processing player licenses: {e}")
        print(f"❌ Error processing player licenses: {e}")
        return db_results

    finally:
        conn.commit()
        conn.close()
        logging.info("-------------------------------------------------------------------")