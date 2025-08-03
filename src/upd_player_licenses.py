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
from utils import parse_date, print_db_insert_results

def upd_player_licenses():
    conn, cursor = get_conn()
    db_results = []

    try:
        logging.info("Updating player licenses...")
        print("ℹ️  Updating player licenses...")
        start_time = time.time()

        # Cache mappings   
        cache_start = time.time()
        season_map = Season.cache_all(cursor) # Dict[int, Season]
        club_name_map = Club.cache_name_map(cursor)  # Dict[str, Club]
        player_id_ext_map = Player.cache_id_ext_map(cursor)  # Dict[int, Player]
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
        print(f"ℹ️  Found {count} player licenses in player_license_raw".replace(",", " "))
        logging.info(f"Found {count} player licenses in player_license_raw")
        logging.info(f"Fetched {count} rows in {time.time() - fetch_start:.2f} seconds")

        if not rows:
            print("⚠️ No player license data found in player_license_raw.")
            logging.warning("No player license data found in player_license_raw.")
            return []

        # License regex to parse license_info_raw
        license_regex = re.compile(r"(?P<type>(?:[A-D]-licens|48-timmarslicens|Paralicens))(?: (?P<age>\w+))? \((?P<date>\d{4}\.\d{2}\.\d{2})\)")

        player_cache_misses = 0
        club_cache_misses = 0
        license_cache_misses = 0
        batch = []
        licenses = []
        
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

            # Parse valid_from date
            valid_from = parse_date(license_date, context=f"license_part: {license_part}")
            if valid_from is None:
                db_results.append({
                    "status": "failed",
                    "row_id": row_id,
                    "reason": "Date parsing error (valid_from)"
                })
                continue

            # Fetch season
            season = season_map.get(season_label)
            if not season:
                logging.debug(f"No season in cache for season_label {season_label}, row_id {row_id}")
                season_id = None  # Will be validated in batch
            else:
                season_id = season.season_id
                valid_to = season.season_end_date
            # if not season:
            #     logging.warning(f"No matching season found for season_label '{season_label}' for row_id {row_id}")
            #     db_results.append({
            #         "status": "failed",
            #         "row_id": row_id,
            #         "reason": "No matching season found for season_label"
            #     })
            #     continue
            # season_id = season.season_id
            # valid_to = season.season_end_date

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

            # Map player using player_id_ext
            player = player_id_ext_map.get(player_id_ext)
            if not player:
                player_cache_misses += 1
                logging.debug(f"No player in cache for player_id_ext {player_id_ext}, row_id {row_id}")
                player_id = None  # Will be validated in batch
            else:
                player_id = player.player_id
            # if not player:
            #     # Fallback to direct lookup
            #     logging.warning(f"No player found for player_id_ext {player_id_ext} in cache, trying direct lookup for row_id {row_id}")
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

            # Map club using club_name
            club = club_name_map.get(club_name)
            if not club:
                club_cache_misses += 1
                logging.debug(f"No club in cache for name {club_name}, row_id {row_id}")
                club_id = None  # Will be validated in batch
            else:
                club_id = club.club_id
            # if not club:
            #     club_cache_misses += 1
            #     logging.warning(f"No club found for name {club_name} in cache, trying club_id_ext {club_id_ext} for row_id {row_id}")
            #     club = Club.get_by_id_ext(cursor, club_id_ext)
            #     if not club:
            #         logging.warning(f"Foreign key violation: club_id_ext {club_id_ext} does not exist in club_alias table, row_id {row_id}")
            #         db_results.append({
            #             "status": "failed",
            #             "row_id": row_id,
            #             "reason": "Foreign key violation: club_id_ext does not exist in club_alias table"
            #         })
            #         continue
            # club_id = club.club_id

            # Map type and age_group to license_id
            license = license_map.get((type, age_group))
            if not license:
                license_cache_misses += 1
                logging.debug(f"No license in cache for type {type}, age_group {age_group}, row_id {row_id}")
                license_id = None  # Will be validated in batch
            else:
                license_id = license.license_id
            # if not license:
            #     license_cache_misses += 1
            #     logging.warning(f"No license found for type {type} and age_group {age_group} in cache, trying direct lookup for row_id {row_id}")
            #     license = License.get_by_type_and_age(cursor, type, age_group)
            #     if not license:
            #         logging.warning(f"Foreign key violation: type {type} and age_group {age_group} not found in license table, row_id {row_id}")
            #         db_results.append({
            #             "status": "failed",
            #             "row_id": row_id,
            #             "reason": "Foreign key violation: type and age_group not found in license table"
            #         })
            #         continue
            # license_id = license.license_id

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

            # # Save to database
            # result = player_license.save_to_db(cursor)
            # db_results.append(result)
            # logging.debug(f"Processed row_id {row_id} in {time.time() - row_start:.2f} seconds")

        # Batch validate licenses
        batch_start = time.time()
        validation_results = PlayerLicense.validate_batch(cursor, licenses)
        db_results.extend(validation_results)

        # Extract valid licenses for saving
        valid_licenses = [licenses[i] for i, result in enumerate(validation_results) if result["status"] == "success"]

        # Batch save valid licenses
        save_results = PlayerLicense.batch_save_to_db(cursor, valid_licenses)
        # # Merge save results into db_results
        # success_indices = [i for i, result in enumerate(validation_results) if result["status"] == "success"]
        # for i, save_result in zip(success_indices, save_results):
        #     db_results[i] = save_result
        # logging.info(f"Batch validation and insert completed in {time.time() - batch_start:.2f} seconds")
        # logging.info(f"Player cache misses: {player_cache_misses}, Club cache misses: {club_cache_misses}, License cache misses: {license_cache_misses}")
        # Merge save results into db_results, preserving validation messages
        offset = len(db_results) - len(validation_results)
        save_index = 0
        for j in range(len(validation_results)):
            i = offset + j
            if validation_results[j]["status"] == "success":
                db_results[i] = save_results[save_index]
                save_index += 1
        logging.info(f"Batch validation and insert completed in {time.time() - batch_start:.2f} seconds")
        logging.info(f"Player cache misses: {player_cache_misses}, Club cache misses: {club_cache_misses}, License cache_misses: {license_cache_misses}")
        
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


# src/upd_player_licenses.py

# import logging
# import time
# import re

# from db import get_conn
# from utils import parse_date, print_db_insert_results

# from models.player_license import PlayerLicense
# from models.season import Season
# from models.club import Club
# from models.player import Player
# from models.license import License


# def scrape_and_parse_raw_licenses(cursor) -> list[PlayerLicense]:
#     """
#     Read raw license rows, parse and validate them into PlayerLicense objects.
#     Skips any rows that fail parsing or lookups.
#     """
#     # 1) Build caches
#     season_map       = Season.cache_all(cursor)            # season_label -> Season
#     club_name_map    = Club.cache_name_map(cursor)         # club_name -> Club
#     player_id_map    = Player.cache_id_ext_map(cursor)     # player_id_ext -> Player
#     license_map      = License.cache_all(cursor)           # (type,age_group) -> License

#     # 2) Regex to parse "Type Age (YYYY.MM.DD)"
#     lic_regex = re.compile(
#         r"(?P<type>(?:[A-D]-licens|48-timmarslicens|Paralicens))"
#         r"(?: (?P<age>\w+))? \((?P<date>\d{4}\.\d{2}\.\d{2})\)"
#     )

#     cursor.execute("""
#         SELECT row_id, season_label, club_name, player_id_ext, license_info_raw
#         FROM player_license_raw
#     """)
#     rows = cursor.fetchall()

#     parsed: list[PlayerLicense] = []
#     for row_id, season_lbl, club_name, player_ext, raw_info in rows:
#         lic_part = (raw_info or "").strip()
#         m = lic_regex.search(lic_part)
#         if not m:
#             logging.warning(f"Invalid license format [{row_id}]: {lic_part}")
#             continue

#         lic_type   = m.group("type").capitalize()
#         age_group  = m.group("age").capitalize() if m.group("age") else None
#         date_str   = m.group("date")
#         valid_from = parse_date(date_str, context=f"row_id {row_id}")
#         if not valid_from:
#             logging.warning(f"Could not parse date [{row_id}]: {date_str}")
#             continue

#         # Lookup season
#         season = season_map.get(season_lbl)
#         if not season:
#             logging.warning(f"No season '{season_lbl}' for row {row_id}")
#             continue
#         season_id  = season.season_id
#         valid_to   = season.season_end_date

#         # Ensure valid_from in season range
#         if not (season.season_start_date <= valid_from < valid_to):
#             logging.warning(f"Date {valid_from} out of season range for '{season_lbl}' ({row_id})")
#             continue

#         # Lookup player
#         player = player_id_map.get(player_ext)
#         if not player:
#             logging.warning(f"No player_ext {player_ext} for row {row_id}")
#             continue
#         player_id = player.player_id

#         # Lookup club
#         club = club_name_map.get(club_name.strip())
#         if not club:
#             logging.warning(f"No club '{club_name}' for row {row_id}")
#             continue
#         club_id = club.club_id

#         # Lookup license type
#         lic_obj = license_map.get((lic_type, age_group))
#         if not lic_obj:
#             logging.warning(f"No License({lic_type},{age_group}) for row {row_id}")
#             continue
#         license_id = lic_obj.license_id

#         # Create PlayerLicense instance
#         parsed.append(PlayerLicense(
#             player_id=player_id,
#             club_id=club_id,
#             season_id=season_id,
#             license_id=license_id,
#             valid_from=valid_from,
#             valid_to=valid_to,
#             row_id=row_id
#         ))

#     return parsed


# def upd_player_licenses():
#     conn, cursor = get_conn()
#     try:
#         logging.info("Updating player licenses…")
#         print("ℹ️  Updating player licenses…")

#         # 1) Scrape & parse into model objects
#         all_licenses = scrape_and_parse_raw_licenses(cursor)
#         logging.info(f"Parsed {len(all_licenses)} candidate licenses")

#         # 2) Bulk‐insert with caching + INSERT OR IGNORE
#         start = time.time()
#         results = PlayerLicense.batch_save(cursor, all_licenses)
#         conn.commit()
#         logging.info(f"Batch insert completed in {time.time() - start:.2f}s")

#         # 3) Report
#         print_db_insert_results(results)

#     except Exception as e:
#         logging.error(f"Error updating player licenses: {e}")
#         conn.rollback()
#         print(f"❌ Error updating player licenses: {e}")

#     finally:
#         conn.close()


# if __name__ == "__main__":
#     upd_player_licenses()
