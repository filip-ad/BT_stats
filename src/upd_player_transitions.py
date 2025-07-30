# # src/upd_player_transitions.py

# from datetime import timedelta
# from db import get_conn
# import logging
# from models.player_license import PlayerLicense
# from models.season import Season
# from models.club import Club
# from models.player import Player
# from models.player_transition import PlayerTransition
# from utils import print_db_insert_results, parse_date, sanitize_name

# def upd_player_transitions():
#     conn, cursor = get_conn()
#     db_results = []

#     try:
#         logging.info("Updating player transitions...")
#         print("ℹ️  Updating player transitions...")

#         cursor.execute('''
#             SELECT row_id, season_id_ext, season_label, firstname, lastname, date_born, year_born, club_from, club_to, transition_date
#             FROM player_transition_raw
#         ''')

#         rows = cursor.fetchall()

#         print(f"ℹ️  Processing {len(rows)} player transitions...")
#         logging.info(f"Processing {len(rows)} player transitions...")

#         if not rows:
#             print("⚠️ No player transition data found in player_transition_raw.")
#             logging.warning("No player transition data found in player_transition_raw.")
#             return db_results
        
#         cursor.execute("SELECT MIN(season_id) FROM season")
#         earliest_season_id = cursor.fetchone()[0] or 1

#         for row in rows:
#             (row_id, season_id_ext, season_label, firstname, lastname, date_born, year_born, club_from, club_to, transition_date_str) = row

#             # Capitalize firstname and lastname
#             firstname = sanitize_name(firstname)
#             lastname = sanitize_name(lastname)

#             # Parse transition date
#             transition_date = parse_date(transition_date_str, context=f"row_id: {row_id}")
#             if transition_date is None:
#                 db_results.append({
#                     "status": "failed",
#                     "row_id": row_id,
#                     "reason": "Invalid transition date format"
#                 })
#                 continue

#             # Get club_from and club_to IDs
#             club_from_obj = Club.get_by_name(cursor, club_from, exact=False)
#             club_to_obj = Club.get_by_name(cursor, club_to, exact=False)
#             if not club_from_obj or not club_to_obj:
#                 logging.warning(f"Could not resolve club_from '{club_from}' or club_to '{club_to}' for row_id {row_id}. Player name {firstname} {lastname}, year_born {year_born}")
#                 db_results.append({
#                     "status": "skipped",
#                     "row_id": row_id,
#                     "reason": "Could not resolve club_from or club_to"
#                 })
#                 continue
#             club_id_from = club_from_obj.club_id
#             club_id_to = club_to_obj.club_id      

#             # Get season for transition date
#             season = Season.get_by_date(cursor, transition_date)
#             if not season:
#                 logging.warning(f"No matching season for transition date {transition_date} in row_id {row_id}")
#                 db_results.append({
#                     "status": "failed",
#                     "row_id": row_id,
#                     "reason": "No matching season for transition date"
#                 })
#                 continue
#             season_id = season.season_id

#             # Find matching players by name and year_born
#             potential_players = Player.search_by_name_and_year(cursor, firstname, lastname, year_born)
#             if not potential_players:
#                 logging.warning(f"No players found matching name '{firstname} {lastname}' and year_born {year_born} for row_id {row_id}. Club from: {club_from}, Club to: {club_to}")
#                 db_results.append({
#                     "status": "skipped",
#                     "row_id": row_id,
#                     "reason": "No players found with matching name and year born"
#                 })
#                 continue

#             # Validate with previous license if not the earliest season
#             valid_player_id = None
#             if season_id == earliest_season_id:
#                 logging.debug(f"Earliest season (season_id {season_id}) for transition in row_id {row_id}, skipping license check")
#                 if len(potential_players) == 1:
#                     valid_player_id = potential_players[0].player_id
#                 else:
#                     logging.warning(f"Multiple players found for '{firstname} {lastname}' (year_born {year_born}) in row_id {row_id}, cannot resolve without license")
#                     db_results.append({
#                         "status": "skipped",
#                         "row_id": row_id,
#                         "reason": "Multiple players found for earliest season, cannot resolve without license"
#                     })
#                     continue
#             else:
#                 # Check licenses in all seasons up to the current season for club_from
#                 valid_player_id = None
#                 if season_id == earliest_season_id:
#                     logging.info(f"Earliest season (season_id {season_id}) for transition in row_id {row_id}, skipping license check")
#                     if len(potential_players) == 1:
#                         valid_player_id = potential_players[0].player_id
#                     else:
#                         logging.warning(f"Multiple players found for '{firstname} {lastname}' (year_born {year_born}) in row_id {row_id}, cannot resolve without license")
#                         db_results.append({
#                             "status": "skipped",
#                             "row_id": row_id,
#                             "reason": "Multiple players found for earliest season, cannot resolve without license"
#                         })
#                         continue
#                 else:
#                     # Check licenses in all seasons up to the current season for club_from
#                     valid_player_ids = []
#                     for player in potential_players:
#                         cursor.execute("""
#                             SELECT player_id, season_id
#                             FROM player_license
#                             WHERE player_id = ? AND club_id = ? AND season_id <= ?
#                         """, (player.player_id, club_id_from, season_id))
#                         licenses = cursor.fetchall()
#                         if licenses:
#                             valid_player_ids.append(player.player_id)

#                     if not valid_player_ids:
#                         logging.warning(f"No valid license found for player '{firstname} {lastname}' (year_born {year_born}) in club {club_from} for season_id <= {season_id} in row_id {row_id}")
#                         db_results.append({
#                             "status": "skipped",
#                             "row_id": row_id,
#                             "reason": "No valid license found for player in departing club in any previous seasons"
#                         })
#                         continue
#                     elif len(valid_player_ids) > 1:
#                         logging.warning(f"Multiple valid players found with licenses for '{firstname} {lastname}' (year_born {year_born}) in row_id {row_id}. Valid player IDs: {valid_player_ids}")
#                         db_results.append({
#                             "status": "skipped",
#                             "row_id": row_id,
#                             "reason": "Multiple valid players found with licenses in club and season"
#                         })
#                         continue
#                     else:
#                         valid_player_id = valid_player_ids[0]

#             # Create PlayerTransition object
#             transition = PlayerTransition(
#                 season_id=season_id,
#                 player_id=valid_player_id,
#                 club_id_from=club_id_from,
#                 club_id_to=club_id_to,
#                 transition_date=transition_date_str
#             )

#             # Save to database
#             result = transition.save_to_db(cursor)
#             db_results.append(result)
            
#         print_db_insert_results(db_results)

#     except Exception as e:
#         logging.error(f"Failed to process licenses: {e}")
#         print(f"❌ Failed to process licenses: {e}")
#         return db_results

#     finally:
#         conn.commit()
#         conn.close()
#         logging.info("-------------------------------------------------------------------")

# src/upd_player_transitions.py

import logging
import time
from datetime import timedelta
from db import get_conn
from models.player_license import PlayerLicense
from models.season import Season
from models.club import Club
from models.player import Player
from models.player_transition import PlayerTransition
from utils import print_db_insert_results, parse_date, sanitize_name

def upd_player_transitions():
    conn, cursor = get_conn()
    db_results = []

    try:
        logging.info("Updating player transitions...")
        print("ℹ️  Updating player transitions...")
        start_time = time.time()

        # Cache mappings
        cache_start = time.time()
        cursor.execute("SELECT name, club_id FROM club_alias UNION SELECT long_name, club_id FROM club_alias")
        club_map = {row[0]: row[1] for row in cursor.fetchall()}

        cursor.execute("SELECT season_id, season_label, season_start_date, season_end_date FROM season")
        season_map = {row[0]: Season(season_id=row[0], season_label=row[1], season_start_date=row[2], season_end_date=row[3]) for row in cursor.fetchall()}

        cursor.execute("SELECT firstname, lastname, year_born, player_id FROM player_alias")
        player_map = {(sanitize_name(row[0]), sanitize_name(row[1]), row[2]): row[3] for row in cursor.fetchall()}
        logging.info(f"Cached mappings in {time.time() - cache_start:.2f} seconds")

        # Fetch raw data
        fetch_start = time.time()
        cursor.execute('''
            SELECT row_id, season_id_ext, season_label, firstname, lastname, date_born, year_born, club_from, club_to, transition_date
            FROM player_transition_raw
        ''')
        rows = cursor.fetchall()
        print(f"ℹ️  Processing {len(rows)} player transitions...")
        logging.info(f"Processing {len(rows)} player transitions...")
        logging.info(f"Fetched {len(rows)} rows in {time.time() - fetch_start:.2f} seconds")

        if not rows:
            print("⚠️ No player transition data found in player_transition_raw.")
            logging.warning("No player transition data found in player_transition_raw.")
            return db_results
        
        cursor.execute("SELECT MIN(season_id) FROM season")
        earliest_season_id = cursor.fetchone()[0] or 1

        transitions_to_insert = []
        for row in rows:
            row_start = time.time()
            (row_id, season_id_ext, season_label, firstname, lastname, date_born, year_born, club_from, club_to, transition_date_str) = row

            # Capitalize firstname and lastname
            firstname = sanitize_name(firstname)
            lastname = sanitize_name(lastname)

            # Parse transition date
            transition_date = parse_date(transition_date_str, context=f"row_id: {row_id}")
            if transition_date is None:
                db_results.append({
                    "status": "failed",
                    "row_id": row_id,
                    "reason": "Invalid transition date format"
                })
                continue

            # Get club_from and club_to IDs
            club_id_from = club_map.get(club_from)
            club_id_to = club_map.get(club_to)
            if not club_id_from or not club_id_to:
                logging.warning(f"Could not resolve club_from '{club_from}' or club_to '{club_to}' for row_id {row_id}. Player name {firstname} {lastname}, year_born {year_born}")
                db_results.append({
                    "status": "skipped",
                    "row_id": row_id,
                    "reason": "Could not resolve club_from or club_to"
                })
                continue

            # Get season for transition date
            season = None
            for s in season_map.values():
                if s.contains_date(transition_date):
                    season = s
                    break
            if not season:
                logging.warning(f"No matching season for transition date {transition_date} in row_id {row_id}")
                db_results.append({
                    "status": "failed",
                    "row_id": row_id,
                    "reason": "No matching season for transition date"
                })
                continue
            season_id = season.season_id

            # Find matching players by name and year_born
            player_id = player_map.get((firstname, lastname, year_born))
            potential_players = [Player.get_by_id(cursor, player_id)] if player_id else []
            if not potential_players:
                logging.warning(f"No players found matching name '{firstname} {lastname}' and year_born {year_born} for row_id {row_id}. Club from: {club_from}, Club to: {club_to}")
                db_results.append({
                    "status": "skipped",
                    "row_id": row_id,
                    "reason": "No players found with matching name and year born"
                })
                continue

            # Validate with previous license if not the earliest season
            valid_player_id = None
            if season_id == earliest_season_id:
                logging.debug(f"Earliest season (season_id {season_id}) for transition in row_id {row_id}, skipping license check")
                if len(potential_players) == 1:
                    valid_player_id = potential_players[0].player_id
                else:
                    logging.warning(f"Multiple players found for '{firstname} {lastname}' (year_born {year_born}) in row_id {row_id}, cannot resolve without license")
                    db_results.append({
                        "status": "skipped",
                        "row_id": row_id,
                        "reason": "Multiple players found for earliest season, cannot resolve without license"
                    })
                    continue
            else:
                # Check licenses in all seasons up to the current season for club_from
                valid_player_ids = []
                for player in potential_players:
                    cursor.execute("""
                        SELECT player_id, season_id
                        FROM player_license
                        WHERE player_id = ? AND club_id = ? AND season_id <= ?
                    """, (player.player_id, club_id_from, season_id))
                    licenses = cursor.fetchall()
                    if licenses:
                        valid_player_ids.append(player.player_id)

                if not valid_player_ids:
                    logging.warning(f"No valid license found for player '{firstname} {lastname}' (year_born {year_born}) in club {club_from} for season_id <= {season_label} in row_id {row_id}")
                    db_results.append({
                        "status": "skipped",
                        "row_id": row_id,
                        "reason": "No valid license found for player in departing club in any previous seasons"
                    })
                    continue
                elif len(valid_player_ids) > 1:
                    logging.warning(f"Multiple valid players found with licenses for '{firstname} {lastname}' (year_born {year_born}) in row_id {row_id}. Valid player IDs: {valid_player_ids}")
                    db_results.append({
                        "status": "skipped",
                        "row_id": row_id,
                        "reason": "Multiple valid players found with licenses in club and season"
                    })
                    continue
                else:
                    valid_player_id = valid_player_ids[0]

            # Create PlayerTransition object
            transition = PlayerTransition(
                season_id=season_id,
                player_id=valid_player_id,
                club_id_from=club_id_from,
                club_id_to=club_id_to,
                transition_date=transition_date_str
            )
            transitions_to_insert.append(transition)
            logging.debug(f"Processed row_id {row_id} in {time.time() - row_start:.2f} seconds")

        # Batch insert transitions
        batch_start = time.time()
        for transition in transitions_to_insert:
            result = transition.save_to_db(cursor)
            db_results.append(result)
        logging.info(f"Batch insert completed in {time.time() - batch_start:.2f} seconds")

        print_db_insert_results(db_results)
        logging.info(f"Total processing time: {time.time() - start_time:.2f} seconds")

    except Exception as e:
        logging.error(f"Error processing transitions: {e}")
        print(f"❌ Error processing transitions: {e}")
        return db_results

    finally:
        conn.commit()
        conn.close()
        logging.info("-------------------------------------------------------------------")

if __name__ == "__main__":
    upd_player_transitions()