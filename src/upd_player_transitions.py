# # src/upd_player_transitions.py


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
        club_name_map           = Club.cache_name_map(cursor)
        seasons_cache           = Season.cache_by_ext(cursor)
        player_map              = Player.cache_all(cursor)
        player_name_year_map    = Player.cache_name_year_map(cursor)
        
        # Cache player licenses for validation
        cursor.execute("""
            SELECT player_id, club_id, season_id
            FROM player_license
        """)
        license_map = {(row[0], row[1], row[2]) for row in cursor.fetchall()}  # Set of (player_id, club_id, season_id)

        logging.info(f"Cached {len(club_name_map)} club names, {len(seasons_cache)} seasons, "
                     f"{len(player_name_year_map)} players, {len(license_map)} licenses "
                     f"in {time.time() - cache_start:.2f} seconds")
        logging.info(f"Cached mappings in {time.time() - cache_start:.2f} seconds")

        # Fetch raw data
        fetch_start = time.time()
        cursor.execute('''
            SELECT 
                row_id, 
                season_id_ext, 
                season_label, 
                firstname, 
                lastname, 
                date_born, 
                year_born, 
                club_from, 
                club_to, 
                transition_date
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

        player_cache_misses = 0
        transitions_to_insert = []

        for row in rows:
            row_start = time.time()
            (row_id, 
             season_id_ext, 
             season_label, 
             firstname, 
             lastname, 
             date_born, 
             year_born, 
             club_from, 
             club_to, 
             transition_date_str) = row

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

            # Normalize and look up club names
            club_from_obj   = club_name_map.get(Club._normalize(club_from))
            club_to_obj     = club_name_map.get(Club._normalize(club_to))

            if not club_from_obj or not club_to_obj:
                logging.warning(f"Could not resolve club_from '{club_from}' or club_to '{club_to}' for row_id {row_id}. Player name {firstname} {lastname}, year_born {year_born}")
                db_results.append({
                    "status": "skipped",
                    "key":    f"{club_from}, {club_to}",
                    "reason": "Could not resolve club_from or club_to"
                })
                continue
            club_id_from    = club_from_obj.club_id
            club_id_to      = club_to_obj.club_id

            
            # Get earliest season_id
            earliest_season_id = min(season.season_id for season in seasons_cache.values() if season.season_id is not None)

            # Get season for transition date
            season = seasons_cache.get(season_id_ext)
            if not season:
                logging.warning(f"Unknown season_id_ext={season_id_ext} for transition row {row_id}")
                db_results.append({
                    "status":   "failed", 
                    "key":      season_id_ext, 
                    "reason":   "Unknown season"
                })
                continue
            for s in seasons_cache.values():
                if s.contains_date(transition_date):
                    season = s
                    break
            if not season:
                # Fallback to Season.get_by_date
                season = Season.get_by_date(cursor, transition_date)
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
            player_key = (firstname, lastname, year_born if year_born else 0)
            player = player_name_year_map.get(player_key)
            potential_players = Player.search_by_name_and_year(cursor, firstname, lastname, year_born)
            if not potential_players:
                player_cache_misses += 1
                logging.warning(f"No players found matching name '{firstname} {lastname}' and year_born {year_born} for row_id {row_id}. Club from: {club_from}, Club to: {club_to}")
                db_results.append({
                    "status": "skipped",
                    "row_id": row_id,
                    "reason": "No players found with matching name and year born"
                })
                continue

            valid_player = None
            if len(potential_players) > 1:
                # Handle multiple matches
                valid_players = [
                    p for p in potential_players
                    if any((p.player_id, club_id_from, s_id) in license_map for s_id in range(earliest_season_id, season_id + 1))
                ]
                if len(valid_players) == 1:
                    valid_player = valid_players[0]
                elif len(valid_players) > 1:
                    logging.warning(f"Multiple valid players with licenses for '{firstname} {lastname}' (year_born {year_born}) in row_id {row_id}. Player IDs: {[p.player_id for p in valid_players]}")
                    db_results.append({
                        "status": "skipped",
                        "row_id": row_id,
                        "reason": "Multiple valid players found with licenses in club and season"
                    })
                    continue
                else:
                    logging.warning(f"No valid license found for any player matching '{firstname} {lastname}' (year_born {year_born}) in club {club_from} for season_id <= {season_id}, row_id {row_id}")
                    db_results.append({
                        "status": "skipped",
                        "row_id": row_id,
                        "reason": "No valid license found for player in departing club in any previous seasons"
                    })
                    continue
            else:
                # Single player match
                valid_player = potential_players[0]
                if season_id != earliest_season_id:
                    has_license = any((valid_player.player_id, club_id_from, s_id) in license_map for s_id in range(earliest_season_id, season_id + 1))
                    if not has_license:
                        logging.warning(f"No valid license found for player '{firstname} {lastname}' (player_id {valid_player.player_id}, year_born {year_born}) in club {club_from} for season_id <= {season_id}, row_id {row_id}")
                        db_results.append({
                            "status": "skipped",
                            "row_id": row_id,
                            "reason": "No valid license found for player in departing club in any previous seasons"
                        })
                        continue

            if not valid_player:
                logging.warning(f"No valid player selected for '{firstname} {lastname}' (year_born {year_born}) in row_id {row_id}")
                db_results.append({
                    "status": "skipped",
                    "row_id": row_id,
                    "reason": "No valid player selected after license validation"
                })
                continue
            player_id = valid_player.player_id

            # Create PlayerTransition object
            transition = PlayerTransition(
                season_id=season_id,
                player_id=player_id,
                club_id_from=club_id_from,
                club_id_to=club_id_to,
                transition_date=transition_date_str
            )
            transitions_to_insert.append(transition)
            logging.debug(f"Processed row_id {row_id} in {time.time() - row_start:.4f} seconds")

        # Batch insert transitions
        batch_start = time.time()
        batch = [(t.season_id, t.player_id, t.club_id_from, t.club_id_to, t.transition_date) for t in transitions_to_insert]
        if batch:
            cursor.executemany("""
                INSERT OR IGNORE INTO player_transition (season_id, player_id, club_id_from, club_id_to, transition_date)
                VALUES (?, ?, ?, ?, ?)
            """, batch)
            inserted_count = cursor.rowcount
            total = len(batch)
            skipped_count = total - inserted_count

            logging.info(f"Batch inserted {inserted_count} transitions "
                 f"(skipped {skipped_count} duplicates) "
                 f"in {time.time() - batch_start:.2f} seconds")

            # Add results for inserted rows (simplified, since row_id not in batch)
            db_results.extend([{
                "status": "success",
                "reason": "Inserted player transition"
            } for _ in range(inserted_count)])

            # Add skipped results for duplicates
            db_results.extend([{
                "status": "skipped",
                "reason": "Transition record already exists"
            } for _ in range(skipped_count)])

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