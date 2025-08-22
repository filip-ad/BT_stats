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
from utils import normalize_key, print_db_insert_results, parse_date, sanitize_name, OperationLogger

def upd_player_transitions():
    conn, cursor = get_conn()
    db_results = []
    logger = OperationLogger(
    verbosity       = 2, 
    print_output    = False, 
    log_to_db       = False, 
    cursor          = cursor
    )

    try:
        logging.info("Updating player transitions...")
        print("ℹ️  Updating player transitions...")
        start_time = time.time()

        # Cache mappings
        cache_start = time.time()
        club_name_cache         = Club.cache_name_map(cursor)
        seasons_cache           = Season.cache_by_ext(cursor)
        player_name_year_cache  = Player.cache_name_year_map(cursor)
        player_license_cache    = PlayerLicense.cache_all(cursor)

        earliest_season_id = min(s.season_id for s in seasons_cache.values() if s.season_id is not None)

        logging.info(f"Cached {len(club_name_cache)} club names, {len(seasons_cache)} seasons, "
                     f"{len(player_name_year_cache)} players, {len(player_license_cache)} licenses "
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
            # logging.warning("No player transition data found in player_transition_raw.")
            logger.failed("", "No player transition data found")
            return db_results

        player_cache_misses = 0
        transitions = []

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

            item_key = f"{firstname}, {lastname}, {year_born}, {club_from}, {club_to}, {transition_date_str}, row_id: {row_id}"

            # Capitalize firstname and lastname
            firstname = sanitize_name(firstname)
            lastname = sanitize_name(lastname)

            # Parse transition date
            transition_date = parse_date(transition_date_str, context=f"row_id: {row_id}")
            if transition_date is None:
                logger.failed(item_key, "Invalid transition date format")
                # db_results.append({
                #     "status": "failed",
                #     "row_id": row_id,
                #     "reason": "Invalid transition date format"
                # })
                continue
            
            # Resolve clubs
            # club_from_obj   = club_name_cache.get(normalize_key(club_from))
            # club_to_obj     = club_name_cache.get(normalize_key(club_to))

            club_from_obj = Club.resolve(cursor, club_from, club_name_cache, logger, item_key, allow_prefix=True)
            club_to_obj   = Club.resolve(cursor, club_to, club_name_cache, logger, item_key, allow_prefix=True)

            if not club_from_obj or not club_to_obj:
                logger.failed(item_key, "Could not resolve club_from or club_to")
                continue
            club_id_from    = club_from_obj.club_id
            club_id_to      = club_to_obj.club_id




            # 1) Try by the raw ext ID from your source
            season = seasons_cache.get(season_id_ext)
            if season:
                season_id = season.season_id

            else:
                # 2) No ext match → try to find by date
                #   a) first in your in-memory cache
                season = next(
                    (s for s in seasons_cache.values() if s.contains_date(transition_date)),
                    None
                )
                #   b) or fall back to the DB
                if not season:
                    season = Season.get_by_date(cursor, transition_date)

                if not season:
                    logger.failed(item_key, "No matching season found for transition date")
                    continue

                season_id = season.season_id            

            # lookup players by name/year
            player_key = (firstname, lastname, year_born)
            candidates = player_name_year_cache.get(player_key)
            if not candidates:
                # fallback to DB search
                candidates = Player.search_by_name_and_year(cursor, firstname, lastname, year_born)
            if not candidates:
                player_cache_misses += 1
                logger.failed(item_key, "No players found matching name and year born")
                continue

            # filter by license validity
            seasons_range = range(earliest_season_id, season_id + 1)
            valid = [
                p for p in candidates
                if PlayerLicense.has_license(player_license_cache, p.player_id, club_id_from, seasons_range)
            ]

            # zero valid players
            if not valid:
                logger.failed(item_key, "No valid licensed players found")
                continue
            
            # multiple valid players
            if len(valid) > 1:
                valid_ids = [p.player_id for p in valid]
                logger.failed(item_key, "Multiple valid players found with licenses in departing club in previous season(s)")
                continue

            # exactly one valid player
            player_id = valid[0].player_id

            # Create PlayerTransition object
            transitions.append(
                PlayerTransition(
                    season_id=season_id,
                    player_id=player_id,
                    club_id_from=club_id_from,
                    club_id_to=club_id_to,
                    transition_date=transition_date_str
                )
            )

        insert_time_start = time.time()
        logging.info(f"Inserting {len(transitions)} player transitions...")    

        for t in transitions:
            t.save_to_db(cursor, logger)

        insert_time_stop = time.time()
        logging.info(f"Inserted {len(transitions)} player transitions in {insert_time_stop - insert_time_start:.2f} seconds")

        logger.summarize()
        conn.commit()
        logging.info(f"Completed in {time.time() - start_time:.2f}s")
        

    except Exception as e:
        logging.error(f"Error processing transitions: {e}")
        print(f"❌ Error processing transitions: {e}")
        return db_results

    finally:
        conn.commit()
        conn.close()
        logging.info("-------------------------------------------------------------------")