# src/utils_scripts/upd_player_rankings.py

from db import get_conn, get_from_db_season
from datetime import datetime
from collections import defaultdict
import logging
from models.player_license import PlayerLicense
from src.models.player import Player
from utils import print_db_insert_results

def upd_player_rankings():
    conn, cursor = get_conn()
    db_results = []

    try:
        logging.info("Updating player rankings...")
        print("ℹ️  Updating player rankings...")

        cursor.execute('''
            SELECT 
                count(*)
            FROM player_ranking_raw
        ''')

        rows = cursor.fetchone()
        count = rows[0] if rows else 0

        print(f"ℹ️  Found {count} player ranking data points in player_ranking_raw")
        logging.info(f"Found {count} player ranking data points in player_ranking_raw")

        cursor.execute('''
            SELECT 
                row_id,
                run_id,
                run_date,
                player_id_ext,
                firstname,
                lastname,
                year_born,
                club_name,
                points,
                points_change_since_last,
                position_world,
                position   
            FROM player_ranking_raw
        ''')

        rows = cursor.fetchall()

        if not rows:
            print("⚠️ No player ranking data found in player_ranking_raw.")
            logging.warning("No player ranking data found in player_ranking_raw.")
            return []

        for row in rows:
            (row_id, run_id, run_date, player_id_ext, firstname, lastname, year_born, club_name, points, points_change_since_last, position_world, position) = row

            # Map and create scraped player
            player_data = {
                "player_id_ext": player_id_ext,
                "firstname": firstname,
                "lastname": lastname,
                "year_born": year_born
            }
            scraped_player = Player.from_dict(player_data)
            scraped_player.sanitize()
            
            # Retrieve existing player from DB
            db_player = Player.get_by_ext(cursor, player_id_ext)

            if db_player is None:
                db_results.append({
                    "status": "skipped",
                    "player": f"{scraped_player.firstname} {scraped_player.lastname}",
                    "reason": "Player not found in DB, skipping"
                })
                logging.warning(f"Skipped player {scraped_player.firstname} {scraped_player.lastname}: Player not found in DB")
                continue

            # Validate scraped data against DB
            if not db_player.validate_against(scraped_player):
                db_results.append({
                    "status": "failed",
                    "player": f"{scraped_player.firstname} {scraped_player.lastname}",
                    "reason": f"Player data mismatch: DB ({db_player.firstname} {db_player.lastname}, {db_player.year_born}) vs scraped ({scraped_player.firstname} {scraped_player.lastname}, {scraped_player.year_born})"
                })
                logging.error(f"Failed for player {scraped_player.firstname} {scraped_player.lastname}: Data mismatch")
                continue

            # If valid, proceed with player_id from db_player
            player_id = db_player.player_id
            db_results.append({
                "status": "success",
                "player": f"{scraped_player.firstname} {scraped_player.lastname}",
                "reason": "Player validated successfully"
            })

            # Validate ranking fields (add more validations as needed)
            if not all([run_id, run_date, points is not None, points_change_since_last is not None, position_world, position]):
                logging.warning(f"Missing required ranking fields for player_id_ext {player_id_ext}")
                db_results.append({
                    "status": "skipped",
                    "player": f"{scraped_player.firstname} {scraped_player.lastname}",
                    "reason": "Missing required ranking fields"
                })
                continue

            try:
                # Insert into player_ranking
                cursor.execute("""
                    INSERT OR IGNORE INTO player_ranking (
                        run_id, run_date, player_id, points, points_change_since_last, position_world, position
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (run_id, run_date, player_id, points, points_change_since_last, position_world, position))

                if cursor.rowcount > 0:
                    db_results.append({
                        "status": "success",
                        "player": f"{scraped_player.firstname} {scraped_player.lastname}",
                        "reason": "Ranking inserted successfully"
                    })
                    logging.info(f"Inserted ranking for player {scraped_player.firstname} {scraped_player.lastname}")
                else:
                    db_results.append({
                        "status": "skipped",
                        "player": f"{scraped_player.firstname} {scraped_player.lastname}",
                        "reason": "Ranking already exists"
                    })
                    logging.info(f"Skipped existing ranking for player {scraped_player.firstname} {scraped_player.lastname}")

                # Optionally, delete the processed row from player_ranking_raw
                # cursor.execute("DELETE FROM player_ranking_raw WHERE row_id = ?", (row_id,))

            except Exception as e:
                logging.error(f"Error inserting ranking for player_id_ext {player_id_ext}: {e}")
                db_results.append({
                    "status": "failed",
                    "player": f"{scraped_player.firstname} {scraped_player.lastname}",
                    "reason": f"Insertion error: {e}"
                })

        # Commit changes after processing all rows
        conn.commit()

        # Print results summary
        print_db_insert_results(db_results)

    except Exception as e:
        logging.error(f"Error in upd_player_rankings: {e}")
        conn.rollback()  # Rollback on error
    finally:
        cursor.close()
        conn.close()

    return db_results