# # src/upd_player_ranking_group.py

import logging
import time
from models.player_ranking_group import PlayerRankingGroup
from models.player import Player
from db import get_conn
from utils import print_db_insert_results

def upd_player_ranking_groups():
    conn, cursor = get_conn()
    logging.info("Updating player ranking groups...")
    print("ℹ️  Updating player ranking groups...")
    start_time = time.time()

    try:
        # Check initial row count
        cursor.execute("SELECT COUNT(*) FROM player_ranking_group")
        initial_count = cursor.fetchone()[0]
        logging.info(f"Initial rows in player_ranking_group: {initial_count}")

        # Load mapping from player_id_ext to canonical player_id
        cursor.execute("SELECT player_id_ext, player_id FROM player_alias")
        player_id_map = {row[0]: row[1] for row in cursor.fetchall()}

        # Load mapping from ranking_group name to id
        cursor.execute("SELECT class_short, ranking_group_id FROM ranking_group")
        ranking_group_map = {row[0]: row[1] for row in cursor.fetchall()}

        # Step 1: Delete all existing ranking groups for relevant players
        cursor.execute("""
            SELECT DISTINCT player_id_ext
            FROM player_license_raw
            WHERE player_id_ext IS NOT NULL AND TRIM(ranking_group_raw) != ''
        """)
        player_id_exts = [row[0] for row in cursor.fetchall()]
        deleted_count = 0
        deleted_players = set()

        for player_id_ext in player_id_exts:
            player_id = player_id_map.get(player_id_ext)
            if not player_id:
                logging.debug(f"Skipping player_id_ext {player_id_ext}: No matching player in player_alias")
                continue
            delete_result = PlayerRankingGroup.delete_by_player_id(cursor, player_id)
            if delete_result["status"] == "success":
                deleted_rows = int(delete_result["reason"].split()[1])
                if deleted_rows > 0:
                    deleted_count += deleted_rows
                    deleted_players.add(player_id)
                    logging.debug(f"Deleted {deleted_rows} ranking group rows for player_id {player_id}")
                else:
                    logging.debug(f"No ranking group rows to delete for player_id {player_id}")
            else:
                logging.error(f"Failed to delete ranking groups for player_id {player_id}: {delete_result['reason']}")

        print(f"🗑️  Deleted {deleted_count} existing player ranking group rows for {len(deleted_players)} players")
        logging.info(f"Deleted {deleted_count} existing player_ranking_group rows for {len(deleted_players)} players")

        # Step 2: Insert new ranking groups
        insert_start = time.time()
        cursor.execute("""
            SELECT player_id_ext, ranking_group_raw
            FROM player_license_raw
            WHERE player_id_ext IS NOT NULL AND TRIM(ranking_group_raw) != ''
        """)
        rows = cursor.fetchall()
        logging.info(f"Fetched {len(rows)} rows from player_license_raw in {time.time() - insert_start:.2f} seconds")

        seen = set()
        db_results = []

        for row in rows:
            row_start = time.time()
            player_id_ext, raw_groups = row
            player_id = player_id_map.get(player_id_ext)
            if not player_id:
                logging.debug(f"Skipping player_id_ext {player_id_ext}: No matching player in player_alias")
                continue

            groups = [g.strip() for g in raw_groups.split(",") if g.strip()]
            for group_name in groups:
                ranking_group_id = ranking_group_map.get(group_name)
                if not ranking_group_id:
                    logging.warning(f"Unknown ranking group: {group_name}")
                    print(f"⚠️ Unknown ranking group: {group_name}")
                    continue

                key = (player_id, ranking_group_id)
                if key in seen:
                    logging.debug(f"Skipped duplicate ranking group for player_id {player_id}, ranking_group_id {ranking_group_id}")
                    continue
                seen.add(key)

                rel = PlayerRankingGroup(player_id=player_id, ranking_group_id=ranking_group_id)
                result = rel.save_to_db(cursor)
                db_results.append(result)
            logging.debug(f"Processed row for player_id_ext {player_id_ext} in {time.time() - row_start:.2f} seconds")

        # Print results
        print_db_insert_results(db_results)
        logging.info(f"Insert completed in {time.time() - insert_start:.2f} seconds")

        # Verify final row count
        cursor.execute("SELECT COUNT(*) FROM player_ranking_group")
        final_count = cursor.fetchone()[0]
        logging.info(f"Final rows in player_ranking_group: {final_count}")

        logging.info(f"Total processing time: {time.time() - start_time:.2f} seconds")

    except Exception as e:
        logging.error(f"Error updating player ranking groups: {e}")
        print(f"❌ Error updating player ranking groups: {e}")

    finally:
        conn.commit()
        conn.close()
        logging.info("-------------------------------------------------------------------")

if __name__ == "__main__":
    upd_player_ranking_groups()