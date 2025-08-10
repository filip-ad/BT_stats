# src/upd_player_ranking_group.py

import logging
import time
from typing import Dict, Set

from db import get_conn
from utils import print_db_insert_results
from models.player import Player
from models.player_ranking_group import PlayerRankingGroup

def upd_player_ranking_groups():
    """
    Update the player_ranking_group table by:
    1. Deleting existing entries for licensed players.
    2. Inserting new entries based on raw license data.
    This ensures the table reflects the latest ranking groups from licenses.
    """
    conn, cursor = get_conn()
    logging.info("Updating player ranking groups...")
    print("ℹ️  Updating player ranking groups...")
    start_time = time.time()

    try:
        # Log initial row count for verification
        cursor.execute("SELECT COUNT(*) FROM player_ranking_group")
        initial_count = cursor.fetchone()[0]
        logging.info(f"Initial rows in player_ranking_group: {initial_count}")

        # Load ext to player_id mapping using existing Player cache function
        # cache returns Dict[str, Player] since player_id_ext is TEXT
        cache = Player.cache_id_ext_map(cursor)
        ext_to_player_map: Dict[str, int] = {ext: player.player_id for ext, player in cache.items()}

        # Load ranking_group mapping: class_short -> ranking_group_id
        cursor.execute("SELECT class_short, ranking_group_id FROM ranking_group")
        ranking_group_map: Dict[str, int] = {row[0]: row[1] for row in cursor.fetchall()}

        # Step 1: Collect unique player_id_exts from license_raw and derive player_ids for deletion
        cursor.execute("""
            SELECT DISTINCT player_id_ext
            FROM player_license_raw
            WHERE player_id_ext IS NOT NULL AND TRIM(ranking_group_raw) != ''
        """)
        player_id_exts: Set[int] = {row[0] for row in cursor.fetchall()}

        # Map to canonical player_ids (skip unmapped); convert ext to str for lookup
        player_ids_to_delete: Set[int] = {
            ext_to_player_map[str(ext)] for ext in player_id_exts if str(ext) in ext_to_player_map
        }

        if player_ids_to_delete:
            # Bulk delete for efficiency
            placeholders = ','.join(['?'] * len(player_ids_to_delete))
            cursor.execute(f"""
                DELETE FROM player_ranking_group
                WHERE player_id IN ({placeholders})
            """, list(player_ids_to_delete))
            deleted_count = cursor.rowcount
            print(f"🗑️  Deleted {deleted_count} existing player ranking group rows for {len(player_ids_to_delete)} players")
            logging.info(f"Deleted {deleted_count} existing player_ranking_group rows for {len(player_ids_to_delete)} players")
        else:
            deleted_count = 0
            logging.info("No player ranking groups to delete")

        # Step 2: Insert new ranking groups
        insert_start = time.time()
        cursor.execute("""
            SELECT player_id_ext, ranking_group_raw
            FROM player_license_raw
            WHERE player_id_ext IS NOT NULL AND TRIM(ranking_group_raw) != ''
        """)
        rows = cursor.fetchall()
        logging.info(f"Fetched {len(rows)} rows from player_license_raw in {time.time() - insert_start:.2f} seconds")

        seen: Set[Tuple[int, int]] = set()  # (player_id, ranking_group_id) to avoid duplicates
        db_results = []

        for player_id_ext, raw_groups in rows:
            # Convert to str for map lookup
            player_id = ext_to_player_map.get(str(player_id_ext))
            if not player_id:
                logging.debug(f"Skipping player_id_ext {player_id_ext}: No matching player in alias map")
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
                    logging.debug(f"Skipped duplicate: player_id {player_id}, ranking_group_id {ranking_group_id}")
                    continue
                seen.add(key)

                # Create and save the relation
                rel = PlayerRankingGroup(player_id=player_id, ranking_group_id=ranking_group_id)
                result = rel.save_to_db(cursor)
                db_results.append(result)

        # Print insert results
        print_db_insert_results(db_results)
        logging.info(f"Insert completed in {time.time() - insert_start:.2f} seconds")

        # Log final row count for verification
        cursor.execute("SELECT COUNT(*) FROM player_ranking_group")
        final_count = cursor.fetchone()[0]
        logging.info(f"Final rows in player_ranking_group: {final_count}")

        logging.info(f"Total processing time: {time.time() - start_time:.2f} seconds")

    except Exception as e:
        logging.error(f"Error updating player ranking groups: {e}")
        print(f"❌ Error updating player ranking groups: {e}")
        conn.rollback()

    finally:
        conn.commit()
        conn.close()
        logging.info("-------------------------------------------------------------------")

if __name__ == "__main__":
    upd_player_ranking_groups()