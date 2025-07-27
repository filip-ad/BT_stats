# src/upd_player_ranking_group.py

import logging
from models.player_ranking_group import PlayerRankingGroup
from db import get_conn
from utils import print_db_insert_results

def upd_player_ranking_groups():
    conn, cursor = get_conn()
    logging.info("Updating player_ranking_group table...")
    print("ℹ️  Updating player_ranking_group table...")

    try:
        # Step 1: Load mapping from player_id_ext to player_id
        cursor.execute("SELECT player_id_ext, player_id FROM player")
        player_id_map = {row[0]: row[1] for row in cursor.fetchall()}

        # Step 2: Load mapping from ranking_group name to id
        cursor.execute("SELECT class_short, ranking_group_id FROM ranking_group")
        ranking_group_map = {row[0]: row[1] for row in cursor.fetchall()}

        # Step 3: Extract raw ranking group info
        cursor.execute("""
            SELECT player_id_ext, ranking_group_raw
            FROM player_license_raw
            WHERE player_id_ext IS NOT NULL AND TRIM(ranking_group_raw) != ''
        """)

        seen = set()
        db_results = []
        deleted_count = 0  # Track how many rows were deleted
        new_groups_count = 0  # Track new groups inserted

        for row in cursor.fetchall():
            player_id_ext, raw_groups = row
            player_id = player_id_map.get(player_id_ext)
            if not player_id:
                continue

            # Step 4: Delete existing ranking groups for this player
            delete_result = PlayerRankingGroup.delete_by_player_id(cursor, player_id)
            if delete_result["status"] == "success":
                logging.debug(delete_result["reason"])
                deleted_count += int(delete_result["reason"].split()[1])  
            else:
                logging.debug(delete_result["reason"])       

            groups = [g.strip() for g in raw_groups.split(",") if g.strip()]
            for group_name in groups:
                ranking_group_id = ranking_group_map.get(group_name)
                if not ranking_group_id:
                    logging.warning(f"Unknown ranking group: {group_name}")
                    print(f"⚠️  Unknown ranking group: {group_name}")
                    continue

                key = (player_id, ranking_group_id)
                if key in seen:
                    continue
                seen.add(key)

                rel = PlayerRankingGroup(player_id=player_id, ranking_group_id=ranking_group_id)
                result = rel.save_to_db(cursor)
                db_results.append(rel.save_to_db(cursor))

        # Print deletion summary
        print(f"🗑️  Deleted existing player ranking groups: {deleted_count}")
        logging.info(f"Deleted {deleted_count} old player_ranking_group rows")

        print_db_insert_results(db_results)

        # Print new groups inserted
        print(f"ℹ️  New player ranking groups inserted: {new_groups_count}")
        logging.info(f"New player ranking groups inserted: {new_groups_count}")

    finally:
        conn.commit()
        conn.close()