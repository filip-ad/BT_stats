# src/upd_players.py

import logging
from models.player import Player
from typing import List
from db import get_conn
from utils import print_db_insert_results

def upd_players():
    conn, cursor = get_conn()
    logging.info("Updating player table...")
    print("ℹ️  Updating player table...")

    try:
    # Fetch unique players from both player_license_raw and player_ranking_raw
        cursor.execute('''
            WITH CombinedPlayers AS (
                SELECT player_id_ext, firstname, lastname, year_born, row_id, 'license' AS source
                FROM player_license_raw
                WHERE player_id_ext IS NOT NULL
                  AND TRIM(firstname) != ''
                  AND TRIM(lastname) != ''
                  AND year_born IS NOT NULL
                UNION
                SELECT player_id_ext, firstname, lastname, year_born, row_id, 'ranking' AS source
                FROM player_ranking_raw
                WHERE player_id_ext IS NOT NULL
                  AND TRIM(firstname) != ''
                  AND TRIM(lastname) != ''
                  AND year_born IS NOT NULL
            ),
            RankedPlayers AS (
                SELECT player_id_ext, firstname, lastname, year_born,
                       ROW_NUMBER() OVER (PARTITION BY player_id_ext ORDER BY 
                           CASE source WHEN 'license' THEN 1 ELSE 2 END, row_id) AS rn
                FROM CombinedPlayers
            )
            SELECT player_id_ext, firstname, lastname, year_born
            FROM RankedPlayers
            WHERE rn = 1
            ORDER BY player_id_ext
        ''')
        players = [
            Player(player_id_ext=row[0], firstname=row[1], lastname=row[2], year_born=row[3])
            for row in cursor.fetchall()
        ]
        logging.info(f"Found {len(players)} unique players from player_license_raw and player_ranking_raw.")
        print(f"ℹ️  Found {len(players)} unique players from player_license_raw and player_ranking_raw.")

        # Save players to the database
        db_results = [player.save_to_db(cursor) for player in players]

        # Print results of database insertions
        print_db_insert_results(db_results)

    finally:
        conn.commit()
        conn.close()
        logging.info("-------------------------------------------------------------------")
