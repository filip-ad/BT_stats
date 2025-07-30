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
        # Fetch unique players from raw sources
        cursor.execute('''
            WITH Combined AS (
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
            Ranked AS (
                SELECT player_id_ext, firstname, lastname, year_born,
                       ROW_NUMBER() OVER (PARTITION BY player_id_ext ORDER BY 
                           CASE source WHEN 'license' THEN 1 ELSE 2 END, row_id) AS rn
                FROM Combined
            )
            SELECT player_id_ext, firstname, lastname, year_born
            FROM Ranked
            WHERE rn = 1
            ORDER BY player_id_ext
        ''')

        players = cursor.fetchall()
        logging.info(f"ℹ️  Found {len(players):,} unique player_id_exts")
        print(f"ℹ️  Found {len(players):,} unique player_id_exts")

        results = []
        for row in players:
            player_id_ext, firstname, lastname, year_born = row
            player = Player(firstname=firstname, lastname=lastname, year_born=year_born)
            result = player.save_to_db(cursor, player_id_ext)
            results.append(result)

        print_db_insert_results(results)

    finally:
        conn.commit()
        conn.close()
        logging.info("✔️  Done updating players.")
