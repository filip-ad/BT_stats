# src/upd_players.py

import logging
from collections import defaultdict
from models.player import Player
from typing import List
from db import get_conn
from utils import print_db_insert_results

def upd_players():
    conn, cursor = get_conn()
    logging.info("Updating player table...")
    print("ℹ️  Updating player table...")

    try:
        # Fetch all players and their oldest license row for each unique player
        cursor.execute('''
            SELECT pl.player_id_ext, pl.firstname, pl.lastname, pl.year_born
            FROM player_license_raw pl
            INNER JOIN (
                SELECT player_id_ext, MIN(row_id) AS min_row_id
                FROM player_license_raw
                WHERE player_id_ext IS NOT NULL
                  AND TRIM(firstname) != ''
                  AND TRIM(lastname) != ''
                  AND year_born IS NOT NULL
                GROUP BY player_id_ext
            ) earliest ON pl.row_id = earliest.min_row_id
            ORDER BY pl.row_id ASC
        ''')

        players: List[Player] = [
            Player(player_id_ext=row[0], firstname=row[1], lastname=row[2], year_born=row[3])
            for row in cursor.fetchall()
        ]

        # Save players to the database
        db_results = [player.save_to_db(cursor) for player in players]

        # Print results of database insertions
        print_db_insert_results(db_results)

    finally:
        conn.commit()
        conn.close()
        logging.info("-------------------------------------------------------------------")
