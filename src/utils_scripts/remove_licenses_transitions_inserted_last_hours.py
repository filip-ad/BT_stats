
import sqlite3
import logging
from datetime import datetime
from db import get_conn

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def fix_player_license_schema_and_delete_recent(cursor, db_path):
    """
    Fixes the player_license table foreign key to reference season (singular) and
    deletes rows from player_license and player_transition created in the last hour.
    """
    try:
        # Start a transaction
        cursor.execute("BEGIN TRANSACTION")
        cursor.execute("PRAGMA foreign_keys = ON")

        # Step 1: Rename the existing player_license table
        cursor.execute("ALTER TABLE player_license RENAME TO player_license_old")

        # Step 2: Create new player_license table with corrected foreign key
        cursor.execute('''
            CREATE TABLE player_license (
                player_license_id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_id INTEGER NOT NULL,
                player_id_ext INTEGER,
                club_id INTEGER NOT NULL,
                club_id_ext INTEGER,
                valid_from DATE,
                valid_to DATE,
                license_id INTEGER NOT NULL,
                season_id INTEGER,
                row_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (player_id) REFERENCES player(player_id),
                FOREIGN KEY (player_id_ext) REFERENCES player(player_id_ext),
                FOREIGN KEY (club_id) REFERENCES club(club_id),
                FOREIGN KEY (club_id_ext) REFERENCES club(club_id_ext),
                FOREIGN KEY (license_id) REFERENCES license(license_id),
                FOREIGN KEY (season_id) REFERENCES season(season_id),
                UNIQUE (player_id, license_id, season_id, club_id)
            )
        ''')

        # Step 3: Copy data from old table to new table
        cursor.execute('''
            INSERT INTO player_license (
                player_license_id, player_id, player_id_ext, club_id, club_id_ext,
                valid_from, valid_to, license_id, season_id, row_created
            )
            SELECT player_license_id, player_id, player_id_ext, club_id, club_id_ext,
                   valid_from, valid_to, license_id, season_id, row_created
            FROM player_license_old
        ''')

        # Step 4: Drop the old table
        cursor.execute("DROP TABLE player_license_old")

        # Step 5: Delete rows from player_transition and player_license created in the last hour
        cursor.execute('''
            DELETE FROM player_transition
            WHERE row_created >= DATETIME('now', 'localtime', '-1 hour')
        ''')
        cursor.execute('''
            DELETE FROM player_license
            WHERE row_created >= DATETIME('now', 'localtime', '-1 hour')
        ''')

        # Commit changes to save to disk
        cursor.connection.commit()
        logging.info("Successfully fixed player_license schema and deleted rows from last hour")

        # Verify deletion
        cursor.execute('''
            SELECT * FROM player_transition
            WHERE row_created >= DATETIME('now', 'localtime', '-1 hour')
        ''')
        transition_rows = cursor.fetchall()
        cursor.execute('''
            SELECT * FROM player_license
            WHERE row_created >= DATETIME('now', 'localtime', '-1 hour')
        ''')
        license_rows = cursor.fetchall()

        if not transition_rows and not license_rows:
            logging.info("No rows remain in player_transition or player_license from the last hour")
        else:
            logging.warning(f"Rows remain after deletion: {len(transition_rows)} in player_transition, {len(license_rows)} in player_license")

    except sqlite3.Error as e:
        # Rollback on error
        cursor.connection.rollback()
        logging.error(f"Database error: {e}")
        raise
    except Exception as e:
        # Rollback on error
        cursor.connection.rollback()
        logging.error(f"Unexpected error: {e}")
        raise

def main():
    # Specify your database file path
    db_path = "../data/table_tennis.db" # Replace with the actual path, e.g., '/home/filip/table_tennis.db'
    
    try:
        # Connect to the database
        conn, cursor = get_conn()  # Unpack the tuple into conn and cursor
        # Fix schema and delete rows
        fix_player_license_schema_and_delete_recent(cursor, db_path)

    except sqlite3.Error as e:
        logging.error(f"Failed to connect to database {db_path}: {e}")
    finally:
        # Close the connection
        conn.close()
        logging.info("Database connection closed")

if __name__ == "__main__":
    main()