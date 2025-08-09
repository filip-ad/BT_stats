import sqlite3
import sys
from db import get_conn

def prune_clubs():
    """
    Connects to the SQLite database at db_path and deletes all
    rows in the `club` table whose club_id is greater than 1000.
    """
    conn, cursor = get_conn()
    # Perform deletion
    cursor.execute("DELETE FROM club WHERE club_id > 950;")
    deleted = cursor.rowcount
    print(f"Deleted {deleted} club record(s) with club_id > 950.")

    # Commit and close
    conn.commit()
    conn.close()

if __name__ == "__main__":
    
    prune_clubs()

    