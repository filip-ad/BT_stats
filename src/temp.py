import sqlite3
from config import DB_NAME
from utils import normalize_key

# Configuration
DB_PATH = DB_NAME

def migrate_entry_group_id_int():
    """Backfill entry_group_id_int for existing tournament_class_entry_raw rows using singles logic."""
    # Connect to database
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Fetch all raw rows with class type (join to tournament_class)
    cursor.row_factory = sqlite3.Row
    cursor.execute("""
        SELECT r.*, tc.tournament_class_type_id
        FROM tournament_class_entry_raw r
        JOIN tournament_class tc ON r.tournament_class_id_ext = tc.tournament_class_id_ext
        ORDER BY r.tournament_class_id_ext
    """)
    raw_rows = cursor.fetchall()
    cursor.row_factory = None

    length = len(raw_rows)
    print(f"Fetched {length} raw entries for backfill.")

    # Group by class_ext and assign IDs (singles logic)
    groups = {}
    for row in raw_rows:
        class_ext = row['tournament_class_id_ext']
        type_id = row['tournament_class_type_id']
        if type_id not in [1, 9]:
            print(f"Skipping non-singles class: {class_ext} (type {type_id})")
            continue
        groups.setdefault(class_ext, []).append(dict(row))

    updated_count = 0
    for class_ext, class_rows in groups.items():
        # Sort: seed_raw ascending (1 is best), then fullname_raw ascending
        def sort_key(r):
            seed_val = int(r.get('seed_raw', float("inf"))) if r.get('seed_raw') and r.get('seed_raw').isdigit() else float("inf")
            name_val = normalize_key(r.get('fullname_raw', ''))
            return (seed_val, name_val)

        sorted_rows = sorted(class_rows, key=sort_key)
        for i, row in enumerate(sorted_rows, 1):
            row_id = row['row_id']
            cursor.execute(
                "UPDATE tournament_class_entry_raw SET entry_group_id_int = ? WHERE row_id = ?",
                (i, row_id)
            )
            if (length % 1000 == 0):
                print(f"Updated {updated_count + 1}/{length} rows...")
            updated_count += 1

    conn.commit()
    print(f"Backfill complete. Updated {updated_count} rows.")
    conn.close()

if __name__ == "__main__":
    migrate_entry_group_id_int()