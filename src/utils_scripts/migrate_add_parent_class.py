# src/utils_scripts/migrate_add_parent_class.py
#
# Purpose
# -------
# Migrate the existing database to a new file with the `tournament_class_id_parent`
# column added to the `tournament_class` table in the correct position (after
# `tournament_class_structure_id`, before `ko_tree_size`).
#
# SQLite does not support adding columns in a specific position, so this script:
#   1. Creates a new database file with the updated schema (from db.py)
#   2. Copies all data from the source database
#   3. For `tournament_class`, maps old columns to new columns with NULL for the new field
#   4. Validates row counts match
#
# The original database is NOT modified.
#
# How to run
# ----------
# 1. cd src/utils_scripts
# 2. python migrate_add_parent_class.py
# 3. Verify the new database is correct
# 4. Rename files as needed (e.g., swap old and new)
#
# Copyright © BT_stats

from __future__ import annotations

import os
import sys
import sqlite3
import time
from pathlib import Path
from contextlib import closing

# Allow imports from parent src directory
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config import DB_NAME

# ────────────────────────────────────────────────────────────────────────────
# Configuration
# ────────────────────────────────────────────────────────────────────────────
# Resolve paths relative to the src directory
SRC_DIR = Path(__file__).parent.parent
SOURCE_DB = (SRC_DIR / DB_NAME).resolve()
OUT_DB = SOURCE_DB.parent / "bt_stats_migrated.db"

# ────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────

def _die(msg: str, code: int = 2) -> None:
    """Exit with a visible error message."""
    print(f"❌ {msg}", file=sys.stderr)
    sys.exit(code)


def _connect_ro(db_path: Path) -> sqlite3.Connection:
    """Open a SQLite connection in read-only mode."""
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    return con


def _connect_rw(db_path: Path) -> sqlite3.Connection:
    """Open a normal read-write SQLite connection."""
    con = sqlite3.connect(str(db_path))
    con.execute("PRAGMA foreign_keys = OFF;")  # Disable during migration
    return con


def get_table_columns(cursor: sqlite3.Cursor, table_name: str) -> list[str]:
    """Get list of column names for a table."""
    cursor.execute(f"PRAGMA table_info([{table_name}])")
    return [row[1] for row in cursor.fetchall()]


def get_all_tables(cursor: sqlite3.Cursor) -> list[str]:
    """Get list of all user tables (excluding sqlite_ internal tables)."""
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' 
        AND name NOT LIKE 'sqlite_%'
        ORDER BY name
    """)
    return [row[0] for row in cursor.fetchall()]


def get_all_indexes(cursor: sqlite3.Cursor) -> list[tuple[str, str]]:
    """Get list of all user-created indexes (name, sql)."""
    cursor.execute("""
        SELECT name, sql FROM sqlite_master 
        WHERE type='index' 
        AND sql IS NOT NULL
        AND name NOT LIKE 'sqlite_%'
        ORDER BY name
    """)
    return [(row[0], row[1]) for row in cursor.fetchall()]


def get_all_views(cursor: sqlite3.Cursor) -> list[tuple[str, str]]:
    """Get list of all views (name, sql)."""
    cursor.execute("""
        SELECT name, sql FROM sqlite_master 
        WHERE type='view' 
        ORDER BY name
    """)
    return [(row[0], row[1]) for row in cursor.fetchall()]


def get_all_triggers(cursor: sqlite3.Cursor) -> list[tuple[str, str]]:
    """Get list of all triggers (name, sql)."""
    cursor.execute("""
        SELECT name, sql FROM sqlite_master 
        WHERE type='trigger' 
        AND sql IS NOT NULL
        ORDER BY name
    """)
    return [(row[0], row[1]) for row in cursor.fetchall()]


# ────────────────────────────────────────────────────────────────────────────
# New schema for tournament_class (with tournament_class_id_parent)
# ────────────────────────────────────────────────────────────────────────────

DDL_TOURNAMENT_CLASS = '''
    CREATE TABLE IF NOT EXISTS tournament_class (
        tournament_class_id                         INTEGER PRIMARY KEY AUTOINCREMENT,
        tournament_class_id_ext                     TEXT,
        tournament_id                               INTEGER NOT NULL,
        tournament_class_type_id                    INTEGER,
        tournament_class_structure_id               INTEGER,
        tournament_class_id_parent                  INTEGER,
        ko_tree_size                                INTEGER,
        startdate                                   DATE,
        longname                                    TEXT,
        shortname                                   TEXT,
        gender                                      TEXT,
        max_rank                                    INTEGER,
        max_age                                     INTEGER,
        url                                         TEXT,
        data_source_id                              INTEGER DEFAULT 1,
        is_valid                                    BOOLEAN DEFAULT 1,
        row_created                                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        row_updated                                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (tournament_id)                 REFERENCES tournament(tournament_id)    ON DELETE CASCADE,
        FOREIGN KEY (tournament_class_type_id)      REFERENCES tournament_class_type(tournament_class_type_id),
        FOREIGN KEY (tournament_class_structure_id) REFERENCES tournament_class_structure(tournament_class_structure_id),
        FOREIGN KEY (tournament_class_id_parent)    REFERENCES tournament_class(tournament_class_id)   ON DELETE SET NULL,
        FOREIGN KEY (data_source_id)                REFERENCES data_source(data_source_id),
        UNIQUE      (tournament_class_id_ext, data_source_id),
        UNIQUE      (tournament_id, shortname, startdate)
    )
'''

# Old columns in tournament_class (source database)
OLD_TOURNAMENT_CLASS_COLUMNS = [
    "tournament_class_id",
    "tournament_class_id_ext",
    "tournament_id",
    "tournament_class_type_id",
    "tournament_class_structure_id",
    "ko_tree_size",
    "startdate",
    "longname",
    "shortname",
    "gender",
    "max_rank",
    "max_age",
    "url",
    "data_source_id",
    "is_valid",
    "row_created",
    "row_updated",
]

# New columns (with tournament_class_id_parent inserted)
NEW_TOURNAMENT_CLASS_COLUMNS = [
    "tournament_class_id",
    "tournament_class_id_ext",
    "tournament_id",
    "tournament_class_type_id",
    "tournament_class_structure_id",
    "tournament_class_id_parent",  # NEW COLUMN
    "ko_tree_size",
    "startdate",
    "longname",
    "shortname",
    "gender",
    "max_rank",
    "max_age",
    "url",
    "data_source_id",
    "is_valid",
    "row_created",
    "row_updated",
]


# ────────────────────────────────────────────────────────────────────────────
# Migration logic
# ────────────────────────────────────────────────────────────────────────────

def run_migration() -> None:
    start = time.perf_counter()
    
    # Validate source exists
    if not SOURCE_DB.exists():
        _die(f"Source DB not found: {SOURCE_DB}")
    
    print(f"📁 Source database: {SOURCE_DB}")
    print(f"📁 Target database: {OUT_DB}")
    
    # Remove target if exists
    if OUT_DB.exists():
        print(f"⚠️  Target exists, removing: {OUT_DB}")
        OUT_DB.unlink()
    
    try:
        with closing(_connect_ro(SOURCE_DB)) as src, closing(_connect_rw(OUT_DB)) as dst:
            src_cursor = src.cursor()
            dst_cursor = dst.cursor()
            
            # Performance pragmas for bulk insert
            dst.executescript("""
                PRAGMA journal_mode=OFF;
                PRAGMA synchronous=OFF;
                PRAGMA temp_store=MEMORY;
                PRAGMA cache_size=-400000;
                PRAGMA page_size=32768;
            """)
            print("✅ Performance PRAGMAs set")
            
            # Get list of all tables from source
            tables = get_all_tables(src_cursor)
            print(f"📋 Found {len(tables)} tables to migrate")
            
            # Step 1: Copy schema and data for all tables EXCEPT tournament_class
            # We need to copy tables in dependency order (FKs), but with FKs disabled it's safe
            
            migration_stats = {}
            
            for table in tables:
                if table == "tournament_class":
                    # Handle specially
                    continue
                
                # Get the CREATE TABLE statement from source
                src_cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,))
                row = src_cursor.fetchone()
                if not row or not row[0]:
                    print(f"  ⚠️  Skipping {table} (no schema)")
                    continue
                
                create_sql = row[0]
                
                # Create table in destination
                dst_cursor.execute(create_sql)
                
                # Copy data
                columns = get_table_columns(src_cursor, table)
                cols_str = ", ".join(f"[{c}]" for c in columns)
                
                src_cursor.execute(f"SELECT {cols_str} FROM [{table}]")
                rows = src_cursor.fetchall()
                
                if rows:
                    placeholders = ", ".join(["?"] * len(columns))
                    dst_cursor.executemany(
                        f"INSERT INTO [{table}] ({cols_str}) VALUES ({placeholders})",
                        [tuple(row) for row in rows]
                    )
                
                migration_stats[table] = len(rows)
                print(f"  ✅ {table}: {len(rows)} rows")
            
            # Step 2: Create tournament_class with NEW schema
            print("\n📝 Creating tournament_class with new schema...")
            dst_cursor.execute(DDL_TOURNAMENT_CLASS)
            
            # Step 3: Copy tournament_class data with NULL for new column
            # Map old columns to new columns
            old_cols_str = ", ".join(f"[{c}]" for c in OLD_TOURNAMENT_CLASS_COLUMNS)
            src_cursor.execute(f"SELECT {old_cols_str} FROM tournament_class")
            rows = src_cursor.fetchall()
            
            # Build insert with new column order (NULL for tournament_class_id_parent)
            new_cols_str = ", ".join(f"[{c}]" for c in NEW_TOURNAMENT_CLASS_COLUMNS)
            placeholders = ", ".join(["?"] * len(NEW_TOURNAMENT_CLASS_COLUMNS))
            
            new_rows = []
            for row in rows:
                # Insert NULL at position 5 (after tournament_class_structure_id)
                new_row = list(row[:5]) + [None] + list(row[5:])
                new_rows.append(tuple(new_row))
            
            dst_cursor.executemany(
                f"INSERT INTO tournament_class ({new_cols_str}) VALUES ({placeholders})",
                new_rows
            )
            
            migration_stats["tournament_class"] = len(rows)
            print(f"  ✅ tournament_class: {len(rows)} rows (with new column)")
            
            # Step 4: Copy indexes
            print("\n📋 Copying indexes...")
            indexes = get_all_indexes(src_cursor)
            for name, sql in indexes:
                try:
                    dst_cursor.execute(sql)
                    print(f"  ✅ Index: {name}")
                except sqlite3.Error as e:
                    print(f"  ⚠️  Index {name} skipped: {e}")
            
            # Step 5: Copy views
            print("\n📋 Copying views...")
            views = get_all_views(src_cursor)
            for name, sql in views:
                try:
                    dst_cursor.execute(sql)
                    print(f"  ✅ View: {name}")
                except sqlite3.Error as e:
                    print(f"  ⚠️  View {name} skipped: {e}")
            
            # Step 6: Copy triggers
            print("\n📋 Copying triggers...")
            triggers = get_all_triggers(src_cursor)
            for name, sql in triggers:
                try:
                    dst_cursor.execute(sql)
                    print(f"  ✅ Trigger: {name}")
                except sqlite3.Error as e:
                    print(f"  ⚠️  Trigger {name} skipped: {e}")
            
            # Commit
            dst.commit()
            
            # Step 7: Verify row counts
            print("\n🔍 Verifying row counts...")
            mismatch = False
            for table, expected_count in migration_stats.items():
                dst_cursor.execute(f"SELECT COUNT(*) FROM [{table}]")
                actual_count = dst_cursor.fetchone()[0]
                if actual_count != expected_count:
                    print(f"  ❌ {table}: expected {expected_count}, got {actual_count}")
                    mismatch = True
            
            if mismatch:
                _die("Row count mismatch detected!")
            else:
                print("  ✅ All row counts verified")
            
            # Step 8: Verify new column exists
            print("\n🔍 Verifying new column...")
            new_cols = get_table_columns(dst_cursor, "tournament_class")
            if "tournament_class_id_parent" in new_cols:
                idx = new_cols.index("tournament_class_id_parent")
                print(f"  ✅ tournament_class_id_parent exists at position {idx}")
                # Verify position (should be after tournament_class_structure_id = 4, so index 5)
                if idx == 5:
                    print(f"  ✅ Column is in correct position")
                else:
                    print(f"  ⚠️  Column position is {idx}, expected 5")
            else:
                _die("New column tournament_class_id_parent not found!")
            
            # Step 9: Integrity check and VACUUM
            print("\n🔧 Running integrity check...")
            dst_cursor.execute("PRAGMA integrity_check")
            result = dst_cursor.fetchone()[0]
            if result != "ok":
                _die(f"Integrity check failed: {result}")
            print("  ✅ Integrity check passed")
            
            print("\n🔧 Running VACUUM...")
            dst.execute("VACUUM")
            print("  ✅ VACUUM complete")
    
    except Exception as e:
        # Clean up on failure
        if OUT_DB.exists():
            OUT_DB.unlink()
        raise
    
    # Summary
    elapsed = time.perf_counter() - start
    src_size = SOURCE_DB.stat().st_size / (1024 * 1024)
    dst_size = OUT_DB.stat().st_size / (1024 * 1024)
    
    print("\n" + "=" * 60)
    print("✅ Migration complete!")
    print(f"   Source: {SOURCE_DB} ({src_size:.1f} MB)")
    print(f"   Target: {OUT_DB} ({dst_size:.1f} MB)")
    print(f"   Time: {elapsed:.1f} seconds")
    print(f"   Tables migrated: {len(migration_stats)}")
    print(f"   Total rows: {sum(migration_stats.values()):,}")
    print("=" * 60)
    print("\n📌 Next steps:")
    print(f"   1. Verify the new database works correctly")
    print(f"   2. Backup your old database")
    print(f"   3. Rename {OUT_DB.name} to {SOURCE_DB.name}")


if __name__ == "__main__":
    run_migration()
