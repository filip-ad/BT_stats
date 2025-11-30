#!/usr/bin/env python3
"""
Migration: Add ON DELETE CASCADE to tournament_class_entry foreign keys.

This migration updates:
1. tournament_class_entry: Add CASCADE on tournament_class_id FK
2. tournament_class_player: Add CASCADE on tournament_class_entry_id FK  
3. match_side: Add CASCADE on represented_entry_id FK

SQLite doesn't support ALTER TABLE for FK changes, so we recreate tables.

Usage:
    cd /home/filip/dev/BT_stats
    source .venv/bin/activate
    python src/utils_scripts/migrate_add_cascade_to_entry_fks.py
"""

import sqlite3
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DB_NAME


def migrate():
    print(f"🔄 Migration: Adding ON DELETE CASCADE to entry-related FKs")
    print(f"   Database: {DB_NAME}")
    
    conn = sqlite3.connect(DB_NAME)
    conn.execute("PRAGMA foreign_keys = OFF;")  # Disable during migration
    cursor = conn.cursor()
    
    try:
        # Start transaction
        cursor.execute("BEGIN TRANSACTION;")
        
        # Find and drop views that reference the tables we're migrating
        # Also drop views that depend on those views (cascading dependencies)
        print("\n0️⃣  Dropping dependent views...")
        cursor.execute("""
            SELECT name, sql FROM sqlite_master 
            WHERE type = 'view'
            ORDER BY name
        """)
        all_views = cursor.fetchall()
        
        # Store all view definitions and drop them all
        # (simpler than resolving dependencies - we recreate them all)
        view_defs = {}
        for view_name, view_sql in all_views:
            view_defs[view_name] = view_sql
            cursor.execute(f"DROP VIEW IF EXISTS {view_name}")
        print(f"   Dropped {len(view_defs)} views")
        
        # 1. Migrate tournament_class_entry
        print("\n1️⃣  Migrating tournament_class_entry...")
        cursor.execute("""
            CREATE TABLE tournament_class_entry_new (
                tournament_class_entry_id                   INTEGER     PRIMARY KEY AUTOINCREMENT,
                tournament_class_entry_id_ext               TEXT,
                tournament_class_entry_group_id_int         INTEGER     NOT NULL,
                tournament_class_id                         INTEGER     NOT NULL,
                seed                                        INTEGER,
                final_position                              INTEGER,
                row_created                                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_updated                                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (tournament_class_id)           REFERENCES tournament_class(tournament_class_id)    ON DELETE CASCADE,
                       
                UNIQUE (tournament_class_id, tournament_class_entry_id_ext),
                UNIQUE (tournament_class_id, tournament_class_entry_group_id_int)
            );
        """)
        cursor.execute("""
            INSERT INTO tournament_class_entry_new 
            SELECT * FROM tournament_class_entry;
        """)
        row_count = cursor.rowcount
        cursor.execute("DROP TABLE tournament_class_entry;")
        cursor.execute("ALTER TABLE tournament_class_entry_new RENAME TO tournament_class_entry;")
        print(f"   ✅ Migrated {row_count} rows")
        
        # Recreate index
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tce_class ON tournament_class_entry(tournament_class_id);")
        
        # 2. Migrate tournament_class_player
        print("\n2️⃣  Migrating tournament_class_player...")
        cursor.execute("""
            CREATE TABLE tournament_class_player_new (
                tournament_class_entry_id                   INTEGER,
                tournament_player_id_ext                    TEXT,
                player_id                                   INTEGER     NOT NULL,
                club_id                                     INTEGER     NOT NULL,
                row_created                                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_updated                                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (tournament_class_entry_id)     REFERENCES tournament_class_entry(tournament_class_entry_id)    ON DELETE CASCADE,
                FOREIGN KEY (player_id)                     REFERENCES player(player_id),
                FOREIGN KEY (club_id)                       REFERENCES club(club_id),

                PRIMARY KEY (tournament_class_entry_id, player_id)
            );
        """)
        cursor.execute("""
            INSERT INTO tournament_class_player_new 
            SELECT * FROM tournament_class_player;
        """)
        row_count = cursor.rowcount
        cursor.execute("DROP TABLE tournament_class_player;")
        cursor.execute("ALTER TABLE tournament_class_player_new RENAME TO tournament_class_player;")
        print(f"   ✅ Migrated {row_count} rows")
        
        # Recreate index
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tcp_player ON tournament_class_player(player_id);")
        
        # 3. Migrate match_side
        print("\n3️⃣  Migrating match_side...")
        cursor.execute("""
            CREATE TABLE match_side_new (
                match_id                                    INTEGER,
                side_no                                     INTEGER,
                represented_entry_id                        INTEGER,
                represented_league_team_id                  INTEGER,
                row_created                                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_updated                                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                PRIMARY KEY (match_id, side_no),            

                FOREIGN KEY (match_id)                      REFERENCES match(match_id)                              ON DELETE CASCADE,
                FOREIGN KEY (represented_league_team_id)    REFERENCES league_team(league_team_id),
                FOREIGN KEY (represented_entry_id)          REFERENCES tournament_class_entry(tournament_class_entry_id)    ON DELETE CASCADE,
                                                                                                                               
                CHECK (side_no IN (1, 2)),
                CHECK (
                    (represented_league_team_id IS NOT NULL AND represented_entry_id IS NULL)
                    OR (represented_league_team_id IS NULL AND represented_entry_id IS NOT NULL)
                    OR (represented_league_team_id IS NULL AND represented_entry_id IS NULL)
                )
            );
        """)
        cursor.execute("""
            INSERT INTO match_side_new 
            SELECT * FROM match_side;
        """)
        row_count = cursor.rowcount
        cursor.execute("DROP TABLE match_side;")
        cursor.execute("ALTER TABLE match_side_new RENAME TO match_side;")
        print(f"   ✅ Migrated {row_count} rows")
        
        # Commit
        cursor.execute("COMMIT;")
        
        # Re-enable FK and verify
        conn.execute("PRAGMA foreign_keys = ON;")
        
        # Verify the new FKs
        print("\n🔍 Verifying new FK constraints...")
        cursor.execute("""
            SELECT m.name AS table_name, p."table" AS ref_table, p.on_delete
            FROM sqlite_master AS m
            JOIN pragma_foreign_key_list(m.name) AS p
            WHERE m.type = 'table' 
            AND m.name IN ('tournament_class_entry', 'tournament_class_player', 'match_side')
            AND p.on_delete = 'CASCADE'
            ORDER BY m.name;
        """)
        for row in cursor.fetchall():
            print(f"   ✅ {row[0]} → {row[1]} (ON DELETE {row[2]})")
        
        # Recreate dropped views
        print("\n🔄 Recreating views...")
        for view_name, view_sql in view_defs.items():
            try:
                cursor.execute(view_sql)
                print(f"   ✅ Recreated view: {view_name}")
            except Exception as e:
                print(f"   ⚠️  Failed to recreate view {view_name}: {e}")
        
        conn.commit()
        
        print("\n✅ Migration completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        cursor.execute("ROLLBACK;")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    migrate()
