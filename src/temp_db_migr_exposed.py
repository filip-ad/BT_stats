import sqlite3
import os

# Define paths (adjust as needed)
ORIGINAL_DB_PATH = 'table_tennis.db'  # Path to the original database file
EXPORT_DB_PATH = 'resolved_table_tennis.db'  # Path to the new exported database

# List of tables to exclude (raw data, logs, backups, etc.)
EXCLUDED_TABLES = [
    'club_name_prefix_match',  # Seems like matching helper, not core resolved
    'log_details',
    'log_runs',
    'player_license_raw',
    'player_license_raw_bkp',
    'player_ranking_raw',
    'player_transition_raw',
    'scrape_run_log',
    'tournament_class_entry_raw',
    'tournament_class_match_raw',
    'tournament_class_raw',
    'tournament_raw',
    # Add any other temporary or raw tables if identified
]

# Existing views from the schema (we'll recreate them in the new DB)
EXISTING_VIEWS = {
    'v_clubs': """
        SELECT 
            c.club_id,
            c.shortname        AS club_shortname,
            c.longname         AS club_longname,
            c.city,
            c.country_code,
            c.active,
            ct.description     AS club_type,
            d.district_id,
            d.name        AS district_shortname,
            GROUP_CONCAT(DISTINCT ce.club_id_ext || ':' || ce.data_source) AS ext_ids,
            GROUP_CONCAT(DISTINCT a.alias || ' (' || a.alias_type || ')')  AS aliases
        FROM club c
        LEFT JOIN club_type ct
            ON ct.club_type_id = c.club_type
        LEFT JOIN district d
            ON d.district_id = c.district_id
        LEFT JOIN club_id_ext ce
            ON ce.club_id = c.club_id
        LEFT JOIN club_name_alias a
            ON a.club_id = c.club_id
        GROUP BY c.club_id
        ORDER BY c.club_id;
    """,
    'v_foreign_keys': """
        SELECT m.name AS table_name, p.*
        FROM sqlite_master AS m
        JOIN pragma_foreign_key_list(m.name) AS p
        WHERE m.type = 'table';
    """,
    'v_player_profile': """
        WITH recent_license AS (
            SELECT pl.player_id,
                c.club_id,
                c.shortname || ' (' || s.label || ')' AS club_with_season,
                pl.season_id,
                ROW_NUMBER() OVER (PARTITION BY pl.player_id ORDER BY s.start_date DESC) AS rn
            FROM player_license pl
            JOIN season s ON pl.season_id = s.season_id
            JOIN club c ON pl.club_id = c.club_id
        ),
        recent_tournament AS (
            SELECT tcp.player_id,
                t.tournament_id,
                t.shortname AS tournament_name,
                tc.shortname AS class_shortname,
                tc.startdate AS class_startdate,
                ROW_NUMBER() OVER (PARTITION BY tcp.player_id ORDER BY tc.startdate DESC) AS rn
            FROM tournament_class_player tcp
            JOIN tournament_class_entry tce
                ON tcp.tournament_class_entry_id = tce.tournament_class_entry_id
            JOIN tournament_class tc
                ON tce.tournament_class_id = tc.tournament_class_id
            JOIN tournament t
                ON tc.tournament_id = t.tournament_id
        ),
        recent_transition AS (
            SELECT pt.player_id,
                cf.shortname || ' → ' || ct.shortname || ' (' || s.label || ')' AS transition_text,
                ROW_NUMBER() OVER (PARTITION BY pt.player_id ORDER BY s.start_date DESC) AS rn
            FROM player_transition pt
            JOIN club cf ON pt.club_id_from = cf.club_id
            JOIN club ct ON pt.club_id_to = ct.club_id
            JOIN season s ON pt.season_id = s.season_id
        ),
        id_exts AS (
            SELECT pie.player_id,
                GROUP_CONCAT(pie.player_id_ext) AS id_ext_list,
                COUNT(*) AS id_ext_count
            FROM player_id_ext pie
            GROUP BY pie.player_id
        ),
        ranking_groups AS (
            SELECT prg.player_id,
                GROUP_CONCAT(rg.class_short, ', ') AS ranking_groups
            FROM player_ranking_group prg
            JOIN ranking_group rg ON prg.ranking_group_id = rg.ranking_group_id
            GROUP BY prg.player_id
        ),
        recent_ranking_points AS (
            SELECT pie.player_id,
                pr.points,
                pr.run_date
            FROM player_id_ext pie
            JOIN (
                SELECT player_id_ext, MAX(run_date) AS max_run_date
                FROM player_ranking
                GROUP BY player_id_ext
            ) latest
            ON pie.player_id_ext = latest.player_id_ext
            JOIN player_ranking pr
            ON pr.player_id_ext = latest.player_id_ext
            AND pr.run_date = latest.max_run_date
        ),
        ranking_points_per_player AS (
            SELECT rrp.player_id,
                rrp.points,
                rrp.run_date
            FROM recent_ranking_points rrp
            JOIN (
                SELECT player_id, MAX(run_date) AS max_date
                FROM recent_ranking_points
                GROUP BY player_id
            ) maxed
            ON rrp.player_id = maxed.player_id
            AND rrp.run_date = maxed.max_date
        )
        SELECT
            -- Player context
            p.player_id,
            CASE 
                WHEN p.is_verified = 1 THEN p.firstname || ' ' || p.lastname
                ELSE p.fullname_raw
            END AS player_name,
            p.year_born,
            p.is_verified,
            COALESCE(id_exts.id_ext_list, '') AS id_exts,
            COALESCE(id_exts.id_ext_count, 0) AS id_ext_count,

            -- Club context
            rl.club_with_season AS recent_club,

            -- Tournament context
            rt.tournament_name || ' - ' || rt.class_shortname || ' (' || rt.class_startdate || ')' AS recent_tournament_class,

            -- Ranking groups (merged list)
            COALESCE(rg.ranking_groups, '') AS ranking_groups,

            -- Ranking points (latest across all player_id_ext)
            CASE 
                WHEN rpp.points IS NOT NULL 
                THEN rpp.points || ' (' || rpp.run_date || ')'
                ELSE ''
            END AS ranking_points,

            -- Transition
            tr.transition_text AS recent_transition

        FROM player p
        LEFT JOIN id_exts
            ON p.player_id = id_exts.player_id
        LEFT JOIN recent_license rl
            ON p.player_id = rl.player_id AND rl.rn = 1
        LEFT JOIN recent_tournament rt
            ON p.player_id = rt.player_id AND rt.rn = 1
        LEFT JOIN ranking_groups rg
            ON p.player_id = rg.player_id
        LEFT JOIN recent_transition tr
            ON p.player_id = tr.player_id AND tr.rn = 1
        LEFT JOIN ranking_points_per_player rpp
            ON p.player_id = rpp.player_id;
    """,
    'v_tnmt_class': """
        SELECT
            tc.tournament_class_id,
            tc.tournament_class_id_ext,
            t.tournament_id,
            t.tournament_id_ext,
            t.shortname                 AS tournament_shortname,
            tc.longname                 AS class_longname,
            tc.shortname                AS class_shortname,
            tct.description             AS tournament_class_type,
            tcs.description             AS tournament_class_structure,
            ts.description              AS tournament_status,
            tc.ko_tree_size,
            tc.startdate                AS class_date,
            t.country_code,
            t.url                       AS tournament_url,
            tc.is_valid,
            tc.row_created,
            tc.row_updated
        FROM tournament_class tc
        JOIN tournament t
        ON t.tournament_id = tc.tournament_id
        LEFT JOIN tournament_class_type tct
        ON tct.tournament_class_type_id = tc.tournament_class_type_id
        LEFT JOIN tournament_class_structure tcs
        ON tcs.tournament_class_structure_id = tc.tournament_class_structure_id
        LEFT JOIN tournament_status ts
        ON ts.tournament_status_id = t.tournament_status_id
        ORDER BY t.tournament_id, tc.tournament_class_id DESC;
    """,
    'v_tournament_class_entries': """
        SELECT
            -- Tournament context
            t.shortname          AS tournament_shortname,

            -- Class context
            tc.shortname         AS class_shortname,
            tc.longname          AS class_longname,
            tc.startdate         AS class_date,

            -- Player context
            p.firstname,
            p.lastname,
            p.fullname_raw       AS player_fullname_raw,
            p.is_verified,

            -- Club context
            c.shortname          AS club_shortname,

            -- Entry details
            tce.seed,
            tce.final_position,
            tcp.tournament_player_id_ext,

            -- IDs last
            t.tournament_id,
            t.tournament_id_ext,
            tc.tournament_class_id,
            tc.tournament_class_id_ext,
            tce.tournament_class_entry_id,
            tce.tournament_class_entry_group_id_int     AS entry_group_id,
            p.player_id,
            c.club_id

        FROM tournament_class_entry tce
        JOIN tournament_class_player tcp
            ON tcp.tournament_class_entry_id = tce.tournament_class_entry_id
        JOIN tournament_class tc
            ON tc.tournament_class_id = tce.tournament_class_id
        JOIN tournament t
            ON t.tournament_id = tc.tournament_id
        JOIN player p
            ON p.player_id = tcp.player_id
        JOIN club c
            ON c.club_id = tcp.club_id
        ORDER BY t.startdate, tc.startdate, tce.seed;
    """
}

# Suggested new views for front-end use cases
NEW_VIEWS = {
    # Updated view for tournament overview: added unique_participants, average_player_age, total_matches, number_of_clubs
    'v_tournament_overview': """
        SELECT
            t.tournament_id,
            t.shortname AS tournament_shortname,
            t.longname AS tournament_longname,
            t.startdate,
            t.enddate,
            t.city,
            t.arena,
            t.country_code,
            tl.description AS tournament_level,
            tt.description AS tournament_type,
            ts.description AS tournament_status,
            COUNT(DISTINCT tc.tournament_class_id) AS class_count,
            COUNT(DISTINCT tce.tournament_class_entry_id) AS total_entries,
            COUNT(DISTINCT tcp.player_id) AS unique_participants,
            COUNT(DISTINCT tcp.club_id) AS number_of_clubs,
            COUNT(DISTINCT tcm.match_id) AS total_matches,
            ROUND(AVG(strftime('%Y', 'now') - p.year_born), 2) AS average_player_age,
            MIN(strftime('%Y', 'now') - p.year_born) AS min_player_age,
            MAX(strftime('%Y', 'now') - p.year_born) AS max_player_age
        FROM tournament t
        LEFT JOIN tournament_level tl ON tl.tournament_level_id = t.tournament_level_id
        LEFT JOIN tournament_type tt ON tt.tournament_type_id = t.tournament_type_id
        LEFT JOIN tournament_status ts ON ts.tournament_status_id = t.tournament_status_id
        LEFT JOIN tournament_class tc ON tc.tournament_id = t.tournament_id
        LEFT JOIN tournament_class_entry tce ON tce.tournament_class_id = tc.tournament_class_id
        LEFT JOIN tournament_class_player tcp ON tcp.tournament_class_entry_id = tce.tournament_class_entry_id
        LEFT JOIN player p ON p.player_id = tcp.player_id AND p.year_born IS NOT NULL
        LEFT JOIN tournament_class_match tcm ON tcm.tournament_class_id = tc.tournament_class_id
        GROUP BY t.tournament_id
        ORDER BY t.startdate DESC;
    """,
    
    # View for player matches and results: lists all matches for a player, with opponents, results, tournament context
    'v_player_matches': """
        SELECT
            p.player_id,
            CASE 
                WHEN p.is_verified = 1 THEN p.firstname || ' ' || p.lastname
                ELSE p.fullname_raw
            END AS player_name,
            m.match_id,
            m.date AS match_date,
            m.best_of,
            m.status,
            CASE m.winner_side
                WHEN mp.side_no THEN 'Win'
                ELSE 'Loss'
            END AS result,
            CASE m.walkover_side
                WHEN mp.side_no THEN 'Walkover Loss'
                WHEN 3 - mp.side_no THEN 'Walkover Win'
                ELSE NULL
            END AS walkover_status,
            GROUP_CONCAT(DISTINCT op.firstname || ' ' || op.lastname) AS opponent_names,
            t.shortname AS tournament_shortname,
            tc.shortname AS class_shortname,
            tcm.stage_round_no,
            tcs.description AS stage_description,
            GROUP_CONCAT(g.game_no || ': ' || 
                CASE WHEN mp.side_no = 1 THEN g.points_side1 || '-' || g.points_side2
                     ELSE g.points_side2 || '-' || g.points_side1 END, '; ') AS game_scores
        FROM match_player mp
        JOIN player p ON p.player_id = mp.player_id
        JOIN match m ON m.match_id = mp.match_id
        JOIN match_player op ON op.match_id = m.match_id AND op.side_no != mp.side_no
        LEFT JOIN tournament_class_match tcm ON tcm.match_id = m.match_id
        LEFT JOIN tournament_class tc ON tc.tournament_class_id = tcm.tournament_class_id
        LEFT JOIN tournament t ON t.tournament_id = tc.tournament_id
        LEFT JOIN tournament_class_stage tcs ON tcs.tournament_class_stage_id = tcm.tournament_class_stage_id
        LEFT JOIN game g ON g.match_id = m.match_id
        GROUP BY m.match_id, mp.player_id
        ORDER BY m.date DESC;
    """,
    
    # View for player results summary: aggregates wins, losses, etc. per player
    'v_player_results_summary': """
        WITH match_stats AS (
            SELECT
                mp.player_id,
                COUNT(DISTINCT m.match_id) AS total_matches,
                SUM(CASE WHEN m.winner_side = mp.side_no THEN 1 ELSE 0 END) AS match_wins
            FROM match_player mp
            JOIN match m ON m.match_id = mp.match_id
            WHERE m.status = 'completed' AND m.walkover_side IS NULL
            GROUP BY mp.player_id
        ),
        game_stats AS (
            SELECT
                mp.player_id,
                COUNT(*) AS total_sets,
                SUM(CASE WHEN (mp.side_no = 1 AND g.points_side1 > g.points_side2) OR 
                            (mp.side_no = 2 AND g.points_side2 > g.points_side1) THEN 1 ELSE 0 END) AS sets_won,
                SUM(CASE WHEN MAX(g.points_side1, g.points_side2) > 11 THEN 1 ELSE 0 END) AS total_deuce_sets,
                SUM(CASE WHEN MAX(g.points_side1, g.points_side2) > 11 AND 
                        ((mp.side_no = 1 AND g.points_side1 > g.points_side2) OR 
                        (mp.side_no = 2 AND g.points_side2 > g.points_side1)) THEN 1 ELSE 0 END) AS deuce_sets_won,
                SUM(CASE WHEN mp.side_no = 1 THEN g.points_side1 ELSE g.points_side2 END) AS total_points_scored,
                SUM(CASE WHEN mp.side_no = 1 THEN g.points_side2 ELSE g.points_side1 END) AS total_points_lost,
                AVG(CASE WHEN mp.side_no = 1 THEN g.points_side1 ELSE g.points_side2 END) AS avg_points_scored_per_set,
                AVG(CASE WHEN mp.side_no = 1 THEN g.points_side2 ELSE g.points_side1 END) AS avg_points_lost_per_set,
                MAX(CASE WHEN mp.side_no = 1 THEN g.points_side1 ELSE g.points_side2 END) AS max_points_scored_in_set,
                MIN(CASE WHEN mp.side_no = 1 THEN g.points_side1 ELSE g.points_side2 END) AS min_points_scored_in_set,
                MAX(CASE WHEN mp.side_no = 1 THEN g.points_side2 ELSE g.points_side1 END) AS max_points_lost_in_set,
                MIN(CASE WHEN mp.side_no = 1 THEN g.points_side2 ELSE g.points_side1 END) AS min_points_lost_in_set
            FROM match_player mp
            JOIN match m ON m.match_id = mp.match_id
            JOIN game g ON g.match_id = m.match_id
            WHERE m.status = 'completed' AND m.walkover_side IS NULL
            GROUP BY mp.player_id
        )
        SELECT 
            p.player_id,
            CASE 
                WHEN p.is_verified = 1 THEN p.firstname || ' ' || p.lastname
                ELSE p.fullname_raw
            END AS player_name,
            ms.total_matches,
            ms.match_wins,
            ms.total_matches - ms.match_wins AS match_losses,
            ROUND(ms.match_wins * 100.0 / NULLIF(ms.total_matches, 0), 2) AS match_win_percentage,
            gs.total_sets,
            gs.sets_won,
            gs.total_sets - gs.sets_won AS sets_lost,
            ROUND(gs.sets_won * 100.0 / NULLIF(gs.total_sets, 0), 2) AS set_win_percentage,
            gs.total_deuce_sets,
            gs.deuce_sets_won,
            gs.total_deuce_sets - gs.deuce_sets_won AS deuce_sets_lost,
            ROUND(gs.deuce_sets_won * 100.0 / NULLIF(gs.total_deuce_sets, 0), 2) AS deuce_win_percentage,
            gs.total_points_scored,
            gs.total_points_lost,
            ROUND(gs.total_points_scored * 100.0 / NULLIF((gs.total_points_scored + gs.total_points_lost), 0), 2) AS points_win_percentage,
            ROUND(gs.avg_points_scored_per_set, 2) AS avg_points_scored_per_set,
            ROUND(gs.avg_points_lost_per_set, 2) AS avg_points_lost_per_set,
            gs.max_points_scored_in_set,
            gs.min_points_scored_in_set,
            gs.max_points_lost_in_set,
            gs.min_points_lost_in_set
        FROM player p
        LEFT JOIN match_stats ms ON ms.player_id = p.player_id
        LEFT JOIN game_stats gs ON gs.player_id = p.player_id;
    """
}

def export_resolved_database():
    # Remove existing export DB if it exists
    if os.path.exists(EXPORT_DB_PATH):
        os.remove(EXPORT_DB_PATH)

    # Connect to original and new DB
    conn_original = sqlite3.connect(ORIGINAL_DB_PATH)
    conn_export = sqlite3.connect(EXPORT_DB_PATH)
    
    try:
        cursor_original = conn_original.cursor()
        cursor_export = conn_export.cursor()

        # Get all tables from original DB
        cursor_original.execute("SELECT name FROM sqlite_master WHERE type='table';")
        all_tables = [row[0] for row in cursor_original.fetchall()]

        # Filter tables to include (exclude raw/logs)
        included_tables = [table for table in all_tables if table not in EXCLUDED_TABLES]

        # For each included table: create schema and copy data
        for table in included_tables:
            # Get CREATE TABLE statement
            cursor_original.execute(f"SELECT sql FROM sqlite_master WHERE name='{table}' AND type='table';")
            create_sql = cursor_original.fetchone()[0]
            
            # Create table in export DB
            cursor_export.execute(create_sql)
            
            # Copy data
            cursor_original.execute(f"SELECT * FROM {table};")
            rows = cursor_original.fetchall()
            if rows:
                placeholders = ','.join('?' * len(rows[0]))
                cursor_export.executemany(f"INSERT INTO {table} VALUES ({placeholders});", rows)

        # Recreate existing views
        for view_name, view_sql in EXISTING_VIEWS.items():
            cursor_export.execute(f"CREATE VIEW {view_name} AS {view_sql}")

        # Create new suggested views
        for view_name, view_sql in NEW_VIEWS.items():
            cursor_export.execute(f"CREATE VIEW {view_name} AS {view_sql}")

        # Commit changes
        conn_export.commit()
        print(f"Exported resolved database to {EXPORT_DB_PATH} with {len(included_tables)} tables and {len(EXISTING_VIEWS) + len(NEW_VIEWS)} views.")

    except Exception as e:
        print(f"Error during export: {e}")
    finally:
        conn_original.close()
        conn_export.close()

# Run the export function
if __name__ == "__main__":
    export_resolved_database()