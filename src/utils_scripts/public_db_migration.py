# src/utils_scripts/public_db_migration.py
#
# Purpose
# -------
# Build a *view-free*, *public-safe*, and *fast* SQLite "publication" database for Pingiskollen.
# This DB is *read-only* for the web app and exposes *no internal numeric IDs*. It contains
# only the denormalized, cache-like tables the UI needs, plus an FTS5 index for search.
#
# Design Principles
# -----------------
# 1) **Separation of concerns**: The Source DB (scrape/resolve) remains the system of record
#    and stays internal. The Publication DB is a derived, public-safe artifact.
# 2) **No heavy joins at runtime**: We pre-compute/materalize what the UI needs into
#    three cache tables:
#      - player_profile_cache
#      - player_results_summary_cache
#      - player_matches_cache
#    and a name FTS index:
#      - player_name_fts
# 3) **Opaque public IDs**: Every player gets a stable `public_id = HMAC(salt, player_id)`.
#    This prevents exposure of internal numeric keys and keeps URLs stable.
# 4) **Deterministic ranking selection per player**: Since a player can have multiple
#    `player_id_ext` rows (one per external system) and many rankings over time, we select
#    a *single* "best" ranking row per player using a deterministic priority:
#       a) latest run_date (across all ext IDs),
#       b) if tie: most recent *actual change* date (points_change_since_last <> 0),
#       c) if tie: higher points,
#       d) tiebreak on ext ID for stability.
#    This avoids join fan-out and guarantees *one profile row per player*.
#
# Safety & Workflow
# -----------------
# - The build writes to OUT_DB.tmp, runs PRAGMA integrity_check + VACUUM, and atomically swaps it
#   into OUT_DB. If anything fails, we never replace the old file.
# - This file uses module variables (no CLI) to make it easy to run from your IDE/venv.
#
# How to run
# ----------
# 1) Adjust SOURCE_DB, OUT_DB, and PUBLIC_SALT (hard-code the salt locally; do not change it later).
# 2) Run:  python src/utils_scripts/public_db_migration.py
# 3) Point Django's PLAYERS_DB_PATH to OUT_DB (open read-only).
#
# Copyright © Pingiskollen

from __future__ import annotations

import datetime as dt
import hashlib, hmac, os, sqlite3, sys, json, time, logging
from logging.handlers import TimedRotatingFileHandler
from contextlib import closing
from pathlib import Path

# ────────────────────────────────────────────────────────────────────────────
# Optional: allow `from src import ...` if we later want to reuse helpers here
# (Not strictly needed right now; safe to keep for future-proofing.)
# ────────────────────────────────────────────────────────────────────────────
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
# Example future imports (currently unused):
# from config import DB_NAME, PUBLIC_DB_NAME
# from db import get_conn

# ────────────────────────────────────────────────────────────────────────────
# Configuration (edit these; no CLI)
# ────────────────────────────────────────────────────────────────────────────
# Use absolute paths to avoid surprises from working directories.
SOURCE_DB   = "/home/filip/dev/pingiskollen/data/table_tennis.db"        # Internal Source DB (resolved base tables only)
OUT_DB      = "/home/filip/dev/pingiskollen/data/pingiskollen_pub.db"    # Publication DB to produce (atomic swap)
PUBLIC_SALT = "e2f5d6b8d1a943b0c45c6b6c713bcd09f74cf3179a4e5e3b8c53f3cfecb0a1ff"  # Stable; never change in production

LOG_FILE    = "/home/filip/dev/pingiskollen/data/logs/public_db_migration.log"
LOG_LEVEL   = os.getenv("PUBDB_LOG_LEVEL", "INFO").upper()


# Optional preference for a specific data source, if in the future you want to prefer e.g. the Swedish list.
# Keep as None for neutral behavior. If set to an integer, we bias ties toward that data_source_id.
PREFER_DATA_SOURCE_ID: int | None = None  # e.g. 3


# ────────────────────────────────────────────────────────────────────────────
# Small helper utilities
# ────────────────────────────────────────────────────────────────────────────

def _die(msg: str, code: int = 2) -> None:
    """Exit with a visible error message. Keep it simple & explicit."""
    print(f"❌ {msg}", file=sys.stderr)
    sys.exit(code)


def _sha256_file(path: str) -> str:
    """Compute SHA-256 of a file to capture an immutable fingerprint of the Source DB."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _hmac_public_id(salt_bytes: bytes, player_id: int) -> str:
    """Opaque, stable, non-reversible public ID for URLs and opponent references."""
    return hmac.new(salt_bytes, str(player_id).encode("utf-8"), hashlib.sha256).hexdigest()


def _connect_ro(db_path: str) -> sqlite3.Connection:
    """Open a SQLite connection in read-only mode (URI with mode=ro)."""
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    return con


def _connect_rw(db_path: str) -> sqlite3.Connection:
    """Open a normal read-write SQLite connection."""
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA foreign_keys = ON;")
    return con

def _setup_logging() -> logging.Logger:
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    logger = logging.getLogger("pubdb")
    logger.setLevel(LOG_LEVEL)

    fmt = logging.Formatter("[%(asctime)s] %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")

    fh = TimedRotatingFileHandler(LOG_FILE, when="midnight", backupCount=30, encoding="utf-8")
    fh.setFormatter(fmt)
    fh.setLevel(LOG_LEVEL)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    sh.setLevel(LOG_LEVEL)

    # avoid duplicate handlers if reloaded
    logger.handlers.clear()
    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger



# ────────────────────────────────────────────────────────────────────────────
# DDL for the Publication DB (schema)
# ────────────────────────────────────────────────────────────────────────────

DDL_PLAYER_PROFILE_CACHE = """
-- One row per *real* player (joined from src.player), with a single chosen ranking snapshot.
CREATE TABLE player_profile_cache (
  public_id               TEXT PRIMARY KEY,  -- HMAC(salt, player_id)
  player_name             TEXT NOT NULL,     -- Display name (verified → First Last; else flip of fullname_raw)
  year_born               INTEGER,           -- Optional for display / filters
  is_verified             INTEGER NOT NULL DEFAULT 0, -- 1 if name was verified/resolved
  recent_club             TEXT,              -- "ShortClub (SeasonLabel)" from the most recent license
  recent_tournament_class TEXT,              -- "TournShort - ClassShort (startdate)" from most recent class
  ranking_groups          TEXT,              -- Comma-separated short labels, e.g. "H3, D2"
  ranking_points          INTEGER,           -- Numeric points (nullable)
  ranking_points_label    TEXT,              -- "1850 (YYYY-MM-DD)" for display
  recent_transition       TEXT               -- "From → To (SeasonLabel)" most recent transition
) WITHOUT ROWID;
"""

DDL_PLAYER_RESULTS_SUMMARY_CACHE = """
-- One row per player with aggregate match/set/point statistics for the public profile.
CREATE TABLE player_results_summary_cache (
  public_id                 TEXT PRIMARY KEY,
  player_name               TEXT NOT NULL,

  -- Matches
  total_matches             INTEGER,
  match_wins                INTEGER,
  match_losses              INTEGER,
  match_win_percentage      REAL,

  -- Sets
  total_sets                INTEGER,
  sets_won                  INTEGER,
  sets_lost                 INTEGER,
  set_win_percentage        REAL,

  -- Deuce sets (any set with max(points) > 11)
  total_deuce_sets          INTEGER,
  deuce_sets_won            INTEGER,
  deuce_sets_lost           INTEGER,
  deuce_win_percentage      REAL,

  -- Points aggregates
  total_points_scored       INTEGER,
  total_points_lost         INTEGER,
  points_win_percentage     REAL,
  avg_points_scored_per_set REAL,
  avg_points_lost_per_set   REAL,
  max_points_scored_in_set  INTEGER,
  min_points_scored_in_set  INTEGER,
  max_points_lost_in_set    INTEGER,
  min_points_lost_in_set    INTEGER
) WITHOUT ROWID;
"""

DDL_PLAYER_MATCHES_CACHE = """
-- Multiple rows per player: one per match they participated in (singles & doubles supported).
-- This table is for fast profile "matches list" rendering, with opponents encoded as:
--     "<opp_public_id>|<name>;<opp_public_id>|<name>"
-- to keep the page linkable without exposing internals.
CREATE TABLE player_matches_cache (
  public_id            TEXT NOT NULL,    -- The player's public id (HMAC)
  player_name          TEXT NOT NULL,    -- Display name for convenience
  match_id             INTEGER,          -- Internal match_id (not used in URLs, but helps with sorting/debug)
  match_date           NUMERIC,          -- ISO or DATE-like numeric string from src.match.date
  best_of              INTEGER,          -- e.g., 5
  status               TEXT,             -- e.g., "completed"
  result               TEXT,             -- "Win" | "Loss" | NULL (if unknown)
  walkover_status      TEXT,             -- "Walkover Win" | "Walkover Loss" | NULL
  opponents_compact    TEXT,             -- "<opp_public_id>|<name>;..."
  opponent_names       TEXT,             -- "Name A, Name B" (for plain text display)
  tournament_shortname TEXT,             -- Contextual metadata for UI
  class_shortname      TEXT,
  stage_description    TEXT,
  stage_round_no       INTEGER,
  game_scores          TEXT              -- "1:11-7; 2:10-12; 3:11-8; ..." rendered from src.game
);
"""

DDL_FTS = """
-- Full-text search on players (name/club/year). public_id is UNINDEXED but carried along for result mapping.
CREATE VIRTUAL TABLE player_name_fts USING fts5(
  public_id UNINDEXED,
  player_name,
  recent_club,
  year_born,
  tokenize='unicode61'
);
"""

DDL_METADATA = """
-- Single-row metadata about this cache build. Useful for ops/debug.
CREATE TABLE cache_metadata (
  built_at        TEXT NOT NULL,   -- UTC ISO timestamp
  source_db_path  TEXT NOT NULL,   -- Absolute path to the Source DB used
  source_db_hash  TEXT NOT NULL,   -- SHA-256 of Source DB at build time
  rows_profile    INTEGER NOT NULL,
  rows_summary    INTEGER NOT NULL,
  rows_matches    INTEGER NOT NULL
);
"""

INDEXES = [
    # Profiles: fast list ordering by (verified desc, points desc, name)
    "CREATE INDEX ix_ppc_verified_points ON player_profile_cache(is_verified DESC, ranking_points DESC, player_name);",
    "CREATE INDEX ix_ppc_name ON player_profile_cache(player_name);",
    # Matches: fast access per player, newest first (tie-break by match_id)
    "CREATE INDEX ix_pmc_player_date ON player_matches_cache(public_id, match_date DESC, match_id DESC);",
    "CREATE INDEX ix_pmc_match_id     ON player_matches_cache(match_id);",
]

# ────────────────────────────────────────────────────────────────────────────
# Common name resolution rule used across inserts:
# - If verified:  "First Last"
# - Else: best-effort flip of "LAST FIRST" → "FIRST LAST"
#   (This mirrors your existing views' behavior.)
# ────────────────────────────────────────────────────────────────────────────
NAME_SQL = """
CASE
  WHEN p.is_verified = 1 THEN TRIM(p.firstname || ' ' || p.lastname)
  ELSE TRIM(
    CASE
      WHEN INSTR(p.fullname_raw, ' ') > 0
      THEN SUBSTR(p.fullname_raw, INSTR(p.fullname_raw, ' ')+1) || ' ' ||
           SUBSTR(p.fullname_raw, 1, INSTR(p.fullname_raw, ' ')-1)
      ELSE p.fullname_raw
    END
  )
END
"""

# ────────────────────────────────────────────────────────────────────────────
# INSERT … SELECT builders (all src.* qualified)
# ---------------------------------------------------------------------------
# Notes on the "ranking points per player" selection:
#  - Your model: player → (many) player_id_ext → (many) player_ranking rows.
#  - We must collapse this to exactly ONE row per player (no fan-out). We do it in stages:
#
#    ext_latest: latest row per (player_id_ext, data_source_id)
#    ext_last_change: last run_date per ext where points actually changed
#    ranking_points_per_player: choose the best ext for each player with this order:
#       1) most recent run_date,
#       2) most recent "last change" run_date (if ties on 1),
#       3) higher points (if ties persist),
#       4) final tiebreak by player_id_ext for stability.
#
#  - Optional bias toward a preferred data_source_id (if configured):
#       ORDER BY (preferred first), then the rest as above.
# ────────────────────────────────────────────────────────────────────────────

def _insert_ppc_sql(prefer_ds: int | None) -> str:
    prefer_clause = "0"  # neutral (no preference)
    if prefer_ds is not None:
        prefer_clause = f"CASE WHEN el.data_source_id = {int(prefer_ds)} THEN 0 ELSE 1 END"

    return f"""
WITH recent_license AS (
    SELECT pl.player_id,
           c.shortname || ' (' || s.label || ')' AS club_with_season,
           ROW_NUMBER() OVER (PARTITION BY pl.player_id ORDER BY s.start_date DESC) AS rn
    FROM src.player_license pl
    JOIN src.season s ON s.season_id = pl.season_id
    JOIN src.club   c ON c.club_id   = pl.club_id
),
recent_tournament AS (
    SELECT tcp.player_id,
           t.shortname AS tournament_name,
           tc.shortname AS class_shortname,
           tc.startdate AS class_startdate,
           ROW_NUMBER() OVER (PARTITION BY tcp.player_id ORDER BY tc.startdate DESC) AS rn
    FROM src.tournament_class_player tcp
    JOIN src.tournament_class_entry tce ON tce.tournament_class_entry_id = tcp.tournament_class_entry_id
    JOIN src.tournament_class tc        ON tc.tournament_class_id        = tce.tournament_class_id
    JOIN src.tournament t               ON t.tournament_id               = tc.tournament_id
),
recent_transition AS (
    SELECT pt.player_id,
           cf.shortname || ' → ' || ct.shortname || ' (' || s.label || ')' AS transition_text,
           ROW_NUMBER() OVER (PARTITION BY pt.player_id ORDER BY s.start_date DESC) AS rn
    FROM src.player_transition pt
    JOIN src.club cf ON pt.club_id_from = cf.club_id
    JOIN src.club ct ON pt.club_id_to   = ct.club_id
    JOIN src.season s ON pt.season_id   = s.season_id
),
ranking_groups AS (
    SELECT prg.player_id, GROUP_CONCAT(rg.class_short, ', ') AS ranking_groups
    FROM src.player_ranking_group prg
    JOIN src.ranking_group rg ON rg.ranking_group_id = prg.ranking_group_id
    GROUP BY prg.player_id
),

-- Latest ranking row per (player_id_ext, data_source_id).
ext_latest AS (
  SELECT
    pie.player_id,
    pr.player_id_ext,
    pr.data_source_id,
    pr.points,
    pr.run_date,
    ROW_NUMBER() OVER (
      PARTITION BY pr.player_id_ext, pr.data_source_id
      ORDER BY pr.run_date DESC
    ) AS rn_ext
  FROM src.player_id_ext pie
  JOIN src.player_ranking pr
    ON pr.player_id_ext = pie.player_id_ext
   AND pr.data_source_id = pie.data_source_id
),

-- Last date where points actually changed per (player_id_ext, data_source_id).
ext_last_change AS (
  SELECT
    pr.player_id_ext,
    pr.data_source_id,
    MAX(CASE WHEN pr.points_change_since_last <> 0 THEN pr.run_date END) AS last_change_run_date
  FROM src.player_ranking pr
  GROUP BY pr.player_id_ext, pr.data_source_id
),

-- Choose ONE "best" ext per player with deterministic tie-breakers.
ranking_points_per_player AS (
  SELECT x.player_id, x.points, x.run_date
  FROM (
    SELECT
      el.*,
      ROW_NUMBER() OVER (
        PARTITION BY el.player_id
        ORDER BY
          {prefer_clause},                                   -- preferred DS first (if configured)
          el.run_date DESC,                                  -- 1) most recent run
          COALESCE(lc.last_change_run_date, '0000-00-00') DESC, -- 2) most recent actual change
          el.points DESC,                                    -- 3) higher points
          el.player_id_ext                                   -- 4) stable tiebreak
      ) AS rn
    FROM ext_latest el
    LEFT JOIN ext_last_change lc
      ON lc.player_id_ext = el.player_id_ext
     AND lc.data_source_id = el.data_source_id
    WHERE el.rn_ext = 1
  ) x
  WHERE x.rn = 1
)

INSERT INTO player_profile_cache(
  public_id, player_name, year_born, is_verified,
  recent_club, recent_tournament_class,
  ranking_groups, ranking_points, ranking_points_label, recent_transition
)
SELECT
  map.public_id,
  {NAME_SQL} AS player_name,
  p.year_born,
  p.is_verified,
  rl.club_with_season AS recent_club,
  CASE WHEN rt.tournament_name IS NOT NULL
       THEN rt.tournament_name || ' - ' || rt.class_shortname || ' (' || rt.class_startdate || ')'
       ELSE NULL
  END AS recent_tournament_class,
  COALESCE(rg.ranking_groups, '') AS ranking_groups,
  rpp.points                       AS ranking_points,
  CASE WHEN rpp.points IS NOT NULL THEN rpp.points || ' (' || rpp.run_date || ')' ELSE '' END AS ranking_points_label,
  tr.transition_text AS recent_transition
FROM src.player p
JOIN tmp_public_id_map map ON map.player_id = p.player_id
LEFT JOIN recent_license rl     ON rl.player_id = p.player_id AND rl.rn = 1
LEFT JOIN recent_tournament rt  ON rt.player_id = p.player_id AND rt.rn = 1
LEFT JOIN ranking_groups rg      ON rg.player_id = p.player_id
LEFT JOIN recent_transition tr   ON tr.player_id = p.player_id AND tr.rn = 1
LEFT JOIN ranking_points_per_player rpp ON rpp.player_id = p.player_id;
"""


# Player results summary: aggregate per player over completed, non-walkover matches
INSERT_SUMMARY = f"""
WITH match_stats AS (
    SELECT mp.player_id,
           COUNT(DISTINCT m.match_id) AS total_matches,
           SUM(CASE WHEN m.winner_side = mp.side_no THEN 1 ELSE 0 END) AS match_wins
    FROM src.match_player mp
    JOIN src.match m ON m.match_id = mp.match_id
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
    FROM src.match_player mp
    JOIN src.match m ON m.match_id = mp.match_id
    JOIN src.game  g ON g.match_id = m.match_id
    WHERE m.status = 'completed' AND m.walkover_side IS NULL
    GROUP BY mp.player_id
)
INSERT INTO player_results_summary_cache(
  public_id, player_name,
  total_matches, match_wins, match_losses, match_win_percentage,
  total_sets, sets_won, sets_lost, set_win_percentage,
  total_deuce_sets, deuce_sets_won, deuce_sets_lost, deuce_win_percentage,
  total_points_scored, total_points_lost, points_win_percentage,
  avg_points_scored_per_set, avg_points_lost_per_set,
  max_points_scored_in_set, min_points_scored_in_set,
  max_points_lost_in_set,   min_points_lost_in_set
)
SELECT
  map.public_id,
  {NAME_SQL} AS player_name,
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
  ROUND(gs.avg_points_scored_per_set, 2),
  ROUND(gs.avg_points_lost_per_set, 2),
  gs.max_points_scored_in_set,
  gs.min_points_scored_in_set,
  gs.max_points_lost_in_set,
  gs.min_points_lost_in_set
FROM src.player p
JOIN tmp_public_id_map map ON map.player_id = p.player_id
LEFT JOIN match_stats ms ON ms.player_id = p.player_id
LEFT JOIN game_stats  gs ON gs.player_id = p.player_id;
"""

# Player matches: one row per (player, match), with opponents compacted and context attached.
INSERT_MATCHES = f"""
WITH base AS (
  SELECT mp.player_id, mp.match_id, mp.side_no,
         m.date AS match_date, m.best_of, m.status, m.walkover_side, m.winner_side AS winner_side_db
  FROM src.match_player mp
  JOIN src.match m ON m.match_id = mp.match_id
),

-- Opponents (supports doubles: there can be 1 or more opponents)
opps_raw AS (
  SELECT
    b.player_id,
    b.match_id,
    op.player_id AS opp_id,
    TRIM(CASE 
      WHEN op.is_verified = 1 THEN op.firstname || ' ' || op.lastname
      ELSE CASE 
        WHEN INSTR(op.fullname_raw, ' ') > 0 
        THEN SUBSTR(op.fullname_raw, INSTR(op.fullname_raw, ' ')+1) || ' ' ||
             SUBSTR(op.fullname_raw, 1, INSTR(op.fullname_raw, ' ')-1)
        ELSE op.fullname_raw
      END
    END) AS opp_name
  FROM base b
  JOIN src.match_player mp2 ON mp2.match_id = b.match_id AND mp2.side_no <> b.side_no
  JOIN src.player op ON op.player_id = mp2.player_id
),

-- Deduplicate accidental doubles (safety net), then compact:
opps AS (
  SELECT r.player_id, r.match_id,
         GROUP_CONCAT(map2.public_id || '|' || r.opp_name, ';') AS opponents_compact,
         GROUP_CONCAT(r.opp_name, ', ') AS opponent_names
  FROM (SELECT DISTINCT player_id, match_id, opp_id, opp_name FROM opps_raw) r
  JOIN tmp_public_id_map map2 ON map2.player_id = r.opp_id
  GROUP BY r.player_id, r.match_id
),

-- Compute winner side from game rows (if not present in match)
games_calc AS (
  SELECT g.match_id,
         SUM(CASE WHEN g.points_side1 > g.points_side2 THEN 1 ELSE 0 END) AS side1_sets_won,
         SUM(CASE WHEN g.points_side2 > g.points_side1 THEN 1 ELSE 0 END) AS side2_sets_won
  FROM src.game g GROUP BY g.match_id
),
games_winner AS (
  SELECT match_id,
         CASE WHEN side1_sets_won > side2_sets_won THEN 1
              WHEN side2_sets_won > side1_sets_won THEN 2 ELSE NULL END AS winner_side_calc
  FROM games_calc
),

-- Render readable score-lines for each perspective (1 or 2)
scores_side1 AS (
  SELECT g.match_id,
         GROUP_CONCAT(g.game_no || ':' || g.points_side1 || '-' || g.points_side2, '; ') AS score_text
  FROM src.game g GROUP BY g.match_id
),
scores_side2 AS (
  SELECT g.match_id,
         GROUP_CONCAT(g.game_no || ':' || g.points_side2 || '-' || g.points_side1, '; ') AS score_text
  FROM src.game g GROUP BY g.match_id
),

-- Tournament/class stage context for UI
ctx AS (
  SELECT tcm.match_id,
         t.shortname  AS tournament_shortname,
         tc.shortname AS class_shortname,
         tcm.stage_round_no,
         tcs.description AS stage_description
  FROM src.tournament_class_match tcm
  LEFT JOIN src.tournament_class tc ON tc.tournament_class_id = tcm.tournament_class_id
  LEFT JOIN src.tournament t        ON t.tournament_id        = tc.tournament_id
  LEFT JOIN src.tournament_class_stage tcs ON tcs.tournament_class_stage_id = tcm.tournament_class_stage_id
)

INSERT INTO player_matches_cache(
  public_id, player_name, match_id, match_date, best_of, status, result, walkover_status,
  opponents_compact, opponent_names, tournament_shortname, class_shortname, stage_description,
  stage_round_no, game_scores
)
SELECT
  map.public_id,
  {NAME_SQL} AS player_name,
  b.match_id,
  b.match_date,
  b.best_of,
  b.status,
  CASE COALESCE(b.winner_side_db, gw.winner_side_calc)
    WHEN b.side_no THEN 'Win'
    WHEN 3 - b.side_no THEN 'Loss'
    ELSE NULL
  END AS result,
  CASE b.walkover_side
    WHEN b.side_no     THEN 'Walkover Loss'
    WHEN 3 - b.side_no THEN 'Walkover Win'
    ELSE NULL
  END AS walkover_status,
  o.opponents_compact,
  o.opponent_names,
  c.tournament_shortname,
  c.class_shortname,
  c.stage_description,
  c.stage_round_no,
  CASE b.side_no WHEN 1 THEN s1.score_text WHEN 2 THEN s2.score_text ELSE NULL END AS game_scores
FROM base b
JOIN src.player p           ON p.player_id = b.player_id
JOIN tmp_public_id_map map  ON map.player_id = b.player_id
LEFT JOIN opps o            ON o.player_id  = b.player_id AND o.match_id = b.match_id
LEFT JOIN games_winner gw   ON gw.match_id  = b.match_id
LEFT JOIN scores_side1 s1   ON s1.match_id  = b.match_id
LEFT JOIN scores_side2 s2   ON s2.match_id  = b.match_id
LEFT JOIN ctx c             ON c.match_id   = b.match_id
ORDER BY b.match_date DESC, b.match_id DESC;
"""

# ────────────────────────────────────────────────────────────────────────────
# Build routine
# ────────────────────────────────────────────────────────────────────────────

def run_public_db_build() -> None:
    logger = _setup_logging()
    start = time.perf_counter()

    # Basic guards
    if not Path(SOURCE_DB).exists():
        _die(f"Source DB not found: {SOURCE_DB}")
    if not PUBLIC_SALT or len(PUBLIC_SALT) < 16:
        _die("Missing/short PUBLIC_SALT. Provide a long random secret (>=16 chars).")

    salt_bytes = PUBLIC_SALT.encode("utf-8")
    source_hash = _sha256_file(SOURCE_DB)
    source_size = os.path.getsize(SOURCE_DB)

    out_tmp = OUT_DB + ".tmp"
    if Path(out_tmp).exists():
        Path(out_tmp).unlink()

    logger.info("▶️  Start build")
    logger.info("source=%s (size=%d bytes, sha256=%s)", SOURCE_DB, source_size, source_hash)
    logger.info("out=%s tmp=%s", OUT_DB, out_tmp)

    try:
        t0 = time.perf_counter()
        with closing(_connect_ro(SOURCE_DB)) as src, closing(_connect_rw(out_tmp)) as dst:
            # Fast bulk build PRAGMAs
            dst.executescript("""
                PRAGMA journal_mode=OFF;
                PRAGMA synchronous=OFF;
                PRAGMA temp_store=MEMORY;
                PRAGMA cache_size=-400000;   -- ~400MB during build
                PRAGMA page_size=32768;
                PRAGMA mmap_size=268435456;  -- 256MB
            """)
            logger.info("PRAGMAs applied for build performance")

            # 1) Create schema
            dst.executescript(DDL_PLAYER_PROFILE_CACHE)
            dst.executescript(DDL_PLAYER_RESULTS_SUMMARY_CACHE)
            dst.executescript(DDL_PLAYER_MATCHES_CACHE)
            dst.executescript(DDL_FTS)
            dst.executescript(DDL_METADATA)
            logger.info("Schema created")
            t_schema = time.perf_counter()

            # 2) Temp map: player_id → public_id
            dst.execute("CREATE TEMP TABLE tmp_public_id_map (player_id INTEGER PRIMARY KEY, public_id TEXT UNIQUE NOT NULL)")
            rows = src.execute("SELECT player_id FROM player").fetchall()
            dst.executemany(
                "INSERT INTO tmp_public_id_map(player_id, public_id) VALUES (?, ?)",
                [(r["player_id"], _hmac_public_id(salt_bytes, r["player_id"])) for r in rows]
            )
            logger.info("Temp id-map built: %d players", len(rows))
            t_map = time.perf_counter()

            # 3) Populate caches
            dst.execute("ATTACH DATABASE ? AS src", (SOURCE_DB,))
            logger.info("Attached source DB as 'src'")

            dst.executescript(_insert_ppc_sql(PREFER_DATA_SOURCE_ID))
            t_ppc = time.perf_counter()
            logger.info("Inserted player_profile_cache")

            dst.executescript(INSERT_SUMMARY)
            t_sum = time.perf_counter()
            logger.info("Inserted player_results_summary_cache")

            dst.executescript(INSERT_MATCHES)
            t_matches = time.perf_counter()
            logger.info("Inserted player_matches_cache")

            # 4) Seed FTS
            dst.execute("""
                INSERT INTO player_name_fts(public_id, player_name, recent_club, year_born)
                SELECT public_id, player_name, COALESCE(recent_club,''), COALESCE(year_born,'')
                FROM player_profile_cache;
            """)
            t_fts = time.perf_counter()
            logger.info("Seeded player_name_fts")

            # 5) Indexes & stats
            for ix in INDEXES:
                dst.execute(ix)
            dst.executescript("ANALYZE; PRAGMA optimize;")
            t_index = time.perf_counter()
            logger.info("Indexes created and ANALYZE/optimize done")

            # 6) Sanity checks
            src_count_players = dst.execute("SELECT COUNT(*) FROM src.player").fetchone()[0]
            prof_count        = dst.execute("SELECT COUNT(*) FROM player_profile_cache").fetchone()[0]
            dup_public_ids    = dst.execute("""
                SELECT COUNT(*) FROM (
                  SELECT public_id, COUNT(*) c
                  FROM player_profile_cache
                  GROUP BY public_id
                  HAVING c > 1
                ) d
            """).fetchone()[0]

            logger.info("Sanity: src.players=%d, profile_rows=%d, duplicate_public_id=%d",
                        src_count_players, prof_count, dup_public_ids)

            if dup_public_ids != 0:
                _die("Duplicate public_id detected in player_profile_cache (should be impossible).")

            # 7) Metadata
            built_at_dt = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
            built_at    = built_at_dt.isoformat().replace("+00:00", "Z")
            rows_sum    = dst.execute("SELECT COUNT(*) FROM player_results_summary_cache").fetchone()[0]
            rows_match  = dst.execute("SELECT COUNT(*) FROM player_matches_cache").fetchone()[0]
            dst.execute(
                "INSERT INTO cache_metadata(built_at, source_db_path, source_db_hash, rows_profile, rows_summary, rows_matches) VALUES (?,?,?,?,?,?)",
                (built_at, os.path.abspath(SOURCE_DB), source_hash, prof_count, rows_sum, rows_match)
            )
            logger.info("Metadata inserted: built_at=%s", built_at)

            # 8) Validate + VACUUM
            ok = dst.execute("PRAGMA integrity_check;").fetchone()[0]
            if ok != "ok":
                _die(f"Integrity check failed: {ok}")
            dst.executescript("VACUUM;")
            t_vac = time.perf_counter()
            logger.info("Integrity check OK and VACUUM done")

        # Atomic swap
        os.replace(out_tmp, OUT_DB)
        out_size = os.path.getsize(OUT_DB)
        logger.info("Swapped tmp into place: %s (size=%d bytes)", OUT_DB, out_size)

        # Phase timings (seconds)
        total = time.perf_counter() - start
        timings = {
            "schema": t_schema - t0,
            "id_map": t_map - t_schema,
            "insert_profiles": t_ppc - t_map,
            "insert_summary": t_sum - t_ppc,
            "insert_matches": t_matches - t_sum,
            "fts_seed": t_fts - t_matches,
            "index_optimize": t_index - t_fts,
            "integrity_vacuum": t_vac - t_index,
            "total": total,
        }

        # One-line JSON summary for easy grepping
        summary = {
            "built_at": built_at,
            "source_db": SOURCE_DB,
            "source_sha256": source_hash,
            "source_size": source_size,
            "out_db": OUT_DB,
            "out_size": out_size,
            "rows": {
                "players_src": src_count_players,
                "player_profile_cache": prof_count,
                "player_results_summary_cache": rows_sum,
                "player_matches_cache": rows_match,
            },
            "timings_sec": {k: round(v, 3) for k, v in timings.items()},
        }
        logger.info("SUMMARY %s", json.dumps(summary, ensure_ascii=False))

    except Exception:
        # Try to remove tmp on failure, but don't mask the original exception
        try:
            if Path(out_tmp).exists():
                Path(out_tmp).unlink()
        finally:
            logger.exception("❌ Build failed")
            raise


# Allow direct execution without CLI args
if __name__ == "__main__":
    run_public_db_build()
