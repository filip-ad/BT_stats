# src/models/league_fixture_match_raw.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Dict, Any, Tuple
import sqlite3
from utils import compute_content_hash as _compute_content_hash


@dataclass
class LeagueFixtureMatchRaw:
    row_id:                 Optional[int] = None
    league_fixture_id_ext:  Optional[str] = None
    league_fixture_match_id_ext: Optional[str] = None
    home_player_id_ext:     Optional[str] = None
    home_player_name:       Optional[str] = None
    away_player_id_ext:     Optional[str] = None
    away_player_name:       Optional[str] = None
    tokens:                 Optional[str] = None            # set-by-set tokens and/or match score
    fixture_standing:       Optional[str] = None            # "3 - 2" etc.
    data_source_id:         int = 3
    content_hash:           Optional[str] = None
    last_seen_at:           Optional[str] = None
    row_created:            Optional[str] = None
    row_updated:            Optional[str] = None

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "LeagueFixtureMatchRaw":
        return LeagueFixtureMatchRaw(
            row_id                = d.get("row_id"),
            league_fixture_id_ext = d.get("league_fixture_id_ext"),
            league_fixture_match_id_ext = d.get("league_fixture_match_id_ext"),
            home_player_id_ext     = d.get("home_player_id_ext"),
            home_player_name      = d.get("home_player_name"),
            away_player_id_ext     = d.get("away_player_id_ext"),
            away_player_name      = d.get("away_player_name"),
            tokens                = d.get("tokens"),
            fixture_standing      = d.get("fixture_standing"),
            data_source_id        = d.get("data_source_id", 3),
            content_hash          = d.get("content_hash"),
            last_seen_at          = d.get("last_seen_at"),
            row_created           = d.get("row_created"),
            row_updated           = d.get("row_updated"),
        )

    def validate(self) -> Tuple[bool, str]:
        missing = []
        for field in ("league_fixture_id_ext", "league_fixture_match_id_ext"):
            if not getattr(self, field):
                missing.append(field)
        if missing:
            return False, f"Missing fields: {', '.join(missing)}"
        return True, ""

    def compute_content_hash(self) -> str:
        return _compute_content_hash(
            self,
            exclude_fields={
                "row_id", "data_source_id", "row_created",
                "row_updated", "last_seen_at", "content_hash"
            }
        )

    def upsert(self, cursor: sqlite3.Cursor) -> Optional[str]:
        is_valid, _ = self.validate()
        if not is_valid:
            return None

        new_hash = self.compute_content_hash()
        sql = """
        INSERT INTO league_fixture_match_raw (
            league_fixture_id_ext, league_fixture_match_id_ext,
            home_player_id_ext, home_player_name,
            away_player_id_ext, away_player_name,
            tokens, fixture_standing,
            data_source_id, content_hash, last_seen_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT (league_fixture_id_ext, league_fixture_match_id_ext, data_source_id) DO UPDATE SET
            home_player_id_ext = excluded.home_player_id_ext,
            home_player_name = excluded.home_player_name,
            away_player_id_ext = excluded.away_player_id_ext,
            away_player_name = excluded.away_player_name,
            tokens = excluded.tokens,
            fixture_standing = excluded.fixture_standing,
            content_hash = excluded.content_hash,
            last_seen_at = CURRENT_TIMESTAMP,
            row_updated = CASE
                WHEN league_fixture_match_raw.content_hash IS NULL OR league_fixture_match_raw.content_hash <> excluded.content_hash
                    THEN CURRENT_TIMESTAMP
                ELSE league_fixture_match_raw.row_updated
            END
        WHERE league_fixture_match_raw.content_hash IS NULL OR league_fixture_match_raw.content_hash <> excluded.content_hash
        RETURNING row_id;
        """

        vals = (
            self.league_fixture_id_ext,
            self.league_fixture_match_id_ext,
            self.home_player_id_ext,
            self.home_player_name,
            self.away_player_id_ext,
            self.away_player_name,
            self.tokens,
            self.fixture_standing,
            self.data_source_id,
            new_hash,
        )

        cursor.execute(sql, vals)
        row = cursor.fetchone()
        if row:
            self.row_id = row[0]
            if cursor.lastrowid == self.row_id:
                return "inserted"
            return "updated"

        touch_sql = """
        UPDATE league_fixture_match_raw
        SET last_seen_at = CURRENT_TIMESTAMP
        WHERE league_fixture_id_ext = ? AND league_fixture_match_id_ext = ? AND data_source_id = ?
        RETURNING row_id;
        """
        cursor.execute(touch_sql, (self.league_fixture_id_ext, self.league_fixture_match_id_ext, self.data_source_id))
        touched = cursor.fetchone()
        if touched:
            self.row_id = touched[0]
        return "unchanged"
