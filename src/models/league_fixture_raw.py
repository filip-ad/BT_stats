# src/models/league_fixture_raw.py

from __future__ import annotations

from dataclasses import dataclass
import datetime
from typing import Optional, Dict, Any, Tuple
import sqlite3
from utils import parse_date, compute_content_hash as _compute_content_hash


@dataclass
class LeagueFixtureRaw:
    row_id:                 Optional[int] = None
    league_fixture_id_ext:  Optional[str] = None
    league_id_ext:          Optional[str] = None
    startdate:              Optional[datetime.date] = None
    round:                  Optional[str] = None
    home_team_name:         Optional[str] = None
    away_team_name:         Optional[str] = None
    home_score:             Optional[int] = None
    away_score:             Optional[int] = None
    status:                 str = "completed"
    url:                    Optional[str] = None
    data_source_id:         int = 3
    content_hash:           Optional[str] = None
    last_seen_at:           Optional[datetime.datetime] = None
    row_created:            Optional[datetime.datetime] = None
    row_updated:            Optional[datetime.datetime] = None

    @staticmethod
    def _to_int(value: Any) -> Optional[int]:
        try:
            if value is None or value == "":
                return None
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "LeagueFixtureRaw":
        return LeagueFixtureRaw(
            row_id                 = d.get("row_id"),
            league_fixture_id_ext  = d.get("league_fixture_id_ext"),
            league_id_ext          = d.get("league_id_ext"),
            startdate              = parse_date(d.get("startdate") or d.get("start_date"), context="LeagueFixtureRaw.from_dict"),
            round                  = d.get("round"),
            home_team_name         = d.get("home_team_name"),
            away_team_name         = d.get("away_team_name"),
            home_score             = LeagueFixtureRaw._to_int(d.get("home_score")),
            away_score             = LeagueFixtureRaw._to_int(d.get("away_score")),
            status                 = d.get("status", "completed"),
            url                    = d.get("url"),
            data_source_id         = d.get("data_source_id", 3),
            content_hash           = d.get("content_hash"),
            last_seen_at           = d.get("last_seen_at"),
            row_created            = d.get("row_created"),
            row_updated            = d.get("row_updated"),
        )

    def validate(self) -> Tuple[bool, str]:
        missing = []
        for field in ("league_fixture_id_ext", "league_id_ext"):
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
        """
        Upsert league_fixture_raw on (league_fixture_id_ext, data_source_id).
        """
        is_valid, _ = self.validate()
        if not is_valid:
            return None

        new_hash = self.compute_content_hash()
        sql = """
        INSERT INTO league_fixture_raw (
            league_fixture_id_ext, league_id_ext, startdate, round,
            home_team_name, away_team_name, home_score, away_score, status, url,
            data_source_id, content_hash, last_seen_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT (league_fixture_id_ext, data_source_id) DO UPDATE SET
            league_id_ext = excluded.league_id_ext,
            startdate = excluded.startdate,
            round = excluded.round,
            home_team_name = excluded.home_team_name,
            away_team_name = excluded.away_team_name,
            home_score = excluded.home_score,
            away_score = excluded.away_score,
            status = excluded.status,
            url = excluded.url,
            content_hash = excluded.content_hash,
            last_seen_at = CURRENT_TIMESTAMP,
            row_updated = CASE
                WHEN league_fixture_raw.content_hash IS NULL OR league_fixture_raw.content_hash <> excluded.content_hash
                    THEN CURRENT_TIMESTAMP
                ELSE league_fixture_raw.row_updated
            END
        WHERE league_fixture_raw.content_hash IS NULL OR league_fixture_raw.content_hash <> excluded.content_hash
        RETURNING row_id;
        """

        vals = (
            self.league_fixture_id_ext,
            self.league_id_ext,
            self.startdate,
            self.round,
            self.home_team_name,
            self.away_team_name,
            self.home_score,
            self.away_score,
            self.status,
            self.url,
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
        UPDATE league_fixture_raw
        SET last_seen_at = CURRENT_TIMESTAMP
        WHERE league_fixture_id_ext = ? AND data_source_id = ?
        RETURNING row_id;
        """
        cursor.execute(touch_sql, (self.league_fixture_id_ext, self.data_source_id))
        touched = cursor.fetchone()
        if touched:
            self.row_id = touched[0]
        return "unchanged"
