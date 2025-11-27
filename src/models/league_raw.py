# src/models/league_raw.py

from __future__ import annotations

from dataclasses import dataclass
import datetime
from typing import Optional, Dict, Any, Tuple
import sqlite3
from utils import parse_date
from utils import compute_content_hash as _compute_content_hash


@dataclass
class LeagueRaw:
    row_id:                 Optional[int] = None
    league_id_ext:          Optional[str] = None            # e.g. "27149" from &k=LS27149
    season_label:           Optional[str] = None            # e.g. "*Säsongen 2025-2026"
    season_id_ext:          Optional[str] = None            # the id from serieoppsett_sesong.php?id=768
    league_level:           Optional[str] = None            # "National" | "Regional" | "District"
    district_id_ext:        Optional[str] = None            # not exposed by Profixio today
    district_description:   Optional[str] = None            # only filled for District level
    name:                   Optional[str] = None            # full name shown in the list
    organiser:              Optional[str] = None            # usually "SBTF" or district name
    active:                 int = 0                         # 1 if current season has matches, else 0
    url:                    Optional[str] = None
    startdate:              Optional[datetime.date] = None
    enddate:                Optional[datetime.date] = None
    data_source_id:         int = 3
    content_hash:           Optional[str] = None
    last_seen_at:           Optional[datetime.datetime] = None
    row_created:            Optional[datetime.datetime] = None
    row_updated:            Optional[datetime.datetime] = None

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "LeagueRaw":
        organiser = d.get("organiser") or d.get("organizer")
        district_desc = d.get("district_description") or d.get("district_name")
        return LeagueRaw(
            row_id               = d.get("row_id"),
            league_id_ext        = d.get("league_id_ext"),
            season_label         = d.get("season_label"),
            season_id_ext        = d.get("season_id_ext"),
            league_level         = d.get("league_level"),
            district_id_ext      = d.get("district_id_ext"),
            district_description = district_desc,
            name                 = d.get("name"),
            organiser            = organiser,
            active               = d.get("active", 0),
            url                  = d.get("url"),
            startdate            = parse_date(d.get("startdate") or d.get("start_date"), context="LeagueRaw.from_dict"),
            enddate              = parse_date(d.get("enddate") or d.get("end_date"), context="LeagueRaw.from_dict"),
            data_source_id       = d.get("data_source_id", 3),
            content_hash         = d.get("content_hash"),
            last_seen_at         = d.get("last_seen_at"),
            row_created          = d.get("row_created"),
            row_updated          = d.get("row_updated"),
        )

    def validate(self) -> Tuple[bool, str]:
        missing = []
        for field in ("league_id_ext", "season_id_ext", "name", "league_level"):
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
        Upsert league_raw on (league_id_ext, data_source_id) with content-hash gating.

        Returns one of: "inserted", "updated", "unchanged", or None (invalid).
        """
        is_valid, err = self.validate()
        if not is_valid:
            return None

        new_hash = self.compute_content_hash()
        sql = """
        INSERT INTO league_raw (
            league_id_ext, season_id_ext, league_level, name, organiser,
            district_id_ext, district_description, active, url, startdate, enddate,
            data_source_id, content_hash, last_seen_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT (league_id_ext, data_source_id) DO UPDATE SET
            season_id_ext = excluded.season_id_ext,
            league_level = excluded.league_level,
            name = excluded.name,
            organiser = excluded.organiser,
            district_id_ext = excluded.district_id_ext,
            district_description = excluded.district_description,
            active = excluded.active,
            url = excluded.url,
            startdate = excluded.startdate,
            enddate = excluded.enddate,
            content_hash = excluded.content_hash,
            last_seen_at = CURRENT_TIMESTAMP,
            row_updated = CASE
                WHEN league_raw.content_hash IS NULL OR league_raw.content_hash <> excluded.content_hash
                    THEN CURRENT_TIMESTAMP
                ELSE league_raw.row_updated
            END
        WHERE league_raw.content_hash IS NULL OR league_raw.content_hash <> excluded.content_hash
        RETURNING row_id;
        """

        vals = (
            self.league_id_ext,
            self.season_id_ext,
            self.league_level,
            self.name,
            self.organiser,
            self.district_id_ext,
            self.district_description,
            self.active,
            self.url,
            self.startdate,
            self.enddate,
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
        UPDATE league_raw
        SET last_seen_at = CURRENT_TIMESTAMP
        WHERE league_id_ext = ? AND data_source_id = ?
        RETURNING row_id;
        """
        cursor.execute(touch_sql, (self.league_id_ext, self.data_source_id))
        touched = cursor.fetchone()
        if touched:
            self.row_id = touched[0]
        return "unchanged"
