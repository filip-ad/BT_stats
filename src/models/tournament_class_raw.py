# src/models/tournament_class_raw.py

from __future__ import annotations

from dataclasses import dataclass
import datetime
from typing import Optional, Dict, Any, List
import sqlite3
from utils import parse_date

@dataclass
class TournamentClassRaw:
    row_id:                         Optional[int]  = None           # Auto-generated ID for raw entry
    tournament_class_id_ext:        Optional[str]  = None           # External ID from ondata.se or other source
    tournament_id_ext:              Optional[str]  = None           # External ID of parent tournament
    date:                           Optional[datetime.date] = None  # Date of the class
    shortname:                      Optional[str]  = None           # Short description of the class
    longname:                       Optional[str]  = None           # Full description of the class
    gender:                         Optional[str]  = None           # Gender category (e.g., "male", "female")
    max_rank:                       Optional[int]  = None           # Maximum rank allowed in the class
    max_age:                        Optional[int]  = None           # Maximum age allowed in the class
    url:                            Optional[str]  = None           # URL for the class
    raw_stages:                     Optional[str]  = None           # Comma-separated stages from HTML links, for structure inference
    raw_stage_hrefs:                Optional[str]  = None           # JSON string of {stage: href} for PDF downloading
    data_source_id:                 int = 1                         # Data source ID (default 1 for 'ondata')

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "TournamentClassRaw":
        """Instantiate from a scraped dict (keys matching column names)."""
        return TournamentClassRaw(
            tournament_class_id_ext         = d.get("tournament_class_id_ext"),
            tournament_id_ext               = d.get("tournament_id_ext"),
            date                            = parse_date(d.get("date"), context="TournamentClassRaw.from_dict"),
            shortname                       = d.get("shortname", ""),
            longname                        = d.get("longname", ""),
            gender                          = d.get("gender"),
            max_rank                        = d.get("max_rank"),
            max_age                         = d.get("max_age"),
            url                             = d.get("url"),
            raw_stages                      = d.get("raw_stages"),
            raw_stage_hrefs                 = d.get("raw_stage_hrefs"),
            data_source_id                  = d.get("data_source_id", 1)
        )

    def validate(self) -> bool:
        """Light validation: Check for minimum required fields before inserting to raw."""
        return bool(self.shortname and self.date and self.tournament_id_ext)

    def insert(self, cursor) -> None:
        """Insert the raw object into the tournament_class_raw table."""
        vals = (
            self.tournament_class_id_ext, self.tournament_id_ext, self.date, self.shortname, self.longname,
            self.gender, self.max_rank, self.max_age, self.url, self.raw_stages, self.raw_stage_hrefs, self.data_source_id
        )
        cursor.execute(
            """
            INSERT INTO tournament_class_raw (
                tournament_class_id_ext, tournament_id_ext, date, shortname, longname, gender,
                max_rank, max_age, url, raw_stages, raw_stage_hrefs, data_source_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            vals
        )
        self.row_id = cursor.lastrowid

    @classmethod
    def get_pending(cls, cursor: sqlite3.Cursor) -> List['TournamentClassRaw']:
        """Fetch pending raw entries that do not yet exist in the regular tournament_class table (based on ext_id and data_source_id)."""
        sql = """
            SELECT r.* FROM tournament_class_raw r
            WHERE NOT EXISTS (
                SELECT 1 FROM tournament_class c
                WHERE c.tournament_class_id_ext = r.tournament_class_id_ext
                AND c.data_source_id = r.data_source_id
            )
            ORDER BY r.row_created ASC;
        """
        cursor.execute(sql)
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        row_dicts = [dict(zip(columns, row)) for row in rows]
        return [cls.from_dict(rd) for rd in row_dicts]