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
    tournament_id_ext:              Optional[str]  = None           # External ID of parent tournament
    tournament_class_id_ext:        Optional[str]  = None           # External ID from ondata.se or other source
    startdate:                      Optional[datetime.date] = None  # Date of the class
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
            tournament_id_ext               = d.get("tournament_id_ext"),
            tournament_class_id_ext         = d.get("tournament_class_id_ext"),
            startdate                       = parse_date(d.get("startdate"), context="TournamentClassRaw.from_dict"),
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
        return bool(self.shortname and self.startdate and self.tournament_id_ext)

    @classmethod
    def get_all(cls, cursor: sqlite3.Cursor) -> List["TournamentClassRaw"]:
        """Fetch all valid raw tournament class entries."""
        cursor.execute("SELECT * FROM tournament_class_raw")
        columns = [
            'row_id', 'tournament_id_ext', 'tournament_class_id_ext', 'startdate',
            'shortname', 'longname', 'gender', 'max_rank', 'max_age', 'url',
            'raw_stages', 'raw_stage_hrefs', 'data_source_id'
        ]
        return [cls.from_dict(dict(zip(columns, row))) for row in cursor.fetchall()]

    def upsert(self, cursor: sqlite3.Cursor) -> str:
        """
        Upsert the raw tournament class data based on (tournament_id_ext, tournament_class_id_ext, data_source_id).
        Returns "inserted" or "updated" to indicate the action performed.
        """
        action = None
        row_id = None

        # Check if a row exists with the unique constraint
        cursor.execute(
            """
            SELECT row_id FROM tournament_class_raw
            WHERE tournament_id_ext = ? AND tournament_class_id_ext = ? AND data_source_id = ?;
            """,
            (self.tournament_id_ext, self.tournament_class_id_ext, self.data_source_id),
        )
        row = cursor.fetchone()

        if row:
            row_id = row[0]
            # Update existing row
            cursor.execute(
                """
                UPDATE tournament_class_raw
                SET startdate = ?,
                    shortname = ?,
                    longname = ?,
                    gender = ?,
                    max_rank = ?,
                    max_age = ?,
                    url = ?,
                    raw_stages = ?,
                    raw_stage_hrefs = ?,
                    row_updated = CURRENT_TIMESTAMP
                WHERE row_id = ?
                RETURNING row_id;
                """,
                (
                    self.startdate,
                    self.shortname,
                    self.longname,
                    self.gender,
                    self.max_rank,
                    self.max_age,
                    self.url,
                    self.raw_stages,
                    self.raw_stage_hrefs,
                    row_id,
                ),
            )
            self.row_id = cursor.fetchone()[0]
            action = "updated"
        else:
            # Insert new row
            cursor.execute(
                """
                INSERT INTO tournament_class_raw (
                    tournament_class_id_ext, tournament_id_ext, startdate, shortname, longname,
                    gender, max_rank, max_age, url, raw_stages, raw_stage_hrefs, data_source_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING row_id;
                """,
                (
                    self.tournament_class_id_ext,
                    self.tournament_id_ext,
                    self.startdate,
                    self.shortname,
                    self.longname,
                    self.gender,
                    self.max_rank,
                    self.max_age,
                    self.url,
                    self.raw_stages,
                    self.raw_stage_hrefs,
                    self.data_source_id,
                ),
            )
            self.row_id = cursor.fetchone()[0]
            action = "inserted"

        return action