# src/models/tournament_class_raw.py

from __future__ import annotations

from dataclasses import dataclass
import datetime
from typing import Optional, Dict, Any
from utils import parse_date

@dataclass
class TournamentClassRaw:
    tournament_class_raw_id:        Optional[int]  = None           # Auto-generated ID for raw entry
    tournament_class_id_ext:        Optional[str]  = None           # External ID from ondata.se or other source
    tournament_id:                  int = None                      # Foreign key to parent tournament (internal ID)
    tournament_class_type_id:       Optional[int]  = None           # Type of class (e.g., "singles", "doubles") - inferred during scrape
    tournament_class_structure_id:  Optional[int]  = None           # Structure (e.g., "knockout", "round-robin") - inferred during scrape
    date:                           Optional[datetime.date] = None  # Date of the class
    longname:                       str = None                      # Full description of the class
    shortname:                      str = None                      # Short description of the class
    gender:                         Optional[str]  = None           # Gender category (e.g., "male", "female")
    max_rank:                       Optional[int]  = None           # Maximum rank allowed in the class
    max_age:                        Optional[int]  = None           # Maximum age allowed in the class
    url:                            Optional[str]  = None           # URL for the class
    data_source_id:                 int = 1                         # Data source ID (default 1 for 'ondata')

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "TournamentClassRaw":
        """Instantiate from a scraped dict (keys matching column names)."""
        return TournamentClassRaw(
            tournament_class_id_ext         = d.get("tournament_class_id_ext"),
            tournament_id                   = d["tournament_id"],
            tournament_class_type_id        = d.get("tournament_class_type_id"),
            tournament_class_structure_id   = d.get("tournament_class_structure_id"),
            date                            = parse_date(d.get("date"), context="TournamentClassRaw.from_dict"),
            longname                        = d.get("longname", ""),
            shortname                       = d.get("shortname", ""),
            gender                          = d.get("gender"),
            max_rank                        = d.get("max_rank"),
            max_age                         = d.get("max_age"),
            url                             = d.get("url"),
            data_source_id                  = d.get("data_source_id", 1)
        )

    def light_validate(self) -> bool:
        """Light validation: Check for minimum required fields before inserting to raw."""
        return bool(self.shortname and self.date and self.tournament_id)

    def insert(self, cursor) -> None:
        """Insert the raw object into the tournament_class_raw table."""
        vals = (
            self.tournament_class_id_ext, self.tournament_id, self.tournament_class_type_id,
            self.tournament_class_structure_id, self.date, self.longname, self.shortname,
            self.gender, self.max_rank, self.max_age, self.url, self.data_source_id
        )
        cursor.execute(
            """
            INSERT INTO tournament_class_raw (
                tournament_class_id_ext, tournament_id, tournament_class_type_id,
                tournament_class_structure_id, date, longname, shortname, gender,
                max_rank, max_age, url, data_source_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            vals
        )
        self.tournament_class_raw_id = cursor.lastrowid