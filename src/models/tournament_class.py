# src/models/tournament_class.py

from dataclasses import dataclass
import datetime
from typing import Optional, List, Dict, Any, Tuple, Set
import logging
import sqlite3
from utils import OperationLogger, parse_date

@dataclass
class TournamentClass:
    tournament_class_id:            Optional[int]  = None           # Canonical ID for class
    tournament_class_id_ext:        Optional[int]  = None           # External ID from ondata.se or other source
    tournament_id:                  int = None                      # Foreign key to parent tournament
    tournament_class_type_id:       Optional[int]  = None           # Type of class (e.g., "singles", "doubles")
    tournament_class_structure_id:  Optional[int]  = None           # Foreign key to tournament structure (e.g., "knockout", "round-robin")
    date:                           Optional[datetime.date] = None  # Date of the class
    longname:                       str = None                      # Full description of the class
    shortname:                      str = None                      # Short description of the class
    gender:                         Optional[str]  = None           # Gender category (e.g., "male", "female")
    max_rank:                       Optional[int]  = None           # Maximum rank allowed in the class
    max_age:                        Optional[int]  = None           # Maximum age allowed in the class
    url:                            Optional[str]  = None           # URL for the class
    data_source_id:                 int = 1                         # Data source ID (default 1 for 'ondata')

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "TournamentClass":
        """Instantiate from a scraped dict (keys matching column names)."""
        return TournamentClass(
            tournament_class_id             = d.get("tournament_class_id"),
            tournament_class_id_ext         = d.get("tournament_class_id_ext"),
            tournament_id                   = d["tournament_id"],
            tournament_class_type_id        = d.get("tournament_class_type_id"),
            tournament_class_structure_id   = d.get("tournament_class_structure_id"),
            date                            = parse_date(d.get("date"), context="TournamentClass.from_dict"),
            longname                        = d.get("longname", ""),
            shortname                       = d.get("shortname", ""),
            gender                          = d.get("gender"),
            max_rank                        = d.get("max_rank"),
            max_age                         = d.get("max_age"),
            url                             = d.get("url"),
            data_source_id                  = d.get("data_source_id", 1)
        )

    def upsert(
            self, 
            cursor, 
            logger: OperationLogger, 
            item_key: str):
        """
        Upsert tournament class to DB, log results.
        Handles unique constraint on (tournament_class_id_ext, data_source_id) or fallback (tournament_id, shortname, date).
        """
        values = (
            self.tournament_class_id_ext, self.tournament_id, self.tournament_class_type_id,
            self.tournament_class_structure_id, self.date, self.longname, self.shortname,
            self.gender, self.max_rank, self.max_age, self.url, self.data_source_id
        )
    
        if self.tournament_class_id_ext:
            
            # Check if exists (logging purposes)
            cursor.execute("SELECT tournament_class_id FROM tournament_class WHERE tournament_class_id_ext = ? AND data_source_id = ?;", (self.tournament_class_id_ext, self.data_source_id))
            existing = cursor.fetchone()
            is_update = existing is not None

            # Primary upsert on (tournament_class_id_ext, data_source_id)
            query_primary = """
                INSERT INTO tournament_class (
                    tournament_class_id_ext, tournament_id, tournament_class_type_id, tournament_class_structure_id,
                date, longname, shortname, gender, max_rank, max_age, url, data_source_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (tournament_class_id_ext, data_source_id) DO UPDATE SET
                tournament_id                   = EXCLUDED.tournament_id,
                tournament_class_type_id        = EXCLUDED.tournament_class_type_id,
                tournament_class_structure_id   = EXCLUDED.tournament_class_structure_id,
                date                            = EXCLUDED.date,
                longname                        = EXCLUDED.longname,
                shortname                       = EXCLUDED.shortname,
                gender                          = EXCLUDED.gender,
                max_rank                        = EXCLUDED.max_rank,
                max_age                         = EXCLUDED.max_age,
                url                             = EXCLUDED.url,
                row_updated                     = CURRENT_TIMESTAMP
            RETURNING tournament_class_id;
        """

        try:
            cursor.execute(query_primary, values)
            row = cursor.fetchone()
            if row:
                self.tournament_class_id = row[0]
                action = "updated" if is_update else "created"
                logger.success(item_key, f"Tournament class {action} (primary: tournament_class_id_ext, data_source_id)")
                return
        except sqlite3.IntegrityError:
            # Fallback if primary conflict or missing ext_id
            logger.warning(item_key, "Fallback upsert due to missing or conflicting tournament_class_id_ext, data_source_id")
            
        # Fallback check if exists
        cursor.execute("SELECT tournament_class_id FROM tournament_class WHERE tournament_id = ? AND shortname = ? AND date = ?;", (self.tournament_id, self.shortname, self.date))
        existing = cursor.fetchone()
        is_update = existing is not None 

        # Fallback upsert on (tournament_id, shortname, date)
        query_fallback = """
            INSERT INTO tournament_class (
                tournament_class_id_ext, tournament_id, tournament_class_type_id, tournament_class_structure_id,
                date, longname, shortname, gender, max_rank, max_age, url, data_source_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (tournament_id, shortname, date) DO UPDATE SET
                tournament_class_id_ext         = EXCLUDED.tournament_class_id_ext,
                tournament_class_type_id        = EXCLUDED.tournament_class_type_id,
                tournament_class_structure_id   = EXCLUDED.tournament_class_structure_id,
                longname                        = EXCLUDED.longname,
                gender                          = EXCLUDED.gender,
                max_rank                        = EXCLUDED.max_rank,
                max_age                         = EXCLUDED.max_age,
                url                             = EXCLUDED.url,
                row_updated                     = CURRENT_TIMESTAMP
            RETURNING tournament_class_id;
        """
        try:
            cursor.execute(query_fallback, values)
            row = cursor.fetchone()
            if row:
                self.tournament_class_id = row[0]
                logger.success(item_key, "Tournament class upserted successfully (fallback on tournament_id, shortname, date)")
                return
        except sqlite3.IntegrityError as e:
            logger.failed(item_key, f"Integrity error during upsert: {e}")
        except Exception as e:
            logger.failed(item_key, f"Unexpected error during upsert: {e}")