# src/models/tournament_class.py

from __future__ import annotations

from dataclasses import dataclass
import datetime
from typing import Optional, List, Dict, Any, Tuple, Set
import logging
import sqlite3
from models.base import BaseModel
from utils import OperationLogger, parse_date

@dataclass
class TournamentClass(BaseModel):
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
    is_valid:                       bool = False                    # Validity flag for the class

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
            data_source_id                  = d.get("data_source_id", 1),
            is_valid                        = d.get("is_valid", False)
        )
    
    @classmethod
    def get_valid_singles_after_cutoff(
        cls, 
        cursor: sqlite3.Cursor, 
        cutoff_date: date
    ) -> List[TournamentClass]:
        sql = """
            SELECT * FROM tournament_class
            WHERE tournament_class_type_id = 1  -- Singles
            AND date >= ?
            AND is_valid = 1
            ORDER BY date ASC;
        """
        results = cls.cached_query(cursor, sql, (cutoff_date,), cache_key_extra="get_valid_singles_after_cutoff")
        return [cls.from_dict(res) for res in results]
    
    @staticmethod
    def get_by_ext_ids(cursor, ext_ids: List[int]) -> List['TournamentClass']:
        """
        Fetch TournamentClass instances by a list of tournament_class_id_ext.
        Returns list of matching classes.
        """
        if not ext_ids:
            return []

        placeholders = ','.join('?' for _ in ext_ids)
        sql = f"""
            SELECT tournament_class_id, tournament_class_id_ext, tournament_id, tournament_class_type_id, tournament_class_structure_id,
                   date, longname, shortname, gender, max_rank, max_age, url, data_source_id
            FROM tournament_class
            WHERE tournament_class_id_ext IN ({placeholders})
        """
        cursor.execute(sql, ext_ids)
        rows = cursor.fetchall()
        classes = []
        for row in rows:
            data = {
                'tournament_class_id': row[0],
                'tournament_class_id_ext': row[1],
                'tournament_id': row[2],
                'tournament_class_type_id': row[3],
                'tournament_class_structure_id': row[4],
                'date': row[5],
                'longname': row[6],
                'shortname': row[7],
                'gender': row[8],
                'max_rank': row[9],
                'max_age': row[10],
                'url': row[11],
                'data_source_id': row[12]
            }
            classes.append(TournamentClass.from_dict(data))
        
        if len(classes) != len(ext_ids):
            missing = set(ext_ids) - {c.tournament_class_id_ext for c in classes}
            logging.warning(f"Missing classes for ext_ids: {missing}")
        
        return classes

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
            self.gender, self.max_rank, self.max_age, self.url, self.data_source_id, self.is_valid
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
                date, longname, shortname, gender, max_rank, max_age, url, data_source_id, is_valid
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                data_source_id                  = EXCLUDED.data_source_id,
                is_valid                        = EXCLUDED.is_valid,
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
                date, longname, shortname, gender, max_rank, max_age, url, data_source_id, is_valid
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (tournament_id, shortname, date) DO UPDATE SET
                tournament_class_id_ext         = EXCLUDED.tournament_class_id_ext,
                tournament_class_type_id        = EXCLUDED.tournament_class_type_id,
                tournament_class_structure_id   = EXCLUDED.tournament_class_structure_id,
                longname                        = EXCLUDED.longname,
                gender                          = EXCLUDED.gender,
                max_rank                        = EXCLUDED.max_rank,
                max_age                         = EXCLUDED.max_age,
                url                             = EXCLUDED.url,
                data_source_id                  = EXCLUDED.data_source_id,
                is_valid                        = EXCLUDED.is_valid,
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

    def validate(
        self, 
        logger: OperationLogger, 
        item_key: str
    ) -> Dict[str, str]:
        """
        Validate TournamentClass fields, set the valid flag, log to OperationLogger.
        Returns dict with status and reason.
        """

        if not (self.shortname and self.date and self.tournament_id):
            reason = "Missing required fields (shortname, date, tournament_id)"
            logger.failed(item_key, reason)
            self.is_valid = False
            return {"status": "failed", "reason": reason}

        # Check for inferred fields (e.g., type_id, structure_id)        
        if self.tournament_class_structure_id == 9:
            reason = "Tournament class structure (eg. groups only, KO only) now known, may need special handling"
            logger.warning(item_key, reason)
            self.is_valid = False
            return {"status": "failed", "reason": reason}
        
        if self.tournament_class_type_id == 9:
            reason = "Tournament class type (eg. singles, doubles etc) unknown, may need special handling"
            logger.warning(item_key, reason)
            self.is_valid = False
            return {"status": "failed", "reason": reason}

        # Warnings (non-fatal, but could set valid=False if strict)
        if not self.tournament_class_id_ext:
            logger.warning(item_key, "No valid external ID (likely upcoming)")
        if not self.longname:
            logger.warning(item_key, "Missing longname")

        self.is_valid = True

        return {
            "status": "success",
            "reason": "Vdsfsdfdsfd"
        }