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

    @classmethod
    def get_filtered_classes(
        cls,
        cursor: sqlite3.Cursor,
        class_id_ext: Optional[int] = None,
        max_classes: Optional[int] = None,
        order: Optional[str] = None
    ) -> List['TournamentClass']:
        """Load and filter tournament classes based on config settings."""
        classes_by_ext = cls.cache_by_id_ext(cursor)
        if class_id_ext:
            tc = classes_by_ext.get(class_id_ext)
            classes = [tc] if tc else []
        else:
            classes = list(classes_by_ext.values())

        order = (order or "").lower()
        if order == "newest":
            classes.sort(key=lambda tc: tc.date or datetime.date.min, reverse=True)
        elif order == "oldest":
            classes.sort(key=lambda tc: tc.date or datetime.date.min)

        if max_classes and max_classes > 0:
            classes = classes[:max_classes]
        return classes
    
    @classmethod
    def cache_by_id_ext(cls, cursor: sqlite3.Cursor) -> Dict[int, 'TournamentClass']:
        """Cache TournamentClass instances by tournament_class_id_ext."""
        sql = """
            SELECT tournament_class_id, tournament_class_id_ext, tournament_id, tournament_class_type_id,
                tournament_class_structure_id, date, longname, shortname, gender, max_rank, max_age,
                url, data_source_id, is_valid
            FROM tournament_class
            WHERE tournament_class_id_ext IS NOT NULL
        """
        rows = cls.cached_query(cursor, sql, cache_key_extra="cache_by_id_ext")
        result = {}
        for row in rows:
            tc = cls.from_dict(row)
            result[tc.tournament_class_id_ext] = tc
        return result
    
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
                'tournament_class_id':              row[0],
                'tournament_class_id_ext':          row[1],
                'tournament_id':                    row[2],
                'tournament_class_type_id':         row[3],
                'tournament_class_structure_id':    row[4],
                'date':                             row[5],
                'longname':                         row[6],
                'shortname':                        row[7],
                'gender':                           row[8],
                'max_rank':                         row[9],
                'max_age':                          row[10],
                'url':                              row[11],
                'data_source_id':                   row[12]
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
            item_key: str
        ):
        """
        Deterministic upsert for tournament_class.

        Rules:
        1) Prefer matching by (tournament_class_id_ext, data_source_id)
        2) Else match by (tournament_id, shortname, date)
        3) If a match is found -> UPDATE; else -> INSERT
        4) If both match different rows -> log failure (manual merge)
        """
        vals = (
            self.tournament_class_id_ext, self.tournament_id, self.tournament_class_type_id,
            self.tournament_class_structure_id, self.date, self.longname, self.shortname,
            self.gender, self.max_rank, self.max_age, self.url, self.data_source_id, self.is_valid
        )

        # 1) Try primary key (ext + data source)
        primary_id = None
        if self.tournament_class_id_ext and self.data_source_id:
            cursor.execute(
                "SELECT tournament_class_id FROM tournament_class "
                "WHERE tournament_class_id_ext = ? AND data_source_id = ?;",
                (self.tournament_class_id_ext, self.data_source_id)
            )
            
            row = cursor.fetchone()
            if row:
                primary_id = row[0]

        # 2) Try fallback key (tournament_id, shortname, date)
        fallback_id = None
        cursor.execute(
            "SELECT tournament_class_id FROM tournament_class "
            "WHERE tournament_id = ? AND shortname = ? AND date = ?;",
            (self.tournament_id, self.shortname, self.date)
        )
        row = cursor.fetchone()
        if row:
            fallback_id = row[0]


        # 3) Conflict: they point to different rows
        if primary_id and fallback_id and primary_id != fallback_id:
            logger.failed(
                item_key,
                (f"Conflicting classes: ext={self.tournament_class_id_ext}/ds={self.data_source_id} → id {primary_id}, "
                f"(tournament_id, shortname, date)=({self.tournament_id}, {self.shortname}, {self.date}) → id {fallback_id}. "
                "Manual merge required.")
            )
            self.tournament_class_id = primary_id  # pick a stable id to proceed with in memory
            return

        target_id = primary_id or fallback_id

        if target_id:
            # UPDATE existing row (attach ext/ds if missing, update other fields)
            cursor.execute(
                """
                UPDATE tournament_class
                SET tournament_class_id_ext       = COALESCE(?, tournament_class_id_ext),
                    tournament_id                 = ?,
                    tournament_class_type_id      = ?,
                    tournament_class_structure_id = ?,
                    date                          = ?,
                    longname                      = ?,
                    shortname                     = ?,
                    gender                        = ?,
                    max_rank                      = ?,
                    max_age                       = ?,
                    url                           = ?,
                    data_source_id                = COALESCE(?, data_source_id),
                    is_valid                      = ?,
                    row_updated                   = CURRENT_TIMESTAMP
                WHERE tournament_class_id = ?
                RETURNING tournament_class_id;
                """,
                (*vals, target_id)
            )
            self.tournament_class_id = cursor.fetchone()[0]
            basis = "id_ext+data_source" if primary_id else "fallback: tournament_id, shortname, date"
            logger.success(item_key, f"Tournament class successfully updated ({basis})")
            return

        # INSERT new row
        try:
            cursor.execute(
                """
                INSERT INTO tournament_class (
                    tournament_class_id_ext, tournament_id, tournament_class_type_id,
                    tournament_class_structure_id, date, longname, shortname, gender,
                    max_rank, max_age, url, data_source_id, is_valid
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING tournament_class_id;
                """,
                vals
            )
            self.tournament_class_id = cursor.fetchone()[0]
            logger.success(item_key, f"Tournament class created (id {self.tournament_class_id} {self.shortname} {self.date})")

            # debug
            logging.info(f"Created: {self}")

            return
        except sqlite3.IntegrityError:
            # Rare race: row appeared between our checks and INSERT.
            # Retry as UPDATE against whichever key now resolves.
            cursor.execute(
                "SELECT tournament_class_id FROM tournament_class "
                "WHERE tournament_class_id_ext = ? AND data_source_id = ?;",
                (self.tournament_class_id_ext, self.data_source_id)
            )
            row = cursor.fetchone()
            if not row:
                cursor.execute(
                    "SELECT tournament_class_id FROM tournament_class "
                    "WHERE tournament_id = ? AND shortname = ? AND date = ?;",
                    (self.tournament_id, self.shortname, self.date)
                )
                row = cursor.fetchone()

            if row:
                target_id = row[0]
                cursor.execute(
                    """
                    UPDATE tournament_class
                    SET tournament_class_id_ext       = COALESCE(?, tournament_class_id_ext),
                        tournament_id                 = ?,
                        tournament_class_type_id      = ?,
                        tournament_class_structure_id = ?,
                        date                          = ?,
                        longname                      = ?,
                        shortname                     = ?,
                        gender                        = ?,
                        max_rank                      = ?,
                        max_age                       = ?,
                        url                           = ?,
                        data_source_id                = COALESCE(?, data_source_id),
                        is_valid                      = ?,
                        row_updated                   = CURRENT_TIMESTAMP
                    WHERE tournament_class_id = ?
                    RETURNING tournament_class_id;
                    """,
                    (*vals, target_id)
                )
                self.tournament_class_id = cursor.fetchone()[0]
                logger.success(item_key, f"Tournament class updated after race")
                return
            raise


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