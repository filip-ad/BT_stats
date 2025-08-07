# src/models/tournament_class.py

from dataclasses import dataclass
import datetime
from typing import Optional, List, Dict, Any, Tuple, Set
import logging
import sqlite3
from utils import parse_date

@dataclass
class TournamentClass:
    tournament_class_id:        Optional[int]  = None           # Canonical ID for class
    tournament_class_id_ext:    Optional[int]  = None           # External ID from ondata.se or other source
    tournament_id:              int = None                      # Foreign key to parent tournament
    type:                       str = None                      # Type of class (e.g., "singles", "doubles")
    date:                       Optional[datetime.date] = None  # Date of the class
    longname:                   str = None                      # Full description of the class
    shortname:                  str = None                      # Short description of the class
    gender:                     Optional[str]  = None           # Gender category (e.g., "male", "female")
    max_rank:                   Optional[int]  = None           # Maximum rank allowed in the class
    max_age:                    Optional[int]  = None           # Maximum age allowed in the class

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "TournamentClass":
        """Instantiate from a scraped dict (keys matching column names)."""
        return TournamentClass(
            tournament_class_id     = d.get("tournament_class_id"),
            tournament_class_id_ext = d.get("tournament_class_id_ext"),
            tournament_id           = d["tournament_id"],
            type                    = d.get("type"),
            date                    = parse_date(d.get("date"), context="TournamentClass.from_dict"),
            longname                = d.get("longname", ""),
            shortname               = d.get("shortname", ""),
            gender                  = d.get("gender"),
            max_rank                = d.get("max_rank"),
            max_age                 = d.get("max_age"),
        )   
    
    def save_to_db(self, cursor) -> dict:
        """
        Save the TournamentClass instance to the database, checking for duplicates.
        Returns a dict with status ("success", "skipped", or "failed"),
        a key for reporting, and a reason message.
        """
        # Validate required fields
        if not (self.shortname and self.date and self.tournament_class_id_ext):
            return {
                "status": "failed",
                "key": self.shortname or str(self.tournament_class_id_ext),
                "reason": "Missing required field (shortname, date, or ext ID)"
            }

        # Check for duplicate by external ID
        cursor.execute(
            "SELECT 1 FROM tournament_class WHERE tournament_class_id_ext = ?",
            (self.tournament_class_id_ext,)
        )
        if cursor.fetchone():
            logging.warning(f"Skipping duplicate class: {self.shortname} ({self.tournament_class_id_ext})")
            return {
                "status": "skipped",
                "key": self.tournament_class_id_ext,
                "reason": "Class already exists"
            }

        try:
            cursor.execute("""
                INSERT INTO tournament_class
                    (tournament_class_id_ext, tournament_id, type, date,
                     longname, shortname, gender, max_rank, max_age)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                self.tournament_class_id_ext,
                self.tournament_id,
                self.type,
                self.date,  # parse_date guaranteed a datetime.date
                self.longname,
                self.shortname,
                self.gender,
                self.max_rank,
                self.max_age,
            ))
            self.tournament_class_id = cursor.lastrowid
            logging.debug(f"Inserted tournament class: {self.shortname} ({self.tournament_class_id})")
            return {
                "status":   "success",
                "key":      f"{self.shortname} ({self.tournament_class_id})",
                "reason":   "Class inserted successfully"
            }
        
        except sqlite3.Error as e:
            logging.error(f"Error inserting tournament class {self.shortname}: {e}")
            return {
                "status":   "failed",
                "key":      self.shortname,
                "reason":   f"Insertion error: {e}"
            }
        
    @staticmethod
    def cache_all(cursor):
        """
        Returns all TournamentClass rows as model instances.
        """
        cursor.execute("""
            SELECT tournament_class_id, tournament_class_id_ext, tournament_id,
                type, date, longname, shortname, gender, max_rank, max_age
            FROM tournament_class
        """)
        rows = cursor.fetchall()
        classes = []
        for cid, ext, tid, typ, dt, ln, sn, gender, mr, ma in rows:
            date_obj = dt if isinstance(dt, datetime.date) else datetime.datetime.fromisoformat(dt).date()
            classes.append(TournamentClass(
                tournament_class_id=cid,
                tournament_class_id_ext=ext,
                tournament_id=tid,
                type=typ,
                date=date_obj,
                longname=ln,
                shortname=sn,
                gender=gender,
                max_rank=mr,
                max_age=ma
            ))
        return classes
    
    @staticmethod
    def cache_by_id(cursor):
        """
        Returns a dict mapping tournament_class_id to TournamentClass instances.
        """
        items = TournamentClass.cache_all(cursor)
        return {tc.tournament_class_id: tc for tc in items}

    @classmethod
    def get_by_id(cls, cursor, class_id: int) -> Optional["TournamentClass"]:
        """
        Fetch a single TournamentClass by its internal ID.
        Returns a TournamentClass or None if not found.
        """
        cursor.execute("""
            SELECT tournament_class_id, tournament_class_id_ext, tournament_id,
                   type, date, longname, shortname, gender, max_rank, max_age
            FROM tournament_class
            WHERE tournament_class_id = ?
        """, (class_id,))
        row = cursor.fetchone()
        if not row:
            return None
        cid, ext, tid, typ, dt, ln, sn, gender, mr, ma = row
        # normalize date field to a datetime.date
        date_obj = dt if isinstance(dt, datetime.date) else datetime.datetime.fromisoformat(dt).date()
        return cls(
            tournament_class_id     = cid,
            tournament_class_id_ext = ext,
            tournament_id           = tid,
            type                    = typ,
            date                    = date_obj,
            longname                = ln,
            shortname               = sn,
            gender                  = gender,
            max_rank                = mr,
            max_age                 = ma
        )