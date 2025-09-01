# src/models/player_transition_raw.py

from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, Optional, Tuple
from db import get_conn
from utils import OperationLogger
import logging

@dataclass
class PlayerTransitionRaw:
    """
    Dataclass for raw player transition data scraped from the web.
    """
    row_id:             Optional[int] = None
    season_id_ext:      Optional[int] = None
    season_label:       Optional[str] = None
    firstname:          Optional[str] = None
    lastname:           Optional[str] = None
    date_born:          Optional[date] = None
    year_born:          Optional[str] = None
    club_from:          Optional[str] = None
    club_to:            Optional[str] = None
    transition_date:    Optional[date] = None

    
    def validate(self) -> Tuple[bool, str]:
        """
        Validate raw fields.
        Returns:
            (is_valid, error_message)
        """
        missing = []
        if not self.firstname:
            missing.append("firstname")
        if not self.lastname:
            missing.append("lastname")
        if not self.date_born:
            missing.append("date_born")
        if not self.year_born:
            missing.append("year_born")
        if not self.club_from:
            missing.append("club_from")
        if not self.club_to:
            missing.append("club_to")
        if not self.transition_date:
            missing.append("transition_date")

        if missing:
            return False, f"Missing/invalid fields: {', '.join(missing)}"
        return True, ""

    def to_dict(self) -> Dict[str, Any]:
        """
        Dict for DB insert/upsert.
        """
        return {
            "season_id_ext":        self.season_id_ext,
            "season_label":         self.season_label,
            "firstname":            self.firstname,
            "lastname":             self.lastname,
            "date_born":            self.date_born,
            "year_born":            self.year_born,
            "club_from":            self.club_from,
            "club_to":              self.club_to,
            "transition_date":      self.transition_date,
        }
    
    
    @staticmethod
    def from_row(row: tuple) -> "PlayerTransitionRaw":
        """
        Construct from SELECT row (same column order as in resolver).
        """
        (row_id, season_id_ext, season_label, firstname, lastname, date_born, year_born, club_from, club_to, transition_date) = row
        return PlayerTransitionRaw(
            row_id=row_id,
            season_id_ext=season_id_ext,
            season_label=season_label,
            firstname=firstname,
            lastname=lastname,
            date_born=date_born,
            year_born=year_born,
            club_from=club_from,
            club_to=club_to,
            transition_date=transition_date
        )

    @classmethod
    def get_all(cls, cursor) -> list["PlayerTransitionRaw"]:
        """
        Fetch all rows from player_transition_raw and return as dataclass objects.
        """
        cursor.execute("""
            SELECT 
                row_id, season_id_ext, season_label, firstname, lastname, date_born, year_born, club_from, club_to, transition_date
            FROM player_transition_raw
        """)
        return [cls.from_row(r) for r in cursor.fetchall()]

    @staticmethod
    def upsert_one(cursor, raw: "PlayerTransitionRaw") -> bool:
        """
        Upsert one row into player_transition_raw.

        Behavior: "staging-only"
        - Try INSERT OR IGNORE to avoid dupes based on your natural/unique key.
        - No special-case updates (incl. club_from, club_to). Reprocessing happens in resolvers/updaters.
        Returns:
            inserted (bool): True if a new row was inserted, False if it already existed.
        """
        cursor.execute("""
            INSERT OR IGNORE INTO player_transition_raw (
                season_id_ext, season_label, firstname, lastname, date_born, year_born, club_from, club_to, transition_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            raw.season_id_ext, raw.season_label, raw.firstname, raw.lastname, raw.date_born,
            raw.year_born, raw.club_from, raw.club_to, raw.transition_date
        ))
        return cursor.rowcount > 0