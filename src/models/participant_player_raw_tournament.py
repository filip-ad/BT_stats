# src/models/participant_player_raw_tournament.py

from dataclasses import dataclass
from datetime import date
from typing import List, Optional, Dict, Any, Tuple
import sqlite3
from models.cache_mixin import CacheMixin


@dataclass
class ParticipantPlayerRawTournament(CacheMixin):
    """
    Raw participant row parsed from a tournament-class PDF/HTML.
    Mirrors participant_raw_tournament table in DB.
    """

    row_id:                             Optional[int] = None
    tournament_id_ext:                  Optional[str] = None    
    tournament_class_id_ext:            Optional[str] = None    
    participant_player_id_ext:          Optional[str] = None       
    fullname_raw:                       Optional[str] = None     
    clubname_raw:                       Optional[str] = None     
    seed_raw:                           Optional[str] = None      
    final_position_raw:                 Optional[str] = None      
    raw_group_id:                       Optional[str] = None  
    data_source_id:                     int = 1      
    row_created:                        Optional[str] = None
    row_updated:                        Optional[str] = None

    @staticmethod
    def from_dict(data: dict) -> 'ParticipantPlayerRawTournament':
        """
        Factory method to create a ParticipantPlayerRawTournament instance from a dictionary.
        """
        return ParticipantPlayerRawTournament(
            tournament_id_ext               = data.get("tournament_id_ext"),
            tournament_class_id_ext         = data.get("tournament_class_id_ext"),
            participant_player_id_ext       = data.get("participant_player_id_ext"),
            fullname_raw                    = data.get("fullname_raw"),
            clubname_raw                    = data.get("clubname_raw"),
            seed_raw                        = data.get("seed_raw"),
            final_position_raw              = data.get("final_position_raw"),
            raw_group_id                    = data.get("raw_group_id"),
            data_source_id                  = data.get("data_source_id", 1)
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Return dictionary for DB insert.
        """
        return {
            "row_id":                       self.row_id,
            "tournament_id_ext":            self.tournament_id_ext,
            "tournament_class_id_ext":      self.tournament_class_id_ext,
            "participant_player_id_ext":    self.participant_player_id_ext,
            "fullname_raw":                 self.fullname_raw,
            "clubname_raw":                 self.clubname_raw,
            "seed_raw":                     self.seed_raw,
            "final_position_raw":           self.final_position_raw,
            "raw_group_id":                 self.raw_group_id,
            "data_source_id":               self.data_source_id,
            "row_created":                  self.row_created,
            "row_updated":                  self.row_updated
        }
    
    def validate(self) -> Tuple[bool, str]:
        """
        Validate fields.
        Returns: (is_valid, error_message)
        """
        missing = []
        if not self.tournament_id_ext:
            missing.append("tournament_id_ext")
        if not self.tournament_class_id_ext:
            missing.append("tournament_class_id_ext")
        if not self.fullname_raw:
            missing.append("fullname_raw")
        if not self.clubname_raw:
            missing.append("clubname_raw")

        if missing:
            self.is_valid = False
            return False, f"Missing/invalid fields: {', '.join(missing)}"

        self.is_valid = True
        return True, ""
    
    @classmethod
    def get_all(cls, cursor: sqlite3.Cursor) -> List['ParticipantPlayerRawTournament']:
        """Fetch all raw tournament participants."""
        # Use row factory to get dictionary-like rows
        cursor.row_factory = sqlite3.Row
        cursor.execute("SELECT * FROM participant_player_raw_tournament")
        rows = cursor.fetchall()
        cursor.row_factory = None  # Reset to default for other operations
        return [cls.from_dict(dict(row)) for row in rows]

    def insert(self, cursor: sqlite3.Cursor) -> None:
        """
        Insert row into participant_player_raw_tournament table.
        """
        cursor.execute("""
            INSERT INTO participant_player_raw_tournament (
                tournament_id_ext, 
                tournament_class_id_ext, 
                participant_player_id_ext,
                fullname_raw, 
                clubname_raw, 
                seed_raw, 
                final_position_raw,
                raw_group_id,
                data_source_id
            )
            VALUES
            (:tournament_id_ext, :tournament_class_id_ext, :participant_player_id_ext,
             :fullname_raw, :clubname_raw, :seed_raw, :final_position_raw, :raw_group_id, :data_source_id)
        """, self.to_dict())


    @classmethod
    def remove_for_class(cls, cursor: sqlite3.Cursor, tournament_class_id_ext: str, data_source_id: int = 1) -> int:
        """Remove all raw participant data for a given tournament class.
        Returns the number of rows deleted.
        """
        cursor.execute(
            """
            DELETE FROM participant_player_raw_tournament
            WHERE tournament_class_id_ext = ? AND data_source_id = ?
            """,
            (tournament_class_id_ext, data_source_id)
        )
        return cursor.rowcount      