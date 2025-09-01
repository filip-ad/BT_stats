# src/models/participant_raw_tournament.py

from dataclasses import dataclass, field
from typing import Optional, Dict, Any
import sqlite3
import json


@dataclass
class ParticipantRawTournament:
    """
    Raw participant row parsed from a tournament-class PDF/HTML.
    Mirrors participant_raw_tournament table in DB.
    """

    # Provenance
    tournament_id_ext:                  str                     # external tournament ID from source
    tournament_class_id_ext:            str                     # external class ID from source
    data_source_id:                     int = 1                 # FK to data_source (1=OnData, 2=Profixio, ...)

    # Raw values (unaltered as parsed)
    fullname_raw:                       str                       # raw player name
    clubname_raw:                       Optional[str] = None      # raw club name
    seed_raw:                           Optional[str] = None      # raw seed (string as parsed)
    final_position_raw:                 Optional[str] = None      # raw final position

    # Optional blob for debugging/unstructured fields
    raw_payload:                        Optional[Dict[str, Any]] = field(default_factory=dict)

    # Metadata (auto-handled by DB, not usually set in code)
    participant_raw_tournament_id:      Optional[int] = None
    row_created:                        Optional[str] = None
    row_updated:                        Optional[str] = None

    def validate(self) -> bool:
        """
        Light validation: only check mandatory fields exist.
        """
        return bool(self.tournament_id_ext and self.tournament_class_id_ext and self.fullname_raw)

    def to_dict(self) -> Dict[str, Any]:
        """
        Return dictionary for DB insert/upsert.
        """
        return {
            "tournament_id_ext":            self.tournament_id_ext,
            "tournament_class_id_ext":      self.tournament_class_id_ext,
            "data_source_id":               self.data_source_id,
            "fullname_raw":                 self.fullname_raw,
            "clubname_raw":                 self.clubname_raw,
            "seed_raw":                     self.seed_raw,
            "final_position_raw":           self.final_position_raw,
            "raw_payload":                  json.dumps(self.raw_payload) if self.raw_payload else None
        }

    def save_to_db(self, cursor: sqlite3.Cursor) -> None:
        """
        Insert row into participant_raw_tournament table.
        """
        cursor.execute("""
            INSERT INTO participant_raw_tournament
            (tournament_id_ext, tournament_class_id_ext, data_source_id,
             fullname_raw, clubname_raw, seed_raw, final_position_raw, raw_payload)
            VALUES
            (:tournament_id_ext, :tournament_class_id_ext, :data_source_id,
             :fullname_raw, :clubname_raw, :seed_raw, :final_position_raw, :raw_payload)
        """, self.to_dict())
