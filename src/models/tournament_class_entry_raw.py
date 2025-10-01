# src/models/tournament_class_entry_raw.py

from dataclasses import dataclass
from typing import List, Optional, Dict, Any, Tuple
import sqlite3
from models.cache_mixin import CacheMixin
from utils import compute_content_hash as _compute_content_hash

@dataclass
class TournamentClassEntryRaw(CacheMixin):
    """
    Raw participant/entry row parsed from a tournament-class PDF/HTML.
    Mirrors tournament_class_entry_raw table in DB.
    """

    row_id:                             Optional[int] = None
    tournament_id_ext:                  Optional[str] = None    
    tournament_class_id_ext:            Optional[str] = None    
    tournament_player_id_ext:           Optional[str] = None       
    fullname_raw:                       Optional[str] = None     
    clubname_raw:                       Optional[str] = None     
    seed_raw:                           Optional[str] = None      
    final_position_raw:                 Optional[str] = None      
    entry_group_id_int:                 Optional[int] = None
    data_source_id:                     int = 1
    content_hash:                       Optional[str] = None
    row_created:                        Optional[str] = None
    row_updated:                        Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict) -> "TournamentClassEntryRaw":
        return cls(
            tournament_id_ext           = data.get("tournament_id_ext"),
            tournament_class_id_ext     = data.get("tournament_class_id_ext"),
            tournament_player_id_ext    = data.get("tournament_player_id_ext"),
            fullname_raw                = data.get("fullname_raw"),
            clubname_raw                = data.get("clubname_raw"),
            seed_raw                    = data.get("seed_raw"),
            final_position_raw          = data.get("final_position_raw"),
            entry_group_id_int          = data.get("entry_group_id_int"),
            data_source_id              = data.get("data_source_id", 1),
            content_hash                = data.get("content_hash")
        )


    def to_dict(self) -> Dict[str, Any]:
        """Return dictionary for DB insert/update."""
        return {
            "row_id":                       self.row_id,
            "tournament_id_ext":            self.tournament_id_ext,
            "tournament_class_id_ext":      self.tournament_class_id_ext,
            "tournament_player_id_ext":     self.tournament_player_id_ext,
            "fullname_raw":                 self.fullname_raw,
            "clubname_raw":                 self.clubname_raw,
            "seed_raw":                     self.seed_raw,
            "final_position_raw":           self.final_position_raw,
            "entry_group_id_int":           self.entry_group_id_int,
            "data_source_id":               self.data_source_id,
            "content_hash":                 self.content_hash,
            "row_created":                  self.row_created,
            "row_updated":                  self.row_updated
        }

    # --- Validation ---

    def validate(self) -> Tuple[bool, str]:
        """Validate fields before insert."""
        missing = []
        if not self.tournament_id_ext:
            missing.append("tournament_id_ext")
        if not self.tournament_class_id_ext:
            missing.append("tournament_class_id_ext")
        if not self.fullname_raw:
            missing.append("fullname_raw")

        if missing:
            return False, f"Missing/invalid fields: {', '.join(missing)}"

        return True, ""

    def compute_hash(self) -> None:
        """Compute and assign content hash for uniqueness check."""
        self.content_hash = _compute_content_hash(
            self,
            exclude_fields={"row_id", "row_created", "row_updated", "content_hash", "entry_group_id_int"}
        )

    def insert(self, cursor: sqlite3.Cursor) -> None:
        """Insert row into tournament_class_entry_raw table."""
        if not self.content_hash:
            self.compute_hash()

        cursor.execute("""
            INSERT OR IGNORE INTO tournament_class_entry_raw (
                tournament_id_ext, 
                tournament_class_id_ext, 
                tournament_player_id_ext,
                fullname_raw, 
                clubname_raw, 
                seed_raw, 
                final_position_raw,
                entry_group_id_int,
                data_source_id,
                content_hash
            )
            VALUES
            (:tournament_id_ext, :tournament_class_id_ext, :tournament_player_id_ext,
             :fullname_raw, :clubname_raw, :seed_raw, :final_position_raw,
             :entry_group_id_int, :data_source_id, :content_hash)
        """, self.to_dict())

    @classmethod
    def remove_for_class(cls, cursor: sqlite3.Cursor, tournament_class_id_ext: str, data_source_id: int = 1) -> int:
        """Remove all raw entry data for a given tournament class."""
        cursor.execute(
            """
            DELETE FROM tournament_class_entry_raw
            WHERE tournament_class_id_ext = ? AND data_source_id = ?
            """,
            (tournament_class_id_ext, data_source_id)
        )
        return cursor.rowcount    
    
    @classmethod
    def batch_update_final_positions(cls, cursor: sqlite3.Cursor, tournament_class_id_ext: str, data_source_id: int, positions: List[Dict[str, Any]]) -> Tuple[int, str]:
        """
        Batch update final positions for a tournament class.
        Returns (number of rows updated, error message if any).
        """
        if not positions:
            return 0, ""

        query = """
        UPDATE tournament_class_entry_raw
        SET final_position_raw = CASE
        """
        values = []
        for pos in positions:
            query += "WHEN tournament_class_id_ext = ? AND fullname_raw = ? AND clubname_raw = ? AND data_source_id = ? THEN ? "
            values.extend([
                tournament_class_id_ext,
                pos["fullname_raw"],
                pos["clubname_raw"],
                data_source_id,
                pos["final_position_raw"]
            ])
        query += """
        END
        WHERE tournament_class_id_ext = ? AND data_source_id = ? AND fullname_raw IN %s
        """
        values.extend([tournament_class_id_ext, data_source_id])
        fullname_values = tuple(pos["fullname_raw"] for pos in positions)

        try:
            cursor.execute(query % str(fullname_values), values)
            return cursor.rowcount, ""
        except Exception as e:
            return 0, f"Failed to batch update final positions: {str(e)}"
  

    @classmethod
    def get_all(cls, cursor: sqlite3.Cursor) -> List["TournamentClassEntryRaw"]:
        """Fetch all raw class entries."""
        cursor.row_factory = sqlite3.Row
        cursor.execute("SELECT * FROM tournament_class_entry_raw")
        rows = cursor.fetchall()
        cursor.row_factory = None
        return [cls.from_dict(dict(row)) for row in rows]
