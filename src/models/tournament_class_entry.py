# src/models/tournament_class_entry.py

from dataclasses import dataclass
import sqlite3
from typing import Optional, Dict, Any, Tuple

from models.cache_mixin import CacheMixin



@dataclass
class TournamentClassEntry(CacheMixin):
    tournament_class_entry_id:              Optional[int] = None
    tournament_class_entry_id_ext:          Optional[str] = None
    tournament_class_entry_group_id_int:    Optional[int] = None
    tournament_class_id:                    Optional[int] = None
    seed:                                   Optional[int] = None
    final_position:                         Optional[int] = None
    row_created:                            Optional[str] = None
    row_updated:                            Optional[str] = None


    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "TournamentClassEntry":
        """Instantiate from a dict (keys matching column names)."""
        return TournamentClassEntry(
            tournament_class_entry_id               = d.get("tournament_class_entry_id"),
            tournament_class_entry_id_ext           = d.get("tournament_class_entry_id_ext"),
            tournament_class_entry_group_id_int     = d.get("tournament_class_entry_group_id_int"),
            tournament_class_id                     = d.get("tournament_class_id"),
            seed                                    = d.get("seed"),
            final_position                          = d.get("final_position"),
            row_created                             = d.get("row_created"),
            row_updated                             = d.get("row_updated")
        )

    def validate(self) -> Tuple[bool, str]:
        """
        Validate fields.
        Returns: (is_valid, error_message)
        """
        missing = []
        if not self.tournament_class_id:
            missing.append("tournament_class_id")

        if missing:
            return False, f"Missing/invalid fields: {', '.join(missing)}"

        return True, ""
    

    def upsert(self, cursor: sqlite3.Cursor) -> Optional[str]:
        """
        Upsert a single TournamentClassEntry with change detection.
        Returns one of: "inserted", "updated", "unchanged", or None (invalid).
        """

        sql = """
        INSERT INTO tournament_class_entry
        (tournament_class_id, tournament_class_entry_id_ext, tournament_class_entry_group_id_int, seed, final_position)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (tournament_class_id, tournament_class_entry_group_id_int)
        DO UPDATE SET
            seed = CASE
                WHEN tournament_class_entry.seed != excluded.seed OR
                     tournament_class_entry.final_position != excluded.final_position
                THEN excluded.seed ELSE tournament_class_entry.seed END,
            final_position = CASE
                WHEN tournament_class_entry.seed != excluded.seed OR
                     tournament_class_entry.final_position != excluded.final_position
                THEN excluded.final_position ELSE tournament_class_entry.final_position END,
            row_updated = CASE
                WHEN tournament_class_entry.seed != excluded.seed OR
                     tournament_class_entry.final_position != excluded.final_position
                THEN CURRENT_TIMESTAMP ELSE tournament_class_entry.row_updated END
        RETURNING tournament_class_entry_id;
        """
        vals = (
            self.tournament_class_id,
            self.tournament_class_entry_id_ext,
            self.tournament_class_entry_group_id_int,
            self.seed,
            self.final_position
        )
        cursor.execute(sql, vals)
        row = cursor.fetchone()
        if row:
            self.tournament_class_entry_id = row[0]
            if cursor.lastrowid:
                return "inserted"
            return "updated"
        return "unchanged"