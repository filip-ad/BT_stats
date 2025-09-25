# src/models/tournament_class_player.py

from dataclasses import dataclass
from typing import Optional, Dict, Any, Tuple
import sqlite3
from models.cache_mixin import CacheMixin

@dataclass
class TournamentClassPlayer(CacheMixin):
    tournament_class_entry_id:              Optional[int] = None
    tournament_player_id_ext:               Optional[str] = None
    player_id:                              int = None
    club_id:                                int = None
    row_created:                            Optional[str] = None
    row_updated:                            Optional[str] = None

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "TournamentClassPlayer":
        return TournamentClassPlayer(
            tournament_class_entry_id       = d.get("tournament_class_entry_id"),
            tournament_player_id_ext        = d.get("tournament_player_id_ext"),
            player_id                       = d.get("player_id"),
            club_id                         = d.get("club_id"),
            row_created                     = d.get("row_created"),
            row_updated                     = d.get("row_updated")
        )

    def validate(self) -> Tuple[bool, str]:
        """
        Validate fields.
        Returns: (is_valid, error_message)
        """
        missing = []
        if not self.tournament_class_entry_id:
            missing.append("tournament_class_entry_id")
        if not self.player_id:
            missing.append("player_id")
        if not self.club_id:
            missing.append("club_id")

        if missing:
            return False, f"Missing/invalid fields: {', '.join(missing)}"

        return True, ""
    

    def upsert(self, cursor: sqlite3.Cursor) -> Optional[str]:
        """
        Upsert a single TournamentClassPlayer with change detection.
        Returns one of: "inserted", "updated", "unchanged", or None (invalid).
        """

        sql = """
        INSERT INTO tournament_class_player
        (tournament_class_entry_id, tournament_player_id_ext, player_id, club_id)
        VALUES (?, ?, ?, ?)
        ON CONFLICT (tournament_class_entry_id, player_id)
        DO UPDATE SET
            tournament_player_id_ext = CASE
                WHEN tournament_class_player.tournament_player_id_ext != excluded.tournament_player_id_ext OR
                     tournament_class_player.club_id != excluded.club_id
                THEN excluded.tournament_player_id_ext ELSE tournament_class_player.tournament_player_id_ext END,
            club_id = CASE
                WHEN tournament_class_player.tournament_player_id_ext != excluded.tournament_player_id_ext OR
                     tournament_class_player.club_id != excluded.club_id
                THEN excluded.club_id ELSE tournament_class_player.club_id END,
            row_updated = CASE
                WHEN tournament_class_player.tournament_player_id_ext != excluded.tournament_player_id_ext OR
                     tournament_class_player.club_id != excluded.club_id
                THEN CURRENT_TIMESTAMP ELSE tournament_class_player.row_updated END
        RETURNING tournament_class_entry_id;
        """
        vals = (
            self.tournament_class_entry_id,
            self.tournament_player_id_ext,
            self.player_id,
            self.club_id
        )
        cursor.execute(sql, vals)
        row = cursor.fetchone()
        if row:
            self.tournament_class_entry_id = row[0]
            if cursor.lastrowid:
                return "inserted"
            return "updated"
        return "unchanged"
    
    # Used in resolve_tournament_class_entries to clear existing players before running matching strategy
    @classmethod
    def remove_for_entry(cls, cursor: sqlite3.Cursor, tournament_class_entry_id: int) -> int:
        """Remove all TournamentClassPlayer records for a given tournament_class_entry_id."""
        cursor.execute("""
            DELETE FROM tournament_class_player
            WHERE tournament_class_entry_id = ?
        """, (tournament_class_entry_id,))
        return cursor.rowcount