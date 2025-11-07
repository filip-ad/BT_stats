# src/models/tournament_class_group_member.py

from dataclasses import dataclass, fields
from typing import Dict, Any, Optional, Tuple
import sqlite3

@dataclass
class TournamentClassGroupMember:
    tournament_class_group_id:      int = None
    tournament_class_entry_id:      int = None
    seed_in_group:                  Optional[int] = None
    row_created:                    Optional[str] = None
    row_updated:                    Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {f.name: getattr(self, f.name) for f in fields(self)}

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "TournamentClassGroupMember":
        return cls(**{k: d.get(k) for k in {f.name for f in fields(cls)}})

    def validate(self) -> Tuple[bool, str]:
        if not self.tournament_class_group_id or not self.tournament_class_entry_id:
            return False, "Missing group_id or entry_id"
        return True, ""

    def insert(self, cursor: sqlite3.Cursor) -> None:
        sql = """
            INSERT OR IGNORE INTO tournament_class_group_member (
                tournament_class_group_id, tournament_class_entry_id, seed_in_group
            ) VALUES (:tournament_class_group_id, :tournament_class_entry_id, :seed_in_group);
        """
        cursor.execute(sql, self.to_dict())

    @classmethod
    def remove_for_entry(cls, cursor: sqlite3.Cursor, tournament_class_entry_id: int) -> int:
        """
        Remove all group-member records for a given tournament_class_entry_id.
        Use this before re-inserting/upserting membership to avoid PK conflicts.
        """
        cursor.execute("""
            DELETE FROM tournament_class_group_member
            WHERE tournament_class_entry_id = ?
        """, (tournament_class_entry_id,))
        return cursor.rowcount