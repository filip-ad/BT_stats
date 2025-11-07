# src/models/tournament_class_group.py

from dataclasses import dataclass, fields
from typing import Dict, Any, Optional, Tuple
import sqlite3

@dataclass
class TournamentClassGroup:
    tournament_class_group_id: Optional[int] = None
    tournament_class_id: int = None
    description: str = ""
    sort_order: Optional[int] = None
    row_created: Optional[str] = None
    row_updated: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {f.name: getattr(self, f.name) for f in fields(self)}

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "TournamentClassGroup":
        return cls(**{k: d.get(k) for k in {f.name for f in fields(cls)}})

    def validate(self) -> Tuple[bool, str]:
        if not self.tournament_class_id or not self.description:
            return False, "Missing tournament_class_id or description"
        return True, ""

    def upsert(self, cursor: sqlite3.Cursor) -> int:
        sql = """
            INSERT INTO tournament_class_group (tournament_class_id, description, sort_order)
            VALUES (:tournament_class_id, :description, :sort_order)
            ON CONFLICT (tournament_class_id, description) DO UPDATE SET
                sort_order = excluded.sort_order,
                row_updated = CURRENT_TIMESTAMP
            RETURNING tournament_class_group_id;
        """
        cursor.execute(sql, self.to_dict())
        self.tournament_class_group_id = cursor.fetchone()[0]
        return self.tournament_class_group_id

    @classmethod
    def get_by_description(cls, cursor: sqlite3.Cursor, tournament_class_id: int, description: str) -> Optional["TournamentClassGroup"]:
        cursor.execute("""
            SELECT * FROM tournament_class_group
            WHERE tournament_class_id = ? AND description = ?;
        """, (tournament_class_id, description))
        row = cursor.fetchone()
        if row:
            columns = [col[0] for col in cursor.description]
            return cls.from_dict(dict(zip(columns, row)))
        return None