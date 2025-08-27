# src/models/tournament_group.py
from dataclasses import dataclass
from typing import Optional
import sqlite3

@dataclass
class TournamentGroup:
    tournament_class_group_id: Optional[int]
    tournament_class_id: int
    description: str               # e.g. "Pool 1"
    sort_order: Optional[int] = None

    def upsert(self, cursor: sqlite3.Cursor) -> "TournamentGroup":
        """
        Upsert by UNIQUE (tournament_class_id, description).
        """
        sql = """
            INSERT INTO tournament_class_group (tournament_class_id, description, sort_order)
            VALUES (?, ?, ?)
            ON CONFLICT(tournament_class_id, description) DO UPDATE SET
                sort_order   = excluded.sort_order,
                row_updated  = CURRENT_TIMESTAMP
            RETURNING tournament_class_group_id;
        """
        cursor.execute(sql, (self.tournament_class_id, self.description, self.sort_order))
        self.tournament_class_group_id = cursor.fetchone()[0]
        return self

    def add_member(self, cursor: sqlite3.Cursor, participant_id: int) -> None:
        """
        Idempotent add of a participant to this group.
        """
        cursor.execute(
            """
            INSERT OR IGNORE INTO tournament_class_group_member (tournament_class_group_id, participant_id)
            VALUES (?, ?)
            """,
            (self.tournament_class_group_id, participant_id),
        )
