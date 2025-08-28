# src/models/tournament_class_group.py
from dataclasses import dataclass
from typing import Any, Optional, Dict
import sqlite3

@dataclass
class TournamentClassGroup:
    tournament_class_group_id: Optional[int]
    tournament_class_id: int
    description: str               # e.g. "Pool 1"
    sort_order: Optional[int] = None

    def upsert(self, cursor: sqlite3.Cursor) -> Dict[str, Any]:
        """
        Portable upsert by UNIQUE (tournament_class_id, description).
        1) Try UPDATE; if a row was updated -> 'updated'
        2) Otherwise INSERT -> 'inserted'
        Returns { "status": "inserted"|"updated", "tournament_class_group_id": int }
        """
        # 1) UPDATE attempt
        cursor.execute(
            """
            UPDATE tournament_class_group
               SET sort_order = ?, row_updated = CURRENT_TIMESTAMP
             WHERE tournament_class_id = ? AND description = ?
            """,
            (self.sort_order, self.tournament_class_id, self.description)
        )
        if cursor.rowcount and cursor.rowcount > 0:
            # fetch the id
            cursor.execute(
                """
                SELECT tournament_class_group_id
                  FROM tournament_class_group
                 WHERE tournament_class_id = ? AND description = ?
                """,
                (self.tournament_class_id, self.description)
            )
            row = cursor.fetchone()
            self.tournament_class_group_id = row[0]
            return {"status": "updated", "tournament_class_group_id": self.tournament_class_group_id}

        # 2) INSERT path
        cursor.execute(
            """
            INSERT INTO tournament_class_group (tournament_class_id, description, sort_order)
            VALUES (?, ?, ?)
            """,
            (self.tournament_class_id, self.description, self.sort_order)
        )
        # Compatible way to get the id without RETURNING:
        self.tournament_class_group_id = cursor.lastrowid
        return {"status": "inserted", "tournament_class_group_id": self.tournament_class_group_id}

    def add_member(self, cursor: sqlite3.Cursor, participant_id: int) -> None:
        cursor.execute(
            """
            INSERT OR IGNORE INTO tournament_class_group_member (tournament_class_group_id, participant_id)
            VALUES (?, ?)
            """,
            (self.tournament_class_group_id, participant_id),
        )
