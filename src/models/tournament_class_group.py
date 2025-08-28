# src/models/tournament_group.py
from ast import Dict
from dataclasses import dataclass
from typing import Any, Optional
import sqlite3

@dataclass
class TournamentClassGroup:
    tournament_class_group_id: Optional[int]
    tournament_class_id: int
    description: str               # e.g. "Pool 1"
    sort_order: Optional[int] = None

    def upsert(self, cursor: sqlite3.Cursor) -> Dict[str, Any]:
        """
        Upsert by UNIQUE (tournament_class_id, description), returning:
          { "status": "inserted"|"updated", "tournament_class_group_id": int }
        """
        sql = """
        WITH upd AS (
            UPDATE tournament_class_group
               SET sort_order = ?, row_updated = CURRENT_TIMESTAMP
             WHERE tournament_class_id = ? AND description = ?
         RETURNING tournament_class_group_id AS id, 'updated' AS op
        ),
        ins AS (
            INSERT INTO tournament_class_group (tournament_class_id, description, sort_order)
            SELECT ?, ?, ?
             WHERE NOT EXISTS (SELECT 1 FROM upd)
         RETURNING tournament_class_group_id AS id, 'inserted' AS op
        )
        SELECT id, op FROM upd
        UNION ALL
        SELECT id, op FROM ins;
        """
        # params order: (upd.sort_order, upd.class_id, upd.desc, ins.class_id, ins.desc, ins.sort_order)
        cursor.execute(sql, (
            self.sort_order, self.tournament_class_id, self.description,
            self.tournament_class_id, self.description, self.sort_order
        ))
        row = cursor.fetchone()
        self.tournament_class_group_id = row[0]
        return {"status": row[1], "tournament_class_group_id": self.tournament_class_group_id}

    def add_member(self, cursor: sqlite3.Cursor, participant_id: int) -> None:
        cursor.execute(
            """
            INSERT OR IGNORE INTO tournament_class_group_member (tournament_class_group_id, participant_id)
            VALUES (?, ?)
            """,
            (self.tournament_class_group_id, participant_id),
        )