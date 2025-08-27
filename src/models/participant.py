# src/models/participant.py

from dataclasses import dataclass
from typing import Optional, Dict, Any, Tuple
import sqlite3


from .base import BaseModel


@dataclass
class Participant(BaseModel):
    participant_id:                     Optional[int] = None
    tournament_class_id:                Optional[int] = None
    tournament_class_seed:              Optional[int] = None
    tournament_class_final_position:    Optional[int] = None
    # Add later fields like league_id if needed

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "Participant":
        """Instantiate from a dict (keys matching column names)."""
        return Participant(
            participant_id                  = d.get("participant_id"),
            tournament_class_id             = d.get("tournament_class_id"),
            tournament_class_seed           = d.get("tournament_class_seed"),
            tournament_class_final_position = d.get("tournament_class_final_position"),
        )

    def validate(self
        ) -> Dict[str, str]:
        """
        Validate Participant fields, log to OperationLogger.
        Returns dict with status and reason.
        """
        if not self.tournament_class_id:
            return {
                "status": "failed", 
                "reason": "Missing required field: tournament_class_id"
            }

        # Add more validations as needed (e.g., seed/position ranges)

        return {"status": "success", "reason": "Validated OK"}

    def insert(
            self, 
            cursor
        ) -> Dict[str, str]:
        """
        Insert participant to DB, log results.
        Since we wipe old entries, no upsert needed - just insert.
        """
        query = """
            INSERT INTO participant (
                tournament_class_id, tournament_class_seed, tournament_class_final_position
            ) VALUES (?, ?, ?)
            RETURNING participant_id;
        """
        values = (
            self.tournament_class_id, self.tournament_class_seed, self.tournament_class_final_position
        )

        try:
            cursor.execute(query, values)
            row = cursor.fetchone()
            if row:
                self.participant_id = row[0]
                # logger.success(item_key, "Participant inserted successfully")
                return {
                    "status": "success",
                    "reason": "Participant inserted successfully"
                }
        except Exception as e:
            return {
                "status": "failed",
                "reason": f"Unexpected error during insert: {e}"
            }

    @classmethod
    def remove_for_class(
        cls, 
        cursor, 
        tournament_class_id: int
    ) -> int:
        cursor.execute(
            "DELETE FROM participant WHERE tournament_class_id = ?", 
            (tournament_class_id,)
        )
        # print(f"Removed {cursor.rowcount} participants for class {tournament_class_id}")
        return cursor.rowcount
    
    @classmethod
    def clear_final_positions(cls, cursor: sqlite3.Cursor, tournament_class_id: int) -> int:
        """
        Clear tournament_class_final_position for participants in a given tournament_class_id.

        Args:
            cursor: SQLite cursor for database queries.
            tournament_class_id: The tournament class ID to clear positions for.

        Returns:
            Number of rows updated (i.e., participants whose positions were cleared).
        """
        cursor.execute(
            """
            UPDATE participant
            SET tournament_class_final_position = NULL
            WHERE tournament_class_id = ?
            """,
            (tournament_class_id,)
        )
        return cursor.rowcount    
    
    @classmethod
    def update_final_position(
        cls,
        cursor: sqlite3.Cursor,
        tournament_class_id: int,
        fullname: str,
        club: str,
        position: int,
        player_name_map: Dict[str, int],
        unverified_name_map: Dict[str, int],
        class_part_by_player: Dict[int, Tuple[int, Optional[int]]],
        club_map: Dict[str, int]
    ) -> Dict[str, str]:
        """Update participant's final position based on name and club."""
        player_id = player_name_map.get(fullname) or unverified_name_map.get(fullname)
        if not player_id or player_id not in class_part_by_player:
            return {"status": "skipped", "reason": "No participant match (name or not in class)"}

        participant_id, db_club_id = class_part_by_player[player_id]
        club_id = club_map.get(club)
        if db_club_id and club_id and db_club_id != club_id:
            return {"status": "skipped", "reason": "Club mismatch"}

        query = """
            UPDATE participant
            SET tournament_class_final_position = ?
            WHERE participant_id = ? AND tournament_class_id = ?
        """
        try:
            cursor.execute(query, (position, participant_id, tournament_class_id))
            if cursor.rowcount > 0:
                return {"status": "success", "reason": "Final position updated"}
            return {"status": "skipped", "reason": "No rows updated"}
        except Exception as e:
            return {"status": "failed", "reason": f"Update failed: {e}"}

    @classmethod
    def cache_by_class_player(cls, cursor: sqlite3.Cursor) -> Dict[int, Dict[int, Tuple[int, Optional[int]]]]:
        """Cache participant_id and club_id by class_id and player_id."""
        query = """
            SELECT p.tournament_class_id, pp.player_id, pp.participant_id, pp.club_id
            FROM participant p
            JOIN participant_player pp ON p.participant_id = pp.participant_id
        """
        rows = cls.cached_query(cursor, query, cache_key_extra="cache_by_class_player")
        result = {}
        for row in rows:
            class_id = row["tournament_class_id"]
            player_id = row["player_id"]
            if class_id not in result:
                result[class_id] = {}
            result[class_id][player_id] = (row["participant_id"], row.get("club_id"))
        return result