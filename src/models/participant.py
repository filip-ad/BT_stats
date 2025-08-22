# src/models/participant.py

from dataclasses import dataclass
from typing import Optional, Dict, Any
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
    