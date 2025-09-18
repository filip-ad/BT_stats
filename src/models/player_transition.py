# src/models/player_transition.py

from dataclasses import dataclass
from typing import Optional, List
import logging

@dataclass
class PlayerTransition:
    season_id:          int
    player_id:          int
    club_id_from:       int
    club_id_to:         int
    transition_date:    str
    row_created:        Optional[str] = None

    @staticmethod
    def from_dict(data: dict):
        return PlayerTransition(
            season_id           = data.get("season_id"),
            player_id           = data.get("player_id"),
            club_id_from        = data.get("club_id_from"),
            club_id_to          = data.get("club_id_to"),
            transition_date     = data.get("transition_date"),
            row_created         = data.get("row_created")
        )

    @staticmethod
    def get_by_player_id(cursor, player_id: int) -> List['PlayerTransition']:
        """Retrieve all transitions for a player by player_id."""
        try:
            cursor.execute("""
                SELECT season_id, player_id, club_id_from, club_id_to, transition_date, row_created
                FROM player_transition
                WHERE player_id = ?
                ORDER BY transition_date
            """, (player_id,))
            rows = cursor.fetchall()
            return [PlayerTransition.from_dict({
                "season_id": row[0],
                "player_id": row[1],
                "club_id_from": row[2],
                "club_id_to": row[3],
                "transition_date": row[4],
                "row_created": row[5]
            }) for row in rows]
        except Exception as e:
            logging.error(f"Error retrieving transitions by player_id {player_id}: {e}")
            return []

    def save_to_db(self, cursor, logger):

        item_key = f"Player ID {self.player_id}, Season ID {self.season_id}"

        # Validate required fields
        if not all([self.season_id, self.player_id, self.club_id_from, self.club_id_to, self.transition_date]):
            logger.failed(item_key, "Missing required fields")
            return {
                "status": "failed",
                "player": f"Player ID {self.player_id}",
                "reason": "Missing required fields"
            }

        # Insert into player_transition
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO player_transition (season_id, player_id, club_id_from, club_id_to, transition_date)
                VALUES (?, ?, ?, ?, ?)
            """, (self.season_id, self.player_id, self.club_id_from, self.club_id_to, self.transition_date))
            
            if cursor.rowcount == 0:
                logger.skipped(item_key, "Transition already exists")
                return {
                    "status": "skipped",
                    "player": f"Player ID {self.player_id}",
                    "reason": "Transition already exists"
                }
            logger.success(item_key, "Transition inserted successfully")
            return {
                "status": "success",
                "player": f"Player ID {self.player_id}",
                "reason": "Transition inserted successfully"
            }
        except Exception as e:
            logger.failed(item_key, f"Insertion error: {e}")
            return {
                "status": "failed",
                "player": f"Player ID {self.player_id}",
                "reason": f"Insertion error: {e}"
            }