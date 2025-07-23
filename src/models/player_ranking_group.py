# src/models/player_ranking_group.py

from dataclasses import dataclass

@dataclass
class PlayerRankingGroup:
    player_id: int
    ranking_group_id: int

    def save_to_db(self, cursor):
        try:
            # Check if player_id exists in player table
            cursor.execute("SELECT 1 FROM player WHERE player_id = ?", (self.player_id,))
            if not cursor.fetchone():
                return {
                    "status": "failed",
                    "player_id": self.player_id,
                    "ranking_group_id": self.ranking_group_id,
                    "reason": f"Foreign key violation: player_id {self.player_id} does not exist in player table"
                }

            # Check if ranking_group_id exists in ranking_group table
            cursor.execute("SELECT 1 FROM ranking_group WHERE ranking_group_id = ?", (self.ranking_group_id,))
            if not cursor.fetchone():
                return {
                    "status": "failed",
                    "player_id": self.player_id,
                    "ranking_group_id": self.ranking_group_id,
                    "reason": f"Foreign key violation: ranking_group_id {self.ranking_group_id} does not exist in ranking_group table"
                }

            # Attempt to insert the record
            cursor.execute("""
                INSERT OR IGNORE INTO player_ranking_group (player_id, ranking_group_id)
                VALUES (?, ?)
            """, (self.player_id, self.ranking_group_id))

            # Check if the insert was successful (rowcount > 0) or skipped (rowcount = 0)
            if cursor.rowcount > 0:
                return {
                    "status": "success",
                    "player_id": self.player_id,
                    "ranking_group_id": self.ranking_group_id,
                    "reason": "Player ranking group saved successfully"
                }
            else:
                return {
                    "status": "skipped",
                    "player_id": self.player_id,
                    "ranking_group_id": self.ranking_group_id,
                    "reason": "Record already exists"
                }

        except Exception as e:
            return {
                "status": "failed",
                "player_id": self.player_id,
                "ranking_group_id": self.ranking_group_id,
                "reason": f"Database error: {str(e)}"
            }
        
    @staticmethod
    def delete_by_player_id(cursor, player_id):
        try:
            cursor.execute("""
                DELETE FROM player_ranking_group WHERE player_id = ?
            """, (player_id,))
            return {
                "status": "success",
                "player_id": player_id,
                "reason": f"Deleted {cursor.rowcount} existing ranking group(s) for player_id {player_id}"
            }
        except Exception as e:
            return {
                "status": "failed",
                "player_id": player_id,
                "reason": f"Database error: {str(e)}"
            }