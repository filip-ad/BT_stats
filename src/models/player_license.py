# src/models/player_license.py

from datetime import datetime
from dataclasses import dataclass
import logging

@dataclass
class PlayerLicense:
    player_id: int
    club_id: int
    season_id: int
    license_id: int
    valid_from: datetime.date
    valid_to: datetime.date

    @staticmethod
    def from_dict(data: dict):
        return PlayerLicense(
            player_id=data["player_id"],
            club_id=data["club_id"],
            season_id=data["season_id"],
            license_id=data["license_id"],
            valid_from=data["valid_from"],
            valid_to=data["valid_to"]
        )

    def save_to_db(self, cursor):
        try:
            # Check if player_id exists in player table
            cursor.execute("SELECT 1 FROM player WHERE player_id = ?", (self.player_id,))
            if not cursor.fetchone():
                return {
                    "status": "failed",
                    "player_id": self.player_id,
                    "reason": f"Foreign key violation: player_id {self.player_id} does not exist in player table"
                }

            # Check if club_id exists in club table
            cursor.execute("SELECT 1 FROM club WHERE club_id = ?", (self.club_id,))
            if not cursor.fetchone():
                return {
                    "status": "failed",
                    "player_id": self.player_id,
                    "reason": f"Foreign key violation: club_id {self.club_id} does not exist in club table"
                }

            # Check if season_id exists in season table
            cursor.execute("SELECT 1 FROM season WHERE season_id = ?", (self.season_id,))
            if not cursor.fetchone():
                return {
                    "status": "failed",
                    "player_id": self.player_id,
                    "reason": f"Foreign key violation: season_id {self.season_id} does not exist in season table"
                }

            # Check if license_id exists in license table
            cursor.execute("SELECT 1 FROM license WHERE license_id = ?", (self.license_id,))
            if not cursor.fetchone():
                return {
                    "status": "failed",
                    "player_id": self.player_id,
                    "reason": f"Foreign key violation: license_id {self.license_id} does not exist in license table"
                }

            # Check if the record already exists
            cursor.execute("""
                SELECT 1 FROM player_license 
                WHERE player_id = ? AND license_id = ? AND season_id = ? AND club_id = ?
            """, (self.player_id, self.license_id, self.season_id, self.club_id))
            if cursor.fetchone():
                return {
                    "status": "skipped",
                    "player_id": self.player_id,
                    "reason": "Player license already exists in database"
                }

            # Insert the player license
            cursor.execute("""
                INSERT INTO player_license (
                    player_id, club_id, valid_from, valid_to, license_id, season_id
                )
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                self.player_id, self.club_id, self.valid_from, self.valid_to,
                self.license_id, self.season_id
            ))

            return {
                "status": "success",
                "player_id": self.player_id,
                "reason": "Player license inserted successfully"
            }

        except Exception as e:
            return {
                "status": "failed",
                "player_id": self.player_id,
                "reason": f"Database error: {str(e)}"
            }