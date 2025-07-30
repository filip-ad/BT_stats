# src/models/player.py

from dataclasses import dataclass
from typing import Optional, List
import logging

@dataclass
class Player:
    player_id_ext: int
    firstname: str
    lastname: str
    year_born: int
    player_id: Optional[int] = None  # Add this optional field for internal ID, as it is not know at the time of creation

    @staticmethod
    def from_dict(data: dict):
        return Player(
            player_id_ext=data.get("player_id_ext"),
            firstname=data.get("firstname"),
            lastname=data.get("lastname"),
            year_born=data.get("year_born"),
            player_id=data.get("player_id", None)  # Default to None if not provided
        )
    
    def sanitize(self):
        # Trim and title-case names
        self.firstname = self.firstname.strip().title()
        self.lastname = self.lastname.strip().title()

    def save_to_db(self, cursor):

        # Sanitize player data
        self.sanitize()

        # Validate required fields
        if not all([self.player_id_ext, self.firstname, self.lastname, self.year_born]):
            return {
                "status": "failed",
                "player": f"{self.firstname} {self.lastname}",
                "reason": "Missing required player fields"
            }

        try:
            cursor.execute("""
                INSERT OR IGNORE INTO player (player_id_ext, firstname, lastname, year_born)
                VALUES (?, ?, ?, ?)
            """, (self.player_id_ext, self.firstname, self.lastname, self.year_born))

            if cursor.rowcount == 0:
                logging.info(f"Skipped player_id_ext {self.player_id_ext}: Player already exists")
                return {
                    "status": "skipped",
                    "player": f"{self.firstname} {self.lastname}",
                    "reason": "Player already exists"
                }

            return {
                "status": "success",
                "player": f"{self.firstname} {self.lastname}",
                "reason": "Player inserted successfully"
            }

        except Exception as e:
            return {
                "status": "failed",
                "player": f"{self.firstname} {self.lastname}",
                "reason": f"Insertion error: {e}"
            }

    @staticmethod
    def get_by_id_ext(cursor, player_id_ext: int) -> Optional['Player']:
        """Retrieve a Player instance by player_id_ext, or None if not found."""
        try:
            cursor.execute("""
                SELECT player_id, player_id_ext, firstname, lastname, year_born
                FROM player WHERE player_id_ext = ?
            """, (player_id_ext,))
            row = cursor.fetchone()
            if row:
                return Player.from_dict({
                    "player_id": row[0],
                    "player_id_ext": row[1],
                    "firstname": row[2],
                    "lastname": row[3],
                    "year_born": row[4]
                })
            return None
        except Exception as e:
            return {
                "status": "failed",
                "player": f"Unknown Player",
                "reason": f"Error retrieving player by player_id_ext {player_id_ext}: {e}"
            }

    @staticmethod
    def get_by_id(cursor, player_id: int) -> Optional['Player']:
        """Retrieve a Player instance by internal player_id, or None if not found."""
        try:
            cursor.execute("""
                SELECT player_id, player_id_ext, firstname, lastname, year_born
                FROM player WHERE player_id = ?
            """, (player_id,))
            row = cursor.fetchone()
            if row:
                return Player.from_dict({
                    "player_id": row[0],
                    "player_id_ext": row[1],
                    "firstname": row[2],
                    "lastname": row[3],
                    "year_born": row[4]
                })
            return None
        except Exception as e:
            return {
                "status": "failed",
                "player": f"Unknown Player",
                "reason": f"Error retrieving player by player_id {player_id}: {e}"
            }

    def validate_against(self, other: 'Player') -> bool:
        """
        Validate if this player's data matches another's, ignoring player_id.
        Assumes both are sanitized.
        """
        return (self.player_id_ext == other.player_id_ext and
                self.firstname == other.firstname and
                self.lastname == other.lastname and
                self.year_born == other.year_born)
    
    @staticmethod
    def search_by_name_and_year(cursor, firstname: str, lastname: str, year_born: int) -> List['Player']:
        """Retrieve Player instances by firstname, lastname, and year_born."""
        try:
            cursor.execute("""
                SELECT player_id, player_id_ext, firstname, lastname, year_born
                FROM player
                WHERE firstname = ? AND lastname = ? AND year_born = ?
            """, (firstname, lastname, year_born))
            rows = cursor.fetchall()
            return [Player.from_dict({
                "player_id": row[0],
                "player_id_ext": row[1],
                "firstname": row[2],
                "lastname": row[3],
                "year_born": row[4]
            }) for row in rows]
        except Exception as e:
            logging.error(f"Error retrieving players by name {firstname} {lastname} and year_born {year_born}: {e}")
            return []