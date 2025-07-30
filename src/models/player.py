# src/models/player.py

from dataclasses import dataclass
from typing import Optional, List
import logging
from utils import sanitize_name

@dataclass
class Player:
    player_id: Optional[int] = None
    firstname: Optional[str] = None
    lastname: Optional[str] = None
    year_born: Optional[int] = None
    aliases: List[dict] = None  # [{player_id_ext, firstname, lastname, year_born}]

    def __post_init__(self):
        if self.aliases is None:
            self.aliases = []

    def sanitize(self):
        if self.firstname:
            self.firstname = self.firstname.strip().title()
        if self.lastname:
            self.lastname = self.lastname.strip().title()

    @staticmethod
    def from_dict(data: dict):
        return Player(
            player_id=data.get("player_id"),
            firstname=data.get("firstname"),
            lastname=data.get("lastname"),
            year_born=data.get("year_born"),
            aliases=data.get("aliases", [])
        )

    @staticmethod
    def get_by_id_ext(cursor, player_id_ext: int) -> Optional['Player']:
        try:
            # Get canonical player_id from alias
            cursor.execute("SELECT player_id FROM player_alias WHERE player_id_ext = ?", (player_id_ext,))
            row = cursor.fetchone()
            if not row:
                return None
            return Player.get_by_id(cursor, row[0])
        except Exception as e:
            logging.error(f"Error retrieving player by player_id_ext {player_id_ext}: {e}")
            return None

    @staticmethod
    def get_by_id(cursor, player_id: int) -> Optional['Player']:
        try:
            # Fetch canonical player
            cursor.execute("""
                SELECT player_id, firstname, lastname, year_born FROM player WHERE player_id = ?
            """, (player_id,))
            row = cursor.fetchone()
            if not row:
                return None

            # Fetch aliases
            cursor.execute("""
                SELECT player_id_ext, firstname, lastname, year_born FROM player_alias WHERE player_id = ?
            """, (player_id,))
            aliases = [
                {"player_id_ext": r[0], "firstname": r[1], "lastname": r[2], "year_born": r[3]}
                for r in cursor.fetchall()
            ]

            return Player(
                player_id=row[0],
                firstname=row[1],
                lastname=row[2],
                year_born=row[3],
                aliases=aliases
            )
        except Exception as e:
            logging.error(f"Error retrieving player by ID {player_id}: {e}")
            return None

    def save_to_db(self, cursor, player_id_ext: int):
        self.sanitize()

        if not all([player_id_ext, self.firstname, self.lastname, self.year_born]):
            return {
                "status": "failed",
                "player": f"{self.firstname} {self.lastname}",
                "reason": "Missing required player fields"
            }

        try:
            # Check if alias already exists
            cursor.execute("SELECT player_id FROM player_alias WHERE player_id_ext = ?", (player_id_ext,))
            if cursor.fetchone():
                return {
                    "status": "skipped",
                    "player": f"{self.firstname} {self.lastname}",
                    "reason": "Alias already exists"
                }

            # Always insert a new canonical player first (1-to-1 assumption)
            cursor.execute("""
                INSERT INTO player (firstname, lastname, year_born)
                VALUES (?, ?, ?)
            """, (self.firstname, self.lastname, self.year_born))
            self.player_id = cursor.lastrowid

            # Link alias to new canonical player
            cursor.execute("""
                INSERT INTO player_alias (player_id, player_id_ext, firstname, lastname, year_born)
                VALUES (?, ?, ?, ?, ?)
            """, (self.player_id, player_id_ext, self.firstname, self.lastname, self.year_born))

            return {
                "status": "success",
                "player": f"{self.firstname} {self.lastname}",
                "reason": "Inserted new canonical player and alias"
            }

        except Exception as e:
            return {
                "status": "failed",
                "player": f"{self.firstname} {self.lastname}",
                "reason": f"Insertion error: {e}"
            }
        
    @staticmethod
    def search_by_name_and_year(cursor, firstname: str, lastname: str, year_born: int) -> List['Player']:
        """Search for Player instances by firstname, lastname, and year_born in player_alias."""
        try:
            firstname = sanitize_name(firstname)
            lastname = sanitize_name(lastname)
            cursor.execute("""
                SELECT DISTINCT p.player_id, p.firstname, p.lastname, p.year_born
                FROM player p
                JOIN player_alias pa ON p.player_id = pa.player_id
                WHERE pa.firstname = ? AND pa.lastname = ? AND pa.year_born = ?
            """, (firstname, lastname, year_born))
            rows = cursor.fetchall()
            players = []
            for row in rows:
                player_id = row[0]
                # Fetch aliases for this player
                cursor.execute("""
                    SELECT player_id_ext, firstname, lastname, year_born
                    FROM player_alias
                    WHERE player_id = ?
                """, (player_id,))
                aliases = [
                    {"player_id_ext": r[0], "firstname": r[1], "lastname": r[2], "year_born": r[3]}
                    for r in cursor.fetchall()
                ]
                players.append(Player(
                    player_id=row[0],
                    firstname=row[1],
                    lastname=row[2],
                    year_born=row[3],
                    aliases=aliases
                ))
            return players
        except Exception as e:
            logging.error(f"Error searching players by name {firstname} {lastname} and year_born {year_born}: {e}")
            return []