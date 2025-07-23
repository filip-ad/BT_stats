# src/models/player.py

from dataclasses import dataclass

@dataclass
class Player:
    player_id_ext: int
    firstname: str
    lastname: str
    year_born: int

    @staticmethod
    def from_dict(data: dict):
        return Player(
            player_id_ext=data.get("player_id_ext"),
            firstname=data.get("firstname"),
            lastname=data.get("lastname"),
            year_born=data.get("year_born"),
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

            # Check if the insert was successful
            # If no rows were inserted, it means the player already exists
            if cursor.rowcount == 0:
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
