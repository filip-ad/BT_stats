# src/models/club.py

from dataclasses import dataclass

@dataclass
class Club:
    club_id_ext: int
    name: str
    city: str = None
    country_code: str = None

    @staticmethod
    def from_dict(data: dict):
        return Club(
            club_id_ext=data.get("club_id_ext"),
            name=data.get("name"),
            city=data.get("city"),
            country_code=data.get("country_code")
        )

    def save_to_db(self, cursor):
        # Check if club_id_ext and name are provided
        if self.club_id_ext is None or self.name is None:
            return {
                "status": "failed",
                "club": self.name,
                "reason": "Missing club_id_ext or name"
            }
        
        # Check if club already exists
        cursor.execute("SELECT club_id FROM club WHERE club_id_ext = ?", (self.club_id_ext,))
        if cursor.fetchone():
            return {
                "status": "skipped",
                "club": self.name,
                "reason": "Club already exists in database"
            }

        # Insert the club into the database
        try:
            cursor.execute("""
                INSERT INTO club (club_id_ext, name, city, country_code)
                VALUES (?, ?, ?, ?)
            """, (self.club_id_ext, self.name, self.city, self.country_code))
            return {
                "status": "success",
                "club": self.name,
                "reason": "Club inserted successfully"
            }
        
        # Handle any database errors
        except Exception as e:
            return {
                "status": "failed",
                "club": self.name,
                "reason": f"Insertion error: {e}"
            }