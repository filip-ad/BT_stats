# src/models/club.py

from dataclasses import dataclass
from typing import Optional, List
import logging

@dataclass
class Club:
    club_id: Optional[int] = None       # Canonical ID from club table
    name: Optional[str] = None          # Canonical abbreviated name
    long_name: Optional[str] = None     # Canonical full name
    city: Optional[str] = None
    country_code: Optional[str] = None
    district_id: Optional[int] = None
    aliases: List[dict] = None          # List of aliases from club_alias (club_id_ext, name, long_name, remarks)

    def __post_init__(self):
        # Initialize aliases as empty list if None
        if self.aliases is None:
            self.aliases = []

    @staticmethod
    def from_dict(data: dict):
        return Club(
            club_id=data.get("club_id"),
            name=data.get("name"),
            long_name=data.get("long_name"),
            city=data.get("city"),
            country_code=data.get("country_code"),
            district_id=data.get("district_id"),
            aliases=data.get("aliases", [])
        )

    @staticmethod
    def get_by_id_ext(cursor, club_id_ext: int) -> Optional['Club']:
        """Retrieve a Club instance by club_id_ext, including canonical data and aliases."""
        try:
            # Get canonical club_id from club_alias
            cursor.execute("""
                SELECT club_id FROM club_alias WHERE club_id_ext = ?
            """, (club_id_ext,))
            row = cursor.fetchone()
            if not row:
                return None
            canonical_club_id = row[0]

            # Fetch canonical club data
            cursor.execute("""
                SELECT club_id, name, long_name, city, country_code, district_id
                FROM club WHERE club_id = ?
            """, (canonical_club_id,))
            club_row = cursor.fetchone()
            if not club_row:
                return None

            # Fetch all aliases
            cursor.execute("""
                SELECT club_id_ext, name, long_name, remarks
                FROM club_alias WHERE club_id = ?
            """, (canonical_club_id,))
            aliases = [
                {"club_id_ext": row[0], "name": row[1], "long_name": row[2], "remarks": row[3]}
                for row in cursor.fetchall()
            ]

            return Club(
                club_id=club_row[0],
                name=club_row[1],
                long_name=club_row[2],
                city=club_row[3],
                country_code=club_row[4],
                district_id=club_row[5],
                aliases=aliases
            )
        except Exception as e:
            logging.error(f"Error retrieving club by club_id_ext {club_id_ext}: {e}")
            return None

    @staticmethod
    def get_by_name(cursor, name: str, exact: bool = True) -> Optional['Club']:
        """Retrieve a Club instance by name or long_name (exact or partial match) from club or club_alias."""
        try:
            # Build conditions for exact or partial match
            condition = "name = ?" if exact else "LOWER(name) LIKE LOWER(?)"
            condition_long = "long_name = ?" if exact else "LOWER(long_name) LIKE LOWER(?)"
            # Add % for partial matches
            search_name = name if exact else f"%{name}%"

            # Search both club and club_alias tables
            query = f"""
                SELECT club_id FROM club
                WHERE {condition} OR {condition_long}
                UNION
                SELECT club_id FROM club_alias
                WHERE {condition} OR {condition_long}
            """
            cursor.execute(query, (search_name, search_name, search_name, search_name))
            row = cursor.fetchone()
            if not row:
                return None
            canonical_club_id = row[0]
            return Club.get_by_id(cursor, canonical_club_id)
        except Exception as e:
            logging.error(f"Error retrieving club by name {name}: {e}")
            return None

    @staticmethod
    def get_by_id(cursor, club_id: int) -> Optional['Club']:
        """Retrieve a Club instance by canonical club_id, including aliases."""
        try:
            cursor.execute("""
                SELECT club_id, name, long_name, city, country_code, district_id
                FROM club WHERE club_id = ?
            """, (club_id,))
            club_row = cursor.fetchone()
            if not club_row:
                return None

            cursor.execute("""
                SELECT club_id_ext, name, long_name, remarks
                FROM club_alias WHERE club_id = ?
            """, (club_id,))
            aliases = [
                {"club_id_ext": row[0], "name": row[1], "long_name": row[2], "remarks": row[3]}
                for row in cursor.fetchall()
            ]

            return Club(
                club_id=club_row[0],
                name=club_row[1],
                long_name=club_row[2],
                city=club_row[3],
                country_code=club_row[4],
                district_id=club_row[5],
                aliases=aliases
            )
        except Exception as e:
            logging.error(f"Error retrieving club by club_id {club_id}: {e}")
            return None