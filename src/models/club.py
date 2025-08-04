# src/models/club.py

from dataclasses import dataclass
from typing import Dict, Optional, List
import logging
import re

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
    def _normalize(name: str) -> str:
        """
        Lower-case, strip, collapse spaces and hyphens so that
        e.g. “Runhällens  BOIS” → “runhällens bois”
        """
        return re.sub(r'[\s\-]+', ' ', name.strip().lower())            

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

    # @staticmethod
    # def get_by_name(cursor, name: str, exact: bool = True) -> Optional['Club']:
    #     """Retrieve a Club instance by name or long_name (exact or partial match) from club or club_alias."""
    #     try:
    #         # Build conditions for exact or partial match
    #         condition = "name = ?" if exact else "LOWER(name) LIKE LOWER(?)"
    #         condition_long = "long_name = ?" if exact else "LOWER(long_name) LIKE LOWER(?)"
    #         # Add % for partial matches
    #         search_name = name if exact else f"%{name}%"

    #         # Search both club and club_alias tables
    #         query = f"""
    #             SELECT club_id FROM club
    #             WHERE {condition} OR {condition_long}
    #             UNION
    #             SELECT club_id FROM club_alias
    #             WHERE {condition} OR {condition_long}
    #         """
    #         cursor.execute(query, (search_name, search_name, search_name, search_name))
    #         row = cursor.fetchone()
    #         if not row:
    #             return None
    #         canonical_club_id = row[0]
    #         return Club.get_by_id(cursor, canonical_club_id)
    #     except Exception as e:
    #         logging.error(f"Error retrieving club by name {name}: {e}")
    #         return None            

    @staticmethod
    def cache_all(cursor) -> Dict[int, 'Club']:
        """Cache all Club objects by club_id, including aliases, using minimal queries."""
        try:
            # Fetch all clubs
            cursor.execute("""
                SELECT club_id, name, long_name, city, country_code, district_id
                FROM club
            """)
            club_map = {
                row[0]: Club(
                    club_id=row[0],
                    name=row[1],
                    long_name=row[2],
                    city=row[3],
                    country_code=row[4],
                    district_id=row[5],
                    aliases=[]
                ) for row in cursor.fetchall()
            }

            # Fetch all aliases in one query
            cursor.execute("""
                SELECT club_id, club_id_ext, name, long_name, remarks
                FROM club_alias
            """)
            for row in cursor.fetchall():
                club_id = row[0]
                if club_id in club_map:
                    club_map[club_id].aliases.append({
                        "club_id_ext": row[1],
                        "name": row[2],
                        "long_name": row[3],
                        "remarks": row[4]
                    })
                else:
                    logging.warning(f"No club found for club_id {club_id} in club_map")

            logging.info(f"Cached {len(club_map)} clubs")
            return club_map
        except Exception as e:
            logging.error(f"Error caching clubs: {e}")
            return {}

    @classmethod
    def cache_name_map(cls, cursor) -> Dict[str, "Club"]:
        """
        Build a single dict from normalized club-name → Club instance.
        Includes both club.name/long_name and all aliases.
        """
        # 1) load every Club
        cursor.execute("""
            SELECT club_id, name, long_name, city, country_code, district_id
            FROM club
        """)
        clubs = {
            row[0]: cls(
                club_id=row[0],
                name=row[1],
                long_name=row[2],
                city=row[3],
                country_code=row[4],
                district_id=row[5],
                aliases=[]
            )
            for row in cursor.fetchall()
        }

        # 2) load aliases
        cursor.execute("""
            SELECT club_id, name, long_name, remarks
            FROM club_alias
        """)
        for club_id, alias_name, alias_long, remarks in cursor.fetchall():
            if club_id not in clubs:
                continue
            clubs[club_id].aliases.append({
                "name": alias_name,
                "long_name": alias_long,
                "remarks": remarks
            })

        # 3) build normalized lookup
        lookup: Dict[str, Club] = {}
        for c in clubs.values():
            for raw in (c.name, c.long_name):
                if raw:
                    lookup[cls._normalize(raw)] = c
            for al in c.aliases:
                for raw in (al["name"], al["long_name"]):
                    if raw:
                        lookup[cls._normalize(raw)] = c

        logging.info(f"Cached {len(lookup)} club‐name mappings")
        return lookup
    
    @classmethod
    def get_by_name(cls, cursor, raw_name: str) -> Optional["Club"]:
        """
        Normalize the input and create a cache of club names.
        Then look up the normalized name in the cache.
        """
        normalized_name = cls._normalize(raw_name)
        return cls.cache_name_map(cursor).get(normalized_name)