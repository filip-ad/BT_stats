# src/models/club.py

from dataclasses import dataclass
from typing import Dict, Optional, List
import logging
import re
import unicodedata

@dataclass
class Club:
    club_id:       Optional[int]    = None     # PK in club
    shortname:     Optional[str]    = None     # club.shortname
    longname:      Optional[str]    = None     # club.longname
    club_type:     Optional[str]    = "club"   # e.g. 'club' or 'national'
    city:          Optional[str]    = None
    country_code:  Optional[str]    = None
    remarks:       Optional[str]    = None
    homepage:      Optional[str]    = None
    active:        Optional[int]    = 1
    district_id:   Optional[int]    = None
    ext_id:        Optional[int]    = None     # from club_ext_id
    aliases:       List[dict]       = None     # {"alias": str, "alias_type": "short"|"long"}

    def __post_init__(self):
        # Initialize aliases as empty list if None
        if self.aliases is None:
            self.aliases = []

    @staticmethod
    def _normalize(name: str) -> str:
        """
        1) Trim + lower
        2) Unicode‐normalize (e.g. Å → A)
        3) Drop anything non‐alphanumeric or space
        4) Collapse whitespace
        """
        s = name.strip().lower()
        s = unicodedata.normalize("NFKD", s)
        s = "".join(ch for ch in s if not unicodedata.combining(ch))
        s = re.sub(r"[^a-z0-9\s]", " ", s)
        s = re.sub(r"\s+", " ", s)
        return s.strip()
    
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
    def get_by_id_ext(cursor, ext_id: int) -> Optional["Club"]:
        """Lookup a club by its external ID (in club_ext_id) and return the canonical Club."""
        cursor.execute("""
            SELECT club_id FROM club_ext_id WHERE club_id_ext = ?
        """, (ext_id,))
        row = cursor.fetchone()
        if not row:
            return None
        return Club.get_by_id(cursor, row[0])

    @staticmethod
    def get_by_id(cursor, club_id: int) -> Optional["Club"]:
        """Load one Club by its canonical club_id (with ext_id and all name‐aliases)."""
        cursor.execute("""
            SELECT 
              c.club_id, c.shortname, c.longname, c.club_type,
              c.city, c.country_code, c.remarks, c.homepage,
              c.active, c.district_id,
              e.club_id_ext
            FROM club c
            LEFT JOIN club_ext_id e ON e.club_id = c.club_id
            WHERE c.club_id = ?
        """, (club_id,))
        row = cursor.fetchone()
        if not row:
            return None

        club = Club(
            club_id      = row[0],
            shortname    = row[1],
            longname     = row[2],
            club_type    = row[3],
            city         = row[4],
            country_code = row[5],
            remarks      = row[6],
            homepage     = row[7],
            active       = row[8],
            district_id  = row[9],
            ext_id       = row[10],
            aliases      = []
        )

        cursor.execute("""
            SELECT alias, alias_type
            FROM club_name_alias
            WHERE club_id = ?
        """, (club_id,))
        for alias, alias_type in cursor.fetchall():
            club.aliases.append({"alias": alias, "alias_type": alias_type})

        return club  

    @staticmethod
    def cache_all(cursor) -> Dict[int, "Club"]:
        """
        Returns a dict mapping canonical club_id → Club instance
        (populates ext_id and name‐aliases in bulk).
        """
        # 1) load all clubs + ext_id
        cursor.execute("""
            SELECT 
              c.club_id, c.shortname, c.longname, c.club_type,
              c.city, c.country_code, c.remarks, c.homepage,
              c.active, c.district_id,
              e.club_id_ext
            FROM club c
            LEFT JOIN club_ext_id e ON e.club_id = c.club_id
        """)
        clubs: Dict[int, Club] = {}
        for row in cursor.fetchall():
            clubs[row[0]] = Club(
                club_id      = row[0],
                shortname    = row[1],
                longname     = row[2],
                club_type    = row[3],
                city         = row[4],
                country_code = row[5],
                remarks      = row[6],
                homepage     = row[7],
                active       = row[8],
                district_id  = row[9],
                ext_id       = row[10],
                aliases      = []
            )

        # 2) load all name‐aliases
        cursor.execute("""
            SELECT club_id, alias, alias_type
            FROM club_name_alias
        """)
        for club_id, alias, alias_type in cursor.fetchall():
            if club_id in clubs:
                clubs[club_id].aliases.append({
                    "alias": alias,
                    "alias_type": alias_type
                })
            else:
                logging.warning(f"alias for unknown club_id {club_id}: {alias}")

        logging.info(f"Cached {len(clubs)} clubs (with ext_id + aliases)")
        return clubs

    @classmethod
    def cache_name_map(cls, cursor) -> Dict[str, "Club"]:
        """
        Returns a dict mapping every normalized variant
        (shortname, longname, *and* every alias) → Club instance.
        """
        by_id = cls.cache_all(cursor)
        lookup: Dict[str, Club] = {}
        for c in by_id.values():
            for variant in (c.shortname, c.longname):
                if variant:
                    lookup[cls._normalize(variant)] = c
            for alias in c.aliases:
                lookup[cls._normalize(alias["alias"])] = c
        logging.info(f"Cached {len(lookup)} normalized name→Club entries")
        return lookup
    
    @classmethod
    def get_by_name(cls, cursor, raw_name: str) -> Optional["Club"]:
        """
        Normalize the input and create a cache of club names.
        Then look up the normalized name in the cache.
        """
        normalized_name = cls._normalize(raw_name)
        return cls.cache_name_map(cursor).get(normalized_name)