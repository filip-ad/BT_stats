# src/models/club.py

from dataclasses import dataclass
from typing import Dict, Optional, List
import logging
import re
import unicodedata
from models.cache_mixin import CacheMixin
from utils import normalize_key

@dataclass
class Club(CacheMixin):
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
    aliases:       List[dict]       = None     # {"alias": str, "alias_type": "short"|"long"}
    id_exts:       List[dict]       = None     # {"id_ext": int, "source": str}

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
            shortname=data.get("shortname"),
            longname=data.get("longname"),
            city=data.get("city"),
            country_code=data.get("country_code"),
            district_id=data.get("district_id"),
            aliases=data.get("aliases", [])
        )
    
    @classmethod
    def cache_name_map(
        cls, 
        cursor
    ) -> Dict[str, 'Club']:
        """
        Build a normalized name to Club map using cached query, including shortname, longname, and all aliases.
        """
        # Fetch all club rows to create Club objects
        sql_clubs = """
            SELECT *
            FROM club
        """
        club_rows = cls.cached_query(cursor, sql_clubs)
        clubs_dict = {row['club_id']: Club.from_dict(row) for row in club_rows}

        # Fetch all possible names (short, long, aliases)
        sql_names = """
            SELECT club_id, name
            FROM (
                SELECT club_id, shortname AS name FROM club WHERE shortname IS NOT NULL AND shortname != ''
                UNION
                SELECT club_id, longname AS name FROM club WHERE longname IS NOT NULL AND longname != ''
                UNION
                SELECT club_id, alias AS name FROM club_name_alias WHERE alias IS NOT NULL AND alias != ''
            )
        """
        name_rows = cls.cached_query(cursor, sql_names)

        club_map: Dict[str, Club] = {}
        for row in name_rows:
            norm_key = normalize_key(row['name'])
            club = clubs_dict.get(row['club_id'])
            if club:
                if norm_key in club_map and club_map[norm_key].club_id != club.club_id:
                    # Optional: log warning for conflicting keys
                    pass
                club_map[norm_key] = club

        return club_map
    
    # @classmethod
    # def cache_name_map(cls, cursor) -> Dict[str, "Club"]:
    #     """
    #     Returns a dict mapping every normalized variant
    #     (shortname, longname, *and* every alias) → Club instance.
    #     """
    #     by_id = cls.cache_all(cursor)
    #     lookup: Dict[str, Club] = {}
    #     for c in by_id.values():
    #         for variant in (c.shortname, c.longname):
    #             if variant:
    #                 lookup[cls._normalize(variant)] = c
    #         for alias in c.aliases:
    #             lookup[cls._normalize(alias["alias"])] = c
    #     logging.info(f"Cached {len(lookup)} normalized name→Club entries")
    #     return lookup
    


    @staticmethod
    def get_by_id_ext(cursor, id_ext: int) -> Optional["Club"]:
        """Lookup a club by its external ID (in club_id_ext) and return the canonical Club."""
        cursor.execute("""
            SELECT club_id FROM club_id_ext WHERE club_id_ext = ?
        """, (id_ext,))
        row = cursor.fetchone()
        if not row:
            return None
        return Club.get_by_id(cursor, row[0])

    @staticmethod
    def get_by_id(cursor, club_id: int) -> Optional["Club"]:
        """Load one Club by its canonical club_id (with id_ext and all name‐aliases)."""
        cursor.execute("""
            SELECT 
              c.club_id, c.shortname, c.longname, c.club_type,
              c.city, c.country_code, c.remarks, c.homepage,
              c.active, c.district_id,
              e.club_id_ext
            FROM club c
            LEFT JOIN club_id_ext e ON e.club_id = c.club_id
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
            id_exts      = [],
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
        (populates id_ext and name‐aliases in bulk).
        """
        # 1) load all clubs + id_ext
        cursor.execute("""
            SELECT 
              c.club_id, c.shortname, c.longname, c.club_type,
              c.city, c.country_code, c.remarks, c.homepage,
              c.active, c.district_id,
              e.club_id_ext
            FROM club c
            LEFT JOIN club_id_ext e ON e.club_id = c.club_id
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
                id_exts      = row[10],
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

        logging.info(f"Cached {len(clubs)} clubs (with id_ext + aliases)")
        return clubs
    
    @staticmethod
    def cache_id_ext_map(cursor) -> Dict[int, 'Club']:
        """
        Build a mapping from external club IDs (club_id_ext) to Club instances.
        """
        # first load all canonical clubs
        clubs_by_id = Club.cache_all(cursor)    # Dict[int, Club]

        # then walk the alias table
        cursor.execute("SELECT club_id, club_id_ext FROM club_id_ext")
        id_ext_map: Dict[int, Club] = {}
        for cid, ext in cursor.fetchall():
            club = clubs_by_id.get(cid)
            if club:
                id_ext_map[ext] = club
            else:
                logging.warning(f"Alias for unknown club_id {cid}: ext={ext}")
        return id_ext_map    


    
    @classmethod
    def get_by_name(cls, cursor, raw_name: str) -> Optional["Club"]:
        """
        Normalize the input and create a cache of club names.
        Then look up the normalized name in the cache.
        """
        normalized_name = cls._normalize(raw_name)
        return cls.cache_name_map(cursor).get(normalized_name)