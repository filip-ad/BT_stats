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
    
    # @classmethod
    # def resolve(
    #     cls,
    #     cursor,
    #     clubname_raw:       str,
    #     club_map:           Dict[str, "Club"],
    #     logger,
    #     item_key:           str,
    #     *,
    #     allow_prefix:       bool = False,
    #     min_ratio:          float = 0.8,
    #     unknown_club_id:    int = 9999,
    # ) -> "Club":
    #     norm = cls._normalize(clubname_raw)
    #     club = club_map.get(norm)

    #     # optional prefix similarity
    #     if not club and allow_prefix and len(norm) >= 5:
    #         club = cls._prefix_match(norm, club_map, min_ratio=min_ratio)
    #         if club:
    #             logger.warning(item_key, f"Club matched by prefix similarity")

    #     if not club:
    #         club = cls.get_by_id(cursor, unknown_club_id)
    #         logger.warning(clubname_raw, f"Club not found. Using 'Unknown club (id: {unknown_club_id})'")
    #         with open("missing_clubs.txt", "a", encoding="utf-8") as f:
    #             f.write(f"Context: {item_key}, Club Raw: {clubname_raw}\n")

    #     return club

    @classmethod
    def resolve(
        cls,
        cursor,
        clubname_raw: str,
        club_map: Dict[str, "Club"],
        logger,
        item_key: str,
        *,
        allow_prefix: bool = False,
        min_ratio: float = 0.8,
        fallback_to_unknown: bool = False,
    ) -> "Club":
        """
        Resolve a club name to a Club object.

        Resolution stages:
        1) Exact match with diacritics preserved
        2) Exact match with relaxed normalization (strip diacritics, preserve nordic)
        3) Optional prefix match (first strict, then relaxed)
            - if multiple candidates tie at the best score → log ambiguity and return None
        4) Fallback to Unknown club (id=9999) if allowed
        """

        # --- Stage 1: exact strict (diacritics preserved)
        norm_strict = normalize_key(clubname_raw, preserve_diacritics=True)
        club = club_map.get(norm_strict)
        if club:
            return club

        # --- Stage 2: exact relaxed (strip diacritics, keep åäöø)
        norm_ascii = normalize_key(clubname_raw, preserve_diacritics=False, preserve_nordic=True)
        club = club_map.get(norm_ascii)
        if club:
            logger.warning(item_key, "Matched club by relaxed ASCII normalization")
            return club

        # --- Stage 3: prefix similarity (only if allowed)
        if allow_prefix and len(norm_strict) >= 3:
            # Try strict first
            club = cls._prefix_match(norm_strict, club_map, min_ratio=min_ratio, mode="strict")
            if not club:
                # Then relaxed
                club = cls._prefix_match(norm_ascii, club_map, min_ratio=min_ratio, mode="ascii")
            if club:
                logger.warning(item_key, "Club matched by prefix similarity")
                return club

        # --- Stage 4: unknown club
        if fallback_to_unknown:
            club = cls.get_by_id(cursor, 9999)
            # with open("missing_clubs.txt", "a", encoding="utf-8") as f:
            #     f.write(f"Context: {item_key}, Club Raw: {clubname_raw}\n")
            return club
       
        return None

    @staticmethod
    def _prefix_match(norm: str, club_map: Dict[str, "Club"], min_ratio: float = 0.75, mode: str = "strict"):
        """
        Hybrid prefix match:
        - score = average of (query coverage, candidate coverage)
        - if multiple clubs tie for best score, return None (ambiguous)
        """
        best_score = 0.0
        best_clubs = []

        for key, club in club_map.items():
            common = 0
            for a, b in zip(norm, key):
                if a != b:
                    break
                common += 1

            ratio_query     = common / len(norm) if norm else 0
            ratio_candidate = common / len(key) if key else 0
            score = (ratio_query + ratio_candidate) / 2

            if score > best_score:
                best_score = score
                best_clubs = [club]
            elif score == best_score and score >= min_ratio:
                best_clubs.append(club)

        if best_score >= min_ratio:
            if len(best_clubs) == 1:
                logging.info(f"Prefix match ({mode}) for '{norm}': {best_clubs[0]} (score: {best_score:.2f})")
                return best_clubs[0]
            else:
                logging.warning(f"Ambiguous prefix match ({mode}) for '{norm}': {[c.shortname for c in best_clubs]} (score: {best_score:.2f})")
                return None
        return None


    

    @classmethod
    def cache_name_map(cls, cursor) -> Dict[str, "Club"]:
        """
        Build a normalized name → Club map including shortname, longname, and aliases.
        """
        clubs = cls.cache_all(cursor)   # club_id → Club
        club_map: Dict[str, Club] = {}

        for club in clubs.values():
            names: List[str] = []
            if club.shortname:
                names.append(club.shortname)
            if club.longname:
                names.append(club.longname)
            for alias in club.aliases:
                if alias["alias"]:
                    names.append(alias["alias"])

            for name in names:
                # norm_key = cls._normalize(name)   # instead of normalize_key(name)
                norm_key = normalize_key(name, preserve_diacritics=True)  # strict map
                if not norm_key:
                    continue
                if norm_key in club_map and club_map[norm_key].club_id != club.club_id:
                    logging.warning(
                        f"Name '{name}' normalized to '{norm_key}' "
                        f"conflicts between clubs {club.club_id} and {club_map[norm_key].club_id}"
                    )
                club_map[norm_key] = club

        logging.info(
            f"Built club_name_map with {len(club_map)} unique normalized names "
            f"from {len(clubs)} clubs"
        )
        return club_map

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


    
    # @classmethod
    # def get_by_name(cls, cursor, raw_name: str) -> Optional["Club"]:
    #     """
    #     Normalize the input and create a cache of club names.
    #     Then look up the normalized name in the cache.
    #     """
    #     normalized_name = cls._normalize(raw_name)
    #     return cls.cache_name_map(cursor).get(normalized_name)