# src/models/player.py

from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple
import logging
from collections import defaultdict

from utils import normalize_key, sanitize_name
from models.cache_mixin import CacheMixin

@dataclass
class Player(CacheMixin):
    player_id:      Optional[int]   = None
    firstname:      Optional[str]   = None
    lastname:       Optional[str]   = None
    year_born:      Optional[int]   = None
    date_born:      Optional[str]   = None  # YYYY-MM-DD format
    fullname_raw:   Optional[str]   = None
    is_verified:    bool            = False
    ext_ids:        List[dict]      = None  # player_id -> [{player_id_ext: str, data_source_id: int}]

    def __post_init__(self):
        '''
        Initialize ext_ids.
        '''
        if self.ext_ids is None:
            self.ext_ids = []

    def sanitize(self):
        '''
        Sanitize player name fields.
        '''
        # Do not sanitize fullname_raw – keep it as original
        if self.firstname:
            self.firstname      = sanitize_name(self.firstname)
        if self.lastname:
            self.lastname       = sanitize_name(self.lastname)
      
    @staticmethod
    def from_dict(
        data: dict
    ) -> "Player":
        '''
        Create a Player instance from a dictionary.
        Sanitize firstname and lastname.
        '''
        player = Player(
            player_id=data.get("player_id"),
            firstname=data.get("firstname"),
            lastname=data.get("lastname"),
            year_born=data.get("year_born"),
            date_born=data.get("date_born"),
            fullname_raw=data.get("fullname_raw"),
            is_verified=data.get("is_verified", False),
            ext_ids=data.get("ext_ids", [])
        )
        player.sanitize()
        return player

    @classmethod
    def cache_name_map_verified(cls, cursor) -> Dict[str, List[int]]:
        """
        Build a normalized full name to list of player_ids map using cached query.
        Note: List because names might not be unique.
        """
        sql = """
            SELECT player_id, firstname, lastname
            FROM player
            WHERE is_verified = 1  -- Assuming verified players only for this map
        """
        rows = cls.cached_query(cursor, sql)

        name_map: Dict[str, List[int]] = {}
        for row in rows:
            full_norm = normalize_key(f"{row['firstname']} {row['lastname']}")
            name_map.setdefault(full_norm, []).append(row['player_id'])
        return name_map 

    @classmethod
    def cache_name_map_unverified(cls, cursor) -> Dict[str, int]:
        """
        Build a normalized full name to player_id map for unverified players using cached query.
        Assumes names are unique for unverified after normalization.
        If collisions occur post-normalization, consider changing to List[int] and handling ambiguity.
        """
        sql = """
            SELECT player_id, fullname_raw
            FROM player
            WHERE is_verified = 0 AND fullname_raw IS NOT NULL AND fullname_raw != ''  -- Unverified players
        """
        rows = cls.cached_query(cursor, sql)

        unverified_map: Dict[str, int] = {}
        for row in rows:
            norm_name = normalize_key(row['fullname_raw'])
            if norm_name in unverified_map:
                logging.warning(f"Normalized name collision for unverified player: {norm_name}")
                # For now, overwrite; if common, change to List[int] and resolve like verified
            unverified_map[norm_name] = row['player_id']
        return unverified_map

    def save_to_db(
            self, 
            cursor, 
            player_id_ext: Optional[str] = None, 
            data_source_id: Optional[int] = None
        ) -> dict:
        self.sanitize()

        # For non-verified (raw) players, require fullname_raw; for verified, require firstname and lastname
        if self.is_verified:
            required_fields = [self.firstname, self.lastname]
            if not all(required_fields):
                return {
                    "status": "failed",
                    "player": f"{self.firstname or ''} {self.lastname or ''}",
                    "reason": "Missing required fields for verified player (firstname and lastname)"
                }
        else:
            if not self.fullname_raw:
                return {
                    "status": "failed",
                    "player": self.fullname_raw or "Unknown",
                    "reason": "Missing required field for non-verified player (fullname_raw)"
                }
            # Optionally derive firstname/lastname from fullname_raw if desired, but avoid unreliable parsing
            # For now, allow NULL/None for firstname/lastname in raw cases

        # Additional type validation to prevent binding errors
        if not isinstance(self.fullname_raw, (str, type(None))):
            logging.error(f"Invalid fullname_raw type: {type(self.fullname_raw)}, value: {self.fullname_raw}")
            return {
                "status": "failed",
                "player": f"{self.firstname or ''} {self.lastname or ''}",
                "reason": f"Invalid type for fullname_raw: expected str or None, got {type(self.fullname_raw)}"
            }
        
        if not isinstance(self.year_born, (int, type(None))):
            logging.error(f"Invalid year_born type: {type(self.year_born)}, value: {self.year_born}")
            return {
                "status": "failed",
                "player": f"{self.firstname or ''} {self.lastname or ''}",
                "reason": f"Invalid type for year_born: expected int or None, got {type(self.year_born)}"
            }

        if not isinstance(self.date_born, (str, type(None))):
            logging.error(f"Invalid date_born type: {type(self.date_born)}, value: {self.date_born}")
            return {
                "status": "failed",
                "player": f"{self.firstname or ''} {self.lastname or ''}",
                "reason": f"Invalid type for date_born: expected str or None, got {type(self.date_born)}"
            }

        try:
            if player_id_ext is not None:
                if data_source_id is None:
                    return {
                        "status": "failed",
                        "player": f"{self.firstname} {self.lastname}",
                        "reason": "data_source_id required when player_id_ext is provided"
                    }
                # Check if ext_id already exists
                cursor.execute(
                    "SELECT player_id FROM player_id_ext WHERE player_id_ext = ? AND data_source_id = ?",
                    (player_id_ext, data_source_id)
                )
                existing = cursor.fetchone()
                if existing:
                    return {
                        "status": "skipped",
                        "player": f"{self.firstname} {self.lastname}",
                        "reason": "Player ext_id already exists",
                        "player_id": existing[0]
                    }

            # Insert new player (allow NULL for firstname/lastname if raw)
            cursor.execute("""
                INSERT INTO player (firstname, lastname, year_born, date_born, fullname_raw, is_verified)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (self.firstname or None, self.lastname or None, self.year_born, self.date_born, self.fullname_raw, self.is_verified))
            self.player_id = cursor.lastrowid

            if player_id_ext is not None:
                cursor.execute("""
                    INSERT INTO player_id_ext (player_id, player_id_ext, data_source_id)
                    VALUES (?, ?, ?)
                """, (self.player_id, player_id_ext, data_source_id))

            return {
                "status": "success",
                "player": self.fullname_raw or f"{self.firstname} {self.lastname}",
                "reason": "Inserted new player",
                "player_id": self.player_id
            }

        except Exception as e:
            return {
                "status": "failed",
                "player": self.fullname_raw or f"{self.firstname} {self.lastname}",
                "reason": f"Insertion error: {e}"
            }

    @staticmethod
    def cache_all(
        cursor
    ) -> Dict[int, 'Player']:
        """Load all Player rows into a map: player_id → Player instance."""
        cursor.execute("SELECT player_id, firstname, lastname, year_born, date_born, fullname_raw, is_verified FROM player")
        players: Dict[int, Player] = {}
        for row in cursor.fetchall():
            p = Player(
                player_id=row[0],
                firstname=row[1],
                lastname=row[2],
                year_born=row[3],
                date_born=row[4],
                fullname_raw=row[5],
                is_verified=bool(row[6]),
                ext_ids=[]
            )
            players[p.player_id] = p
        # fetch ext_ids
        cursor.execute(
            "SELECT player_id, player_id_ext, data_source_id FROM player_id_ext"
        )
        for pid, pid_ext, ds_id in cursor.fetchall():
            if pid in players:
                players[pid].ext_ids.append({
                    'player_id_ext': pid_ext,
                    'data_source_id': ds_id
                })
            else:
                logging.warning(f"Ext_id for unknown player_id {pid}: ext={pid_ext}")
        logging.info(f"Loaded {len(players)} players with ext_ids")
        return players

    @staticmethod
    def cache_name_year_map(
        cursor
    ) -> Dict[Tuple[str, str, int], List['Player']]:
        """
        Build a map from (firstname, lastname, year_born) to
        a list of Player objects, deduplicated by player_id.
        Uses fullname_raw if firstname/lastname are NULL.
        """
        # 1) Load all Player objects into a dict by player_id
        players_by_id = Player.cache_all(cursor)

        # 2) Query rows from player
        cursor.execute("""
            SELECT player_id, COALESCE(firstname, ''), COALESCE(lastname, ''), year_born, fullname_raw
            FROM player
        """)
        rows = cursor.fetchall()

        # 3) Build intermediate mapping: key → { player_id: Player, ... }
        temp_map: Dict[Tuple[str,str,int], Dict[int, 'Player']] = defaultdict(dict)
        for pid, fn, ln, yb, fr in rows:
            if pid not in players_by_id:
                logging.warning(f"Row references missing player_id {pid}")
                continue

            # If no first/last, use a key based on fullname_raw
            if not fn and not ln and fr:
                key = (sanitize_name(fr), '', yb if yb is not None else 0)
            else:
                key = (
                    sanitize_name(fn or ""),
                    sanitize_name(ln or ""),
                    yb if yb is not None else 0
                )
            # store or overwrite by pid, ensuring one entry per player_id
            temp_map[key][pid] = players_by_id[pid]

        # 4) Flatten to the final shape: key → List[Player]
        name_year_map: Dict[Tuple[str,str,int], List['Player']] = {
            key: list(pid_map.values())
            for key, pid_map in temp_map.items()
        }

        logging.info(f"Cached {len(name_year_map)} name/year keys")
        return name_year_map
    
    @staticmethod
    def cache_id_ext_map(
        cursor
    ) -> Dict[Tuple[str, int], "Player"]:
        """
        Build a mapping from (player_id_ext, data_source_id) to Player objects.
        Uses cache_all() to load players with their ext_ids, then iterates
        over each player’s ext_ids and inserts an entry for each unique (player_id_ext, data_source_id).
        Skips NULL externals.
        """
        try:
            players = Player.cache_all(cursor)
            id_ext_map: Dict[Tuple[str, int], Player] = {}
            for player in players.values():
                for ext in player.ext_ids:
                    pid_ext = ext.get('player_id_ext')
                    ds_id = ext.get('data_source_id')
                    if pid_ext is not None and ds_id is not None:
                        id_ext_map[(pid_ext, ds_id)] = player
            return id_ext_map
        except Exception as e:
            logging.error(f"Error building cache_id_ext_map: {e}")
            return {}

    @staticmethod
    def search_by_name_and_year(
        cursor, 
        firstname: str, 
        lastname: str, 
        year_born: Optional[int]
    ) -> List['Player']:
        """
        Fallback DB search on player table. 
        Handles cases where first/last are NULL by using fullname_raw.
        """
        try:
            fn = sanitize_name(firstname) if firstname else None
            ln = sanitize_name(lastname) if lastname else None
            yb = year_born if year_born is not None else None
            params = []
            where_clauses = []

            if fn:
                where_clauses.append("(firstname = ? OR (firstname IS NULL AND fullname_raw LIKE ?))")
                params.extend([fn, f"%{fn}%"])
            if ln:
                where_clauses.append("(lastname = ? OR (lastname IS NULL AND fullname_raw LIKE ?))")
                params.extend([ln, f"%{ln}%"])
            if yb is not None:
                where_clauses.append("year_born = ?")
                params.append(yb)
            else:
                where_clauses.append("(year_born IS NULL OR year_born = 0)")

            if not where_clauses:
                return []

            where_sql = " AND ".join(where_clauses)
            cursor.execute(
                f"""
                SELECT player_id, firstname, lastname, year_born, date_born, fullname_raw, is_verified
                FROM player
                WHERE {where_sql}
                """,
                params
            )
            found = []
            for row in cursor.fetchall():
                pid = row[0]
                p = Player(
                    player_id=pid,
                    firstname=row[1],
                    lastname=row[2],
                    year_born=row[3],
                    date_born=row[4],
                    fullname_raw=row[5],
                    is_verified=bool(row[6]),
                    ext_ids=[]
                )
                # collect ext_ids
                cursor.execute(
                    "SELECT player_id_ext, data_source_id FROM player_id_ext WHERE player_id = ?",
                    (pid,)
                )
                p.ext_ids = [
                    {'player_id_ext': r[0], 'data_source_id': r[1]}
                    for r in cursor.fetchall()
                ]
                found.append(p)
            return found
        except Exception as e:
            logging.error(f"Error in search_by_name_and_year: {e}")
            return []


    @staticmethod
    def get_by_id_ext(
        cursor, 
        player_id_ext: str,
        data_source_id: int
    ) -> Optional['Player']:
        try:
            # Get canonical player_id from player_id_ext
            cursor.execute("SELECT player_id FROM player_id_ext WHERE player_id_ext = ? AND data_source_id = ?", (player_id_ext, data_source_id))
            row = cursor.fetchone()
            if not row:
                return None
            return Player.get_by_id(cursor, row[0])
        except Exception as e:
            logging.error(f"Error retrieving player by player_id_ext {player_id_ext} and data_source_id {data_source_id}: {e}")
            return None

    @staticmethod
    def get_by_id(
        cursor, 
        player_id: int
    ) -> Optional['Player']:
        try:
            # Fetch canonical player
            cursor.execute("""
                SELECT player_id, firstname, lastname, year_born, date_born, fullname_raw, is_verified FROM player WHERE player_id = ?
            """, (player_id,))
            row = cursor.fetchone()
            if not row:
                return None

            # Fetch ext_ids
            cursor.execute("""
                SELECT player_id_ext, data_source_id FROM player_id_ext WHERE player_id = ?
            """, (player_id,))
            ext_ids = [
                {"player_id_ext": r[0], "data_source_id": r[1]}
                for r in cursor.fetchall()
            ]

            return Player(
                player_id=row[0],
                firstname=row[1],
                lastname=row[2],
                year_born=row[3],
                date_born=row[4],
                fullname_raw=row[5],
                is_verified=bool(row[6]),
                ext_ids=ext_ids
            )
        except Exception as e:
            logging.error(f"Error retrieving player by ID {player_id}: {e}")
            return None
        
        
    @staticmethod
    def insert_unverified(
        cursor,
        fullname_raw: str,
        year_born: Optional[int] = None,
        data_source_id: Optional[int] = None
    ) -> Optional[int]:
        """
        Insert a new unverified player using fullname_raw and return the new player_id.
        """
        player = Player(
            fullname_raw=fullname_raw,
            year_born=year_born,
            is_verified=False
        )
        res = player.save_to_db(
            cursor,
            player_id_ext=None,
            data_source_id=data_source_id
        )

        if res["status"] in ("success", "skipped") and "player_id" in res:
            return res["player_id"]
        return None