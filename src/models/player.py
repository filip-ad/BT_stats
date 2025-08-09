# src/models/player.py

from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple
import logging
from collections import defaultdict
from utils import sanitize_name, normalize_key

def sanitize_name(name: str) -> str:
    """Normalize names: strip, lower-case first letter, title."""
    return name.strip().title() if name else ''

@dataclass
class Player:
    player_id:      Optional[int]   = None
    firstname:      Optional[str]   = None
    lastname:       Optional[str]   = None
    year_born:      Optional[int]   = None
    aliases:        List[dict]      = None  # [{player_id_ext, firstname, lastname, year_born}]

    # raw‐only field
    player_id_raw:  Optional[int]   = None

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
    def cache_all(cursor) -> Dict[int, 'Player']:
        """Load all Player rows into a map: player_id → Player instance."""
        cursor.execute("SELECT player_id, firstname, lastname, year_born FROM player")
        players: Dict[int, Player] = {}
        for pid, fn, ln, yb in cursor.fetchall():
            p = Player(pid, fn, ln, yb, [])
            players[pid] = p
        # fetch aliases and attach
        cursor.execute(
            "SELECT player_id, player_id_ext, firstname, lastname, year_born FROM player_alias"
        )
        for pid, pid_ext, fn, ln, yb in cursor.fetchall():
            if pid in players:
                players[pid].aliases.append({
                    'player_id_ext': pid_ext,
                    'firstname': fn,
                    'lastname': ln,
                    'year_born': yb,
                })
            else:
                logging.warning(f"Alias for unknown player_id {pid}: ext={pid_ext}")
        logging.info(f"Loaded {len(players)} players with aliases")
        return players
    
    @staticmethod
    def cache_name_year_map(cursor) -> Dict[Tuple[str, str, int], List['Player']]:
        """
        Build a map from (firstname, lastname, year_born) to
        a list of Player objects, deduplicated by player_id.
        """
        # 1) Load all Player objects into a dict by player_id
        players_by_id = Player.cache_all(cursor)

        # 2) Query DISTINCT alias rows
        cursor.execute("""
            SELECT DISTINCT firstname, lastname, year_born, player_id
              FROM player_alias
        """)
        rows = cursor.fetchall()

        # 3) Build intermediate mapping: key → { player_id: Player, ... }
        temp_map: Dict[Tuple[str,str,int], Dict[int, 'Player']] = defaultdict(dict)
        for fn, ln, yb, pid in rows:
            if pid not in players_by_id:
                logging.warning(f"Alias references missing player_id {pid}")
                continue

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
    def cache_name_map(cursor) -> Dict[str, List[int]]:
        """
        Build a map from normalized "firstname lastname" → list of player_ids.
        Includes both canonical names and aliases.

        normalize_key("Hamrén Öberg") -> "hamren oberg"
        """
        name_map: Dict[str, List[int]] = defaultdict(list)

        # 1) Base names from player table
        cursor.execute("SELECT player_id, firstname, lastname FROM player")
        for pid, fn, ln in cursor.fetchall():
            full = f"{fn or ''} {ln or ''}".strip()
            key = normalize_key(full)
            name_map[key].append(pid)

        # 2) Add aliases
        cursor.execute("SELECT DISTINCT player_id, firstname, lastname FROM player_alias")
        for pid, fn, ln in cursor.fetchall():
            full = f"{fn or ''} {ln or ''}".strip()
            key = normalize_key(full)
            if pid not in name_map[key]:
                name_map[key].append(pid)

        logging.info(f"Cached {len(name_map)} unique name keys (players+aliases)")
        return name_map    
    
    @staticmethod
    def cache_id_ext_map(cursor) -> Dict[int, "Player"]:
        """
        Build a mapping from external player IDs (player_id_ext) to Player objects.
        Uses cache_all() to load players with their aliases, then iterates
        over each player’s aliases and inserts an entry for each player_id_ext.
        """
        try:
            players = Player.cache_all(cursor)
            id_ext_map: Dict[int, Player] = {}
            for player in players.values():
                for alias in player.aliases:
                    pid_ext = alias.get('player_id_ext')
                    if pid_ext is not None:
                        id_ext_map[pid_ext] = player
            return id_ext_map
        except Exception as e:
            logging.error(f"Error building cache_id_ext_map: {e}")
            return {}    
    
    @staticmethod
    def search_by_name_and_year(cursor, firstname: str, lastname: str, year_born: int) -> List['Player']:
        """Fallback DB search on player & player_alias."""
        try:
            fn = sanitize_name(firstname)
            ln = sanitize_name(lastname)
            yb = year_born if year_born is not None else 0
            cursor.execute(
                """
                SELECT DISTINCT p.player_id, p.firstname, p.lastname, p.year_born
                FROM player p
                JOIN player_alias pa ON p.player_id = pa.player_id
                WHERE pa.firstname = ? AND pa.lastname = ? AND pa.year_born = ?
                """,
                (fn, ln, yb)
            )
            found = []
            for pid, fn2, ln2, yb2 in cursor.fetchall():
                # collect aliases
                cursor.execute(
                    "SELECT player_id_ext, firstname, lastname, year_born FROM player_alias WHERE player_id = ?",
                    (pid,)
                )
                aliases = [
                    {'player_id_ext': r[0], 'firstname': r[1], 'lastname': r[2], 'year_born': r[3]}
                    for r in cursor.fetchall()
                ]
                found.append(Player(pid, fn2, ln2, yb2, aliases))
            return found
        except Exception as e:
            logging.error(f"Error in search_by_name_and_year: {e}")
            return []


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
                logging.warning(f"Skipping duplicate player alias: {self.firstname} {self.lastname} ({player_id_ext})")
                return {
                    "status": "skipped",
                    "player": f"{self.firstname} {self.lastname}",
                    "reason": "Player alias already exists"
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

    def add_alias(self,
                  cursor,
                  player_id_ext:int,
                  firstname:str,
                  lastname:str,
                  year_born:int) -> dict:
        """
        Record an extra external‐ID alias for this player.
        """

        self.sanitize()
        try:
            cursor.execute(
                """
                INSERT OR IGNORE INTO player_alias
                  (player_id, player_id_ext, firstname, lastname, year_born)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    self.player_id,
                    player_id_ext,
                    firstname.strip().title(),
                    lastname.strip().title(),
                    year_born or 0
                )
            )
            if cursor.rowcount == 1:
                return {
                    "status":        "success",
                    "player":        f"{self.firstname} {self.lastname}",
                    "player_id_ext": player_id_ext,
                    "reason":        "Alias added successfully"
                }
            else:
                return {
                    "status":        "skipped",
                    "player":        f"{self.firstname} {self.lastname}",
                    "player_id_ext": player_id_ext,
                    "reason":        "Alias already existed"
                }
            
        except Exception as e:
            logging.error(f"Error inserting alias {player_id_ext}→{self.player_id}: {e}")
            return {
                "status":        "failed",
                "player":        f"{self.firstname} {self.lastname}",
                "player_id_ext": player_id_ext,
                "reason":        f"Error inserting alias: {e}"
            }
        
    @classmethod
    def save_to_db_raw(cls, cursor, fullname_raw: str) -> int:
        """
        Ensure a raw‐player exists with this fullname.
        Returns its player_id_raw.

        Applies minimal whitespace normalization to match
        what search_by_name_raw is expecting.
        """
        # 1) Strip leading/trailing whitespace
        clean = fullname_raw.strip()
        # 2) Collapse any internal runs of whitespace to a single space
        clean = " ".join(clean.split())

        # 3) Lookup existing
        existing = cls.search_by_name_raw(cursor, clean)
        if existing is not None:
            return existing

        # 4) Insert the cleaned name
        sql = f"""
            INSERT INTO player_raw (fullname_raw)
            VALUES (?)
        """
        cursor.execute(sql, (clean,))
        return cursor.lastrowid
    
    @classmethod
    def search_by_name_raw(
        cls, 
        cursor, 
        fullname_raw: str,
        cache_raw_name_map=None
    ) -> Optional[int]:
        """
        Return player_id_raw if a raw-player with this fullname exists, using cache if provided.
        Falls back to substring match if not found.
        """
        clean_key = normalize_key(fullname_raw)

        # 1) Exact match via cache
        if cache_raw_name_map is not None:
            if clean_key in cache_raw_name_map:
                return cache_raw_name_map[clean_key][0]["player_id_raw"]

        # 2) Substring fallback in cache
        parts = clean_key.split()
        if cache_raw_name_map and len(parts) == 2:
            first_tok, last_tok = parts
            for cand_key, rows in cache_raw_name_map.items():
                cand_parts = cand_key.split()
                if len(cand_parts) >= 3 and first_tok in cand_key and last_tok in cand_key:
                    return rows[0]["player_id_raw"]

        return None
        # """
        # Return player_id_raw if a raw‐player with this fullname exists, else None.
        # We do minimal whitespace normalization here so that
        # leading/trailing spaces or double-spaces don’t break the match.
        # """
        # # 1) Strip leading/trailing whitespace...
        # clean = fullname_raw.strip()
        # # 2) Collapse any internal runs of whitespace to a single space
        # clean = " ".join(clean.split())

        # sql = """
        #     SELECT player_id_raw
        #       FROM player_raw
        #      WHERE fullname_raw = ?
        # """
        # cursor.execute(sql, (clean,))
        # row = cursor.fetchone()
        # return row[0] if row else None
    
    @staticmethod
    def cache_raw_name_map(cursor) -> Dict[str, int]:
        """
        Build a map from normalized fullname_raw → player_id_raw.
        """
        cursor.execute("SELECT player_id_raw, fullname_raw FROM player_raw")
        out: Dict[str, int] = {}
        for raw_id, fullname in cursor.fetchall():
            key = normalize_key(fullname)
            out[key] = raw_id
        logging.info(f"Cached {len(out)} raw player names")
        return out