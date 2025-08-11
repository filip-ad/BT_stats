# src/models/player.py

from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple
import logging
from collections import defaultdict

from utils import normalize_key, sanitize_name

@dataclass
class Player:
    player_id:      Optional[int]   = None
    firstname:      Optional[str]   = None
    lastname:       Optional[str]   = None
    year_born:      Optional[int]   = None
    fullname_raw:   Optional[str]   = None
    is_verified:    bool            = False
    aliases:        List[dict]      = None  # player_id -> [{player_id_ext, firstname, lastname, year_born}]

    def __post_init__(self):
        '''
        Initialize player aliases.
        '''
        if self.aliases is None:
            self.aliases = []

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
        '''
        return Player(
            player_id=data.get("player_id"),
            firstname=data.get("firstname"),
            lastname=data.get("lastname"),
            year_born=data.get("year_born"),
            fullname_raw=data.get("fullname_raw"),
            is_verified=data.get("is_verified", False),
            aliases=data.get("aliases", [])
        )
    
    def save_to_db(
            self, 
            cursor, 
            player_id_ext: Optional[int] = None, 
            source_system: Optional[str] = None
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

        try:
            if player_id_ext is not None:
                # Check if alias already exists (for licensed with external)
                cursor.execute("SELECT player_id FROM player_alias WHERE player_id_ext = ?", (player_id_ext,))
                existing = cursor.fetchone()
                if existing:
                    # logging.warning(f"Skipping duplicate player alias: {self.firstname} {self.lastname} ({player_id_ext})")
                    return {
                        "status": "skipped",
                        "player": f"{self.firstname} {self.lastname}",
                        "reason": "Player alias already exists"
                    }
                player_id_ext_val = player_id_ext
            else:
                # For raw players, allow NULL external
                player_id_ext_val = None

            # Insert new canonical player (allow NULL for firstname/lastname if raw)
            cursor.execute("""
                INSERT INTO player (firstname, lastname, year_born, fullname_raw, is_verified)
                VALUES (?, ?, ?, ?, ?)
            """, (self.firstname or None, self.lastname or None, self.year_born, self.fullname_raw, self.is_verified))
            self.player_id = cursor.lastrowid

            # Insert alias (external optional; use canonical values where possible)
            alias_first = self.firstname or None
            alias_last = self.lastname or None
            alias_full = self.fullname_raw or ""
            cursor.execute("""
                INSERT INTO player_alias (player_id, player_id_ext, firstname, lastname, year_born, fullname_raw, source_system)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (self.player_id, player_id_ext_val, alias_first, alias_last, self.year_born, alias_full, source_system))

            return {
                "status": "success",
                "player": self.fullname_raw or f"{self.firstname} {self.lastname}",
                "reason": "Inserted new canonical player and alias"
            }

        except Exception as e:
            return {
                "status": "failed",
                "player": self.fullname_raw or f"{self.firstname} {self.lastname}",
                "reason": f"Insertion error: {e}"
            }

    def add_alias(self,
                  cursor,
                  player_id_ext: Optional[int],
                  firstname: str,
                  lastname: str,
                  year_born: Optional[int],
                  fullname_raw: Optional[str],
                  source_system: Optional[str] = None) -> dict:
        """
        Record an extra external-ID alias for this player. Allows NULL external for name variations.
        """
        try:
            sanitized_first = sanitize_name(firstname)  if firstname else None
            sanitized_last  = sanitize_name(lastname)   if lastname else None
            # fullname_raw remains unsanitized

            cursor.execute(
                """
                INSERT OR IGNORE INTO player_alias
                  (player_id, player_id_ext, firstname, lastname, year_born, fullname_raw, source_system)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    self.player_id,
                    player_id_ext,
                    sanitized_first,
                    sanitized_last,
                    year_born,
                    fullname_raw or "",
                    source_system or ""
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

    @staticmethod
    def cache_all(
        cursor
    ) -> Dict[int, 'Player']:
        """Load all Player rows into a map: player_id → Player instance."""
        cursor.execute("SELECT player_id, firstname, lastname, year_born, fullname_raw, is_verified FROM player")
        players: Dict[int, Player] = {}
        for pid, fn, ln, yb, fr, iv in cursor.fetchall():
            p = Player(pid, fn, ln, yb, fr, iv, [])
            players[pid] = p
        # fetch aliases and attach
        cursor.execute(
            "SELECT player_id, player_id_ext, firstname, lastname, year_born, fullname_raw, source_system FROM player_alias"
        )
        for pid, pid_ext, fn, ln, yb, fr, ss in cursor.fetchall():
            if pid in players:
                players[pid].aliases.append({
                    'player_id_ext': pid_ext,
                    'firstname': fn,
                    'lastname': ln,
                    'year_born': yb,
                    'fullname_raw': fr,
                    'source_system': ss
                })
            else:
                logging.warning(f"Alias for unknown player_id {pid}: ext={pid_ext}")
        logging.info(f"Loaded {len(players)} players with aliases")
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

        # 2) Query DISTINCT alias rows
        cursor.execute("""
            SELECT DISTINCT COALESCE(firstname, ''), COALESCE(lastname, ''), year_born, player_id, fullname_raw
              FROM player_alias
        """)
        rows = cursor.fetchall()

        # 3) Build intermediate mapping: key → { player_id: Player, ... }
        temp_map: Dict[Tuple[str,str,int], Dict[int, 'Player']] = defaultdict(dict)
        for fn, ln, yb, pid, fr in rows:
            if pid not in players_by_id:
                logging.warning(f"Alias references missing player_id {pid}")
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
    def cache_name_map(
        cursor
    ) -> Dict[str, List[int]]:
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
    def cache_id_ext_map(
        cursor
    ) -> Dict[int, "Player"]:
        """
        Build a mapping from external player IDs (player_id_ext) to Player objects.
        Uses cache_all() to load players with their aliases, then iterates
        over each player’s aliases and inserts an entry for each player_id_ext.
        Skips NULL externals.
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
        
    # @staticmethod
    # def cache_unverified_name_map(cursor) -> Dict[str, int]:
    #     """
    #     Build a map from normalized fullname_raw → player_id for unverified players.
    #     Only includes players where is_verified = FALSE and fullname_raw is not NULL/empty.
    #     """
    #     unverified_map: Dict[str, int] = {}
    #     cursor.execute("""
    #         SELECT player_id, fullname_raw 
    #         FROM player 
    #         WHERE is_verified = FALSE AND fullname_raw IS NOT NULL AND fullname_raw != ''
    #     """)
    #     for pid, fr in cursor.fetchall():
    #         clean = " ".join(fr.strip().split())  # Clean as in fallback_unverified
    #         key = normalize_key(clean)  # Or just use clean if no normalize needed
    #         if key not in unverified_map:  # Avoid duplicates, though unlikely
    #             unverified_map[key] = pid
    #     logging.info(f"Cached {len(unverified_map)} unverified player names")
    #     return unverified_map        

    @staticmethod
    def cache_unverified_name_map(cursor) -> Dict[str, int]:
        """
        Build a map from cleaned fullname_raw → player_id for unverified players.
        Only includes players where is_verified = FALSE and fullname_raw is not NULL/empty.
        Cleaning removes extra spaces but preserves case/accents for "rawness."
        """
        unverified_map: Dict[str, int] = {}
        cursor.execute("""
            SELECT player_id, fullname_raw 
            FROM player 
            WHERE is_verified = FALSE AND fullname_raw IS NOT NULL AND fullname_raw != ''
        """)
        for pid, fr in cursor.fetchall():
            clean = " ".join(fr.strip().split())  # Minimal clean: trim extra spaces
            if clean not in unverified_map:  # Avoid duplicates
                unverified_map[clean] = pid
        logging.info(f"Cached {len(unverified_map)} unverified player names")
        return unverified_map       
    
    @staticmethod
    def search_by_name_and_year(
        cursor, 
        firstname: str, 
        lastname: str, 
        year_born: Optional[int]
    ) -> List['Player']:
        """
        Fallback DB search on player & player_alias. 
        Handles cases where first/last are NULL by using fullname_raw.
        """
        try:
            fn = sanitize_name(firstname) if firstname else None
            ln = sanitize_name(lastname) if lastname else None
            yb = year_born if year_born is not None else None
            params = []
            where_clauses = []

            if fn:
                where_clauses.append("(pa.firstname = ? OR (pa.firstname IS NULL AND pa.fullname_raw LIKE ?))")
                params.extend([fn, f"%{fn}%"])
            if ln:
                where_clauses.append("(pa.lastname = ? OR (pa.lastname IS NULL AND pa.fullname_raw LIKE ?))")
                params.extend([ln, f"%{ln}%"])
            if yb is not None:
                where_clauses.append("pa.year_born = ?")
                params.append(yb)
            else:
                where_clauses.append("(pa.year_born IS NULL OR pa.year_born = 0)")

            if not where_clauses:
                return []

            where_sql = " AND ".join(where_clauses)
            cursor.execute(
                f"""
                SELECT DISTINCT p.player_id, p.firstname, p.lastname, p.year_born, p.fullname_raw, p.is_verified
                FROM player p
                JOIN player_alias pa ON p.player_id = pa.player_id
                WHERE {where_sql}
                """,
                params
            )
            found = []
            for pid, fn2, ln2, yb2, fr2, iv2 in cursor.fetchall():
                # collect aliases
                cursor.execute(
                    "SELECT player_id_ext, firstname, lastname, year_born, fullname_raw, source_system FROM player_alias WHERE player_id = ?",
                    (pid,)
                )
                aliases = [
                    {'player_id_ext': r[0], 'firstname': r[1], 'lastname': r[2], 'year_born': r[3], 'fullname_raw': r[4], 'source_system': r[5]}
                    for r in cursor.fetchall()
                ]
                found.append(Player(pid, fn2, ln2, yb2, fr2, iv2, aliases))
            return found
        except Exception as e:
            logging.error(f"Error in search_by_name_and_year: {e}")
            return []


    @staticmethod
    def get_by_id_ext(
        cursor, 
        player_id_ext: int
    ) -> Optional['Player']:
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
    def get_by_id(
        cursor, 
        player_id: int
    ) -> Optional['Player']:
        try:
            # Fetch canonical player
            cursor.execute("""
                SELECT player_id, firstname, lastname, year_born, fullname_raw, is_verified FROM player WHERE player_id = ?
            """, (player_id,))
            row = cursor.fetchone()
            if not row:
                return None

            # Fetch aliases
            cursor.execute("""
                SELECT player_id_ext, firstname, lastname, year_born, fullname_raw, source_system FROM player_alias WHERE player_id = ?
            """, (player_id,))
            aliases = [
                {"player_id_ext": r[0], "firstname": r[1], "lastname": r[2], "year_born": r[3], "fullname_raw": r[4], "source_system": r[5]}
                for r in cursor.fetchall()
            ]

            return Player(
                player_id=row[0],
                firstname=row[1],
                lastname=row[2],
                year_born=row[3],
                fullname_raw=row[4],
                is_verified=bool(row[5]),
                aliases=aliases
            )
        except Exception as e:
            logging.error(f"Error retrieving player by ID {player_id}: {e}")
            return None
        
    def update_verification(
            self, 
            cursor, 
            is_verified: bool = True
        ):
        """Update the is_verified flag for this player."""
        try:
            cursor.execute(
                "UPDATE player SET is_verified = ? WHERE player_id = ?",
                (is_verified, self.player_id)
            )
            self.is_verified = is_verified
            return {"status": "success", "reason": "Verification updated"}
        except Exception as e:
            logging.error(f"Error updating verification for player {self.player_id}: {e}")
            return {
                "status": "failed", 
                "reason": f"Update error: {e}"
                }

    def merge_with(
            self, 
            cursor, 
            other_player_id: int
        ):
        """
        Merge another player into this one: Transfer aliases, update FKs in other tables (e.g., participants, matches),
        then delete the other player.
        Assumes you handle FK updates in related tables (e.g., player_participants, matches).
        """
        try:
            # Transfer aliases
            cursor.execute(
                "UPDATE player_alias SET player_id = ? WHERE player_id = ?",
                (self.player_id, other_player_id)
            )

            # Update example related tables (adapt to your schema)
            # cursor.execute("UPDATE player_participants SET player_id = ? WHERE player_id = ?", (self.player_id, other_player_id))
            # cursor.execute("UPDATE matches SET player1_id = ? WHERE player1_id = ?", (self.player_id, other_player_id))
            # cursor.execute("UPDATE matches SET player2_id = ? WHERE player2_id = ?", (self.player_id, other_player_id))
            # cursor.execute("UPDATE matches SET winner_id = ? WHERE winner_id = ?", (self.player_id, other_player_id))

            # Delete the old player
            cursor.execute("DELETE FROM player WHERE player_id = ?", (other_player_id,))

            # Reload aliases for this player
            self.aliases = [
                {"player_id_ext": r[0], "firstname": r[1], "lastname": r[2], "year_born": r[3], "fullname_raw": r[4], "source_system": r[5]}
                for r in cursor.execute(
                    "SELECT player_id_ext, firstname, lastname, year_born, fullname_raw, source_system FROM player_alias WHERE player_id = ?",
                    (self.player_id,)
                ).fetchall()
            ]

            return {"status": "success", "reason": f"Merged player {other_player_id} into {self.player_id}"}
        except Exception as e:
            logging.error(f"Error merging player {other_player_id} into {self.player_id}: {e}")
            return {"status": "failed", "reason": f"Merge error: {e}"}