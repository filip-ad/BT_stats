# src/models/player_participant.py

import logging
from typing import List, Dict, Optional, Tuple, Any
from dataclasses import dataclass
from datetime import date
import unicodedata
from db import get_conn
from models.player_license import PlayerLicense
from models.club import Club
from models.tournament_class import TournamentClass
from models.player import Player
from utils import normalize_key

@dataclass
class PlayerParticipant:
    tournament_class_id:    int
    fullname_raw:           str
    club_name_raw:          str

    # Filled by save_to_db
    player_id:              Optional[int] = None
    club_id:                Optional[int] = None


    @staticmethod
    def from_dict(data: dict) -> "PlayerParticipant":
        """
        Construct a PlayerParticipant from a raw dict with keys:
            - tournament_class_id
            - fullname_raw
            - club_name_raw
        """
        return PlayerParticipant(
            tournament_class_id = data["tournament_class_id"],
            fullname_raw        = data["fullname_raw"],
            club_name_raw       = data["club_name_raw"],
        )
    
    def save_to_db(
        self,
        cursor,
        class_date: date,
        club_map: Dict[str, Club],
        license_map: dict,
        player_name_map: Dict[str, List[int]]
    ) -> dict:
        """
        Lookup strategy:
        1) Normalize club_name_raw and lookup in club_map which holds cannonical and alias clubs.
        2) Normalize all parts of fullname_raw and create all conmbinations, then lookup in player_name_map.
        3) If exactly one match, use that player_id.
        4) If multiple matches, search for a player license for the club (and prefeably on class_date).
        5) If no valid license found, search the transition table for the name and club.
        6) If no matches, search the player_raw table for the fullname_raw and club_name_raw.
        7) If not found in player_raw, insert a new raw player entry.
        Returns {"status","key","reason"}.
        """

        # 1) Club lookup
        norm_club = Club._normalize(self.club_name_raw)
        club = club_map.get(norm_club)
        if not club:
            return {
                "status": "failed",
                "key":    f"{self.tournament_class_id}_{self.fullname_raw}",
                "reason": "Club not found"
            }
        club_id = club.club_id

        # 2) Normalize fullname_raw: strip diacritics + lowercase
        # Example: 'Harry Hamrén' -> 'harry hamren'
        clean_name = normalize_key(self.fullname_raw)

        # 3) Name-only lookup
        # pids = Player.find_by_name(cursor, clean_name)

       # 3) Name-only lookup via prebuilt map + split‐candidates
        parts = clean_name.split()
        candidates = [
           normalize_key(f"{fn} {ln}")
           for i in range(1, len(parts))
           for ln in [" ".join(parts[:i])]
           for fn in [" ".join(parts[i:])]
       ]
        matches = set()
        for key in candidates:
           matches.update(player_name_map.get(key, []))
        pids = list(matches)

        pid = None
        match_type = None

        # --- branch A: exactly one player match ---
        if len(pids) == 1:
            pid = pids[0]
            match_type = "unique name"
    
            # —— QA check: does this player actually hold a license for this club at class_date?
            cursor.execute("""
                SELECT 1
                FROM player_license
                WHERE player_id = ?
                AND club_id    = ?
                AND date(valid_from) <= date(?)
                AND date(valid_to)   >= date(?)
            """, (pid, club_id, class_date.isoformat(), class_date.isoformat()))
            has_license = bool(cursor.fetchone())
            logging.info(
                f"QA: unique-name match for '{self.fullname_raw}' → pid={pid}, "
                f"{'HAS' if has_license else 'NO'} license at club_id={club_id} on {class_date}"
            )

        # --- branch B: multiple name matches → disambiguate by license ---
        elif len(pids) > 1:
            # Build SQL to find which of these pids has a valid license on class_date
            placeholders = ",".join("?" for _ in pids)
            sql = f"""
                SELECT DISTINCT player_id
                  FROM player_license
                 WHERE player_id IN ({placeholders})
                   AND club_id = ?
                   AND date(valid_from) <= date(?)
                   AND date(valid_to)   >= date(?)
            """
            params = [*pids, club_id, class_date.isoformat(), class_date.isoformat()]
            cursor.execute(sql, params)         
            licensed = [row[0] for row in cursor.fetchall()]

            if len(licensed) == 1:
                pid = licensed[0]
                match_type = "license"

            elif len(licensed) > 1:
                # ambiguous license candidates → fatal error   
                 return {
                    "status": "failed",
                    "key":    f"{self.tournament_class_id}_{clean_name}",
                    "reason": "Ambiguous player candidates with valid licenses"
                } 
            
            # ——— Step 5: Transition lookup ———
            # If we still haven’t found a pid but have name‐candidates, try transfers
            if pid is None and pids:
                placeholders = ",".join("?" for _ in pids)
                sql = f"""
                    SELECT player_id
                    FROM player_transition
                    WHERE (club_id_to   = ? OR club_id_from = ?)
                    AND date(transition_date) <= date(?)
                    AND player_id IN ({placeholders})
                """
                params = [club_id, club_id,class_date.isoformat(), *pids]
                cursor.execute(sql, params)
                trans = [row[0] for row in cursor.fetchall()]

                if len(trans) == 1:
                    pid = trans[0]
                    match_type = "transition"
                    
                elif len(trans) > 1:
                    return {
                        "status": "failed",
                        "key":    f"{self.tournament_class_id}_{self.fullname_raw}",
                        "reason": f"Ambiguous transition matches",
                        "match_type": "transition"                    }
            # else: no transitions found → will fall through to raw‐fallback

        # --- branch C: no name matches → raw fallback ---
        else:
 
            # insert or ignore this raw‐player
            cursor.execute(
                "INSERT OR IGNORE INTO player_raw (fullname_raw, year_born, club_name_raw) VALUES (?, 0, ?)",
                (self.fullname_raw, self.club_name_raw)
            )
            if cursor.rowcount:
                raw_id = cursor.lastrowid
                return {
                    "status": "success",
                    "key":    f"raw_{raw_id}",
                    "reason": "New raw player added",
                    "match_type": "raw name"
                }

        # — Step 5: Raw‐fallback for any unresolved pid —
        if pid is None:
            # ensure raw table exists
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS player_raw (
                    row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fullname_raw TEXT NOT NULL,
                    year_born INTEGER DEFAULT 0,
                    club_name_raw TEXT NOT NULL,
                    row_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(fullname_raw, club_name_raw)
                )
            ''')

            # upsert raw‐player
            cursor.execute(
                "INSERT OR IGNORE INTO player_raw (fullname_raw, year_born, club_name_raw) VALUES (?, 0, ?)",
                (self.fullname_raw, self.club_name_raw)
            )
            if cursor.rowcount:
                raw_id = cursor.lastrowid
                return {
                    "status":   "success",
                    "key":      f"raw_{raw_id}",
                    "reason":   "New raw player added",
                    "match_type": "raw name"
                }
            else:
                cursor.execute(
                    "SELECT row_id FROM player_raw WHERE fullname_raw = ? AND club_name_raw = ?",
                    (self.fullname_raw, self.club_name_raw)
                )
                raw_id = cursor.fetchone()[0]
                return {
                    "status":   "skipped",
                    "key":      f"raw_{raw_id}",
                    "reason":   "Raw player already exists",
                    "match_type": "raw name"
                }

        # — Step 6: final insert, pid must now be set —

        # 4) If we reached here, we have a single pid → insert participant
        cursor.execute(
            """
            INSERT OR IGNORE INTO player_participant
              (tournament_class_id, player_id, club_id, fullname_raw, club_name_raw)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                self.tournament_class_id,
                pid,
                club_id,
                self.fullname_raw,
                self.club_name_raw
            )
        )
        if cursor.rowcount:
            return {
                "status": "success",
                "key":    f"{self.tournament_class_id}_{pid}",
                "reason": "Participant added successfully",
                "match_type": match_type
            }
        else:
            return {
                "status": "skipped",
                "key":    f"{self.tournament_class_id}_{pid}",
                "reason": "Participant already exists",
                "match_type": match_type
            }

    # — Suggestions for future enhancements:
    # • Split fullname_raw into surname vs. given‐names, check against player_alias table.
    # • If no license found on class_date, broaden search window ±n days or seasons.
    # • Apply fuzzy matching on normalized names (e.g. max edit‐distance) across all player_alias entries.