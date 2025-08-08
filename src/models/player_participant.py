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
    
    @staticmethod
    def remove_for_class(cursor, tournament_class_id: int) -> int:
        """
        Delete all participants for a given tournament_class_id.
        Call this before re-inserting the up-to-date list.
        """
        cursor.execute(
            "DELETE FROM player_participant WHERE tournament_class_id = ?",
            (tournament_class_id,)
        )
        # Return the number of rows deleted
        return cursor.rowcount
    
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
        club      = club_map.get(norm_club)

        warnings = []

        if not club and len(norm_club) >= 5:
            # prefix fallback over all normalized variants
            prefix_keys = [k for k in club_map if k.startswith(norm_club)]
            if len(prefix_keys) == 1:
                club = club_map[prefix_keys[0]]
                warnings.append("Club name matched by prefix")
                all_aliases = [a["alias"] for a in club.aliases]
                logging.info(
                    f"Prefix‐matched “{self.club_name_raw}” → "
                    f"shortname={club.shortname!r}, aliases={all_aliases}"
                )

                # record this prefix match for later review
                cursor.execute("""
                    INSERT OR IGNORE INTO club_name_prefix_match (
                        tournament_class_id,
                        club_raw_name,
                        matched_club_id,
                        matched_club_aliases
                    ) VALUES (?, ?, ?, ?)
                """, (
                    self.tournament_class_id,
                    self.club_name_raw,
                    club.club_id,
                    ",".join(all_aliases)
                ))

            elif prefix_keys:
                # ambiguous — more than one candidate
                logging.warning(
                    f"Ambiguous prefix matches for “{self.club_name_raw}”: "
                    f"{[club_map[k].shortname for k in prefix_keys]}"
                )

        if not club:
            logging.warning(
                f"Club not found for “{self.club_name_raw}” (norm: {norm_club})"
            )
            cursor.execute("""
                INSERT OR IGNORE INTO club_missing
                    (club_name_raw, club_name_norm)
                VALUES (?, ?)
            """, (self.club_name_raw, norm_club))
            return {
                "status": "failed",
                "key":    f"{self.tournament_class_id}_{self.fullname_raw}",
                "reason": "Club not found (tried exact + prefix)",
                "warnings": warnings
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
            logging.debug(
                f"QA: unique-name match for '{self.fullname_raw}' → pid={pid}, "
                f"{'HAS' if has_license else 'NO'} license at club_id={club_id} on {class_date}"
            )
            if not has_license:
                warnings.append(f"Player did not have a valid license in the club on the day of the tournament")
                logging.debug(
                    f"Player {self.fullname_raw} (pid={pid}) does not have a valid license at {club.shortname!r} (club_id={club_id}) on {class_date}"
                )


        elif len(pids) > 1:
            # First, try licenses valid on class_date
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
                return {
                    "status":   "failed",
                    "key":      f"{self.tournament_class_id}_{clean_name}",
                    "reason":   "Ambiguous player candidates with valid licenses",
                    "warnings": warnings
                }

            # Fallback: allow any previous license for this club
            if len(licensed) == 0:
                cursor.execute(f"""
                    SELECT DISTINCT player_id
                    FROM player_license
                    WHERE player_id IN ({placeholders})
                    AND club_id = ?
                """, [*pids, club_id])
                any_licensed = [row[0] for row in cursor.fetchall()]

                if len(any_licensed) == 1:
                    pid = any_licensed[0]
                    match_type = "expired license"
                    warnings.append(
                        "Matched via prior season license (expired before class_date)"
                    )
                elif len(any_licensed) > 1:
                    return {
                        "status":   "failed",
                        "key":      f"{self.tournament_class_id}_{clean_name}",
                        "reason":   "Ambiguous player candidates with expired licenses",
                        "warnings": warnings
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
                        "status":       "failed",
                        "key":          f"{self.tournament_class_id}_{self.fullname_raw}",
                        "reason":       "Ambiguous transition matches",
                        "match_type":   "transition",
                        "warnings":     warnings
                    }
    
        # try “any‐season licence + name substring” for tricky reversed names
        if pid is None:
            parts = clean_name.split()  # e.g. ["tidblom","niclas"]
            # build a loose LIKE match against firstname or lastname
            cursor.execute("""
                SELECT DISTINCT p.player_id
                FROM player p
                JOIN player_license pl ON p.player_id = pl.player_id
                WHERE pl.club_id = ?
                AND (
                        lower(p.firstname) LIKE ?
                    OR lower(p.lastname)  LIKE ?
                )
            """, (
                club_id,
                f"%{parts[-1]}%",   # match e.g. "niclas"
                f"%{parts[0]}%"     # match e.g. "tidblom"
            ))
            name_lic_pids = [row[0] for row in cursor.fetchall()]

            if len(name_lic_pids) == 1:
                # we’ve found exactly one licence‐holder whose name contains the parts
                pid        = name_lic_pids[0]
                match_type = "license_name_fallback"
                warnings.append(
                    "Matched via any‐season licence + name substring"
                )                

        # --- branch C: no name matches → try any-season licence, then raw fallback ---
        if len(pids) == 0:
            # 1) Any-season licence for this club?
            cursor.execute("""
                SELECT DISTINCT player_id
                FROM player_license
                WHERE club_id = ?
            """, (club_id,))
            lic_any = [row[0] for row in cursor.fetchall()]

            if len(lic_any) == 1:
                # found exactly one player who’s ever had a licence here → treat as canonical
                pid        = lic_any[0]
                match_type = "license_fallback"
                warnings.append("Matched via any-season licence for club")

                # insert as canonical
                cursor.execute("""
                    INSERT OR IGNORE INTO player_participant
                    (tournament_class_id, player_id, player_id_raw, club_id)
                    VALUES (?, ?, NULL, ?)
                """, (self.tournament_class_id, pid, club_id))

                if cursor.rowcount == 1:
                    status = "success"
                    reason = "Participant added successfully (any-season licence match)"
                else:
                    status = "skipped"
                    reason = "Participant already exists (any-season licence match)"

                return {
                    "status":     status,
                    "key":        f"{self.tournament_class_id}_{pid}",
                    "reason":     reason,
                    "match_type": match_type,
                    "warnings":   warnings
                }

            # 2) Still no single licence → raw fallback
            clean_name   = " ".join(self.fullname_raw.strip().split())
            existing_raw = Player.search_by_name_raw(cursor, clean_name)
            if existing_raw is not None:
                raw_id     = existing_raw
                match_type = "raw_fallback_existing"
            else:
                raw_id     = Player.save_to_db_raw(cursor, clean_name)
                match_type = "raw_fallback_new"

            cursor.execute("""
                INSERT OR IGNORE INTO player_participant
                (tournament_class_id, player_id, player_id_raw, club_id)
                VALUES (?, NULL, ?, ?)
            """, (self.tournament_class_id, raw_id, club_id))

            if cursor.rowcount == 1:
                status = "success"
                reason = (
                    "Participant added successfully (new raw player)"
                    if match_type == "raw_fallback_new"
                    else "Participant added successfully (existing raw player)"
                )
            else:
                status = "skipped"
                reason = "Participant already exists (raw player)"

            return {
                "status":     status,
                "key":        f"raw_{raw_id}",
                "reason":     reason,
                "match_type": match_type,
                "warnings":   warnings
            }        

        # --- branch A/B final insert for canonical pid ---
        cursor.execute("""
            INSERT OR IGNORE INTO player_participant
                (tournament_class_id, player_id, player_id_raw, club_id)
            VALUES (?, ?, NULL, ?)
        """, (
            self.tournament_class_id,
            pid,
            club_id
        ))
        if cursor.rowcount == 1:
            return {
                "status":       "success",
                "key":          f"{self.tournament_class_id}_{pid}",
                "reason":       "Participant added successfully (matching canonical player)",
                "match_type":   match_type,
                "warnings":     warnings
            }
        else:
            return {
                "status":       "skipped",
                "key":          f"{self.tournament_class_id}_{pid}",
                "reason":       "Participant already exists (matching canonical player)",
                "match_type":   match_type,
                "warnings":     warnings
            }

