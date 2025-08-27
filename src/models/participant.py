# src/models/participant.py

from collections import defaultdict
from dataclasses import dataclass
from typing import DefaultDict, List, Optional, Dict, Any, Set, Tuple
import sqlite3
from models.cache_mixin import CacheMixin
from utils import name_keys_for_lookup_all_splits, normalize_key


@dataclass
class Participant(CacheMixin):
    participant_id:                     Optional[int] = None
    tournament_class_id:                Optional[int] = None
    tournament_class_seed:              Optional[int] = None
    tournament_class_final_position:    Optional[int] = None
    # Add later fields like league_id if needed

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "Participant":
        """Instantiate from a dict (keys matching column names)."""
        return Participant(
            participant_id                  = d.get("participant_id"),
            tournament_class_id             = d.get("tournament_class_id"),
            tournament_class_seed           = d.get("tournament_class_seed"),
            tournament_class_final_position = d.get("tournament_class_final_position"),
        )

    def validate(self
        ) -> Dict[str, str]:
        """
        Validate Participant fields, log to OperationLogger.
        Returns dict with status and reason.
        """
        if not self.tournament_class_id:
            return {
                "status": "failed", 
                "reason": "Missing required field: tournament_class_id"
            }

        # Add more validations as needed (e.g., seed/position ranges)

        return {"status": "success", "reason": "Validated OK"}

    def insert(
            self, 
            cursor
        ) -> Dict[str, str]:
        """
        Insert participant to DB, log results.
        Since we wipe old entries, no upsert needed - just insert.
        """
        query = """
            INSERT INTO participant (
                tournament_class_id, tournament_class_seed, tournament_class_final_position
            ) VALUES (?, ?, ?)
            RETURNING participant_id;
        """
        values = (
            self.tournament_class_id, self.tournament_class_seed, self.tournament_class_final_position
        )

        try:
            cursor.execute(query, values)
            row = cursor.fetchone()
            if row:
                self.participant_id = row[0]
                # logger.success(item_key, "Participant inserted successfully")
                return {
                    "status": "success",
                    "reason": "Participant inserted successfully"
                }
        except Exception as e:
            return {
                "status": "failed",
                "reason": f"Unexpected error during insert: {e}"
            }

    @classmethod
    def remove_for_class(
        cls, 
        cursor, 
        tournament_class_id: int
    ) -> int:
        cursor.execute(
            "DELETE FROM participant WHERE tournament_class_id = ?", 
            (tournament_class_id,)
        )
        # print(f"Removed {cursor.rowcount} participants for class {tournament_class_id}")
        return cursor.rowcount
    
    @classmethod
    def clear_final_positions(cls, cursor: sqlite3.Cursor, tournament_class_id: int) -> int:
        """
        Clear tournament_class_final_position for participants in a given tournament_class_id.

        Args:
            cursor: SQLite cursor for database queries.
            tournament_class_id: The tournament class ID to clear positions for.

        Returns:
            Number of rows updated (i.e., participants whose positions were cleared).
        """
        cursor.execute(
            """
            UPDATE participant
            SET tournament_class_final_position = NULL
            WHERE tournament_class_id = ?
            """,
            (tournament_class_id,)
        )
        return cursor.rowcount
    
    @classmethod
    def update_final_position(
        cls, 
        cursor: sqlite3.Cursor, 
        participant_id: 
        int, position: int
    ) -> Dict[str, str]:
        """
        Update tournament_class_final_position for a specific participant_id.

        Args:
            cursor: SQLite cursor for database queries.
            participant_id: The participant ID to update.
            position: The final position to set.

        Returns:
            Dict with 'status' and optional 'reason'.
        """
        try:
            sql = """
                UPDATE participant
                SET tournament_class_final_position = ?
                WHERE participant_id = ?
            """
            cursor.execute(sql, (position, participant_id))
            if cursor.rowcount == 1:
                return {"status": "success"}
            elif cursor.rowcount == 0:
                return {"status": "failed", "reason": "No matching participant for participant_id"}
            else:
                return {"status": "failed", "reason": "Multiple updates—data inconsistency"}
        except Exception as e:
            return {"status": "failed", "reason": str(e)}
        
    @classmethod
    def cache_by_class_player(
        cls, 
        cursor: sqlite3.Cursor
    ) -> Dict[int, Dict[int, Tuple[int, Optional[int]]]]:
        """Cache participant_id and club_id by class_id and player_id."""
        query = """
            SELECT p.tournament_class_id, pp.player_id, pp.participant_id, pp.club_id
            FROM participant p
            JOIN participant_player pp ON p.participant_id = pp.participant_id
        """
        rows = cls.cached_query(cursor, query, cache_key_extra="cache_by_class_player")
        result = {}
        for row in rows:
            class_id = row["tournament_class_id"]
            player_id = row["player_id"]
            if class_id not in result:
                result[class_id] = {}
            result[class_id][player_id] = (row["participant_id"], row.get("club_id"))
        return result

    @classmethod
    def build_class_roster_index(
        cls,
        cursor: sqlite3.Cursor,
        tournament_class_id: int
    ) -> Dict[str, Any]:
        """
        Build an in-memory index for fast lookups:
          - by_name_key:  normalized name-key -> list of roster entries
          - by_player_id: player_id -> roster entry
        A 'roster entry' is a dict with {participant_id, player_id, club_id, name_keys, club_keys}
        """
        sql = """
            SELECT 
                p.tournament_class_id,
                pp.participant_id,
                pp.player_id,
                pp.club_id,
                COALESCE(TRIM(pl.firstname || ' ' || pl.lastname), pl.fullname_raw) AS full_name,
                c.shortname AS club_short,
                c.longname  AS club_long
            FROM participant p
            JOIN participant_player pp ON pp.participant_id = p.participant_id
            JOIN player pl            ON pl.player_id = pp.player_id
            LEFT JOIN club  c         ON c.club_id     = pp.club_id
            WHERE p.tournament_class_id = ?
        """
        rows = cls.cached_query(cursor, sql, (tournament_class_id,), cache_key_extra=f"roster:{tournament_class_id}")

        by_name_key: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
        by_player_id: Dict[int, Dict[str, Any]] = {}
        roster_list: List[Dict[str, Any]] = []

        for r in rows:
            participant_id = r["participant_id"]
            player_id      = r["player_id"]
            club_id        = r["club_id"]
            full_name      = (r["full_name"] or "").strip()

            # Name keys (all splits + orderings)
            nkeys = name_keys_for_lookup_all_splits(full_name)

            # Club keys (normalized variants incl. None-safe)
            club_keys: Set[str] = set()
            for ck in (r.get("club_short"), r.get("club_long")):
                if ck:
                    club_keys.add(normalize_key(ck))

            entry = {
                "participant_id": participant_id,
                "player_id": player_id,
                "club_id": club_id,
                "name_keys": set(nkeys),
                "club_keys": club_keys,
            }
            roster_list.append(entry)
            by_player_id[player_id] = entry
            for nk in entry["name_keys"]:
                by_name_key[nk].append(entry)

        return {
            "by_name_key": by_name_key,
            "by_player_id": by_player_id,
            "roster_list": roster_list,
        }

    @classmethod
    def find_participant_for_class_by_name_club(
        cls,
        roster_index: Dict[str, Any],
        fullname: str,
        club_id: Optional[int],
        club_map,  # already built (Club.cache_name_map), not used here but kept for parity/extensibility
    ) -> Optional[int]:
        """
        Given a class roster index, a PDF 'fullname' and a resolved club_id,
        find the corresponding participant_id.

        Matching priority:
          1) name-key match + exact club_id match
          2) name-key match (unique) even if club doesn't match/unknown
        Returns participant_id or None if ambiguous/not found.
        """
        by_name_key = roster_index["by_name_key"]
        name_keys   = name_keys_for_lookup_all_splits(fullname)

        candidates: List[Dict[str, Any]] = []
        for nk in name_keys:
            for e in by_name_key.get(nk, []):
                candidates.append(e)

        if not candidates:
            return None

        # Prefer exact club_id matches when club_id is provided
        filtered = [e for e in candidates if club_id is not None and e["club_id"] == club_id]
        if len(filtered) == 1:
            return filtered[0]["participant_id"]
        if len(filtered) > 1:
            return None  # ambiguous with same club; better to skip

        # No club match or club unknown—fallback to unique name match
        # Deduplicate by participant_id
        uniq = {}
        for e in candidates:
            uniq[e["participant_id"]] = e
        if len(uniq) == 1:
            return next(iter(uniq.values()))["participant_id"]

        return None  # ambiguous
