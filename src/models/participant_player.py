# src/models/participant_player.py

from ast import List, Tuple
from collections import defaultdict
from dataclasses import dataclass
import logging
from typing import Optional, Dict, Any
import sqlite3
from models.cache_mixin import CacheMixin
from models.club import Club
from utils import name_keys_for_lookup_all_splits

@dataclass
class ParticipantPlayer(CacheMixin):
    participant_player_id:          Optional[int] = None
    participant_player_id_ext:      Optional[str] = None
    participant_id:                 int = None
    player_id:                      int = None
    club_id:                        Optional[int] = None

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "ParticipantPlayer":
        """Instantiate from a dict (keys matching column names)."""

        def _as_int(v):
            if isinstance(v, int) or v is None:
                return v
            if isinstance(v, str) and v.strip().isdigit():
                return int(v.strip())
            if isinstance(v, dict):
                # tolerate dicts like {"player_id": 123}
                for k in ("player_id", "id", "rowid", "new_id", "lastrowid"):
                    if k in v and isinstance(v[k], int):
                        return v[k]
            return v  # let validate() catch anything else

        return ParticipantPlayer(
            participant_player_id     = _as_int(d.get("participant_player_id")),
            participant_player_id_ext = d.get("participant_player_id_ext"),
            participant_id            = _as_int(d["participant_id"]),
            player_id                 = _as_int(d["player_id"]),
            club_id                   = _as_int(d.get("club_id")),
        )

    def validate(
            self
        ) -> Dict[str, str]:
        """
        Validate ParticipantPlayer fields, log to OperationLogger.
        Returns dict with status and reason.
        """
        if not (self.participant_id and self.player_id):
            reason = "Missing required fields: participant_id or player_id"
            return {
                "status": "failed", 
                "reason": reason
            }

        return {
            "status": "success", 
            "reason": "Validated OK"
        }

    def insert(
            self, 
            cursor
        ) -> Dict[str, str]:
        sql = """
            INSERT INTO participant_player (
                participant_player_id_ext, participant_id, player_id, club_id
            ) VALUES (?, ?, ?, ?)
            RETURNING participant_player_id;
        """
        vals = (self.participant_player_id_ext, self.participant_id, self.player_id, self.club_id)
        try:
            cursor.execute(sql, vals)
            self.participant_player_id = cursor.fetchone()[0]
            return {
                "status": "success",
                "reason": "Participating player inserted successfully"
            }
        except sqlite3.IntegrityError as e:
            return {
                "status": "failed",
                "reason": f"Participating player insert failed: {e}"
            }


# Add to models/participant_player.py (or participant.py if more appropriate; assuming ParticipantPlayer since it links player/club)

@classmethod
def find_participant_id_by_name_club(
    cls,
    cursor,
    tournament_class_id: int,
    fullname_raw: str,
    clubname_raw: str,
    club_map: Dict[str, Club],
    cache_key_extra: Optional[str] = None  # For cached_query
) -> Optional[int]:
    """
    Find participant_id for a given tournament_class_id by matching normalized fullname and club.
    Uses name_keys_for_lookup_all_splits for variations, resolves club_id.
    Returns participant_id if unique match, None otherwise.
    """
    # Resolve club
    club = Club.resolve(cursor, clubname_raw, club_map, logger=None, item_key="", allow_prefix=True)  # Logger optional or pass if needed
    if not club:
        return None
    club_id = club.club_id

    # Get all participants for class (cached)
    sql = """
        SELECT pp.participant_id, p.fullname_raw, pp.club_id
        FROM participant_player pp
        JOIN participant part ON pp.participant_id = part.participant_id
        JOIN player p ON pp.player_id = p.player_id
        WHERE part.tournament_class_id = ?
    """
    rows = cls.cached_query(cursor, sql, (tournament_class_id,), cache_key_extra)
    class_participants = [dict(row) for row in rows]

    # Build in-memory map: normalized fullname keys -> participant_id (grouped by club_id)
    participant_map: Dict[Tuple[str, int], List[int]] = defaultdict(list)
    for entry in class_participants:
        fullname = entry['fullname_raw']
        if not fullname:
            continue
        keys = name_keys_for_lookup_all_splits(fullname)
        for k in keys:
            key = (k, entry['club_id'])
            participant_map[key].append(entry['participant_id'])

    # Lookup
    keys = name_keys_for_lookup_all_splits(fullname_raw)
    candidates = set()
    for k in keys:
        key = (k, club_id)
        if key in participant_map:
            candidates.update(participant_map[key])

    if len(candidates) == 1:
        return list(candidates)[0]
    elif len(candidates) > 1:
        logging.warning(f"Ambiguous match for fullname '{fullname_raw}' in club {clubname_raw} for class {tournament_class_id}")
        return None  # Or return first, but strict for now
    return None