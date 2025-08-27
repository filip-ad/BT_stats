# src/models/participant_player.py

from dataclasses import dataclass
from typing import Optional, Dict, Any
import sqlite3
from models.cache_mixin import CacheMixin

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
