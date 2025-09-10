# src/models/participant_player.py

from dataclasses import dataclass
from typing import Optional, Dict, Any, Tuple
import sqlite3
from models.cache_mixin import CacheMixin
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

    def validate(self) -> Tuple[bool, str]:
        """
        Validate fields.
        Returns: (is_valid, error_message)
        """
        missing = []
        if not self.participant_id:
            missing.append("participant_id")
        if not self.player_id:
            missing.append("player_id")

        if missing:
            return False, f"Missing/invalid fields: {', '.join(missing)}"

        return True, ""
    
    def upsert(self, cursor, participant_id: int, player_id: int) -> Dict[str, str]:
        """
        Upsert participant_player into DB, updating existing record if participant_id and player_id match.
        """
        query = """
            INSERT INTO participant_player (participant_player_id, participant_player_id_ext, participant_id, player_id, club_id)
            VALUES ((SELECT participant_player_id FROM participant_player WHERE participant_id = ? AND player_id = ?), ?, ?, ?, ?)
            ON CONFLICT(participant_id, player_id) DO UPDATE SET
                participant_player_id_ext = excluded.participant_player_id_ext,
                club_id = excluded.club_id
            RETURNING participant_player_id
        """
        values = (participant_id, player_id, self.participant_player_id_ext, participant_id, player_id, self.club_id)
        try:
            cursor.execute(query, values)
            row = cursor.fetchone()
            if row:
                self.participant_player_id = row[0]
                return {"status": "success", "reason": "ParticipantPlayer upserted successfully"}
        except Exception as e:
            return {"status": "failed", "reason": f"Unexpected error during upsert: {e}"}

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


    @classmethod
    def cache_by_class_name_fast(cls, cursor: sqlite3.Cursor) -> Dict[int, Dict[str, Any]]:
        """
        Build fast, in-memory indices per class:

        {
            class_id: {
            "by_code": { "077": participant_id, "77": participant_id, ... },
            "by_name_club": { (name_key, club_id): participant_id, ... },
            "by_name_only": { name_key: [participant_id, ...], ... },
            },
            ...
        }

            NOTE: `by_code` uses participant_player.participant_player_id_ext as the PDF “code”.
            If that field isn’t used in your data source, resolution gracefully falls back
            to name+club and name-only.
        """
        sql = """
            SELECT 
                p.tournament_class_id,
                pp.participant_id,
                pp.participant_player_id_ext AS code,
                pl.player_id,
                COALESCE(TRIM(pl.firstname || ' ' || pl.lastname), pl.fullname_raw) AS full_name,
                pp.club_id
            FROM participant p
            JOIN participant_player pp ON pp.participant_id = p.participant_id
            JOIN player pl            ON pl.player_id = pp.player_id
        """
        rows = cls.cached_query(cursor, sql, (), cache_key_extra="cache_by_class_name_fast")

        out: Dict[int, Dict[str, Any]] = {}
        for r in rows:
            class_id = r["tournament_class_id"]
            d = out.setdefault(class_id, {
                "by_code": {},
                "by_name_club": {},
                "by_name_only": {},
            })
            pid   = r["participant_id"]
            code  = (r.get("code") or "").strip()
            cid   = r.get("club_id")
            fname = (r.get("full_name") or "").strip()

            # by_code (store both raw and de-zero-left variant)
            if code:
                d["by_code"][code] = pid
                dezero = code.lstrip("0") or "0"
                d["by_code"].setdefault(dezero, pid)

            # names (all splits / order variants)
            for nk in name_keys_for_lookup_all_splits(fname):
                if cid:  # name+club
                    d["by_name_club"][(nk, cid)] = pid
                # name-only list
                lst = d["by_name_only"].setdefault(nk, [])
                if pid not in lst:
                    lst.append(pid)

        return out

