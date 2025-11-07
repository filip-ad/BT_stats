# src/models/match_player.py

from dataclasses import dataclass, fields
from typing import Dict, Any, Optional, Tuple
import sqlite3

@dataclass
class MatchPlayer:
    match_id:           Optional[int] = None
    side_no:            int = None
    player_id:          int = None
    player_order:       int = None
    club_id:            int = None
    row_created:        Optional[str] = None
    row_updated:        Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {f.name: getattr(self, f.name) for f in fields(self)}

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "MatchPlayer":
        return cls(**{k: d.get(k) for k in {f.name for f in fields(cls)}})

    def validate(self) -> Tuple[bool, str]:
        if self.match_id is None or self.side_no not in (1, 2) or self.player_id is None or self.club_id is None:
            return False, "Missing required fields"
        return True, ""

    def insert(self, cursor: sqlite3.Cursor) -> None:
        sql = """
            INSERT INTO match_player (match_id, side_no, player_id, player_order, club_id)
            VALUES (:match_id, :side_no, :player_id, :player_order, :club_id);
        """
        cursor.execute(sql, self.to_dict())