# src/models/match_side.py

from dataclasses import dataclass, fields
from typing import Dict, Any, Optional, Tuple
import sqlite3

@dataclass
class MatchSide:
    match_id:                       Optional[int] = None
    side_no:                        int = None
    represented_entry_id:           Optional[int] = None
    represented_league_team_id:     Optional[int] = None
    row_created:                    Optional[str] = None
    row_updated:                    Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {f.name: getattr(self, f.name) for f in fields(self)}

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "MatchSide":
        return cls(**{k: d.get(k) for k in {f.name for f in fields(cls)}})

    def validate(self) -> Tuple[bool, str]:
        if self.match_id is None or self.side_no not in (1, 2):
            return False, "Invalid match_id or side_no"
        if self.represented_entry_id is None:
            return False, "Missing represented_entry_id"
        return True, ""

    def insert(self, cursor: sqlite3.Cursor) -> None:
        sql = """
            INSERT INTO match_side (match_id, side_no, represented_entry_id, represented_league_team_id)
            VALUES (:match_id, :side_no, :represented_entry_id, :represented_league_team_id);
        """
        cursor.execute(sql, self.to_dict())