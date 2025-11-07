# src/models/match.py

from dataclasses import dataclass, fields
from typing import Dict, Any, Optional, Tuple
import sqlite3
from datetime import date

@dataclass
class Match:
    match_id:       Optional[int] = None
    best_of:        Optional[int] = None
    date:           Optional[date] = None
    status:         str = 'completed'
    winner_side:    Optional[int] = None
    walkover_side:  Optional[int] = None
    row_created:    Optional[str] = None
    row_updated:    Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {f.name: getattr(self, f.name) for f in fields(self) if f.name != 'match_id' or self.match_id is not None}

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Match":
        return cls(**{k: d.get(k) for k in {f.name for f in fields(cls)}})

    def validate(self) -> Tuple[bool, str]:
        missing = []
        if self.best_of is None:
            missing.append("best_of")
        if missing:
            return False, f"Missing fields: {', '.join(missing)}"
        return True, ""

    def insert(self, cursor: sqlite3.Cursor) -> int:
        sql = """
            INSERT INTO match (best_of, date, status, winner_side, walkover_side)
            VALUES (:best_of, :date, :status, :winner_side, :walkover_side)
            RETURNING match_id;
        """
        cursor.execute(sql, self.to_dict())
        self.match_id = cursor.fetchone()[0]
        return self.match_id