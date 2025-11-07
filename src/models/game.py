# src/models/game.py

from dataclasses import dataclass, fields
from typing import Dict, Any, Optional, Tuple
import sqlite3

@dataclass
class Game:
    match_id:           Optional[int] = None
    game_no:            int = None
    points_side1:       int = None
    points_side2:       int = None
    row_created:        Optional[str] = None
    row_updated:        Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {f.name: getattr(self, f.name) for f in fields(self)}

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Game":
        return cls(**{k: d.get(k) for k in {f.name for f in fields(cls)}})

    def validate(self) -> Tuple[bool, str]:
        missing = []
        if self.match_id is None:
            missing.append("match_id")
        if self.game_no is None:
            missing.append("game_no")
        if self.points_side1 is None or self.points_side2 is None:
            missing.append("points")
        if missing:
            return False, f"Missing fields: {', '.join(missing)}"
        return True, ""

    def insert(self, cursor: sqlite3.Cursor) -> None:
        sql = """
            INSERT INTO game (match_id, game_no, points_side1, points_side2)
            VALUES (:match_id, :game_no, :points_side1, :points_side2);
        """
        cursor.execute(sql, self.to_dict())