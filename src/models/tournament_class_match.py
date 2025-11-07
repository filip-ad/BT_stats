# src/models/tournament_class_match.py

from dataclasses import dataclass, fields
from typing import Dict, Any, List, Optional, Tuple
import sqlite3

@dataclass
class TournamentClassMatch:
    tournament_class_id:                int = None
    match_id:                           int = None
    tournament_class_match_id_ext:      Optional[str] = None
    tournament_class_stage_id:          int = None
    tournament_class_group_id:          Optional[int] = None
    stage_round_no:                     Optional[int] = None
    draw_pos:                           Optional[int] = None
    row_created:                        Optional[str] = None
    row_updated:                        Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {f.name: getattr(self, f.name) for f in fields(self)}

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "TournamentClassMatch":
        return cls(**{k: d.get(k) for k in {f.name for f in fields(cls)}})

    def validate(self) -> Tuple[bool, str]:
        missing = []
        if not self.tournament_class_id:
            missing.append("tournament_class_id")
        if not self.match_id:
            missing.append("match_id")
        if not self.tournament_class_stage_id:
            missing.append("tournament_class_stage_id")
        if missing:
            return False, f"Missing fields: {', '.join(missing)}"
        return True, ""

    def insert(self, cursor: sqlite3.Cursor) -> None:
        sql = """
            INSERT OR IGNORE INTO tournament_class_match (
                tournament_class_id, match_id, tournament_class_match_id_ext, tournament_class_stage_id,
                tournament_class_group_id, stage_round_no, draw_pos
            ) VALUES (
                :tournament_class_id, :match_id, :tournament_class_match_id_ext, :tournament_class_stage_id,
                :tournament_class_group_id, :stage_round_no, :draw_pos
            );
        """
        cursor.execute(sql, self.to_dict())

    @classmethod
    def exists(cls, cursor: sqlite3.Cursor, tournament_class_id: int, match_id_ext: str) -> bool:
        cursor.execute("""
            SELECT 1 FROM tournament_class_match
            WHERE tournament_class_id = ? AND tournament_class_match_id_ext = ?;
        """, (tournament_class_id, match_id_ext))
        return bool(cursor.fetchone())

    @classmethod
    def set_group_for_match_ext(
        cls,
        cursor: sqlite3.Cursor,
        tournament_class_id: int,
        tournament_class_match_id_ext: str,
        tournament_class_group_id: int
    ) -> int:
        """
        Assign tournament_class_group_id for a resolved match identified by
        (tournament_class_id, tournament_class_match_id_ext).
        Returns the number of affected rows (0/1).
        """
        cursor.execute("""
            UPDATE tournament_class_match
            SET tournament_class_group_id = ?
            WHERE tournament_class_id = ? AND tournament_class_match_id_ext = ?
        """, (tournament_class_group_id, tournament_class_id, tournament_class_match_id_ext))
        return cursor.rowcount
    
    @classmethod
    def remove_for_class(cls, cursor: sqlite3.Cursor, tournament_class_id: int) -> int:
        """
        Remove all resolved matches for a given tournament_class_id.

        Strategy:
          1) Collect match_ids from tournament_class_match for the class.
          2) DELETE FROM match WHERE match_id IN (...).
             With proper FK cascades:
               - game, match_side, match_player will cascade from match.
               - tournament_class_match will cascade from match_id (child of match).
        Returns the number of matches deleted (rows deleted from match).
        """
        cursor.execute("""
            SELECT match_id
            FROM tournament_class_match
            WHERE tournament_class_id = ?
              AND match_id IS NOT NULL
        """, (tournament_class_id,))
        ids = [row[0] for row in cursor.fetchall()]
        if not ids:
            return 0

        # SQLite has a variable limit; stay safely under 999 parameters.
        def _chunks(seq: List[int], size: int = 900):
            for i in range(0, len(seq), size):
                yield seq[i:i+size]

        total_deleted = 0
        for chunk in _chunks(ids):
            placeholders = ",".join("?" * len(chunk))
            cursor.execute(f"DELETE FROM match WHERE match_id IN ({placeholders})", chunk)
            # rowcount here reflects rows deleted from `match`
            total_deleted += cursor.rowcount or 0

        return total_deleted