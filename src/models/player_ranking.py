# src/models/player_ranking.py

from dataclasses import dataclass
from datetime import date
from typing import Optional, Tuple, Dict, Any
import sqlite3

@dataclass
class PlayerRanking:
    """
    Normalized player ranking row.
    Mirrors player_ranking table in DB.
    """
    run_id_ext:                 str
    run_date:                   date
    player_id_ext:              str
    points:                     int = 0
    points_change_since_last:   int = 0
    position_world:             int = 0
    position:                   int = 0
    data_source_id:             int = 3
    row_created:                Optional[date] = None
    row_updated:                Optional[date] = None

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "PlayerRanking":
        """
        Construct from dictionary (e.g., from resolver).
        """
        return PlayerRanking(
            run_id_ext                      = data["run_id_ext"],
            run_date                        = data["run_date"],
            player_id_ext                   = data["player_id_ext"],
            points                          = data.get("points", 0),
            points_change_since_last        = data.get("points_change_since_last", 0),
            position_world                  = data.get("position_world", 0),
            position                        = data.get("position", 0),
            data_source_id                  = data.get("data_source_id", 3),
            row_created                     = data.get("row_created"),
            row_updated                     = data.get("row_updated")
        )

    def validate(self, cursor: sqlite3.Cursor, valid_exts: Optional[set]=None) -> Tuple[bool, str]:
        if not all([self.run_id_ext, self.run_date, self.player_id_ext]):
            return False, "Missing required field(s): run_id_ext, run_date, player_id_ext"

        # Fast cache check
        if valid_exts is not None:
            if (self.player_id_ext, self.data_source_id) not in valid_exts:
                return False, f"Missing mapping in player_id_ext"
        else:
            cursor.execute("""
                SELECT 1
                FROM player_id_ext
                WHERE player_id_ext = ? AND data_source_id = ?
            """, (self.player_id_ext, self.data_source_id))
            if not cursor.fetchone():
                return False, f"Missing mapping in player_id_ext"

        return True, ""


    def upsert(self, cursor: sqlite3.Cursor) -> Optional[str]:
        """
        Upsert a single PlayerRanking with change detection.
        Returns one of: "inserted", "updated", "unchanged", or None (invalid).
        """

        sql = """
        INSERT INTO player_ranking (
            run_id_ext, run_date, player_id_ext, points, points_change_since_last,
            position_world, position, data_source_id, row_created, row_updated
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT (player_id_ext, data_source_id, run_date)
        DO UPDATE SET
            run_id_ext = CASE
                WHEN player_ranking.points != excluded.points
                OR player_ranking.points_change_since_last != excluded.points_change_since_last
                OR player_ranking.position_world != excluded.position_world
                OR player_ranking.position != excluded.position
                OR player_ranking.run_id_ext != excluded.run_id_ext
                THEN excluded.run_id_ext ELSE player_ranking.run_id_ext END,
            points = CASE
                WHEN player_ranking.points != excluded.points
                OR player_ranking.points_change_since_last != excluded.points_change_since_last
                OR player_ranking.position_world != excluded.position_world
                OR player_ranking.position != excluded.position
                OR player_ranking.run_id_ext != excluded.run_id_ext
                THEN excluded.points ELSE player_ranking.points END,
            points_change_since_last = CASE
                WHEN player_ranking.points != excluded.points
                OR player_ranking.points_change_since_last != excluded.points_change_since_last
                OR player_ranking.position_world != excluded.position_world
                OR player_ranking.position != excluded.position
                OR player_ranking.run_id_ext != excluded.run_id_ext
                THEN excluded.points_change_since_last ELSE player_ranking.points_change_since_last END,
            position_world = CASE
                WHEN player_ranking.points != excluded.points
                OR player_ranking.points_change_since_last != excluded.points_change_since_last
                OR player_ranking.position_world != excluded.position_world
                OR player_ranking.position != excluded.position
                OR player_ranking.run_id_ext != excluded.run_id_ext
                THEN excluded.position_world ELSE player_ranking.position_world END,
            position = CASE
                WHEN player_ranking.points != excluded.points
                OR player_ranking.points_change_since_last != excluded.points_change_since_last
                OR player_ranking.position_world != excluded.position_world
                OR player_ranking.position != excluded.position
                OR player_ranking.run_id_ext != excluded.run_id_ext
                THEN excluded.position ELSE player_ranking.position END,
            row_updated = CASE
                WHEN player_ranking.points != excluded.points
                OR player_ranking.points_change_since_last != excluded.points_change_since_last
                OR player_ranking.position_world != excluded.position_world
                OR player_ranking.position != excluded.position
                OR player_ranking.run_id_ext != excluded.run_id_ext
                THEN CURRENT_TIMESTAMP ELSE player_ranking.row_updated END
        RETURNING player_id_ext;
        """
        vals = (
            self.run_id_ext,
            self.run_date,
            self.player_id_ext,
            self.points,
            self.points_change_since_last,
            self.position_world,
            self.position,
            self.data_source_id
        )
        cursor.execute(sql, vals)
        row = cursor.fetchone()
        if row:
            # Either inserted or updated-with-change
            if cursor.lastrowid:
                return "inserted"
            return "updated"
        return "unchanged"