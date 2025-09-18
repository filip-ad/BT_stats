# src/models/player_ranking_raw.py

from dataclasses import dataclass
from typing import Optional, List, Dict, Any, Tuple
import sqlite3
from datetime import date
from utils import compute_content_hash as _compute_content_hash

@dataclass
class PlayerRankingRaw:
    """
    Raw player ranking row scraped from Profixio/OnData.
    Mirrors player_ranking_raw table in DB.
    """
    row_id:                         Optional[int]
    run_id_ext:                     Optional[str]
    run_date:                       Optional[date]
    player_id_ext:                  Optional[str]
    firstname:                      Optional[str]
    lastname:                       Optional[str]
    year_born:                      Optional[str]
    club_name:                      Optional[str]
    points:                         Optional[int]
    points_change_since_last:       Optional[int]
    position_world:                 Optional[int]
    position:                       Optional[int]
    data_source_id:                 Optional[int] = 3
    content_hash:                   Optional[str] = None
    last_seen_at:                   Optional[date] = None
    row_created:                    Optional[date] = None
    row_updated:                    Optional[date] = None

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "PlayerRankingRaw":
        """
        Construct from dictionary (e.g., from scraped data).
        """
        return PlayerRankingRaw(
            row_id                          = data.get("row_id"),
            run_id_ext                      = data.get("run_id_ext"),
            run_date                        = data.get("run_date"),
            player_id_ext                   = data.get("player_id_ext"),
            firstname                       = data.get("firstname"),
            lastname                        = data.get("lastname"),
            year_born                       = data.get("year_born"),
            club_name                       = data.get("club_name"),
            points                          = data.get("points"),
            points_change_since_last        = data.get("points_change_since_last"),
            position_world                  = data.get("position_world"),
            position                        = data.get("position"),
            data_source_id                  = data.get("data_source_id", 3),
            content_hash                    = data.get("content_hash"),
            last_seen_at                    = data.get("last_seen_at"),
            row_created                     = data.get("row_created"),
            row_updated                     = data.get("row_updated")
        )
    
    def validate(self) -> Tuple[bool, str]:
        """Validate raw fields. Returns: (is_valid, error_message)."""
        missing = []
        if not self.run_id_ext:
            missing.append("run_id_ext")
        if not self.run_date:
            missing.append("run_date")
        if not self.player_id_ext:
            missing.append("player_id_ext")
        if not self.firstname:
            missing.append("firstname")
        if not self.lastname:
            missing.append("lastname")
        if not self.year_born:
            missing.append("year_born")
        if self.points is None:
            missing.append("points")
        if self.position is None:
            missing.append("position")

        if missing:
            return False, f"Missing/invalid fields: {', '.join(missing)}"
        return True, ""

    def compute_content_hash(self) -> str:
        """
        Compute stable hash for this row (exclude volatile/meta fields).
        """
        return _compute_content_hash(
            self,
            exclude_fields={
                "row_id",
                "data_source_id",
                "row_created",
                "row_updated",
                "last_seen_at",
                "content_hash"
            },
        )

    # All but known bad data (specific run_id_ext + run_date combinations)
    # Used in resolve_player_rankings.py
    @classmethod
    def get_all(cls, cursor: sqlite3.Cursor) -> List["PlayerRankingRaw"]:
        """
        Fetch all rows from player_ranking_raw and return as dataclass objects.
        """
        cursor.execute("""
            SELECT
                row_id, run_id_ext, run_date, player_id_ext, firstname, lastname,
                year_born, club_name, points, points_change_since_last, position_world,
                position, data_source_id, content_hash, last_seen_at, row_created, row_updated
            FROM player_ranking_raw
            WHERE NOT (
                (run_date = '2023-10-02' AND run_id_ext = '346') OR
                (run_date = '2012-07-02' AND run_id_ext = '166') OR
                (run_date = '2011-07-04' AND run_id_ext = '150') OR
                (run_date = '2010-08-02' AND run_id_ext = '139')
            )
        """)
        return [cls.from_dict({
            "row_id": r[0],
            "run_id_ext": r[1],
            "run_date": r[2],
            "player_id_ext": r[3],
            "firstname": r[4],
            "lastname": r[5],
            "year_born": r[6],
            "club_name": r[7],
            "points": r[8],
            "points_change_since_last": r[9],
            "position_world": r[10],
            "position": r[11],
            "data_source_id": r[12],
            "content_hash": r[13],
            "last_seen_at": r[14],
            "row_created": r[15],
            "row_updated": r[16]
        }) for r in cursor.fetchall()]
    

    def upsert(self, cursor: sqlite3.Cursor) -> Optional[str]:
        """Upserting row with content-hash gating. Returns: 'inserted', 'updated', 'unchanged', or None (invalid)."""

        # Computing content hash
        new_hash = self.compute_content_hash()

        # Inserting or updating row
        sql = """
        INSERT INTO player_ranking_raw (
            run_id_ext, run_date, player_id_ext, firstname, lastname, year_born,
            club_name, points, points_change_since_last, position_world, position,
            data_source_id, content_hash, last_seen_at, row_created, row_updated
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT (run_id_ext, player_id_ext)
        DO UPDATE SET
            run_date = CASE
                WHEN player_ranking_raw.content_hash IS NULL OR player_ranking_raw.content_hash <> excluded.content_hash
                THEN excluded.run_date ELSE player_ranking_raw.run_date END,
            firstname = CASE
                WHEN player_ranking_raw.content_hash IS NULL OR player_ranking_raw.content_hash <> excluded.content_hash
                THEN excluded.firstname ELSE player_ranking_raw.firstname END,
            lastname = CASE
                WHEN player_ranking_raw.content_hash IS NULL OR player_ranking_raw.content_hash <> excluded.content_hash
                THEN excluded.lastname ELSE player_ranking_raw.lastname END,
            year_born = CASE
                WHEN player_ranking_raw.content_hash IS NULL OR player_ranking_raw.content_hash <> excluded.content_hash
                THEN excluded.year_born ELSE player_ranking_raw.year_born END,
            club_name = CASE
                WHEN player_ranking_raw.content_hash IS NULL OR player_ranking_raw.content_hash <> excluded.content_hash
                THEN excluded.club_name ELSE player_ranking_raw.club_name END,
            points = CASE
                WHEN player_ranking_raw.content_hash IS NULL OR player_ranking_raw.content_hash <> excluded.content_hash
                THEN excluded.points ELSE player_ranking_raw.points END,
            points_change_since_last = CASE
                WHEN player_ranking_raw.content_hash IS NULL OR player_ranking_raw.content_hash <> excluded.content_hash
                THEN excluded.points_change_since_last ELSE player_ranking_raw.points_change_since_last END,
            position_world = CASE
                WHEN player_ranking_raw.content_hash IS NULL OR player_ranking_raw.content_hash <> excluded.content_hash
                THEN excluded.position_world ELSE player_ranking_raw.position_world END,
            position = CASE
                WHEN player_ranking_raw.content_hash IS NULL OR player_ranking_raw.content_hash <> excluded.content_hash
                THEN excluded.position ELSE player_ranking_raw.position END,
            content_hash = CASE
                WHEN player_ranking_raw.content_hash IS NULL OR player_ranking_raw.content_hash <> excluded.content_hash
                THEN excluded.content_hash ELSE player_ranking_raw.content_hash END,
            row_updated = CASE
                WHEN player_ranking_raw.content_hash IS NULL OR player_ranking_raw.content_hash <> excluded.content_hash
                THEN CURRENT_TIMESTAMP ELSE player_ranking_raw.row_updated END,
            last_seen_at = CURRENT_TIMESTAMP
        RETURNING row_id;
        """
        vals = (
            self.run_id_ext, self.run_date, self.player_id_ext, self.firstname, self.lastname,
            self.year_born, self.club_name, self.points, self.points_change_since_last,
            self.position_world, self.position, self.data_source_id, new_hash
        )
        cursor.execute(sql, vals)
        row = cursor.fetchone()
        if row:
            self.row_id = row[0]
            if cursor.lastrowid == self.row_id:
                return "inserted"
            return "updated"
        return "unchanged"