# src/models/match.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, List, Tuple, Dict, Any
import sqlite3
import logging

@dataclass
class Match:
    # Core match
    match_id: Optional[int] = None
    best_of: Optional[int] = None
    date: Optional[str] = None  # 'YYYY-MM-DD' or None

    # External identity (for idempotence)
    match_id_ext: Optional[str] = None
    data_source_id: Optional[int] = None  # REQUIRED if match_id_ext is provided

    # Competition context (competition_type_id = 1 for TournamentClass)
    competition_type_id: int = 1                       # fixed for tournament classes
    tournament_class_id: Optional[int] = None          # REQUIRED when competition_type_id=1
    tournament_class_group_id: Optional[int] = None    # nullable
    tournament_class_stage_id: Optional[int] = None    # nullable

    # In-memory buffers to stage sides and games before save
    _sides: List[Tuple[int, int]] = field(default_factory=list)       # list of (side, participant_id)
    _games: List[Tuple[int, int]] = field(default_factory=list)       # list of (side1_points, side2_points)

    # ------------- Builders -------------

    def add_side_participant(self, side: int, participant_id: int) -> None:
        """
        Stage a side for this match. Side must be 1 or 2.
        """
        if side not in (1, 2):
            raise ValueError("side must be 1 or 2")
        self._sides.append((side, participant_id))

    def add_game(self, game_no: int, side1_points: int, side2_points: int) -> None:
        """
        Stage a game (set). game_no is implicit by insertion order on save.
        """
        self._games.append((side1_points, side2_points))

    # ------------- Persistence -------------

    def _find_existing_match_id(self, cursor: sqlite3.Cursor) -> Optional[int]:
        """
        If match_id_ext + data_source_id are provided, return existing match_id if mapped.
        """
        if not self.match_id_ext or self.data_source_id is None:
            return None
        cursor.execute(
            """
            SELECT match_id 
            FROM match_id_ext
            WHERE match_id_ext = ? AND data_source_id = ?
            """,
            (self.match_id_ext, self.data_source_id),
        )
        row = cursor.fetchone()
        return row[0] if row else None

    def _insert_match_row(self, cursor: sqlite3.Cursor) -> int:
        cursor.execute(
            """
            INSERT INTO match (best_of, date)
            VALUES (?, ?)
            RETURNING match_id
            """,
            (self.best_of, self.date),
        )
        mid = cursor.fetchone()[0]
        return mid

    def _ensure_match_id_ext(self, cursor: sqlite3.Cursor) -> None:
        """
        Create link in match_id_ext if we have an ext id. Idempotent via PK.
        """
        if not self.match_id_ext or self.data_source_id is None:
            return
        cursor.execute(
            """
            INSERT OR IGNORE INTO match_id_ext (match_id, match_id_ext, data_source_id)
            VALUES (?, ?, ?)
            """,
            (self.match_id, self.match_id_ext, self.data_source_id),
        )

    def _upsert_match_competition(self, cursor: sqlite3.Cursor) -> None:
        """
        Upsert row in match_competition keyed by (match_id, competition_type_id).
        Conforms to your CHECKs:
          - For competition_type_id=1 we must provide tournament_class_id (not NULL)
          - Group/stage are optional for TC
        """
        if self.competition_type_id != 1:
            raise NotImplementedError("Only competition_type_id=1 (TournamentClass) is implemented here.")
        if not self.tournament_class_id:
            raise ValueError("tournament_class_id is required for competition_type_id=1")

        cursor.execute(
            """
            INSERT INTO match_competition (
                match_id, competition_type_id, tournament_class_id,
                fixture_id, tournament_class_group_id, tournament_class_stage_id
            )
            VALUES (?, ?, ?, NULL, ?, ?)
            ON CONFLICT(match_id, competition_type_id) DO UPDATE SET
                tournament_class_id       = excluded.tournament_class_id,
                tournament_class_group_id = excluded.tournament_class_group_id,
                tournament_class_stage_id = excluded.tournament_class_stage_id,
                row_updated               = CURRENT_TIMESTAMP
            """,
            (
                self.match_id,
                self.competition_type_id,
                self.tournament_class_id,
                self.tournament_class_group_id,
                self.tournament_class_stage_id,
            ),
        )

    def _save_sides(self, cursor: sqlite3.Cursor) -> None:
        """
        Insert or replace sides by (match_id, side) uniqueness.
        """
        # Optional: ensure at most one entry per side in the staged list
        latest_for_side: Dict[int, int] = {}
        for side, pid in self._sides:
            latest_for_side[side] = pid

        for side in (1, 2):
            pid = latest_for_side.get(side)
            if pid is None:
                continue
            cursor.execute(
                """
                INSERT INTO match_side (match_id, side, participant_id)
                VALUES (?, ?, ?)
                ON CONFLICT(match_id, side) DO UPDATE SET
                    participant_id = excluded.participant_id,
                    row_updated    = CURRENT_TIMESTAMP
                """,
                (self.match_id, side, pid),
            )

    def _save_games(self, cursor: sqlite3.Cursor) -> None:
        """
        Replace existing games with the staged list.
        """
        cursor.execute("DELETE FROM game WHERE match_id = ?", (self.match_id,))
        for i, (s1, s2) in enumerate(self._games, start=1):
            winning_side = None
            if s1 is not None and s2 is not None:
                if s1 > s2:
                    winning_side = 1
                elif s2 > s1:
                    winning_side = 2
            cursor.execute(
                """
                INSERT INTO game (match_id, game_nbr, side1_points, side2_points, winning_side)
                VALUES (?, ?, ?, ?, ?)
                """,
                (self.match_id, i, s1, s2, winning_side),
            )

    def save_to_db(self, cursor: sqlite3.Cursor) -> Dict[str, Any]:
        """
        Idempotent save:
          1) Reuse existing match via (match_id_ext, data_source_id) if present
          2) Else insert a new match
          3) Ensure match_id_ext mapping (if provided)
          4) Upsert match_competition (TournamentClass context)
          5) Upsert sides (1/2)
          6) Replace games
        """
        try:
            existing = self._find_existing_match_id(cursor)
            if existing:
                self.match_id = existing
            else:
                self.match_id = self._insert_match_row(cursor)

            self._ensure_match_id_ext(cursor)
            self._upsert_match_competition(cursor)
            self._save_sides(cursor)
            self._save_games(cursor)

            return {"status": "success", "match_id": self.match_id}
        except Exception as e:
            logging.exception("Error saving match")
            return {"status": "failed", "reason": str(e)}
