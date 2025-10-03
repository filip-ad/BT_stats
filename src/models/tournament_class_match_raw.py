# models/tournament_class_match_raw.py

from __future__ import annotations
from dataclasses import dataclass, fields
from typing import Optional, Dict, Any, Iterable, Tuple
import sqlite3

from utils import compute_content_hash as _compute_content_hash

@dataclass
class TournamentClassMatchRaw:
    # --- table columns ---
    row_id:                                     Optional[int]   = None
    tournament_id_ext:                          str             = ""
    tournament_class_id_ext:                    str             = ""
    group_id_ext:                               Optional[str]   = None                 # nullable to support KO/SWISS
    match_id_ext:                               Optional[str]   = None
    s1_player_id_ext:                           Optional[str]   = None             # may contain "028/030" for doubles
    s2_player_id_ext:                           Optional[str]   = None
    s1_fullname_raw:                            Optional[str]   = None
    s2_fullname_raw:                            Optional[str]   = None
    s1_clubname_raw:                            Optional[str]   = None
    s2_clubname_raw:                            Optional[str]   = None
    game_point_tokens:                          Optional[str]   = None            # e.g. "8, 7, -3, 5"
    best_of:                                    Optional[int]   = None
    raw_line_text:                              Optional[str]   = None
    tournament_class_stage_id:                  Optional[int]   = None    # 1 = GROUP for this scraper
    data_source_id:                             int             = 1
    content_hash:                               Optional[str]   = None
    row_created:                                Optional[str]   = None
    row_updated:                                Optional[str]   = None


    def to_dict(self) -> Dict[str, Any]:
        d = {f.name: getattr(self, f.name) for f in fields(self)}
        # strip non-table fields if any are ever added
        return {k: v for k, v in d.items() if k in {
            "row_id",
            "tournament_id_ext", 
            "tournament_class_id_ext", 
            "group_id_ext",
            "match_id_ext",
            "s1_player_id_ext", 
            "s2_player_id_ext",
            "s1_fullname_raw", 
            "s2_fullname_raw",
            "s1_clubname_raw", 
            "s2_clubname_raw",
            "game_point_tokens", 
            "best_of", 
            "raw_line_text",
            "tournament_class_stage_id",
            "data_source_id", 
            "content_hash",
            "row_created", 
            "row_updated"
        }}

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "TournamentClassMatchRaw":
        return cls(**{k: d.get(k) for k in {f.name for f in fields(cls)}})

    def validate(self) -> Tuple[bool, str]:
        """
        Validate required fields for the RAW match row.
        Only core identifiers are enforced; all other fields are optional for raw scraping.
        """
        missing = []
        if not self.tournament_id_ext:
            missing.append("tournament_id_ext")
        if not self.tournament_class_id_ext:
            missing.append("tournament_class_id_ext")
        if self.tournament_class_stage_id is None:
            missing.append("tournament_class_stage_id")
        if not self.data_source_id:
            missing.append("data_source_id")

        if missing:
            self.is_valid = False
            return False, f"Missing/invalid fields: {', '.join(missing)}"

        self.is_valid = True
        return True, ""
    
    def compute_hash(self) -> None:
        """Stable hash of meaningful content for change detection."""
        self.content_hash = _compute_content_hash(
            self,
            exclude_fields={
                "row_id",
                "row_created",
                "row_updated",
                "content_hash",
            }
        )

    def insert(self, cursor: sqlite3.Cursor) -> None:
        """Insert one raw match row."""

        cursor.execute("""
            INSERT OR IGNORE INTO tournament_class_match_raw (
                tournament_id_ext,
                tournament_class_id_ext,
                group_id_ext,
                match_id_ext,
                s1_player_id_ext, s2_player_id_ext,
                s1_fullname_raw,  s2_fullname_raw,
                s1_clubname_raw,  s2_clubname_raw,
                game_point_tokens,
                best_of,
                raw_line_text,
                tournament_class_stage_id,
                data_source_id,
                content_hash
            )
            VALUES (
                :tournament_id_ext,
                :tournament_class_id_ext,
                :group_id_ext,
                :match_id_ext,
                :s1_player_id_ext, :s2_player_id_ext,
                :s1_fullname_raw,  :s2_fullname_raw,
                :s1_clubname_raw,  :s2_clubname_raw,
                :game_point_tokens,
                :best_of,
                :raw_line_text,
                :tournament_class_stage_id,
                :data_source_id,
                :content_hash
            )
        """, self.to_dict())

    @classmethod
    def remove_for_class(
        cls,
        cursor: sqlite3.Cursor,
        tournament_class_id_ext: str,
        data_source_id: int = 1,
        tournament_class_stage_id: int | None = None,
    ) -> int:
        """
        Remove raw match rows for a given class/source.
        If tournament_class_stage_id is provided, only delete rows in that stage.
        """
        if tournament_class_stage_id is None:
            cursor.execute(
                """
                DELETE FROM tournament_class_match_raw
                WHERE tournament_class_id_ext = ? AND data_source_id = ?
                """,
                (tournament_class_id_ext, data_source_id),
            )
        else:
            cursor.execute(
                """
                DELETE FROM tournament_class_match_raw
                WHERE tournament_class_id_ext = ?
                AND data_source_id = ?
                AND tournament_class_stage_id = ?
                """,
                (tournament_class_id_ext, data_source_id, tournament_class_stage_id),
            )
        return cursor.rowcount
