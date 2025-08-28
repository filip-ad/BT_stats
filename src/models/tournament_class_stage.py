# src/models/tournament_class_stage.py
from dataclasses import dataclass
from typing import Optional, Dict
import sqlite3
import logging

@dataclass
class TournamentClassStage:
    tournament_class_stage_id: int
    shortname: str
    description: str
    is_knockout: int
    round_order: Optional[int]

    @staticmethod
    def id_by_code(cursor: sqlite3.Cursor, code: str) -> Optional[int]:
        try:
            cursor.execute(
                "SELECT tournament_class_stage_id FROM tournament_class_stage WHERE shortname = ?",
                (code,)
            )
            row = cursor.fetchone()
            return row[0] if row else None
        except Exception as e:
            logging.error(f"Error getting stage id for code {code}: {e}")
            return None

    @staticmethod
    def cache_all(cursor: sqlite3.Cursor) -> Dict[str, int]:
        """{ shortname -> id }"""
        try:
            cursor.execute("SELECT shortname, tournament_class_stage_id FROM tournament_class_stage")
            return {row[0]: row[1] for row in cursor.fetchall()}
        except Exception as e:
            logging.error(f"Error caching stages: {e}")
            return {}
