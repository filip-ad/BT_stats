# src/models/tournament_stage.py

from dataclasses import dataclass
from typing import Optional, Dict
import logging

@dataclass
class TournamentStage:
    stage_id: Optional[int] = None
    code: str = None          # e.g., 'GROUP', 'R16', 'F'
    label: str = None         # e.g., 'Group', 'Round of 16'
    is_knockout: bool = False
    round_order: Optional[int] = None

    @staticmethod
    def from_dict(data: dict) -> 'TournamentStage':
        return TournamentStage(
            stage_id=data.get("stage_id"),
            code=data.get("code"),
            label=data.get("label"),
            is_knockout=bool(data.get("is_knockout")),
            round_order=data.get("round_order")
        )

    @staticmethod
    def get_by_id(cursor, stage_id: int) -> Optional['TournamentStage']:
        try:
            cursor.execute("""
                SELECT stage_id, code, label, is_knockout, round_order
                FROM stage
                WHERE stage_id = ?
            """, (stage_id,))
            row = cursor.fetchone()
            if row:
                return TournamentStage.from_dict({
                    "stage_id": row[0],
                    "code": row[1],
                    "label": row[2],
                    "is_knockout": row[3],
                    "round_order": row[4]
                })
        except Exception as e:
            logging.error(f"Error retrieving stage by id {stage_id}: {e}")
        return None

    @staticmethod
    def get_by_code(cursor, code: str) -> Optional['TournamentStage']:
        try:
            cursor.execute("""
                SELECT stage_id, code, label, is_knockout, round_order
                FROM stage
                WHERE code = ?
            """, (code,))
            row = cursor.fetchone()
            if row:
                return TournamentStage.from_dict({
                    "stage_id": row[0],
                    "code": row[1],
                    "label": row[2],
                    "is_knockout": row[3],
                    "round_order": row[4]
                })
        except Exception as e:
            logging.error(f"Error retrieving stage by code '{code}': {e}")
        return None

    @staticmethod
    def id_by_code(cursor, code: str) -> Optional[int]:
        try:
            cursor.execute("SELECT stage_id FROM stage WHERE code = ?", (code,))
            row = cursor.fetchone()
            return row[0] if row else None
        except Exception as e:
            logging.error(f"Error retrieving stage_id by code '{code}': {e}")
            return None

    @staticmethod
    def cache_all(cursor) -> Dict[str, 'TournamentStage']:
        """Cache all stages by code."""
        try:
            cursor.execute("SELECT stage_id, code, label, is_knockout, round_order FROM stage")
            return {
                row[1]: TournamentStage(
                    stage_id=row[0],
                    code=row[1],
                    label=row[2],
                    is_knockout=bool(row[3]),
                    round_order=row[4]
                ) for row in cursor.fetchall()
            }
        except Exception as e:
            logging.error(f"Error caching stages: {e}")
            return {}
