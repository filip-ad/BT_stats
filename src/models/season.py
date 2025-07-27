# src/models/season.py

from dataclasses import dataclass
from typing import Optional
import logging

@dataclass
class Season:
    season_id: Optional[int] = None
    season_id_ext: Optional[int] = None
    season_start_date: Optional[str] = None  # 'YYYY-MM-DD'
    season_end_date: Optional[str] = None    # 'YYYY-MM-DD'
    season_start_year: Optional[int] = None
    season_end_year: Optional[int] = None
    season_description: Optional[str] = None
    season_label: Optional[str] = None

    @staticmethod
    def from_dict(data: dict):
        return Season(
            season_id=data.get("season_id"),
            season_id_ext=data.get("season_id_ext"),
            season_start_date=data.get("season_start_date"),
            season_end_date=data.get("season_end_date"),
            season_start_year=data.get("season_start_year"),
            season_end_year=data.get("season_end_year"),
            season_description=data.get("season_description"),
            season_label=data.get("season_label"),
        )

    @staticmethod
    def get_by_id(cursor, season_id: int) -> Optional['Season']:
        """Retrieve a Season instance by internal season_id, or None if not found."""
        try:
            cursor.execute('''
                SELECT season_id, season_id_ext, season_start_date, season_end_date,
                       season_start_year, season_end_year, season_description, season_label
                FROM season
                WHERE season_id = ?
            ''', (season_id,))
            row = cursor.fetchone()
            if row:
                keys = ['season_id', 'season_id_ext', 'season_start_date', 'season_end_date',
                        'season_start_year', 'season_end_year', 'season_description', 'season_label']
                return Season.from_dict(dict(zip(keys, row)))
            return None
        except Exception as e:
            logging.error(f"Error retrieving season by season_id {season_id}: {e}")
            return None

    @staticmethod
    def get_by_id_ext(cursor, season_id_ext: int) -> Optional['Season']:
        """Retrieve a Season instance by season_id_ext, or None if not found."""
        try:
            cursor.execute('''
                SELECT season_id, season_id_ext, season_start_date, season_end_date,
                       season_start_year, season_end_year, season_description, season_label
                FROM season
                WHERE season_id_ext = ?
            ''', (season_id_ext,))
            row = cursor.fetchone()
            if row:
                keys = ['season_id', 'season_id_ext', 'season_start_date', 'season_end_date',
                        'season_start_year', 'season_end_year', 'season_description', 'season_label']
                return Season.from_dict(dict(zip(keys, row)))
            return None
        except Exception as e:
            logging.error(f"Error retrieving season by season_id_ext {season_id_ext}: {e}")
            return None

    @staticmethod
    def get_by_label(cursor, season_label: str) -> Optional['Season']:
        """Retrieve a Season instance by season_label, or None if not found."""
        try:
            cursor.execute('''
                SELECT season_id, season_id_ext, season_start_date, season_end_date,
                       season_start_year, season_end_year, season_description, season_label
                FROM season
                WHERE season_label = ?
            ''', (season_label,))
            row = cursor.fetchone()
            if row:
                keys = ['season_id', 'season_id_ext', 'season_start_date', 'season_end_date',
                        'season_start_year', 'season_end_year', 'season_description', 'season_label']
                return Season.from_dict(dict(zip(keys, row)))
            return None
        except Exception as e:
            logging.error(f"Error retrieving season by season_label {season_label}: {e}")
            return None

    @staticmethod
    def get_by_date(cursor, date_object: str) -> Optional['Season']:
        """Retrieve a Season instance that contains the given date, or None if not found."""
        try:
            cursor.execute('''
                SELECT season_id, season_id_ext, season_start_date, season_end_date,
                       season_start_year, season_end_year, season_description, season_label
                FROM season
                WHERE season_start_date <= ? AND season_end_date >= ?
            ''', (date_object, date_object))
            row = cursor.fetchone()
            if row:
                keys = ['season_id', 'season_id_ext', 'season_start_date', 'season_end_date',
                        'season_start_year', 'season_end_year', 'season_description', 'season_label']
                return Season.from_dict(dict(zip(keys, row)))
            return None
        except Exception as e:
            logging.error(f"Error retrieving season by date {date_object}: {e}")
            return None

    def contains_date(self, date: str) -> bool:
        """Check if the given date falls within this season's range."""
        if self.season_start_date and self.season_end_date:
            return self.season_start_date <= date <= self.season_end_date
        return False