# src/models/season.py

from dataclasses import dataclass
from typing import Optional, Dict, Any
from datetime import date
import logging
import sqlite3

@dataclass
class Season:
    season_id:      Optional[int] = None
    season_id_ext:  Optional[int] = None
    start_date:     Optional[date] = None
    end_date:       Optional[date] = None
    start_year:     Optional[int] = None
    end_year:       Optional[int] = None
    description:    Optional[str] = None
    label:          Optional[str] = None

    @staticmethod
    def from_dict(data: dict):
        return Season(
            season_id=data.get("season_id"),
            season_id_ext=data.get("season_id_ext"),
            start_date=data.get("start_date"),
            end_date=data.get("end_date"),
            start_year=data.get("start_year"),
            end_year=data.get("end_year"),
            description=data.get("description"),
            label=data.get("label"),
        )
    
    @staticmethod
    def from_row(row: sqlite3.Row) -> "Season":
        # row: (season_id, season_id_ext, start_date, end_date,
        #       start_year, end_year, description, label)
        return Season(
            season_id    = row[0],
            season_id_ext= row[1],
            start_date   = row[2],
            end_date     = row[3],
            start_year   = row[4],
            end_year     = row[5],
            description  = row[6],
            label        = row[7],
        )    
    
    @classmethod
    def cache_by_ext(cls, cursor) -> Dict[int, "Season"]:
        """
        Load all seasons into a dict: season_id_ext → Season
        """
        cursor.execute("""
            SELECT season_id, season_id_ext,
                   start_date, end_date,
                   start_year, end_year,
                   description, label
              FROM season
        """)
        cache: Dict[int, Season] = {}
        for row in cursor.fetchall():
            s = cls.from_row(row)
            cache[s.season_id_ext] = s
        logging.info(f"Cached {len(cache)} seasons by external ID")
        return cache

    @staticmethod
    def get_by_id(cursor, season_id: int) -> Optional['Season']:
        """Retrieve a Season instance by internal season_id, or None if not found."""
        try:
            cursor.execute('''
                SELECT season_id, season_id_ext, start_date, end_date,
                       start_year, end_year, description, label
                FROM season
                WHERE season_id = ?
            ''', (season_id,))
            row = cursor.fetchone()
            if row:
                keys = ['season_id', 'season_id_ext', 'start_date', 'end_date',
                        'start_year', 'end_year', 'description', 'label']
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
                SELECT season_id, season_id_ext, start_date, end_date,
                       start_year, end_year, description, label
                FROM season
                WHERE season_id_ext = ?
            ''', (season_id_ext,))
            row = cursor.fetchone()
            if row:
                keys = ['season_id', 'season_id_ext', 'start_date', 'end_date',
                        'start_year', 'end_year', 'description', 'label']
                return Season.from_dict(dict(zip(keys, row)))
            return None
        except Exception as e:
            logging.error(f"Error retrieving season by season_id_ext {season_id_ext}: {e}")
            return None

    @staticmethod
    def get_by_label(cursor, label: str) -> Optional['Season']:
        """Retrieve a Season instance by label, or None if not found."""
        try:
            cursor.execute('''
                SELECT season_id, season_id_ext, start_date, end_date,
                       start_year, end_year, description, label
                FROM season
                WHERE label = ?
            ''', (label,))
            row = cursor.fetchone()
            if row:
                keys = ['season_id', 'season_id_ext', 'start_date', 'end_date',
                        'start_year', 'end_year', 'description', 'label']
                return Season.from_dict(dict(zip(keys, row)))
            return None
        except Exception as e:
            logging.error(f"Error retrieving season by label {label}: {e}")
            return None

    @staticmethod
    def get_by_date(cursor, date_object: str) -> Optional['Season']:
        """Retrieve a Season instance that contains the given date, or None if not found."""
        try:
            cursor.execute('''
                SELECT season_id, season_id_ext, start_date, end_date,
                       start_year, end_year, description, label
                FROM season
                WHERE start_date <= ? AND end_date >= ?
            ''', (date_object, date_object))
            row = cursor.fetchone()
            if row:
                keys = ['season_id', 'season_id_ext', 'start_date', 'end_date',
                        'start_year', 'end_year', 'description', 'label']
                return Season.from_dict(dict(zip(keys, row)))
            return None
        except Exception as e:
            logging.error(f"Error retrieving season by date {date_object}: {e}")
            return None

    def contains_date(self, date: str) -> bool:
        """Check if the given date falls within this season's range."""
        if self.start_date and self.end_date:
            return self.start_date <= date <= self.end_date
        return False
    
    @staticmethod
    def cache_all(cursor) -> Dict[str, 'Season']:
        """Cache all seasons by label."""
        try:
            cursor.execute("""
                SELECT season_id, label, start_date, end_date
                FROM season
            """)
            return {
                row[1]: Season(
                    season_id=row[0],
                    label=row[1],
                    start_date=row[2],
                    end_date=row[3]
                ) for row in cursor.fetchall() if row[1]
            }
        except Exception as e:
            logging.error(f"Error caching seasons: {e}")
            return {}