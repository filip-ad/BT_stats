# src/models/district.py

from dataclasses import dataclass
from typing import Optional
import logging

@dataclass
class District:
    district_id: Optional[int] = None
    district_id_ext: Optional[int] = None
    name: Optional[str] = None

    @staticmethod
    def from_dict(data: dict):
        return District(
            district_id=data.get("district_id"),
            district_id_ext=data.get("district_id_ext"),
            name=data.get("name"),
        )

    @staticmethod
    def get_by_id_ext(cursor, district_id_ext: int) -> Optional['District']:
        """Retrieve a District instance by district_id_ext, or None if not found."""
        try:
            cursor.execute("""
                SELECT district_id, district_id_ext, name
                FROM district WHERE district_id_ext = ?
            """, (district_id_ext,))
            row = cursor.fetchone()
            if row:
                return District.from_dict({
                    "district_id": row[0],
                    "district_id_ext": row[1],
                    "name": row[2]
                })
            return None
        except Exception as e:
            logging.error(f"Error retrieving district by district_id_ext {district_id_ext}: {e}")
            return None