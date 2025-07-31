# src/models/license.py

from dataclasses import dataclass
from typing import Optional, Dict, Tuple
import logging

@dataclass
class License:
    license_id: Optional[int] = None            # Canonical ID from license table
    type: str = None                    # e.g., 'A-licens', 'D-licens'
    age_group: Optional[str] = None     # e.g., 'Barn', 'Senior', or None

    @staticmethod
    def from_dict(data: dict):
        return License(
            license_id=data.get("license_id"),
            type=data.get("type"),
            age_group=data.get("age_group")
        )

    @staticmethod
    def get_by_type_and_age(cursor, type: str, age_group: Optional[str] = None) -> Optional['License']:
        """Retrieve a License instance by type and age_group, or None if not found."""
        try:
            if age_group is None:
                cursor.execute("""
                    SELECT license_id, type, age_group
                    FROM license
                    WHERE type = ? AND (age_group IS NULL OR age_group = '')
                """, (type,))
            else:
                cursor.execute("""
                    SELECT license_id, type, age_group
                    FROM license
                    WHERE type = ? AND age_group = ?
                """, (type, age_group))
            row = cursor.fetchone()
            if row:
                return License.from_dict({
                    "license_id": row[0],
                    "type": row[1],
                    "age_group": row[2]
                })
            return None
        except Exception as e:
            logging.error(f"Error retrieving license by type {type} and age group {age_group}: {e}")
            return None

    @staticmethod
    def get_by_id(cursor, license_id: int) -> Optional['License']:
        """Retrieve a License instance by license_id, or None if not found."""
        try:
            cursor.execute("""
                SELECT license_id, type, age_group
                FROM license
                WHERE license_id = ?
            """, (license_id,))
            row = cursor.fetchone()
            if row:
                return License.from_dict({
                    "license_id": row[0],
                    "type": row[1],
                    "age_group": row[2]
                })
            return None
        except Exception as e:
            logging.error(f"Error retrieving license by license_id {license_id}: {e}")
            return None

    @staticmethod
    def cache_all(cursor) -> Dict[Tuple[str, Optional[str]], 'License']:
        """Cache all licenses by (type, age_group)."""
        try:
            cursor.execute("SELECT license_id, type, age_group FROM license")
            return {
                (row[1], row[2] if row[2] else None): License(
                    license_id=row[0],
                    type=row[1],
                    age_group=row[2]
                ) for row in cursor.fetchall()
            }
        except Exception as e:
            logging.error(f"Error caching licenses: {e}")
            return {}            