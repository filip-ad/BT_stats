# src/models/license.py

from dataclasses import dataclass
from typing import Optional
import logging

@dataclass
class License:
    license_id: Optional[int] = None            # Canonical ID from license table
    license_type: str = None                    # e.g., 'A-licens', 'D-licens'
    license_age_group: Optional[str] = None     # e.g., 'Barn', 'Senior', or None

    @staticmethod
    def from_dict(data: dict):
        return License(
            license_id=data.get("license_id"),
            license_type=data.get("license_type"),
            license_age_group=data.get("license_age_group")
        )

    @staticmethod
    def get_by_type_and_age(cursor, license_type: str, license_age_group: Optional[str] = None) -> Optional['License']:
        """Retrieve a License instance by license_type and license_age_group, or None if not found."""
        try:
            if license_age_group is None:
                cursor.execute("""
                    SELECT license_id, license_type, license_age_group
                    FROM license
                    WHERE license_type = ? AND (license_age_group IS NULL OR license_age_group = '')
                """, (license_type,))
            else:
                cursor.execute("""
                    SELECT license_id, license_type, license_age_group
                    FROM license
                    WHERE license_type = ? AND license_age_group = ?
                """, (license_type, license_age_group))
            row = cursor.fetchone()
            if row:
                return License.from_dict({
                    "license_id": row[0],
                    "license_type": row[1],
                    "license_age_group": row[2]
                })
            return None
        except Exception as e:
            logging.error(f"Error retrieving license by type {license_type} and age group {license_age_group}: {e}")
            return None

    @staticmethod
    def get_by_id(cursor, license_id: int) -> Optional['License']:
        """Retrieve a License instance by license_id, or None if not found."""
        try:
            cursor.execute("""
                SELECT license_id, license_type, license_age_group
                FROM license
                WHERE license_id = ?
            """, (license_id,))
            row = cursor.fetchone()
            if row:
                return License.from_dict({
                    "license_id": row[0],
                    "license_type": row[1],
                    "license_age_group": row[2]
                })
            return None
        except Exception as e:
            logging.error(f"Error retrieving license by license_id {license_id}: {e}")
            return None