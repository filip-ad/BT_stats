# src/models/tournament.py

from dataclasses import dataclass
from typing import Optional
import logging
import sqlite3

@dataclass
class Tournament:
    tournament_id: Optional[int] = None     # Canonical ID from tournament table
    name: str = None                        # Tournament name
    startdate: str = None                   # Start date (string format, e.g., '2023.10.01')
    enddate: str = None                     # End date (string format)
    city: str = None                        # City name
    arena: str = None                       # Arena name
    country_code: str = None                # Country code (e.g., 'SWE')
    ondata_id: str = None                   # External ID from ondata.se
    url: str = None                         # Full tournament URL
    status: str = None                      # Status: 'ONGOING', 'UPCOMING', or 'ENDED'

    @staticmethod
    def from_dict(data: dict):
        """Create a Tournament instance from a dictionary."""
        return Tournament(
            tournament_id=data.get("tournament_id"),
            name=data.get("name"),
            startdate=data.get("start_date"),
            enddate=data.get("end_date"),
            city=data.get("city"),
            arena=data.get("arena"),
            country_code=data.get("country_code"),
            ondata_id=data.get("ondata_id"),
            url=data.get("url"),
            status=data.get("status")
        )

    @staticmethod
    def get_by_id(cursor, tournament_id: int) -> Optional['Tournament']:
        """Retrieve a Tournament instance by tournament_id, or None if not found."""
        try:
            cursor.execute("""
                SELECT tournament_id, name, startdate, enddate, city, arena, country_code,
                       ondata_id, url, status, row_created
                FROM tournament
                WHERE tournament_id = ?
            """, (tournament_id,))
            row = cursor.fetchone()
            if row:
                return Tournament.from_dict({
                    "tournament_id": row[0],
                    "name": row[1],
                    "start_date": row[2],
                    "end_date": row[3],
                    "city": row[4],
                    "arena": row[5],
                    "country_code": row[6],
                    "ondata_id": row[7],
                    "url": row[8],
                    "status": row[9],
                    "row_created": row[10]
                })
            return None
        except Exception as e:
            logging.error(f"Error retrieving tournament by tournament_id {tournament_id}: {e}")
            return None

    @staticmethod
    def get_by_ondata_id(cursor, ondata_id: str) -> Optional['Tournament']:
        """Retrieve a Tournament instance by ondata_id, or None if not found."""
        try:
            cursor.execute("""
                SELECT tournament_id, name, startdate, enddate, city, arena, country_code,
                       ondata_id, url, status, row_created
                FROM tournament
                WHERE ondata_id = ?
            """, (ondata_id,))
            row = cursor.fetchone()
            if row:
                return Tournament.from_dict({
                    "tournament_id": row[0],
                    "name": row[1],
                    "start_date": row[2],
                    "end_date": row[3],
                    "city": row[4],
                    "arena": row[5],
                    "country_code": row[6],
                    "ondata_id": row[7],
                    "url": row[8],
                    "status": row[9],
                    "row_created": row[10]
                })
            return None
        except Exception as e:
            logging.error(f"Error retrieving tournament by ondata_id {ondata_id}: {e}")
            return None

    def save_to_db(self, cursor):
        """Save the Tournament instance to the database, checking for duplicates."""
        if not self.name or not self.ondata_id:
            return {
                "status": "failed",
                "tournament": self.name or "Unknown",
                "reason": "Missing required fields (name or ondata_id)"
            }

        # Check for duplicate by ondata_id
        if Tournament.get_by_ondata_id(cursor, self.ondata_id):
            logging.debug(f"Skipping duplicate tournament: {self.name}")
            return {
                "status": "skipped",
                "tournament": self.name,
                "reason": "Tournament already exists"
            }

        try:
            cursor.execute("""
                INSERT INTO tournament (name, startdate, enddate, city, arena, country_code, ondata_id, url, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (self.name, self.startdate, self.enddate, self.city, self.arena,
                  self.country_code, self.ondata_id, self.url, self.status))
            self.tournament_id = cursor.lastrowid
            logging.debug(f"Inserted tournament into DB: {self.name}")
            return {
                "status": "success",
                "tournament": self.name,
                "reason": "Tournament inserted successfully",
                "tournament_id": self.tournament_id
            }
        except sqlite3.Error as e:
            logging.error(f"Error inserting tournament {self.name}: {e}")
            return {
                "status": "failed",
                "tournament": self.name,
                "reason": f"Insertion error: {e}"
            }