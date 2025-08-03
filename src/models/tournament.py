# src/models/tournament.py

from dataclasses import dataclass
from datetime import date
from typing import Optional
import logging
import sqlite3
from utils import parse_date
from typing import Optional, Any, Dict, List


@dataclass
class Tournament:
    tournament_id: Optional[int] = None     # Canonical ID from tournament table
    name: str = None                        # Tournament name
    startdate: Optional[date] = None       # Start date as a date object
    enddate:   Optional[date] = None       # End date as a date object
    city: str = None                        # City name
    arena: str = None                       # Arena name
    country_code: str = None                # Country code (e.g., 'SWE')
    ondata_id: str = None                   # External ID from ondata.se
    url: str = None                         # Full tournament URL
    status: str = None                      # Status: 'ONGOING', 'UPCOMING', or 'ENDED'

    @staticmethod
    def from_dict(data: dict):
        """
        Build a Tournament from a dict.
        """

        sd = data.get("start_date") or data.get("startdate")
        ed = data.get("end_date")   or data.get("enddate")

        return Tournament(
            tournament_id=data.get("tournament_id"),
            name=data.get("name"),
            startdate=parse_date(sd, context="Tournament.from_dict"),
            enddate=  parse_date(ed, context="Tournament.from_dict"),
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
                    "tournament_id":    row[0],
                    "name":             row[1],
                    "start_date":       row[2],
                    "end_date":         row[3],
                    "city":             row[4],
                    "arena":            row[5],
                    "country_code":     row[6],
                    "ondata_id":        row[7],
                    "url":              row[8],
                    "status":           row[9],
                })
            return None
        except Exception as e:
            logging.error(f"Error retrieving tournament by tournament_id {tournament_id}: {e}")
            return None

    @staticmethod
    def get_by_ondata_id(cursor, ondata_id: str) -> Optional["Tournament"]:
        """Retrieve a Tournament instance by ondata_id, or None if not found."""
        try:
            cursor.execute("""
                SELECT tournament_id, name, startdate, enddate,
                       city, arena, country_code, ondata_id, url, status
                  FROM tournament
                 WHERE ondata_id = ?
            """, (ondata_id,))
            row = cursor.fetchone()
            if not row:
                return None

            # build directly, passing a dict into from_dict
            data = {
                "tournament_id": row[0],
                "name":          row[1],
                "startdate":     row[2],
                "enddate":       row[3],
                "city":          row[4],
                "arena":         row[5],
                "country_code":  row[6],
                "ondata_id":     row[7],
                "url":           row[8],
                "status":        row[9],
            }
            return Tournament.from_dict(data)

        except Exception as e:
            logging.error(f"Error retrieving tournament by ondata_id {ondata_id}: {e}")
            return None


    def save_to_db(self, cursor):
        """Save the Tournament instance to the database, checking for duplicates."""
        if not (self.name and self.ondata_id and self.startdate and self.enddate):
            return {
                "status": "failed",
                "key": self.name or "Unknown",
                "reason": "Missing one of required fields (name, ondata_id, dates)"
            }

        # Check for duplicate by ondata_id
        if Tournament.get_by_ondata_id(cursor, self.ondata_id):
            logging.debug(f"Skipping duplicate tournament: {self.name}")
            return {
                "status": "skipped",
                "key": self.name,
                "reason": "Tournament already exists"
            }

        try:
            cursor.execute("""
                INSERT INTO tournament (name, startdate, enddate, city, arena, country_code, ondata_id, url, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                self.name, 
                self.startdate, 
                self.enddate, 
                self.city, 
                self.arena,
                self.country_code, 
                self.ondata_id, 
                self.url, 
                self.status
                ))
            self.tournament_id = cursor.lastrowid
            logging.debug(f"Inserted tournament into DB: {self.name}")
            return {
                "status": "success",
                "key": self.name,
                "reason": "Tournament inserted successfully"
            }
        except sqlite3.Error as e:
            logging.error(f"Error inserting tournament {self.name}: {e}")
            return {
                "status": "failed",
                "key": self.name,
                "reason": f"Insertion error: {e}"
            }
        
    @staticmethod
    def get_by_status(cursor, statuses: List[str] = ["ONGOING", "ENDED"]) -> List["Tournament"]:
        """
        Load all tournaments whose status is in the given list.
        Returns a list of Tournament instances.
        """
        placeholder = ",".join("?" for _ in statuses)
        sql = f"""
            SELECT tournament_id, name, startdate, enddate,
                   city, arena, country_code,
                   ondata_id, url, status
              FROM tournament
             WHERE status IN ({placeholder})
        """
        try:
            cursor.execute(sql, statuses)
            rows = cursor.fetchall()
            result: List[Tournament] = []
            for (tid, name, sd, ed, city, arena, ccode, oid, url, status) in rows:
                result.append(
                    Tournament.from_dict({
                        "tournament_id": tid,
                        "name":          name,
                        "start_date":    sd,
                        "end_date":      ed,
                        "city":          city,
                        "arena":         arena,
                        "country_code":  ccode,
                        "ondata_id":     oid,
                        "url":           url,
                        "status":        status,
                    })
                )
            return result

        except Exception as e:
            logging.error(f"Error in Tournament.fetch_by_status({statuses}): {e}")
            return []