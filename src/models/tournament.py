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
    tournament_id_ext: Optional[str] = None # External ID from ondata.se
    name: str = None                        # Tournament name
    startdate: Optional[date] = None        # Start date as a date object
    enddate:   Optional[date] = None        # End date as a date object
    city: str = None                        # City name
    arena: str = None                       # Arena name
    country_code: str = None                # Country code (e.g., 'SWE')
    ondata_id: str = None                   # External ID from ondata.se
    url: str = None                         # Full tournament URL
    status: str = None                      # Status: 'ONGOING', 'UPCOMING', or 'ENDED'
    data_source: str = "ondata"             # Data source, default is 'ondata'

    @staticmethod
    def from_dict(data: dict):
        """
        Build a Tournament from a dict.
        """
        sd = data.get("start_date") or data.get("startdate")
        ed = data.get("end_date")   or data.get("enddate")

        return Tournament(
            tournament_id=data.get("tournament_id"),
            tournament_id_ext=data.get("tournament_id_ext"),
            name=data.get("name"),
            startdate=parse_date(sd, context="Tournament.from_dict"),
            enddate=  parse_date(ed, context="Tournament.from_dict"),
            city=data.get("city"),
            arena=data.get("arena"),
            country_code=data.get("country_code"),
            url=data.get("url"),
            status=data.get("status"),
            data_source=data.get("data_source", "ondata")
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
                    "tournament_id":        row[0],
                    "tournament_id_ext":    row[1],
                    "name":                 row[2],
                    "start_date":           row[3],
                    "end_date":             row[4],
                    "city":                 row[5],
                    "arena":                row[6],
                    "country_code":         row[7],
                    "url":                  row[8],
                    "status":               row[9],
                    "data_source":          row[10],
                })
            return None
        except Exception as e:
            logging.error(f"Error retrieving tournament by tournament_id {tournament_id}: {e}")
            return None

    @staticmethod
    def get_by_id_ext(cursor, tournament_id_ext: str) -> Optional["Tournament"]:
        """Retrieve a Tournament instance by tournament_id_ext, or None if not found."""
        try:
            cursor.execute("""
                SELECT tournament_id, tournament_id_ext, name, startdate, enddate,
                       city, arena, country_code, url, status, data_source
                  FROM tournament
                 WHERE tournament_id_ext = ?
            """, (tournament_id_ext,))
            row = cursor.fetchone()
            if not row:
                return None

            # build directly, passing a dict into from_dict
            data = {
                "tournament_id":        row[0],
                "tournament_id_ext":    row[1],
                "name":                 row[2],
                "startdate":            row[3],
                "enddate":              row[4],
                "city":                 row[5],
                "arena":                row[6],
                "country_code":         row[7],
                "url":                  row[8],
                "status":               row[9],
                "data_source":          row[10],
            }
            return Tournament.from_dict(data)

        except Exception as e:
            logging.error(f"Error retrieving tournament by ondata_id {tournament_id_ext}: {e}")
            return None


    def save_to_db(self, cursor):
        """Save the Tournament instance to the database, checking for duplicates."""
        if not (self.name and self.startdate and self.enddate):
            return {
                "status":   "failed",
                "key":      self.name or "Unknown",
                "reason":   "Missing one of required fields (name, startdate, enddate)"
            }

        # Check for duplicate by tournament_id_ext
        if Tournament.get_by_id_ext(cursor, self.tournament_id_ext):
            logging.warning(f"Skipping duplicate tournament: {self.name} ({self.tournament_id_ext})")
            return {
                "status":   "skipped",
                "key":      self.tournament_id_ext,
                "reason":   "Tournament already exists"
            }

        try:
            cursor.execute("""
                INSERT INTO tournament (
                    tournament_id_ext, 
                    name, 
                    startdate, 
                    enddate, 
                    city, 
                    arena, 
                    country_code, 
                    url, 
                    status, 
                    data_source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                self.tournament_id_ext,
                self.name,
                self.startdate,
                self.enddate,
                self.city, 
                self.arena,
                self.country_code, 
                self.url, 
                self.status,
                self.data_source
            ))
            self.tournament_id = cursor.lastrowid
            logging.debug(f"Inserted tournament into DB: {self.name}")
            return {
                "status":   "success",
                "key":      f"{self.name} ({self.tournament_id})",
                "reason":   "Tournament inserted successfully"
            }
        
        except sqlite3.Error as e:
            logging.error(f"Error inserting tournament {self.name}: {e}")
            return {
                "status":   "failed",
                "key":      self.name,
                "reason":   f"Insertion error: {e}"
            }
        
    @staticmethod
    def get_by_status(cursor, statuses: List[str] = ["ONGOING", "ENDED"]) -> List["Tournament"]:
        """
        Load all tournaments whose status is in the given list.
        Returns a list of Tournament instances.
        """
        placeholder = ",".join("?" for _ in statuses)
        sql = f"""
            SELECT 
                tournament_id, 
                tournament_id_ext, 
                name, 
                startdate, 
                enddate,
                city, 
                arena, 
                country_code,
                url, 
                status, 
                data_source
              FROM tournament
             WHERE status IN ({placeholder})
        """
        try:
            cursor.execute(sql, statuses)
            rows = cursor.fetchall()
            result: List[Tournament] = []
            for (tid, tid_ext, name, sd, ed, city, arena, ccode, url, status, src) in rows:
                result.append(
                    Tournament.from_dict({
                        "tournament_id":        tid,
                        "tournament_id_ext":    tid_ext,
                        "name":                 name,
                        "start_date":           sd,
                        "end_date":             ed,
                        "city":                 city,
                        "arena":                arena,
                        "country_code":         ccode,
                        "url":                  url,
                        "status":               status,
                        "data_source":          src
                    })
                )
            return result

        except Exception as e:
            logging.error(f"Error in Tournament.get_by_status({statuses}): {e}")
            return []