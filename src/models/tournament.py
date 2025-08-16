# src/models/tournament.py

from dataclasses import dataclass
from datetime import date
import datetime
from typing import Optional, Dict
import logging
import sqlite3
from utils import parse_date, OperationLogger


@dataclass
class Tournament:
    tournament_id:      Optional[int] = None        # Canonical ID from tournament table
    tournament_id_ext:  Optional[str] = None        # External ID from ondata.se
    longname:           Optional[str] = None        # Full tournament name
    shortname:          Optional[str] = None        # Short name or abbreviation
    startdate:          Optional[date] = None       # Start date as a date object
    enddate:            Optional[date] = None       # End date as a date object
    city:               Optional[str] = None        # City name
    arena:              Optional[str] = None        # Arena name
    country_code:       Optional[str] = None        # Country code (e.g., 'SWE')
    url:                Optional[str] = None        # Full tournament URL
    status:             Optional[str] = None        # Status: 'ONGOING', 'UPCOMING', or 'ENDED'
    data_source_id:     int = 1                     # Data source ID (default 1 for 'ondata')

    @staticmethod
    def from_dict(data: dict) -> 'Tournament':
        """
        Factory method to create a Tournament instance from a dictionary.
        Handles date parsing and defaults.
        """
        sd = data.get("start_date") or data.get("startdate")
        ed = data.get("end_date")   or data.get("enddate")

        return Tournament(
            tournament_id       = data.get("tournament_id"),
            tournament_id_ext   = data.get("tournament_id_ext"),
            longname            = data.get("longname"),
            shortname           = data.get("shortname"),
            startdate           = parse_date(sd, context="Tournament.from_dict"),
            enddate             = parse_date(ed, context="Tournament.from_dict"),
            city                = data.get("city"),
            arena               = data.get("arena"),
            country_code        = data.get("country_code"),
            url                 = data.get("url"),
            status              = data.get("status"),
            data_source_id      = data.get("data_source_id", 1)
        )

    def validate(
            self, 
            logger:     OperationLogger,
            item_key:   Optional[str]
        ) -> Dict[str, any]:
        """
        Validate Tournament fields, log to OperationalLogger.
        Returns dict with status, reason, warnings (True equivalent is "success" status).
        Called explicitly in pipeline after from_dict.
        """

        # Warn
        if not self.tournament_id_ext and not self.url:
            logger.warning(item_key, "No valid external ID or URL (likely upcoming)")
        if not self.longname:
            logger.warning(item_key, "Missing longname")

        # Fail
        if not (self.shortname and self.startdate and self.enddate):
            reason = "Missing required fields"
            logger.failed(item_key, reason)
            return {"status": "failed", "reason": reason}

        return {
            "status": "success", 
            "reason": "Validated OK"}

    # Warn
    def upsert_to_db(
            self, 
            cursor: sqlite3.Cursor, 
            logger: OperationLogger
        ) -> None:
        """
        Upsert Tournament to DB: insert if not exists, update if duplicate based on UNIQUE constraints.
        Logs results using the provided OperationLogger instance.
        """
        item_key = f"{self.shortname} ({self.startdate.isoformat() if self.startdate else 'None'})"

        # Check for existing by (tournament_id_ext, data_source_id)
        cursor.execute("""
            SELECT 
                tournament_id 
            FROM tournament 
            WHERE tournament_id_ext = ? AND data_source_id = ?
        """, (self.tournament_id_ext, self.data_source_id))
        existing_id = cursor.fetchone()

        if existing_id:
            try:
                cursor.execute("""
                    UPDATE tournament
                    SET longname = ?, 
                        shortname = ?, 
                        startdate = ?, 
                        enddate = ?,
                        city = ?, 
                        arena = ?, 
                        country_code = ?, 
                        url = ?, 
                        status = ?
                    WHERE tournament_id = ?
                """, (
                    self.longname, 
                    self.shortname,
                    self.startdate.isoformat()  if self.startdate   else None,
                    self.enddate.isoformat()    if self.enddate     else None,
                    self.city,
                    self.arena, 
                    self.country_code, 
                    self.url, 
                    self.status,
                    existing_id[0]
                ))
                self.tournament_id = existing_id[0]
                self.tournament_id_ext = self.tournament_id_ext
                item_key = f"{self.shortname} ({self.startdate.isoformat() if self.startdate else 'None'}, id: {self.tournament_id}, ext_id: {self.tournament_id_ext})"
                logger.success(item_key, "Tournament updated successfully")
            except Exception as e:
                print(f"Error updating tournament {self.shortname}: {e}")
                logger.failure(item_key, str(e))
            return

        # Check for existing by (shortname, startdate) if no ext ID
        cursor.execute("""
            SELECT 
                tournament_id 
            FROM tournament 
            WHERE shortname = ? AND startdate = ?
        """, (self.shortname, self.startdate.isoformat() if self.startdate else None))
        existing_id = cursor.fetchone()

        if existing_id:
            try:
                cursor.execute("""
                    UPDATE tournament
                    SET longname = ?, 
                        startdate = ?, 
                        enddate = ?,
                        city = ?, 
                        arena = ?, 
                        country_code = ?, 
                        url = ?, 
                        status = ?, 
                        data_source_id = ?
                    WHERE tournament_id = ?
                """, (
                    self.longname,
                    self.startdate.isoformat() if self.startdate else None,
                    self.enddate.isoformat() if self.enddate else None,
                    self.city, 
                    self.arena, 
                    self.country_code, 
                    self.url, 
                    self.status, 
                    self.data_source_id,
                    existing_id[0]
                ))
                self.tournament_id = existing_id[0]
                self.tournament_id_ext = self.tournament_id_ext
                item_key = f"{self.shortname} ({self.startdate.isoformat() if self.startdate else 'None'}, id: {self.tournament_id}, ext_id: {self.tournament_id_ext})"
                logger.success(item_key, "Tournament updated successfully")
            except Exception as e:
                print(f"Error updating tournament {self.shortname}: {e}")
                logger.failure(item_key, str(e))
            return

        # Insert new row if no duplicates
        try:
            cursor.execute("""
                INSERT INTO tournament (
                    tournament_id_ext, 
                    longname,
                    shortname, 
                    startdate, 
                    enddate,
                    city, 
                    arena, 
                    country_code, 
                    url, 
                    status, 
                    data_source_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                self.tournament_id_ext, 
                self.longname, 
                self.shortname,
                self.startdate.isoformat() if self.startdate else None,
                self.enddate.isoformat() if self.enddate else None,
                self.city, 
                self.arena, 
                self.country_code, 
                self.url, 
                self.status, 
                self.data_source_id
            ))
            self.tournament_id = existing_id[0]
            self.tournament_id_ext = self.tournament_id_ext
            item_key = f"{self.shortname} ({self.startdate.isoformat() if self.startdate else 'None'}, id: {self.tournament_id}, ext_id: {self.tournament_id_ext})"
        except Exception as e:
            print(f"Error inserting tournament {self.shortname}: {e}")
            logger.failure(item_key, str(e))