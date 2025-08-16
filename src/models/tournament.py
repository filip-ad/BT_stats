# src/models/tournament.py
from __future__ import annotations  # <= postpone annotation evaluation (robust)
from typing import List, Optional, Dict, Any, Sequence  # make sure this import exists
from ast import List
from dataclasses import dataclass
from datetime import date
import datetime
from typing import Optional, Dict
import logging
import sqlite3
from utils import parse_date, OperationLogger



@dataclass
class Tournament:
    tournament_id:          Optional[int] = None        # Canonical ID from tournament table
    tournament_id_ext:      Optional[str] = None        # External ID from ondata.se
    longname:               Optional[str] = None        # Full tournament name
    shortname:              Optional[str] = None        # Short name or abbreviation
    startdate:              Optional[date] = None       # Start date as a date object
    enddate:                Optional[date] = None       # End date as a date object
    city:                   Optional[str] = None        # City name
    arena:                  Optional[str] = None        # Arena name
    country_code:           Optional[str] = None        # Country code (e.g., 'SWE')
    url:                    Optional[str] = None        # Full tournament URL
    tournament_status_id:   Optional[int] = 6           # Status: 'ONGOING', 'UPCOMING', or 'ENDED'
    data_source_id:         Optional[int] = 1           # Data source ID (default 1 for 'ondata')

    STATUS_MAP = {
        'UPCOMING'  : 1,
        'ONGOING'   : 2,
        'ENDED'     : 3,
        'CANCELLED' : 4,
        'POSTPONED' : 5,
        'UNKNOWN'   : 6
    }

    @staticmethod
    def from_dict(data: dict) -> 'Tournament':
        """
        Factory method to create a Tournament instance from a dictionary.
        Handles date parsing and defaults.
        """
        sd = data.get("start_date") or data.get("startdate")
        ed = data.get("end_date")   or data.get("enddate")

        return Tournament(
            tournament_id           = data.get("tournament_id"),
            tournament_id_ext       = data.get("tournament_id_ext"),
            longname                = data.get("longname"),
            shortname               = data.get("shortname"),
            startdate               = parse_date(sd, context="Tournament.from_dict"),
            enddate                 = parse_date(ed, context="Tournament.from_dict"),
            city                    = data.get("city"),
            arena                   = data.get("arena"),
            country_code            = data.get("country_code"),
            url                     = data.get("url"),
            tournament_status_id    = data.get("tournament_status_id", 6),
            data_source_id          = data.get("data_source_id", 1)
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
            return {
                "status": "failed", 
                "key":    item_key,
                "reason": reason
            }

        return {
            "status":   "success", 
            "key":      item_key,
            "reason":   "Validated OK"
        }

    @classmethod
    def get_by_status(
        cls, 
        cursor: sqlite3.Cursor, 
        statuses: List[str]
    ) -> List["Tournament"]:
        status_ids = [cls.STATUS_MAP.get(s.upper(), 6) for s in statuses]
        sql = """
            SELECT * FROM tournament
            WHERE tournament_status_id IN ({})
            ORDER BY startdate ASC;
        """.format(', '.join('?' for _ in status_ids))
        cursor.execute(sql, tuple(status_ids))
        columns = [col[0] for col in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return [cls.from_dict(res) for res in results]

    def upsert(
            self, 
            cursor, 
            logger: OperationLogger,
            item_key: str
        ):
        """
        Upsert tournament to DB, log results.
        Handles unique constraints on (tournament_id_ext, data_source_id) or fallback (shortname, startdate).
        """
        values = (
            self.tournament_id_ext, self.longname, self.shortname, self.startdate, self.enddate,
            self.city, self.arena, self.country_code, self.url, self.tournament_status_id, self.data_source_id
        )

        if self.tournament_id_ext:

            # Check if exists (logging purposes)
            cursor.execute("SELECT tournament_id FROM tournament WHERE tournament_id_ext = ? AND data_source_id = ?;", (self.tournament_id_ext, self.data_source_id))
            existing = cursor.fetchone()
            is_update = existing is not None

            # Primary upsert on (tournament_id_ext, data_source_id)
            query_primary = """
                INSERT INTO tournament (
                    tournament_id_ext, longname, shortname, startdate, enddate, city, arena, 
                    country_code, url, tournament_status_id, data_source_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (tournament_id_ext, data_source_id) DO UPDATE SET
                    longname                = EXCLUDED.longname,
                    shortname               = EXCLUDED.shortname,
                    startdate               = EXCLUDED.startdate,
                    enddate                 = EXCLUDED.enddate,
                    city                    = EXCLUDED.city,
                    arena                   = EXCLUDED.arena,
                    country_code            = EXCLUDED.country_code,
                    url                     = EXCLUDED.url,
                    tournament_status_id    = EXCLUDED.tournament_status_id,
                    row_updated             = CURRENT_TIMESTAMP
                RETURNING tournament_id;
            """
            cursor.execute(query_primary, values)
            row = cursor.fetchone()
            if row:
                self.tournament_id = row[0]
                action = "updated" if is_update else "created"
                logger.success(item_key, f"Tournament {action} (primary: tournament_id_ext, data_source_id)")
                return
        else:
            logger.warning(item_key, "Fallback upsert due to missing or conflicting tournament_id_ext, data_source_id")

        # Fallback check if exists
        cursor.execute("SELECT tournament_id FROM tournament WHERE shortname = ? AND startdate = ?;", (self.shortname, self.startdate))
        existing = cursor.fetchone()
        is_update = existing is not None

        # Fallback upsert
        query_fallback = """
            INSERT INTO tournament (
                tournament_id_ext, longname, shortname, startdate, enddate, city, arena, 
                country_code, url, tournament_status_id, data_source_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (shortname, startdate) DO UPDATE SET
                tournament_id_ext       = EXCLUDED.tournament_id_ext,
                longname                = EXCLUDED.longname,
                enddate                 = EXCLUDED.enddate,
                city                    = EXCLUDED.city,
                arena                   = EXCLUDED.arena,
                country_code            = EXCLUDED.country_code,
                url                     = EXCLUDED.url,
                tournament_status_id    = EXCLUDED.tournament_status_id,
                row_updated             = CURRENT_TIMESTAMP
            RETURNING tournament_id;
        """
        cursor.execute(query_fallback, values)
        row = cursor.fetchone()
        if row:
            self.tournament_id = row[0]
            action = "updated" if is_update else "created"
            logger.success(item_key, f"Tournament {action} (fallback: shortname, startdate)")
            return
        else:
            logger.failed(item_key, "Upsert failed (no row returned)")