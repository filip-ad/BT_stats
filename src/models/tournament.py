# src/models/tournament.py
from __future__ import annotations  # <= postpone annotation evaluation (robust)

from typing import List, Optional, Dict, Any, Sequence  # make sure this import exists
from ast import List
from dataclasses import dataclass
from datetime import date
from typing import Optional, Dict
import sqlite3
from models.cache_mixin import CacheMixin
from utils import parse_date, OperationLogger



@dataclass
class Tournament(CacheMixin):
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

    # Rewrite this and get rid of the MAP....
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
    
    # Used for testing in upd_tournament_classes
    @staticmethod
    def get_by_ext_ids(cursor, logger: OperationLogger, ext_ids: List[str]) -> List['Tournament']:
        """
        Fetch Tournament instances by a list of tournament_id_ext.
        Returns list of matching tournaments.
        """
        if not ext_ids:
            return []

        placeholders = ','.join('?' for _ in ext_ids)
        sql = f"""
            SELECT * FROM tournament
            WHERE tournament_id_ext IN ({placeholders})
        """

        # print(sql)

        cursor.execute(sql, ext_ids)
        columns = [col[0] for col in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        tournaments = [Tournament.from_dict(row) for row in rows]

        # print(tournaments)
        
        if len(tournaments) != len(ext_ids):
            missing = set(ext_ids) - {t.tournament_id_ext for t in tournaments}
            logger.warning("", f"Missing tournaments for ext_ids: {missing}")
        
        return tournaments
    

    def upsert(self, cursor, logger: OperationLogger, item_key: str):
        """
        Robust upsert that reconciles prior fallback rows (shortname,startdate)
        with newly-known (tournament_id_ext, data_source_id).
        """
        vals = (
            self.tournament_id_ext, self.longname, self.shortname, self.startdate, self.enddate,
            self.city, self.arena, self.country_code, self.url, self.tournament_status_id, self.data_source_id
        )

        # 1) Prefer an exact match on (tournament_id_ext, data_source_id)
        primary_id = None
        if self.tournament_id_ext:
            cursor.execute(
                "SELECT tournament_id FROM tournament WHERE tournament_id_ext = ? AND data_source_id = ?;",
                (self.tournament_id_ext, self.data_source_id),
            )
            row = cursor.fetchone()
            if row:
                primary_id = row[0]

        # 2) Else, look for fallback match on (shortname, startdate)
        fallback_id = None
        cursor.execute(
            "SELECT tournament_id FROM tournament WHERE shortname = ? AND startdate = ?;",
            (self.shortname, self.startdate),
        )
        row = cursor.fetchone()
        if row:
            fallback_id = row[0]

        # If both exist and are different rows, don't try to be clever; log and stop to avoid data corruption
        if primary_id and fallback_id and primary_id != fallback_id:
            logger.failed(
                item_key,
                f"Conflicting tournaments: ext={self.tournament_id_ext}/ds={self.data_source_id} → id {primary_id}, "
                f"shortname/startdate → id {fallback_id}. Manual merge required."
            )
            self.tournament_id = primary_id
            return

        target_id = primary_id or fallback_id

        if target_id:
            # UPDATE existing row (attach ext if it was missing, update other fields)
            cursor.execute(
                """
                UPDATE tournament
                SET tournament_id_ext     = COALESCE(?, tournament_id_ext),
                    longname              = ?,
                    shortname             = ?,
                    startdate             = ?,
                    enddate               = ?,
                    city                  = ?,
                    arena                 = ?,
                    country_code          = ?,
                    url                   = ?,
                    tournament_status_id  = ?,
                    data_source_id        = COALESCE(?, data_source_id),
                    row_updated           = CURRENT_TIMESTAMP
                WHERE tournament_id = ?
                RETURNING tournament_id;
                """,
                (*vals, target_id),
            )
            self.tournament_id = cursor.fetchone()[0]
            logger.success(item_key, f"Tournament successfully updated")
            return

        # 3) No existing row → INSERT (with a retry if a concurrent row slipped in)
        try:
            cursor.execute(
                """
                INSERT INTO tournament (
                    tournament_id_ext, longname, shortname, startdate, enddate, city, arena,
                    country_code, url, tournament_status_id, data_source_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING tournament_id;
                """,
                vals,
            )
            self.tournament_id = cursor.fetchone()[0]
            logger.success(item_key, "Tournament created")
            return
        except sqlite3.IntegrityError:
            # Retry path: someone inserted between our checks and INSERT
            cursor.execute(
                "SELECT tournament_id FROM tournament WHERE tournament_id_ext = ? AND data_source_id = ?;",
                (self.tournament_id_ext, self.data_source_id),
            )
            row = cursor.fetchone()
            if not row:
                cursor.execute(
                    "SELECT tournament_id FROM tournament WHERE shortname = ? AND startdate = ?;",
                    (self.shortname, self.startdate),
                )
                row = cursor.fetchone()
            if row:
                target_id = row[0]
                cursor.execute(
                    """
                    UPDATE tournament
                    SET tournament_id_ext     = COALESCE(?, tournament_id_ext),
                        longname              = ?,
                        shortname             = ?,
                        startdate             = ?,
                        enddate               = ?,
                        city                  = ?,
                        arena                 = ?,
                        country_code          = ?,
                        url                   = ?,
                        tournament_status_id  = ?,
                        data_source_id        = COALESCE(?, data_source_id),
                        row_updated           = CURRENT_TIMESTAMP
                    WHERE tournament_id = ?
                    RETURNING tournament_id;
                    """,
                    (*vals, target_id),
                )
                self.tournament_id = cursor.fetchone()[0]
                logger.success(item_key, f"Tournament updated after race condition")
                return
            raise