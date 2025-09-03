# src/models/tournament.py
from __future__ import annotations  # <= postpone annotation evaluation (robust)

from typing import List, Optional, Dict, Tuple # make sure this import exists
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

    def validate(self) -> Tuple[bool, str]:
        """
        Validate fields.
        Returns:
            (is_valid, error_message)
        """
        missing = []
        if not self.shortname and not self.longname:
            missing.append("shortname and longname")
        if not self.startdate:
            missing.append("startdate")

        if missing:
            return False, f"Missing/invalid fields: {', '.join(missing)}"
        return True, ""
    
    @classmethod
    def get_internal_tournament_ids(
        cls,
        cursor: sqlite3.Cursor,
        tournament_id_exts: List[str],
        data_source_id: int
    ) -> List[int]:
        """Convert external tournament IDs to internal tournament_ids."""
        if not tournament_id_exts:
            return []

        placeholders = ",".join("?" for _ in tournament_id_exts)
        sql = f"""
            SELECT tournament_id
            FROM tournament_id_ext
            WHERE tournament_id_ext IN ({placeholders})
            AND data_source_id = ?
        """
        params = tournament_id_exts + [data_source_id]
        cursor.execute(sql, params)
        return [row[0] for row in cursor.fetchall()]

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

        cursor.execute(sql, ext_ids)
        columns = [col[0] for col in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        tournaments = [Tournament.from_dict(row) for row in rows]
        
        if len(tournaments) != len(ext_ids):
            missing = set(ext_ids) - {t.tournament_id_ext for t in tournaments}
            logger.warning("", f"Missing tournaments for ext_ids: {missing}")
        
        return tournaments
    

    def upsert(self, cursor: sqlite3.Cursor) -> Optional[str]:
        """
        Upsert tournament data based on (tournament_id_ext, data_source_id) if tournament_id_ext is provided,
        otherwise based on (shortname, startdate).
        Returns "inserted" or "updated" on success, None on no change.
        """

        vals = (
            self.tournament_id_ext, self.longname, self.shortname, self.startdate, self.enddate,
            self.city, self.arena, self.country_code, self.url, self.tournament_status_id, self.data_source_id
        )

        action = None
        tournament_id = None

        if self.tournament_id_ext is not None:
            cursor.execute(
                "SELECT tournament_id FROM tournament WHERE tournament_id_ext = ? AND data_source_id = ?;",
                (self.tournament_id_ext, self.data_source_id),
            )
            row = cursor.fetchone()
            if row:
                tournament_id = row[0]
                # UPDATE (do not change tournament_id_ext, as it's the lookup key and assumed consistent)
                cursor.execute(
                    """
                    UPDATE tournament
                    SET longname              = ?,
                        shortname             = ?,
                        startdate             = ?,
                        enddate               = ?,
                        city                  = ?,
                        arena                 = ?,
                        country_code          = ?,
                        url                   = ?,
                        tournament_status_id  = ?,
                        row_updated           = CURRENT_TIMESTAMP
                    WHERE tournament_id = ?
                    RETURNING tournament_id;
                    """,
                    (self.longname, self.shortname, self.startdate, self.enddate,
                     self.city, self.arena, self.country_code, self.url, self.tournament_status_id, tournament_id),
                )
                self.tournament_id = cursor.fetchone()[0]
                action = "updated"

        if action is None:
            # Not found by tournament_id_ext (or it was None), check by shortname/startdate
            if self.shortname and self.startdate:
                cursor.execute(
                    "SELECT tournament_id FROM tournament WHERE shortname = ? AND startdate = ?;",
                    (self.shortname, self.startdate),
                )
                row = cursor.fetchone()
                if row:
                    tournament_id = row[0]
                    # UPDATE (include setting tournament_id_ext and data_source_id, e.g., filling them in if previously None)
                    cursor.execute(
                        """
                        UPDATE tournament
                        SET tournament_id_ext     = ?,
                            longname              = ?,
                            startdate             = ?,
                            enddate               = ?,
                            city                  = ?,
                            arena                 = ?,
                            country_code          = ?,
                            url                   = ?,
                            tournament_status_id  = ?,
                            data_source_id        = ?,
                            row_updated           = CURRENT_TIMESTAMP
                        WHERE tournament_id = ?
                        RETURNING tournament_id;
                        """,
                        (self.tournament_id_ext, self.longname, self.startdate, self.enddate,
                         self.city, self.arena, self.country_code, self.url, self.tournament_status_id,
                         self.data_source_id, tournament_id),
                    )
                    self.tournament_id = cursor.fetchone()[0]
                    action = "updated"

        if action is None:
            # INSERT (only if we have enough data)
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
            action = "inserted"

        return action