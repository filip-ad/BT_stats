# src/models/tournament_raw.py
from __future__ import annotations

from datetime import date
from typing import List, Optional, Dict, Tuple
from dataclasses import dataclass
import sqlite3
from utils import OperationLogger


@dataclass
class TournamentRaw:
    row_id:             Optional[int] = None
    tournament_id_ext:  Optional[str] = None
    longname:           Optional[str] = None
    shortname:          Optional[str] = None
    startdate:          Optional[date] = None
    enddate:            Optional[date] = None
    city:               Optional[str] = None
    arena:              Optional[str] = None
    country_code:       Optional[str] = None
    url:                Optional[str] = None
    data_source_id:     int = 1
    is_listed:          bool = True

    @staticmethod
    def from_dict(data: dict) -> 'TournamentRaw':
        """
        Factory method to create a TournamentRaw instance from a dictionary.
        """
        return TournamentRaw(
            row_id              = data.get("row_id"),
            tournament_id_ext   = data.get("tournament_id_ext"),
            longname            = data.get("longname"),
            shortname           = data.get("shortname"),
            startdate           = data.get("startdate"),
            enddate             = data.get("enddate"),
            city                = data.get("city"),
            arena               = data.get("arena"),
            country_code        = data.get("country_code"),
            url                 = data.get("url"),
            data_source_id      = data.get("data_source_id", 1),
            is_listed           = data.get("is_listed", True)
        )

    # def validate(self) -> Tuple[bool, str]:
    #     """
    #     Validate raw fields.
    #     Returns:
    #         (is_valid, error_message)
    #     """
    #     missing = []
    #     if not self.shortname:
    #         missing.append("shortname")
    #     if not self.startdate:
    #         missing.append("startdate")

    #     if missing:
    #         return False, f"Missing/invalid fields: {', '.join(missing)}"
    #     return True, ""
    
    def validate(self) -> Tuple[bool, str]:
        """
        Valid if either:
        1. tournament_id_ext + data_source_id
        OR
        2. shortname + startdate
        """
        if (self.tournament_id_ext and self.data_source_id) or (self.shortname and self.startdate):
            return True, ""
        return False, "Missing required fields: (tournament_id_ext and data_source_id) or (shortname and startdate)"



    def upsert(self, cursor: sqlite3.Cursor) -> str:
        """
        Upsert raw tournament data based on (tournament_id_ext, data_source_id) if tournament_id_ext is provided,
        otherwise based on (shortname, startdate, data_source_id).
        Returns "inserted" or "updated" to indicate the action performed.
        """

        vals = (
            self.tournament_id_ext, self.longname, self.shortname, self.startdate, self.enddate,
            self.city, self.arena, self.country_code, self.url, self.data_source_id, self.is_listed
        )

        action = None
        row_id = None

        if self.tournament_id_ext is not None:
            cursor.execute(
                "SELECT row_id FROM tournament_raw WHERE tournament_id_ext = ? AND data_source_id = ?;",
                (self.tournament_id_ext, self.data_source_id),
            )
            row = cursor.fetchone()
            if row:
                row_id = row[0]
                # UPDATE (do not change tournament_id_ext, as it's the lookup key and assumed consistent)
                cursor.execute(
                    """
                    UPDATE tournament_raw
                    SET longname        = ?,
                        shortname       = ?,
                        startdate       = ?,
                        enddate         = ?,
                        city            = ?,
                        arena           = ?,
                        country_code    = ?,
                        url             = ?,
                        is_listed       = ?,
                        row_updated     = CURRENT_TIMESTAMP
                    WHERE row_id = ?
                    RETURNING row_id;
                    """,
                    (self.longname, self.shortname, self.startdate, self.enddate,
                     self.city, self.arena, self.country_code, self.url, self.is_listed, row_id),
                )
                self.row_id = cursor.fetchone()[0]
                action = "updated"

        if action is None:
            # Not found by tournament_id_ext (or it was None), check by shortname/startdate
            cursor.execute(
                "SELECT row_id FROM tournament_raw WHERE shortname = ? AND startdate = ? AND data_source_id = ?;",
                (self.shortname, self.startdate, self.data_source_id),
            )
            row = cursor.fetchone()
            if row:
                row_id = row[0]
                # UPDATE (include setting tournament_id_ext, e.g., filling it in if previously None)
                cursor.execute(
                    """
                    UPDATE tournament_raw
                    SET tournament_id_ext = ?,
                        longname        = ?,
                        shortname       = ?,
                        startdate       = ?,
                        enddate         = ?,
                        city            = ?,
                        arena           = ?,
                        country_code    = ?,
                        url             = ?,
                        is_listed       = ?,
                        row_updated     = CURRENT_TIMESTAMP
                    WHERE row_id = ?
                    RETURNING row_id;
                    """,
                    (self.tournament_id_ext, self.longname, self.shortname, self.startdate, self.enddate,
                     self.city, self.arena, self.country_code, self.url, self.is_listed, row_id),
                )
                self.row_id = cursor.fetchone()[0]
                action = "updated"
            else:
                # INSERT
                cursor.execute(
                    """
                    INSERT INTO tournament_raw (
                        tournament_id_ext, longname, shortname, startdate, enddate, city, arena,
                        country_code, url, data_source_id, is_listed
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    RETURNING row_id;
                    """,
                    vals,
                )
                self.row_id = cursor.fetchone()[0]
                action = "inserted"

        return action

    # def upsert(self, cursor: sqlite3.Cursor) -> str:
    #     """
    #     Upsert raw tournament data based on (tournament_id_ext, data_source_id) if tournament_id_ext is provided,
    #     otherwise based on (shortname, startdate, data_source_id).
    #     Returns "inserted" or "updated" to indicate the action performed.
    #     """

    #     vals = (
    #         self.tournament_id_ext, self.longname, self.shortname, self.startdate, self.enddate,
    #         self.city, self.arena, self.country_code, self.url, self.data_source_id
    #     )

    #     action = None
    #     row_id = None

    #     if self.tournament_id_ext is not None:
    #         cursor.execute(
    #             "SELECT row_id FROM tournament_raw WHERE tournament_id_ext = ? AND data_source_id = ?;",
    #             (self.tournament_id_ext, self.data_source_id),
    #         )
    #         row = cursor.fetchone()
    #         if row:
    #             row_id = row[0]
    #             # UPDATE (do not change tournament_id_ext, as it's the lookup key and assumed consistent)
    #             cursor.execute(
    #                 """
    #                 UPDATE tournament_raw
    #                 SET longname        = ?,
    #                     shortname       = ?,
    #                     startdate       = ?,
    #                     enddate         = ?,
    #                     city            = ?,
    #                     arena           = ?,
    #                     country_code    = ?,
    #                     url             = ?,
    #                     row_updated     = CURRENT_TIMESTAMP
    #                 WHERE row_id = ?
    #                 RETURNING row_id;
    #                 """,
    #                 (self.longname, self.shortname, self.startdate, self.enddate,
    #                 self.city, self.arena, self.country_code, self.url, row_id),
    #             )
    #             self.row_id = cursor.fetchone()[0]
    #             action = "updated"

    #     if action is None:
    #         # Not found by tournament_id_ext (or it was None), check by shortname/startdate
    #         cursor.execute(
    #             "SELECT row_id FROM tournament_raw WHERE shortname = ? AND startdate = ? AND data_source_id = ?;",
    #             (self.shortname, self.startdate, self.data_source_id),
    #         )
    #         row = cursor.fetchone()
    #         if row:
    #             row_id = row[0]
    #             # UPDATE (include setting tournament_id_ext, e.g., filling it in if previously None)
    #             cursor.execute(
    #                 """
    #                 UPDATE tournament_raw
    #                 SET tournament_id_ext = ?,
    #                     longname        = ?,
    #                     shortname       = ?,
    #                     startdate       = ?,
    #                     enddate         = ?,
    #                     city            = ?,
    #                     arena           = ?,
    #                     country_code    = ?,
    #                     url             = ?,
    #                     row_updated     = CURRENT_TIMESTAMP
    #                 WHERE row_id = ?
    #                 RETURNING row_id;
    #                 """,
    #                 (self.tournament_id_ext, self.longname, self.shortname, self.startdate, self.enddate,
    #                 self.city, self.arena, self.country_code, self.url, row_id),
    #             )
    #             self.row_id = cursor.fetchone()[0]
    #             action = "updated"
    #         else:
    #             # INSERT
    #             cursor.execute(
    #                 """
    #                 INSERT INTO tournament_raw (
    #                     tournament_id_ext, longname, shortname, startdate, enddate, city, arena,
    #                     country_code, url, data_source_id
    #                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    #                 RETURNING row_id;
    #                 """,
    #                 vals,
    #             )
    #             self.row_id = cursor.fetchone()[0]
    #             action = "inserted"

    #     return action
    
    @classmethod
    def get_all(cls, cursor) -> List['TournamentRaw']:
        """
        Fetch all records from tournament_raw table.
        Returns a list of TournamentRaw objects.
        """
        cursor.execute("""
            SELECT row_id, tournament_id_ext, longname, shortname, startdate, enddate,
                   city, arena, country_code, url, data_source_id, is_listed
            FROM tournament_raw
            WHERE data_source_id = 1
        """)
        return [cls(*row) for row in cursor.fetchall()]