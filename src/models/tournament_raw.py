# src/models/tournament_raw.py
from __future__ import annotations

from datetime import date
from typing import List, Optional, Tuple
from dataclasses import dataclass
import sqlite3
from utils import parse_date


@dataclass
class TournamentRaw:
    row_id:                 Optional[int] = None
    tournament_id_ext:      Optional[str] = None
    shortname:              Optional[str] = None
    longname:               Optional[str] = None
    startdate:              Optional[date] = None
    enddate:                Optional[date] = None
    registration_end_date:  Optional[date] = None
    city:                   Optional[str] = None
    arena:                  Optional[str] = None
    country_code:           Optional[str] = None
    url:                    Optional[str] = None
    tournament_level:       Optional[str] = None
    tournament_type:        Optional[str] = None
    organiser_name:         Optional[str] = None
    organiser_email:        Optional[str] = None
    organiser_phone:        Optional[str] = None
    data_source_id:         int = 1
    is_listed:              bool = True

    @staticmethod
    def from_dict(data: dict) -> 'TournamentRaw':
        """
        Factory method to create a TournamentRaw instance from a dictionary.
        """
        return TournamentRaw(
            row_id                  = data.get("row_id"),
            tournament_id_ext       = data.get("tournament_id_ext"),
            shortname               = data.get("shortname"),
            longname                = data.get("longname"),
            startdate               = parse_date(data.get("startdate")),
            enddate                 = parse_date(data.get("enddate")),
            registration_end_date   = parse_date(data.get("registration_end_date")),
            city                    = data.get("city"),
            arena                   = data.get("arena"),
            country_code            = data.get("country_code"),
            url                     = data.get("url"),
            tournament_level        = data.get("tournament_level"),
            tournament_type         = data.get("tournament_type"),
            organiser_name          = data.get("organiser_name"),
            organiser_email         = data.get("organiser_email"),
            organiser_phone         = data.get("organiser_phone"),
            data_source_id          = data.get("data_source_id", 1),
            is_listed               = data.get("is_listed", True)
        )
    
    def validate(self) -> Tuple[bool, str]:
        """
        Valid if either:
        1. tournament_id_ext + data_source_id
        OR
        2. shortname + startdate + arena
        """
        if (self.tournament_id_ext and self.data_source_id) or (self.shortname and self.startdate and self.arena):
            return True, ""
        return False, "Missing required fields: (tournament_id_ext and data_source_id) or (shortname and startdate and arena)"


    def upsert(self, cursor: sqlite3.Cursor) -> str:
        """
        Upsert raw tournament data based on (tournament_id_ext, data_source_id) if tournament_id_ext is provided,
        otherwise based on (shortname, startdate, arena, data_source_id).
        Returns "inserted" or "updated" to indicate the action performed.
        """

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
                    SET shortname               = ?,
                        longname                = ?,
                        startdate               = ?,
                        enddate                 = ?,
                        registration_end_date   = ?,
                        city                    = ?,
                        arena                   = ?,
                        country_code            = ?,
                        url                     = ?,
                        tournament_level        = ?,
                        tournament_type         = ?,
                        organiser_name          = ?,
                        organiser_email         = ?,
                        organiser_phone         = ?,
                        is_listed               = ?,
                        row_updated             = CURRENT_TIMESTAMP
                    WHERE row_id = ?
                    RETURNING row_id;
                    """,
                    (self.shortname, 
                     self.longname, 
                     self.startdate, 
                     self.enddate, 
                     self.registration_end_date,
                     self.city, 
                     self.arena, 
                     self.country_code, 
                     self.url, 
                     self.tournament_level, 
                     self.tournament_type,
                     self.organiser_name, 
                     self.organiser_email, 
                     self.organiser_phone, 
                     self.is_listed, 
                     row_id),
                )
                self.row_id = cursor.fetchone()[0]
                action = "updated"

        if action is None:
            # Not found by tournament_id_ext (or it was None), check by shortname/startdate/arena/data source
            cursor.execute(
                "SELECT row_id FROM tournament_raw WHERE shortname = ? AND startdate = ? AND arena = ? AND data_source_id = ?;",
                (self.shortname, self.startdate, self.arena, self.data_source_id),
            )
            row = cursor.fetchone()
            if row:
                row_id = row[0]
                # UPDATE (include setting tournament_id_ext, e.g., filling it in if previously None)
                cursor.execute(
                    """
                    UPDATE tournament_raw
                    SET tournament_id_ext       = ?,
                        shortname               = ?,
                        longname                = ?,
                        startdate               = ?,
                        enddate                 = ?,
                        registration_end_date   = ?,
                        city                    = ?,
                        arena                   = ?,
                        country_code            = ?,
                        url                     = ?,
                        tournament_level        = ?,
                        tournament_type         = ?,
                        organiser_name          = ?,
                        organiser_email         = ?,
                        organiser_phone         = ?,
                        is_listed               = ?,
                        row_updated         = CURRENT_TIMESTAMP
                    WHERE row_id = ?
                    RETURNING row_id;
                    """,
                    (self.tournament_id_ext, 
                     self.shortname, 
                     self.longname, 
                     self.startdate, 
                     self.enddate, 
                     self.registration_end_date,
                     self.city, 
                     self.arena, 
                     self.country_code, 
                     self.url, 
                     self.tournament_level, 
                     self.tournament_type,
                     self.organiser_name, 
                     self.organiser_email, 
                     self.organiser_phone, 
                     self.is_listed, 
                     row_id),
                )
                self.row_id = cursor.fetchone()[0]
                action = "updated"
            else:
                # INSERT
                cursor.execute(
                    """
                    INSERT INTO tournament_raw (
                        tournament_id_ext, 
                        shortname, 
                        longname, 
                        startdate, 
                        enddate, 
                        registration_end_date,
                        city, 
                        arena,
                        country_code,
                        url,
                        tournament_level,
                        tournament_type,
                        organiser_name,
                        organiser_email,
                        organiser_phone,
                        data_source_id,
                        is_listed
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    RETURNING row_id;
                    """,
                    (self.tournament_id_ext, 
                     self.shortname, 
                     self.longname, 
                     self.startdate, 
                     self.enddate, 
                     self.registration_end_date,
                     self.city, 
                     self.arena, 
                     self.country_code, 
                     self.url, 
                     self.tournament_level, 
                     self.tournament_type,
                     self.organiser_name, 
                     self.organiser_email, 
                     self.organiser_phone, 
                     self.data_source_id,
                     self.is_listed, 
                    )
                )
                self.row_id = cursor.fetchone()[0]
                action = "inserted"

        return action

    @classmethod
    def get_all(cls, cursor) -> List['TournamentRaw']:
        """
        Fetch all records from tournament_raw table.
        Returns a list of TournamentRaw objects.
        """
        cursor.execute("""
            SELECT 
                row_id,
                tournament_id_ext,
                shortname,
                longname,
                startdate,
                enddate,
                registration_end_date,
                city,
                arena,
                country_code,
                url,
                tournament_level,
                tournament_type,
                organiser_name,
                organiser_email,
                organiser_phone,
                data_source_id,
                is_listed
            FROM tournament_raw
            WHERE data_source_id = 1
        """)
        return [cls(*row) for row in cursor.fetchall()]