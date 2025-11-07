# src/models/tournament.py
from __future__ import annotations  # <= postpone annotation evaluation (robust)


from typing import List, Optional, Tuple # make sure this import exists
from dataclasses import dataclass
from datetime import date
import sqlite3
from models.cache_mixin import CacheMixin
from utils import parse_date, OperationLogger


@dataclass
class Tournament(CacheMixin):
    tournament_id:              Optional[int] = None        # Canonical ID from tournament table
    tournament_id_ext:          Optional[str] = None        # External ID from ondata.se
    shortname:                  Optional[str] = None        # Short name or abbreviation
    longname:                   Optional[str] = None        # Full tournament name
    startdate:                  Optional[date] = None       # Start date as a date object
    enddate:                    Optional[date] = None       # End date as a date object
    registration_end_date:      Optional[date] = None       # Registration end date as a date object
    city:                       Optional[str] = None        # City name
    arena:                      Optional[str] = None        # Arena name
    country_code:               Optional[str] = None        # Country code (e.g., 'SWE')
    url:                        Optional[str] = None        # Full tournament URL
    tournament_level_id:        Optional[int] = 1           # Tournament level (e.g., 'Professional')
    tournament_type_id:         Optional[int] = 1           # Tournament type (e.g., 'Single Elimination')
    tournament_status_id:       Optional[int] = 6           # Status: 'ONGOING', 'UPCOMING', or 'ENDED'
    organiser_name:             Optional[str] = None        # Organiser name
    organiser_email:            Optional[str] = None        # Organiser email
    organiser_phone:            Optional[str] = None        # Organiser phone
    is_valid:                   Optional[bool] = True       # Validation flag (set after validate() call)
    data_source_id:             Optional[int] = 1           # Data source ID (default 1 for 'ondata')
    row_created:                Optional[str] = None        # Timestamp of creation
    row_updated:                Optional[str] = None        # Timestamp of last update

    @staticmethod
    def from_dict(data: dict) -> 'Tournament':
        """
        Factory method to create a Tournament instance from a dictionary.
        Handles date parsing and defaults.
        """
        sd = data.get("start_date") or data.get("startdate")
        ed = data.get("end_date")   or data.get("enddate")

        return Tournament(
            tournament_id               = data.get("tournament_id"),
            tournament_id_ext           = data.get("tournament_id_ext"),
            shortname                   = data.get("shortname"),
            longname                    = data.get("longname"),
            startdate                   = parse_date(sd, context="Tournament.from_dict (startdate)"),
            enddate                     = parse_date(ed, context="Tournament.from_dict (enddate)"),
            registration_end_date       = parse_date(data.get("registration_end_date"), context="Tournament.from_dict (reg date)"),
            city                        = data.get("city"),
            arena                       = data.get("arena"),
            country_code                = data.get("country_code"),
            url                         = data.get("url"),
            tournament_level_id         = data.get("tournament_level_id", 1),
            tournament_type_id          = data.get("tournament_type_id", 1),
            tournament_status_id        = data.get("tournament_status_id", 6),
            organiser_name              = data.get("organiser_name"),
            organiser_email             = data.get("organiser_email"),
            organiser_phone             = data.get("organiser_phone"),
            data_source_id              = data.get("data_source_id", 1),
            is_valid                    = data.get("is_valid", True),
            row_created                 = data.get("row_created"),
            row_updated                 = data.get("row_updated")
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
            SELECT tournament_id, tournament_id_ext 
            FROM tournament 
            WHERE tournament_id_ext IN ({placeholders}) AND data_source_id = ?
        """
        params = tournament_id_exts + [data_source_id]
        cursor.execute(sql, params)
        return [row[0] for row in cursor.fetchall()]


    # Used in get_filtered_classes in upd_tournament_classes (filtering by id_exts)
    # @classmethod
    # def get_internal_tournament_ids(
    #     cls, 
    #     cursor: sqlite3.Cursor, 
    #     ext_ids: List[str], 
    #     data_source_id: int
    # ) -> List[int]:
    #     """
    #     Convert list of external tournament IDs (with data_source_id) to internal tournament_ids.
    #     Uses cached query to reduce database load.
    #     """
    #     if not ext_ids:
    #         return []

    #     placeholders = ", ".join(["?"] * len(ext_ids))
    #     sql = f"""
    #         SELECT tournament_id, tournament_id_ext 
    #         FROM tournament 
    #         WHERE tournament_id_ext IN ({placeholders}) AND data_source_id = ?
    #     """
    #     params = tuple(ext_ids) + (data_source_id,)
    #     results = cls.cached_query(cursor, sql, params, cache_key_extra=f"tournament_ids_ds_{data_source_id}")

    #     # Create a mapping of ext_id to tournament_id
    #     ext_id_to_id = {row["tournament_id_ext"]: row["tournament_id"] for row in results}

    #     # Return tournament_ids in the same order as input ext_ids, excluding unmatched IDs
    #     return [ext_id_to_id[ext_id] for ext_id in ext_ids if ext_id in ext_id_to_id]
    
    # Used for testing in upd_tournament_classes (filtering by id_exts)
    @staticmethod
    def get_by_ext_ids(cursor, ext_ids: List[str]) -> List['Tournament']:
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
        
        return tournaments
    
    # Used in scrape_participants_ondata, to get tournament ext id which is not available in tournament_class
    @staticmethod
    def get_all(cursor) -> List['Tournament']:
        """
        Retrieve all Tournament objects from the database.
        Returns a list of Tournament instances.
        """
        try:
            cursor.execute(
                """
                SELECT 
                    tournament_id, 
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
                    tournament_level_id, 
                    tournament_type_id, 
                    tournament_status_id, 
                    organiser_name, 
                    organiser_email, 
                    organiser_phone, 
                    is_valid, 
                    data_source_id
                FROM tournament
                """
            )
            rows = cursor.fetchall()
            tournaments = []
            for row in rows:
                tournament = Tournament(
                    tournament_id               = row[0],
                    tournament_id_ext           = row[1] if row[1] else None,
                    shortname                   = row[2] if row[2] else None,
                    longname                    = row[3] if row[3] else None,
                    startdate                   = parse_date(row[4], context="Tournament.get_all (startdate)") if row[4] else None,
                    enddate                     = parse_date(row[5], context="Tournament.get_all (enddate)") if row[5] else None,
                    registration_end_date       = parse_date(row[6], context="Tournament.get_all (reg date)") if row[6] else None,
                    city                        = row[7] if row[7] else None,
                    arena                       = row[8] if row[8] else None,
                    country_code                = row[9] if row[9] else None,
                    url                         = row[10] if row[10] else None,
                    tournament_level_id         = row[11] if row[11] is not None else 1,
                    tournament_type_id          = row[12] if row[12] is not None else 1,
                    tournament_status_id        = row[13] if row[13] is not None else 6,
                    organiser_name              = row[14] if row[14] else None,
                    organiser_email             = row[15] if row[15] else None,
                    organiser_phone             = row[16] if row[16] else None,
                    is_valid                    = bool(row[17]) if row[17] is not None else True,
                    data_source_id              = row[18] if row[18] is not None else 1
                )
                tournaments.append(tournament)
            return tournaments
        except sqlite3.Error as e:
            # Log error if needed (assuming logger is available)
            print(f"Database error retrieving tournaments: {e}")
            return []
        
    # Used in resolve_tournament_classes to map ext_id to internal id    
    @classmethod
    def get_id_map_by_ext(
        cls, cursor, ext_ids: List[str], data_source_id: int
    ) -> dict[str, int]:
        """
        Bulk fetch tournament_id by tournament_id_ext for a given data_source_id.
        Returns a mapping {tournament_id_ext -> tournament_id}.
        """
        if not ext_ids:
            return {}

        placeholders = ", ".join("?" * len(ext_ids))
        query = f"""
            SELECT tournament_id, tournament_id_ext
            FROM tournament
            WHERE tournament_id_ext IN ({placeholders})
              AND data_source_id = ?
        """
        rows = cls.cached_query(
            cursor,
            query,
            tuple(ext_ids) + (data_source_id,),
            cache_key_extra=f"tournament_ids_ds_{data_source_id}",
        )
        return {row["tournament_id_ext"]: row["tournament_id"] for row in rows}
    
    @classmethod
    def get_id_ext_map_by_id(
        cls,
        cursor: sqlite3.Cursor,
        ids: List[int],
    ) -> dict[int, str]:
        """
        Bulk fetch external tournament IDs by internal IDs.
        Returns a mapping {tournament_id -> tournament_id_ext} exactly as stored
        in the DB (no zero-padding or formatting).
        """
        if not ids:
            return {}

        placeholders = ", ".join("?" * len(ids))
        query = f"""
            SELECT tournament_id, tournament_id_ext
            FROM tournament
            WHERE tournament_id IN ({placeholders})
        """
        rows = cls.cached_query(
            cursor,
            query,
            tuple(ids),
            cache_key_extra=f"id_ext_by_id_{len(ids)}",
        )
        # Keys are ints (tournament_id), values are raw strings (tournament_id_ext)
        return {row["tournament_id"]: row["tournament_id_ext"] for row in rows}

   # Used in scrape_participants_ondata to get ongoing and ended tournaments 
    @classmethod
    def get_valid_ongoing_ended(cls, cursor: sqlite3.Cursor) -> List["Tournament"]:
        """
        Fetch valid tournaments that are ongoing or ended based on the current date.
        Returns tournaments with startdate <= current date, enddate >= current date, and is_valid = 1.
        """
        current_date = date.today()
        sql = """
            SELECT * FROM tournament
            WHERE startdate <= ?
            AND is_valid = 1;
        """
        cursor.execute(sql, (current_date, ))
        columns = [col[0] for col in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return [cls.from_dict(res) for res in results]
    
    def upsert(self, cursor: sqlite3.Cursor) -> Optional[str]:
        """
        Upsert tournament data based on (tournament_id_ext, data_source_id) if tournament_id_ext is provided,
        otherwise based on (shortname, startdate, arena, data_source_id).
        Returns "inserted" or "updated" on success, None on no change.
        """
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
                # Prepare values for UPDATE
                vals = (
                    self.shortname or None,
                    self.longname or None,
                    self.startdate,
                    self.enddate,
                    self.registration_end_date,
                    self.city or None,
                    self.arena or None,
                    self.country_code or None,
                    self.url or None,
                    self.tournament_level_id,
                    self.tournament_type_id,
                    self.tournament_status_id,
                    self.organiser_name or None,
                    self.organiser_email or None,
                    self.organiser_phone or None,
                    self.is_valid,
                    tournament_id
                )
                cursor.execute(
                    """
                    UPDATE tournament
                    SET shortname             = ?,
                        longname              = ?,
                        startdate             = ?,
                        enddate               = ?,
                        registration_end_date = ?,
                        city                  = ?,
                        arena                 = ?,
                        country_code          = ?,
                        url                   = ?,
                        tournament_level_id   = ?,
                        tournament_type_id    = ?,
                        tournament_status_id  = ?,
                        organiser_name        = ?,
                        organiser_email       = ?,
                        organiser_phone       = ?,
                        is_valid              = ?,
                        row_updated           = CURRENT_TIMESTAMP
                    WHERE tournament_id = ?
                    RETURNING tournament_id;
                    """,
                    vals
                )
                self.tournament_id = cursor.fetchone()[0]
                action = "updated"

        if action is None and self.shortname and self.startdate and self.arena:
            cursor.execute(
                "SELECT tournament_id FROM tournament WHERE shortname = ? AND startdate = ? AND arena = ? AND data_source_id = ?;",
                (self.shortname, self.startdate, self.arena, self.data_source_id),
            )
            row = cursor.fetchone()
            if row:
                tournament_id = row[0]
                # Prepare values for UPDATE with fallback key
                vals = (
                    self.tournament_id_ext or None,
                    self.shortname or None,
                    self.longname or None,
                    self.startdate,
                    self.enddate,
                    self.registration_end_date,
                    self.city or None,
                    self.arena or None,
                    self.country_code or None,
                    self.url or None,  # Ensure url is explicitly included
                    self.tournament_level_id,
                    self.tournament_type_id,
                    self.tournament_status_id,
                    self.organiser_name or None,
                    self.organiser_email or None,
                    self.organiser_phone or None,
                    self.is_valid,
                    tournament_id
                )
                cursor.execute(
                    """
                    UPDATE tournament
                    SET tournament_id_ext     = ?,
                        shortname             = ?,
                        longname              = ?,
                        startdate             = ?,
                        enddate               = ?,
                        registration_end_date = ?,
                        city                  = ?,
                        arena                 = ?,
                        country_code          = ?,
                        url                   = ?,
                        tournament_level_id   = ?,
                        tournament_type_id    = ?,
                        tournament_status_id  = ?,
                        organiser_name        = ?,
                        organiser_email       = ?,
                        organiser_phone       = ?,
                        is_valid              = ?,
                        row_updated           = CURRENT_TIMESTAMP
                    WHERE tournament_id = ?
                    RETURNING tournament_id;
                    """,
                    vals
                )
                self.tournament_id = cursor.fetchone()[0]
                action = "updated"

        if action is None:
            # INSERT (only if we have enough data)
            vals = (
                self.tournament_id_ext or None,
                self.shortname or None,
                self.longname or None,
                self.startdate,
                self.enddate,
                self.registration_end_date,
                self.city or None,
                self.arena or None,
                self.country_code or None,
                self.url or None,  # Ensure url is explicitly included
                self.tournament_level_id,
                self.tournament_type_id,
                self.tournament_status_id,
                self.organiser_name or None,
                self.organiser_email or None,
                self.organiser_phone or None,
                self.data_source_id
            )
            cursor.execute(
                """
                INSERT INTO tournament (
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
                    tournament_level_id,
                    tournament_type_id,
                    tournament_status_id,
                    organiser_name,
                    organiser_email,
                    organiser_phone,
                    data_source_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING tournament_id;
                """,
                vals
            )
            self.tournament_id = cursor.fetchone()[0]
            action = "inserted"

        return action