# src/models/tournament_raw.py
from __future__ import annotations

from datetime import date
from typing import List, Optional, Tuple
from dataclasses import dataclass
import sqlite3
from utils import parse_date, compute_content_hash

'''
    ### Table definition for tournament_raw

    row_id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    tournament_id_ext               TEXT,
    shortname                       TEXT,
    longname                        TEXT,
    startdate                       DATE,
    enddate                         DATE,
    registration_end_date           DATE,
    city                            TEXT,
    arena                           TEXT,
    country_code                    TEXT,
    url                             TEXT,
    tournament_level                TEXT,
    tournament_type                 TEXT,
    organiser_name                  TEXT,
    organiser_email                 TEXT,
    organiser_phone                 TEXT,
    data_source_id                  INTEGER DEFAULT 1,
    is_listed                       BOOLEAN DEFAULT 1,
    content_hash                    TEXT,
    last_seen_at                    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    row_created                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    row_updated                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (data_source_id)    REFERENCES data_source(data_source_id),

    UNIQUE (tournament_id_ext, data_source_id),
    UNIQUE (shortname, startdate, arena, data_source_id)
'''


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
    content_hash:           Optional[str] = None
    last_seen_at:           Optional[date] = None
    row_created:            Optional[date] = None
    row_updated:            Optional[date] = None

    @staticmethod
    def from_dict(data: dict) -> 'TournamentRaw':
        """
        Factory method to create a TournamentRaw instance from a dictionary.
        row_created and row_updated are managed by the database and should not be set here.
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
            is_listed               = data.get("is_listed", True),
            content_hash            = data.get("content_hash"),
            last_seen_at            = parse_date(data.get("last_seen_at")),
            row_created             = parse_date(data.get("row_created")),
            row_updated             = parse_date(data.get("row_updated")),
        )
        
    def compute_content_hash(self) -> str:
        """
        Compute a stable hash for raw tournament content to detect meaningful changes.
        Delegates to utils.compute_content_hash with table-specific exclusions.
        """
        return compute_content_hash(
            self,
            exclude_fields={
                "row_id",
                "data_source_id",
                "row_created",
                "row_updated",
                "last_seen_at",
                "content_hash"
            }
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
    
    @classmethod
    def get_all(cls, cursor) -> List['TournamentRaw']:
        """
        Fetch all records from tournament_raw table using SELECT *.
        Returns a list of TournamentRaw objects (dynamic mapping by column name).
        """
        cursor.execute("""
            SELECT * FROM tournament_raw
            WHERE data_source_id = 1
        """)
        rows = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        return [cls.from_dict(dict(zip(column_names, row))) for row in rows]


    def upsert(self, cursor: sqlite3.Cursor) -> Optional[str]:
        """
        Atomic upsert with hash gating for raw tournament data.
        Uses (tournament_id_ext, data_source_id) if provided, else (shortname, startdate, arena, data_source_id).
        Always updates last_seen_at; content/row_updated only if hash changed.

        Returns one of:
            "inserted"   – new row created
            "updated"    – existing row updated (content changed)
            "unchanged"  – row existed but no content change
            None         – invalid or no operation
        """

        # --- 1. Validation ---
        is_valid, err = self.validate()
        if not is_valid:
            return None  # Skip invalid rows entirely

        new_hash = self.compute_content_hash()

        # --- 2. Common values tuple (all content fields + hash) ---
        common_vals = (
            self.shortname, self.longname,
            self.startdate if self.startdate else None,
            self.enddate if self.enddate else None,
            self.registration_end_date if self.registration_end_date else None,
            self.city, self.arena, self.country_code, self.url,
            self.tournament_level, self.tournament_type,
            self.organiser_name, self.organiser_email, self.organiser_phone,
            self.is_listed, self.data_source_id, new_hash
        )

        # --- 3. SQL: choose conflict key branch ---
        if self.tournament_id_ext is not None:
            sql = """
            INSERT INTO tournament_raw (
                tournament_id_ext, shortname, longname, startdate, enddate, registration_end_date,
                city, arena, country_code, url, tournament_level, tournament_type,
                organiser_name, organiser_email, organiser_phone, is_listed, data_source_id, content_hash, last_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT (tournament_id_ext, data_source_id) DO UPDATE SET
                shortname = CASE WHEN tournament_raw.content_hash IS NULL OR tournament_raw.content_hash <> excluded.content_hash
                                THEN excluded.shortname ELSE tournament_raw.shortname END,
                longname = CASE WHEN tournament_raw.content_hash IS NULL OR tournament_raw.content_hash <> excluded.content_hash
                                THEN excluded.longname ELSE tournament_raw.longname END,
                startdate = CASE WHEN tournament_raw.content_hash IS NULL OR tournament_raw.content_hash <> excluded.content_hash
                                THEN excluded.startdate ELSE tournament_raw.startdate END,
                enddate = CASE WHEN tournament_raw.content_hash IS NULL OR tournament_raw.content_hash <> excluded.content_hash
                            THEN excluded.enddate ELSE tournament_raw.enddate END,
                registration_end_date = CASE WHEN tournament_raw.content_hash IS NULL OR tournament_raw.content_hash <> excluded.content_hash
                                            THEN excluded.registration_end_date ELSE tournament_raw.registration_end_date END,
                city = CASE WHEN tournament_raw.content_hash IS NULL OR tournament_raw.content_hash <> excluded.content_hash
                            THEN excluded.city ELSE tournament_raw.city END,
                arena = CASE WHEN tournament_raw.content_hash IS NULL OR tournament_raw.content_hash <> excluded.content_hash
                            THEN excluded.arena ELSE tournament_raw.arena END,
                country_code = CASE WHEN tournament_raw.content_hash IS NULL OR tournament_raw.content_hash <> excluded.content_hash
                                    THEN excluded.country_code ELSE tournament_raw.country_code END,
                url = CASE WHEN tournament_raw.content_hash IS NULL OR tournament_raw.content_hash <> excluded.content_hash
                        THEN excluded.url ELSE tournament_raw.url END,
                tournament_level = CASE WHEN tournament_raw.content_hash IS NULL OR tournament_raw.content_hash <> excluded.content_hash
                                        THEN excluded.tournament_level ELSE tournament_raw.tournament_level END,
                tournament_type = CASE WHEN tournament_raw.content_hash IS NULL OR tournament_raw.content_hash <> excluded.content_hash
                                    THEN excluded.tournament_type ELSE tournament_raw.tournament_type END,
                organiser_name = CASE WHEN tournament_raw.content_hash IS NULL OR tournament_raw.content_hash <> excluded.content_hash
                                    THEN excluded.organiser_name ELSE tournament_raw.organiser_name END,
                organiser_email = CASE WHEN tournament_raw.content_hash IS NULL OR tournament_raw.content_hash <> excluded.content_hash
                                    THEN excluded.organiser_email ELSE tournament_raw.organiser_email END,
                organiser_phone = CASE WHEN tournament_raw.content_hash IS NULL OR tournament_raw.content_hash <> excluded.content_hash
                                    THEN excluded.organiser_phone ELSE tournament_raw.organiser_phone END,
                is_listed = CASE WHEN tournament_raw.content_hash IS NULL OR tournament_raw.content_hash <> excluded.content_hash
                                THEN excluded.is_listed ELSE tournament_raw.is_listed END,
                content_hash = excluded.content_hash,
                last_seen_at = CURRENT_TIMESTAMP,
                row_updated = CASE WHEN tournament_raw.content_hash IS NULL OR tournament_raw.content_hash <> excluded.content_hash
                                THEN CURRENT_TIMESTAMP ELSE tournament_raw.row_updated END
            RETURNING row_id;
            """
            vals = (self.tournament_id_ext,) + common_vals
        else:
            sql = """
            INSERT INTO tournament_raw (
                tournament_id_ext, shortname, longname, startdate, enddate, registration_end_date,
                city, arena, country_code, url, tournament_level, tournament_type,
                organiser_name, organiser_email, organiser_phone, is_listed, data_source_id, content_hash, last_seen_at
            ) VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT (shortname, startdate, arena, data_source_id) DO UPDATE SET
                tournament_id_ext = COALESCE(excluded.tournament_id_ext, tournament_raw.tournament_id_ext),
                longname = CASE WHEN tournament_raw.content_hash IS NULL OR tournament_raw.content_hash <> excluded.content_hash
                                THEN excluded.longname ELSE tournament_raw.longname END,
                enddate = CASE WHEN tournament_raw.content_hash IS NULL OR tournament_raw.content_hash <> excluded.content_hash
                            THEN excluded.enddate ELSE tournament_raw.enddate END,
                registration_end_date = CASE WHEN tournament_raw.content_hash IS NULL OR tournament_raw.content_hash <> excluded.content_hash
                                            THEN excluded.registration_end_date ELSE tournament_raw.registration_end_date END,
                city = CASE WHEN tournament_raw.content_hash IS NULL OR tournament_raw.content_hash <> excluded.content_hash
                            THEN excluded.city ELSE tournament_raw.city END,
                country_code = CASE WHEN tournament_raw.content_hash IS NULL OR tournament_raw.content_hash <> excluded.content_hash
                                    THEN excluded.country_code ELSE tournament_raw.country_code END,
                url = CASE WHEN tournament_raw.content_hash IS NULL OR tournament_raw.content_hash <> excluded.content_hash
                        THEN excluded.url ELSE tournament_raw.url END,
                tournament_level = CASE WHEN tournament_raw.content_hash IS NULL OR tournament_raw.content_hash <> excluded.content_hash
                                        THEN excluded.tournament_level ELSE tournament_raw.tournament_level END,
                tournament_type = CASE WHEN tournament_raw.content_hash IS NULL OR tournament_raw.content_hash <> excluded.content_hash
                                    THEN excluded.tournament_type ELSE tournament_raw.tournament_type END,
                organiser_name = CASE WHEN tournament_raw.content_hash IS NULL OR tournament_raw.content_hash <> excluded.content_hash
                                    THEN excluded.organiser_name ELSE tournament_raw.organiser_name END,
                organiser_email = CASE WHEN tournament_raw.content_hash IS NULL OR tournament_raw.content_hash <> excluded.content_hash
                                    THEN excluded.organiser_email ELSE tournament_raw.organiser_email END,
                organiser_phone = CASE WHEN tournament_raw.content_hash IS NULL OR tournament_raw.content_hash <> excluded.content_hash
                                    THEN excluded.organiser_phone ELSE tournament_raw.organiser_phone END,
                is_listed = CASE WHEN tournament_raw.content_hash IS NULL OR tournament_raw.content_hash <> excluded.content_hash
                                THEN excluded.is_listed ELSE tournament_raw.is_listed END,
                content_hash = excluded.content_hash,
                last_seen_at = CURRENT_TIMESTAMP,
                row_updated = CASE WHEN tournament_raw.content_hash IS NULL OR tournament_raw.content_hash <> excluded.content_hash
                                THEN CURRENT_TIMESTAMP ELSE tournament_raw.row_updated END
            RETURNING row_id;
            """
            vals = common_vals

        # --- 4. Execute SQL ---
        cursor.execute(sql, vals)
        row = cursor.fetchone()

        if not row:
            # No row returned means conflict happened but no changes (unchanged)
            return "unchanged"

        self.row_id = row[0]

        # If lastrowid matches the row_id, this was an INSERT
        if cursor.lastrowid == self.row_id:
            return "inserted"

        # Otherwise it was an UPDATE path: check if content actually changed
        cursor.execute("SELECT content_hash FROM tournament_raw WHERE row_id = ?;", (self.row_id,))
        old_hash = cursor.fetchone()
        if old_hash and old_hash[0] == new_hash:
            return "unchanged"
        return "updated"



           # def upsert(self, cursor: sqlite3.Cursor) -> str:
    #     """
    #     Upsert raw tournament data based on (tournament_id_ext, data_source_id) if tournament_id_ext is provided,
    #     otherwise based on (shortname, startdate, arena, data_source_id).
    #     Returns "inserted" or "updated" to indicate the action performed.
    #     """

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
    #                 SET shortname               = ?,
    #                     longname                = ?,
    #                     startdate               = ?,
    #                     enddate                 = ?,
    #                     registration_end_date   = ?,
    #                     city                    = ?,
    #                     arena                   = ?,
    #                     country_code            = ?,
    #                     url                     = ?,
    #                     tournament_level        = ?,
    #                     tournament_type         = ?,
    #                     organiser_name          = ?,
    #                     organiser_email         = ?,
    #                     organiser_phone         = ?,
    #                     is_listed               = ?,
    #                     row_updated             = CURRENT_TIMESTAMP
    #                 WHERE row_id = ?
    #                 RETURNING row_id;
    #                 """,
    #                 (self.shortname, 
    #                  self.longname, 
    #                  self.startdate, 
    #                  self.enddate, 
    #                  self.registration_end_date,
    #                  self.city, 
    #                  self.arena, 
    #                  self.country_code, 
    #                  self.url, 
    #                  self.tournament_level, 
    #                  self.tournament_type,
    #                  self.organiser_name, 
    #                  self.organiser_email, 
    #                  self.organiser_phone, 
    #                  self.is_listed, 
    #                  row_id),
    #             )
    #             self.row_id = cursor.fetchone()[0]
    #             action = "updated"

    #     if action is None:
    #         # Not found by tournament_id_ext (or it was None), check by shortname/startdate/arena/data source
    #         cursor.execute(
    #             "SELECT row_id FROM tournament_raw WHERE shortname = ? AND startdate = ? AND arena = ? AND data_source_id = ?;",
    #             (self.shortname, self.startdate, self.arena, self.data_source_id),
    #         )
    #         row = cursor.fetchone()
    #         if row:
    #             row_id = row[0]
    #             # UPDATE (include setting tournament_id_ext, e.g., filling it in if previously None)
    #             cursor.execute(
    #                 """
    #                 UPDATE tournament_raw
    #                 SET tournament_id_ext       = ?,
    #                     shortname               = ?,
    #                     longname                = ?,
    #                     startdate               = ?,
    #                     enddate                 = ?,
    #                     registration_end_date   = ?,
    #                     city                    = ?,
    #                     arena                   = ?,
    #                     country_code            = ?,
    #                     url                     = ?,
    #                     tournament_level        = ?,
    #                     tournament_type         = ?,
    #                     organiser_name          = ?,
    #                     organiser_email         = ?,
    #                     organiser_phone         = ?,
    #                     is_listed               = ?,
    #                     row_updated         = CURRENT_TIMESTAMP
    #                 WHERE row_id = ?
    #                 RETURNING row_id;
    #                 """,
    #                 (self.tournament_id_ext, 
    #                  self.shortname, 
    #                  self.longname, 
    #                  self.startdate, 
    #                  self.enddate, 
    #                  self.registration_end_date,
    #                  self.city, 
    #                  self.arena, 
    #                  self.country_code, 
    #                  self.url, 
    #                  self.tournament_level, 
    #                  self.tournament_type,
    #                  self.organiser_name, 
    #                  self.organiser_email, 
    #                  self.organiser_phone, 
    #                  self.is_listed, 
    #                  row_id),
    #             )
    #             self.row_id = cursor.fetchone()[0]
    #             action = "updated"
    #         else:
    #             # INSERT
    #             cursor.execute(
    #                 """
    #                 INSERT INTO tournament_raw (
    #                     tournament_id_ext, 
    #                     shortname, 
    #                     longname, 
    #                     startdate, 
    #                     enddate, 
    #                     registration_end_date,
    #                     city, 
    #                     arena,
    #                     country_code,
    #                     url,
    #                     tournament_level,
    #                     tournament_type,
    #                     organiser_name,
    #                     organiser_email,
    #                     organiser_phone,
    #                     data_source_id,
    #                     is_listed
    #                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    #                 RETURNING row_id;
    #                 """,
    #                 (self.tournament_id_ext, 
    #                  self.shortname, 
    #                  self.longname, 
    #                  self.startdate, 
    #                  self.enddate, 
    #                  self.registration_end_date,
    #                  self.city, 
    #                  self.arena, 
    #                  self.country_code, 
    #                  self.url, 
    #                  self.tournament_level, 
    #                  self.tournament_type,
    #                  self.organiser_name, 
    #                  self.organiser_email, 
    #                  self.organiser_phone, 
    #                  self.data_source_id,
    #                  self.is_listed, 
    #                 )
    #             )
    #             self.row_id = cursor.fetchone()[0]
    #             action = "inserted"

    #     return action