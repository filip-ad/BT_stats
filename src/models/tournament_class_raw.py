# src/models/tournament_class_raw.py
from __future__ import annotations

from dataclasses import dataclass
import datetime
import json
from typing import Optional, Dict, Any, List, Tuple
import sqlite3
from utils import parse_date, compute_content_hash

@dataclass
class TournamentClassRaw:
    row_id:                         Optional[int]  = None           # Auto-generated ID for raw entry
    tournament_id_ext:              Optional[str]  = None           # External ID of parent tournament
    tournament_class_id_ext:        Optional[str]  = None           # External ID from ondata.se or other source
    startdate:                      Optional[datetime.date] = None  # Date of the class
    shortname:                      Optional[str]  = None           # Short description of the class
    longname:                       Optional[str]  = None           # Full description of the class
    gender:                         Optional[str]  = None           # Gender category (e.g., "male", "female")
    max_rank:                       Optional[int]  = None           # Maximum rank allowed in the class
    max_age:                        Optional[int]  = None           # Maximum age allowed in the class
    url:                            Optional[str]  = None           # URL for the class
    raw_stages:                     Optional[str]  = None           # Comma-separated stages from HTML links, for structure inference
    raw_stage_hrefs:                Optional[str]  = None           # JSON string of {stage: href} for PDF downloading
    data_source_id:                 int = 1                         # Data source ID (default 1 for 'ondata')
    content_hash:                   Optional[str] = None            # Hash of the content for change detection
    last_seen_at:                   Optional[datetime.datetime] = None  # Last time the entry was seen
    row_created:                    Optional[datetime.datetime] = None  # When the row was created
    row_updated:                    Optional[datetime.datetime] = None  # When the row was last updated

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "TournamentClassRaw":
        """Instantiate from a scraped dict (keys matching column names)."""

        # Normalize raw_stage_hrefs: always store as JSON string (or None)
        rhs = d.get("raw_stage_hrefs")
        if isinstance(rhs, dict):
            rhs = json.dumps(rhs)
            
        return TournamentClassRaw(
            row_id                          = d.get("row_id"),
            tournament_id_ext               = d.get("tournament_id_ext"),
            tournament_class_id_ext         = d.get("tournament_class_id_ext"),
            startdate                       = parse_date(d.get("startdate"), context="TournamentClassRaw.from_dict"),
            shortname                       = d.get("shortname", ""),
            longname                        = d.get("longname", ""),
            gender                          = d.get("gender"),
            max_rank                        = d.get("max_rank"),
            max_age                         = d.get("max_age"),
            url                             = d.get("url"),
            raw_stages                      = d.get("raw_stages"),
            raw_stage_hrefs                 = rhs,
            data_source_id                  = d.get("data_source_id", 1),
            content_hash                    = d.get("content_hash"),
            last_seen_at                    = d.get("last_seen_at"),
            row_created                     = d.get("row_created"),
            row_updated                     = d.get("row_updated")
        )

    def validate(self) -> Tuple[bool, str]:
        """
        Validate fields.
        Returns: (is_valid, error_message)
        """
        missing = []
        if not self.shortname:
            missing.append("shortname")
        if not self.startdate:
            missing.append("startdate")
        if not self.tournament_id_ext:
            missing.append("tournament_id_ext")
        if not self.tournament_class_id_ext:
            missing.append("tournament_class_id_ext")
        if missing:
            return False, f"Missing/invalid fields: {', '.join(missing)}"
        return True, ""
    
    def compute_content_hash(self) -> str:
        """
        Compute a stable hash for raw tournament class content to detect meaningful changes.
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
    
    # @classmethod
    # def get_all(cls, cursor: sqlite3.Cursor) -> List["TournamentClassRaw"]:
    #     """Fetch all valid raw tournament class entries."""
    #     cursor.execute("SELECT * FROM tournament_class_raw")
    #     columns = [
    #         'row_id', 'tournament_id_ext', 'tournament_class_id_ext', 'startdate',
    #         'shortname', 'longname', 'gender', 'max_rank', 'max_age', 'url',
    #         'raw_stages', 'raw_stage_hrefs', 'data_source_id'
    #     ]
    #     return [cls.from_dict(dict(zip(columns, row))) for row in cursor.fetchall()]

    @classmethod
    def get_all(cls, cursor: sqlite3.Cursor) -> List["TournamentClassRaw"]:
        """Fetch all valid raw tournament class entries."""
        cursor.execute("SELECT * FROM tournament_class_raw")
        column_names = [desc[0] for desc in cursor.description]
        return [cls.from_dict(dict(zip(column_names, row))) for row in cursor.fetchall()]
    
    
    def upsert(self, cursor: sqlite3.Cursor) -> Optional[str]:
        """
        Atomic upsert with hash gating for raw tournament class data.
        Uses (tournament_id_ext, tournament_class_id_ext, data_source_id) as unique key.
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
            self.tournament_id_ext,
            self.tournament_class_id_ext,
            self.startdate if self.startdate else None,
            self.shortname,
            self.longname,
            self.gender,
            self.max_rank,
            self.max_age,
            self.url,
            self.raw_stages,
            self.raw_stage_hrefs,
            self.data_source_id,
            new_hash
        )

        # --- 3. SQL: upsert with conflict handling ---
        sql = """
        INSERT INTO tournament_class_raw (
            tournament_id_ext, tournament_class_id_ext, startdate, shortname, longname,
            gender, max_rank, max_age, url, raw_stages, raw_stage_hrefs, data_source_id,
            content_hash, last_seen_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT (tournament_id_ext, tournament_class_id_ext, data_source_id) DO UPDATE SET
            startdate = CASE WHEN tournament_class_raw.content_hash IS NULL OR tournament_class_raw.content_hash <> excluded.content_hash
                            THEN excluded.startdate ELSE tournament_class_raw.startdate END,
            shortname = CASE WHEN tournament_class_raw.content_hash IS NULL OR tournament_class_raw.content_hash <> excluded.content_hash
                            THEN excluded.shortname ELSE tournament_class_raw.shortname END,
            longname = CASE WHEN tournament_class_raw.content_hash IS NULL OR tournament_class_raw.content_hash <> excluded.content_hash
                            THEN excluded.longname ELSE tournament_class_raw.longname END,
            gender = CASE WHEN tournament_class_raw.content_hash IS NULL OR tournament_class_raw.content_hash <> excluded.content_hash
                          THEN excluded.gender ELSE tournament_class_raw.gender END,
            max_rank = CASE WHEN tournament_class_raw.content_hash IS NULL OR tournament_class_raw.content_hash <> excluded.content_hash
                            THEN excluded.max_rank ELSE tournament_class_raw.max_rank END,
            max_age = CASE WHEN tournament_class_raw.content_hash IS NULL OR tournament_class_raw.content_hash <> excluded.content_hash
                           THEN excluded.max_age ELSE tournament_class_raw.max_age END,
            url = CASE WHEN tournament_class_raw.content_hash IS NULL OR tournament_class_raw.content_hash <> excluded.content_hash
                       THEN excluded.url ELSE tournament_class_raw.url END,
            raw_stages = CASE WHEN tournament_class_raw.content_hash IS NULL OR tournament_class_raw.content_hash <> excluded.content_hash
                              THEN excluded.raw_stages ELSE tournament_class_raw.raw_stages END,
            raw_stage_hrefs = CASE WHEN tournament_class_raw.content_hash IS NULL OR tournament_class_raw.content_hash <> excluded.content_hash
                                   THEN excluded.raw_stage_hrefs ELSE tournament_class_raw.raw_stage_hrefs END,
            content_hash = excluded.content_hash,
            last_seen_at = CURRENT_TIMESTAMP,
            row_updated = CASE WHEN tournament_class_raw.content_hash IS NULL OR tournament_class_raw.content_hash <> excluded.content_hash
                              THEN CURRENT_TIMESTAMP ELSE tournament_class_raw.row_updated END
        RETURNING row_id;
        """

        # --- 4. Execute SQL ---
        try:
            cursor.execute(sql, common_vals)
            row = cursor.fetchone()

            if not row:
                # No row returned means conflict happened but no changes (unchanged)
                return "unchanged"

            self.row_id = row[0]

            # If lastrowid matches the row_id, this was an INSERT
            if cursor.lastrowid == self.row_id:
                return "inserted"

            # Otherwise it was an UPDATE path: check if content actually changed
            cursor.execute("SELECT content_hash FROM tournament_class_raw WHERE row_id = ?;", (self.row_id,))
            old_hash = cursor.fetchone()
            if old_hash and old_hash[0] == new_hash:
                return "unchanged"
            return "updated"

        except sqlite3.Error as e:
            # This error will be caught and logged by the scraper, so we return None
            return None

    # def upsert(self, cursor: sqlite3.Cursor) -> str:
    #     """
    #     Upsert the raw tournament class data based on (tournament_id_ext, tournament_class_id_ext, data_source_id).
    #     Returns "inserted" or "updated" to indicate the action performed.
    #     """
    #     action = None
    #     row_id = None

    #     # Check if a row exists with the unique constraint
    #     cursor.execute(
    #         """
    #         SELECT row_id FROM tournament_class_raw
    #         WHERE tournament_id_ext = ? AND tournament_class_id_ext = ? AND data_source_id = ?;
    #         """,
    #         (self.tournament_id_ext, self.tournament_class_id_ext, self.data_source_id),
    #     )
    #     row = cursor.fetchone()

    #     if row:
    #         row_id = row[0]
    #         # Update existing row
    #         cursor.execute(
    #             """
    #             UPDATE tournament_class_raw
    #             SET startdate = ?,
    #                 shortname = ?,
    #                 longname = ?,
    #                 gender = ?,
    #                 max_rank = ?,
    #                 max_age = ?,
    #                 url = ?,
    #                 raw_stages = ?,
    #                 raw_stage_hrefs = ?,
    #                 row_updated = CURRENT_TIMESTAMP
    #             WHERE row_id = ?
    #             RETURNING row_id;
    #             """,
    #             (
    #                 self.startdate,
    #                 self.shortname,
    #                 self.longname,
    #                 self.gender,
    #                 self.max_rank,
    #                 self.max_age,
    #                 self.url,
    #                 self.raw_stages,
    #                 self.raw_stage_hrefs,
    #                 row_id,
    #             ),
    #         )
    #         self.row_id = cursor.fetchone()[0]
    #         action = "updated"
    #     else:
    #         # Insert new row
    #         cursor.execute(
    #             """
    #             INSERT INTO tournament_class_raw (
    #                 tournament_class_id_ext, tournament_id_ext, startdate, shortname, longname,
    #                 gender, max_rank, max_age, url, raw_stages, raw_stage_hrefs, data_source_id
    #             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    #             RETURNING row_id;
    #             """,
    #             (
    #                 self.tournament_class_id_ext,
    #                 self.tournament_id_ext,
    #                 self.startdate,
    #                 self.shortname,
    #                 self.longname,
    #                 self.gender,
    #                 self.max_rank,
    #                 self.max_age,
    #                 self.url,
    #                 self.raw_stages,
    #                 self.raw_stage_hrefs,
    #                 self.data_source_id,
    #             ),
    #         )
    #         self.row_id = cursor.fetchone()[0]
    #         action = "inserted"

    #     return action