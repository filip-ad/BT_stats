# src/models/player_license_raw.py

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Tuple
import sqlite3
import json


@dataclass
class PlayerLicenseRaw:
    """
    Raw player license row scraped from Profixio/OnData.
    Mirrors player_license_raw table in DB.
    """

    # Raw fields from scrape
    season_id_ext:                      Optional[str]
    season_label:                       Optional[str]
    club_name:                          Optional[str]
    club_id_ext:                        Optional[str]
    player_id_ext:                      Optional[str]
    firstname:                          Optional[str]
    lastname:                           Optional[str]
    
    year_born:                          Optional[int]
    license_info_raw:                   Optional[str]
    gender:                             Optional[str] = None
    ranking_group_raw:                  Optional[str] = None

    # Optional blob for debugging/unstructured fields
    raw_payload:                        Optional[Dict[str, Any]] = field(default_factory=dict)

    # Provenance
    row_id:                             Optional[int] = None

    # Metadata
    row_created:                        Optional[str] = None
    row_updated:                        Optional[str] = None
    
    def validate(self) -> Tuple[bool, str]:
        """
        Validate raw fields.
        Returns:
            (is_valid, error_message)
        """
        missing = []
        if not self.player_id_ext:
            missing.append("player_id_ext")
        if not self.license_info_raw:
            missing.append("license_info_raw")
        if not self.year_born:
            missing.append("year_born")
        if not self.firstname:
            missing.append("firstname")
        if not self.lastname:
            missing.append("lastname")
        if not self.gender:
            missing.append("gender")
        if not self.year_born:
            missing.append("year_born")
        if not self.club_id_ext:
            missing.append("club_id_ext")

        if missing:
            return False, f"Missing/invalid fields: {', '.join(missing)}"
        return True, ""

    def to_dict(self) -> Dict[str, Any]:
        """
        Dict for DB insert/upsert.
        """
        return {
            "season_id_ext":        self.season_id_ext,
            "season_label":         self.season_label,
            "club_name":            self.club_name,
            "club_id_ext":          self.club_id_ext,
            "player_id_ext":        self.player_id_ext,
            "firstname":            self.firstname,
            "lastname":             self.lastname,
            "gender":               self.gender,
            "year_born":            self.year_born,
            "license_info_raw":     self.license_info_raw,
            "ranking_group_raw":    self.ranking_group_raw,
            "raw_payload":          json.dumps(self.raw_payload) if self.raw_payload else None
        }

    @staticmethod
    def from_row(row: tuple) -> "PlayerLicenseRaw":
        """
        Construct from SELECT row (same column order as in resolver).
        """
        (row_id, season_id_ext, season_label, club_name, club_id_ext,
         player_id_ext_str, firstname, lastname, gender, year_born, license_info_raw) = row
        return PlayerLicenseRaw(
            row_id=row_id,
            season_id_ext       = season_id_ext,
            season_label        = season_label,
            club_name           = club_name,
            club_id_ext         = club_id_ext,
            player_id_ext       = player_id_ext_str,
            firstname           = firstname,
            lastname            = lastname,
            gender              = gender,
            year_born           = year_born,
            license_info_raw    = license_info_raw
        )
    
    @classmethod
    def get_all(cls, cursor: sqlite3.Cursor) -> List["PlayerLicenseRaw"]:
        """
        Fetch all rows from player_license_raw and return as dataclass objects.
        """
        cursor.execute("""
            SELECT 
                row_id, season_id_ext, season_label, club_name, club_id_ext,
                CAST(player_id_ext AS TEXT) AS player_id_ext_str,
                firstname, lastname, gender, year_born, license_info_raw
            FROM player_license_raw
        """)
        return [cls.from_row(r) for r in cursor.fetchall()]
    
    @classmethod
    def get_duplicates(cls, cursor: sqlite3.Cursor) -> Dict[Tuple[str, str, str, str], int]:
        """
        Return a map of duplicate raw licenses:
        key = (player_id_ext, club_id_ext, season_id_ext, license_key)
        value = min(row_id)
        """
        cursor.execute("""
            SELECT 
                CAST(player_id_ext AS TEXT) AS player_id_ext,
                CAST(club_id_ext   AS TEXT) AS club_id_ext,
                CAST(season_id_ext AS TEXT) AS season_id_ext,
                LOWER(TRIM(SUBSTR(license_info_raw, 1, INSTR(license_info_raw, '(') - 1))) AS license_key,
                MIN(row_id) AS min_row_id
            FROM player_license_raw
            GROUP BY 1,2,3,4
            HAVING COUNT(*) > 1
        """)
        return {(r[0], r[1], r[2], r[3]): r[4] for r in cursor.fetchall()}

    @staticmethod
    def upsert_one(cursor, raw: "PlayerLicenseRaw") -> bool:
        """
        Upsert one row into player_license_raw.

        Behavior: "staging-only"
        - Try INSERT OR IGNORE to avoid dupes based on your natural/unique key.
        - No special-case updates (incl. ranking_group_raw). Reprocessing happens in resolvers/updaters.
        Returns:
            inserted (bool): True if a new row was inserted, False if it already existed.
        """
        cursor.execute("""
            INSERT OR IGNORE INTO player_license_raw (
                season_label, season_id_ext, club_name, club_id_ext, player_id_ext,
                firstname, lastname, gender, year_born, license_info_raw, ranking_group_raw
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            raw.season_label, raw.season_id_ext, raw.club_name, raw.club_id_ext, raw.player_id_ext,
            raw.firstname, raw.lastname, raw.gender, raw.year_born, raw.license_info_raw, raw.ranking_group_raw
        ))
        return cursor.rowcount > 0

    # Used by resolve_player_ranking_groups
    @staticmethod
    def fetch_rows_with_ranking_groups(cursor):
        """
        Return (player_id_ext, ranking_group_raw) for all rows that have a non-empty ranking_group_raw.
        Season is intentionally ignored (current-only model).
        """
        cursor.execute("""
            SELECT player_id_ext, ranking_group_raw
            FROM player_license_raw
            WHERE player_id_ext IS NOT NULL
              AND TRIM(COALESCE(ranking_group_raw, '')) <> ''
        """)
        return cursor.fetchall()
