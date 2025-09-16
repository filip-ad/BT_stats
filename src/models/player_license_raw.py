# src/models/player_license_raw.py

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Tuple
import sqlite3
from utils import compute_content_hash as _compute_content_hash


@dataclass
class PlayerLicenseRaw:
    """
    Raw player license row scraped from Profixio/OnData.
    Mirrors player_license_raw table in DB.
    """

    # Raw fields from scrape
    row_id:                             Optional[int]
    season_id_ext:                      Optional[str]
    season_label:                       Optional[str]
    club_name:                          Optional[str]
    club_id_ext:                        Optional[str]
    player_id_ext:                      Optional[str]
    firstname:                          Optional[str]
    lastname:                           Optional[str]
    gender:                             Optional[str]
    year_born:                          Optional[str]
    license_info_raw:                   Optional[str]
    ranking_group_raw:                  Optional[str]
    data_source_id:                     Optional[int] = 1
    content_hash:                       Optional[str] = None
    last_seen_at:                       Optional[str] = None
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

    # def to_dict(self) -> Dict[str, Any]:
    #     """
    #     Dict for DB insert/upsert.
    #     """
    #     return {
    #         "season_id_ext":        self.season_id_ext,
    #         "season_label":         self.season_label,
    #         "club_name":            self.club_name,
    #         "club_id_ext":          self.club_id_ext,
    #         "player_id_ext":        self.player_id_ext,
    #         "firstname":            self.firstname,
    #         "lastname":             self.lastname,
    #         "gender":               self.gender,
    #         "year_born":            self.year_born,
    #         "license_info_raw":     self.license_info_raw,
    #         "ranking_group_raw":    self.ranking_group_raw,
    #         "raw_payload":          json.dumps(self.raw_payload) if self.raw_payload else None
    #     }

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

    def compute_content_hash(self) -> str:
        """
        Compute stable hash for this row (exclude volatile/meta fields).
        """
        return _compute_content_hash(
            self,
            exclude_fields={
                "row_id",
                "data_source_id",
                "row_created",
                "row_updated",
                "last_seen_at",
                "content_hash"
            },
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

    # @staticmethod
    # def upsert_one(cursor, raw: "PlayerLicenseRaw") -> bool:
    #     """
    #     Upsert one row into player_license_raw.

    #     Behavior: "staging-only"
    #     - Try INSERT OR IGNORE to avoid dupes based on your natural/unique key.
    #     - No special-case updates (incl. ranking_group_raw). Reprocessing happens in resolvers/updaters.
    #     Returns:
    #         inserted (bool): True if a new row was inserted, False if it already existed.
    #     """
    #     cursor.execute("""
    #         INSERT OR IGNORE INTO player_license_raw (
    #             season_label, season_id_ext, club_name, club_id_ext, player_id_ext,
    #             firstname, lastname, gender, year_born, license_info_raw, ranking_group_raw
    #         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    #     """, (
    #         raw.season_label, raw.season_id_ext, raw.club_name, raw.club_id_ext, raw.player_id_ext,
    #         raw.firstname, raw.lastname, raw.gender, raw.year_born, raw.license_info_raw, raw.ranking_group_raw
    #     ))
    #     return cursor.rowcount > 0

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

    def upsert(self, cursor: sqlite3.Cursor) -> Optional[str]:
        """
        Upsert with content-hash gating.

        Returns one of: "inserted", "updated", "unchanged", or None (invalid).
        """
        is_valid, err = self.validate()
        if not is_valid:
            return None

        new_hash = self.compute_content_hash()

        # NOTE: we intentionally DO NOT update any UNIQUE-key columns on conflict:
        #   (season_id_ext, player_id_ext, club_name, year_born, firstname, lastname, license_info_raw)
        sql = """
        INSERT INTO player_license_raw (
            season_label, season_id_ext, club_name, club_id_ext, player_id_ext,
            firstname, lastname, gender, year_born, license_info_raw, ranking_group_raw,
            data_source_id, content_hash, last_seen_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT (season_id_ext, player_id_ext, club_name, year_born, firstname, lastname, license_info_raw)
        DO UPDATE SET
            -- only update non-key columns if content actually changed
            season_label = CASE
                WHEN player_license_raw.content_hash IS NULL OR player_license_raw.content_hash <> excluded.content_hash
                THEN excluded.season_label ELSE player_license_raw.season_label END,
            club_id_ext = CASE
                WHEN player_license_raw.content_hash IS NULL OR player_license_raw.content_hash <> excluded.content_hash
                THEN excluded.club_id_ext ELSE player_license_raw.club_id_ext END,
            gender = CASE
                WHEN player_license_raw.content_hash IS NULL OR player_license_raw.content_hash <> excluded.content_hash
                THEN excluded.gender ELSE player_license_raw.gender END,
            ranking_group_raw = CASE
                WHEN player_license_raw.content_hash IS NULL OR player_license_raw.content_hash <> excluded.content_hash
                THEN excluded.ranking_group_raw ELSE player_license_raw.ranking_group_raw END,
            content_hash = CASE
                WHEN player_license_raw.content_hash IS NULL OR player_license_raw.content_hash <> excluded.content_hash
                THEN excluded.content_hash ELSE player_license_raw.content_hash END,
            row_updated = CASE
                WHEN player_license_raw.content_hash IS NULL OR player_license_raw.content_hash <> excluded.content_hash
                THEN CURRENT_TIMESTAMP ELSE player_license_raw.row_updated END
        WHERE player_license_raw.content_hash IS NULL OR player_license_raw.content_hash <> excluded.content_hash
        RETURNING row_id;
        """

        vals = (
            self.season_label, self.season_id_ext, self.club_name, self.club_id_ext, self.player_id_ext,
            self.firstname, self.lastname, self.gender, self.year_born, self.license_info_raw, self.ranking_group_raw,
            self.data_source_id, new_hash
        )

        cursor.execute(sql, vals)
        row = cursor.fetchone()

        if row:
            # Either inserted or updated-with-change
            self.row_id = row[0]
            # Heuristic same as your tournaments: INSERT sets lastrowid == row_id
            if cursor.lastrowid == self.row_id:
                return "inserted"
            return "updated"

        # Unchanged content (conflict existed but WHERE prevented update).
        # We still "touch" last_seen_at without changing content.
        touch_sql = """
        UPDATE player_license_raw
           SET last_seen_at = CURRENT_TIMESTAMP
         WHERE season_id_ext = ?
           AND player_id_ext = ?
           AND club_name     = ?
           AND year_born     = ?
           AND firstname     = ?
           AND lastname      = ?
           AND license_info_raw = ?
        RETURNING row_id;
        """
        cursor.execute(
            touch_sql,
            (
                self.season_id_ext, self.player_id_ext, self.club_name, self.year_born,
                self.firstname, self.lastname, self.license_info_raw
            ),
        )
        touched = cursor.fetchone()
        if touched:
            self.row_id = touched[0]
        return "unchanged"
