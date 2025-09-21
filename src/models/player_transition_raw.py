# src/models/player_transition_raw.py

from dataclasses import dataclass
from datetime import date
from typing import Optional, Tuple
from utils import compute_content_hash as _compute_content_hash

@dataclass
class PlayerTransitionRaw:
    """
    Dataclass for raw player transition data scraped from the web.
    """
    row_id:             Optional[int] = None
    season_id_ext:      Optional[str] = None
    season_label:       Optional[str] = None
    firstname:          Optional[str] = None
    lastname:           Optional[str] = None
    date_born:          Optional[date] = None
    year_born:          Optional[str] = None
    club_from:          Optional[str] = None
    club_to:            Optional[str] = None
    transition_date:    Optional[date] = None
    data_source_id:     int = 3
    content_hash:       Optional[str] = None

    
    def validate(self) -> Tuple[bool, str]:
        """
        Validate raw fields.
        Returns:
            (is_valid, error_message)
        """
        missing = []
        if not self.firstname:
            missing.append("firstname")
        if not self.lastname:
            missing.append("lastname")
        if not self.date_born:
            missing.append("date_born")
        if not self.year_born:
            missing.append("year_born")
        if not self.club_from:
            missing.append("club_from")
        if not self.club_to:
            missing.append("club_to")
        if not self.transition_date:
            missing.append("transition_date")

        if missing:
            return False, f"Missing/invalid fields: {', '.join(missing)}"
        return True, ""
    
    def compute_content_hash(self) -> str:
        return _compute_content_hash(
            self,
            exclude_fields={
                "row_id", "data_source_id",
                "row_created", "row_updated", "last_seen_at", "content_hash"
            }
        )
    
    @staticmethod
    def from_row(row: tuple) -> "PlayerTransitionRaw":
        """
        Construct from SELECT row (same column order as in resolver).
        """
        (row_id, season_id_ext, season_label, firstname, lastname, date_born, year_born, club_from, club_to, transition_date) = row
        return PlayerTransitionRaw(
            row_id=row_id,
            season_id_ext=season_id_ext,
            season_label=season_label,
            firstname=firstname,
            lastname=lastname,
            date_born=date_born,
            year_born=year_born,
            club_from=club_from,
            club_to=club_to,
            transition_date=transition_date
        )

    @classmethod
    def get_all(cls, cursor) -> list["PlayerTransitionRaw"]:
        """
        Fetch all rows from player_transition_raw and return as dataclass objects.
        """
        cursor.execute("""
            SELECT 
                row_id, season_id_ext, season_label, firstname, lastname, date_born, year_born, club_from, club_to, transition_date
            FROM player_transition_raw
        """)
        return [cls.from_row(r) for r in cursor.fetchall()]

    # @staticmethod
    # def upsert_one(cursor, raw: "PlayerTransitionRaw") -> bool:
    #     """
    #     Upsert one row into player_transition_raw.

    #     Behavior: "staging-only"
    #     - Try INSERT OR IGNORE to avoid dupes based on your natural/unique key.
    #     - No special-case updates (incl. club_from, club_to). Reprocessing happens in resolvers/updaters.
    #     Returns:
    #         inserted (bool): True if a new row was inserted, False if it already existed.
    #     """
    #     cursor.execute("""
    #         INSERT OR IGNORE INTO player_transition_raw (
    #             season_id_ext, season_label, firstname, lastname, date_born, year_born, club_from, club_to, transition_date
    #         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    #     """, (
    #         raw.season_id_ext, raw.season_label, raw.firstname, raw.lastname, raw.date_born,
    #         raw.year_born, raw.club_from, raw.club_to, raw.transition_date
    #     ))
    #     return cursor.rowcount > 0

    def upsert_one(self, cursor) -> str:
        """
        Upsert one row into player_transition_raw with content-hash gating.
        Returns:
            "inserted"   → new row created
            "updated"    → existing row updated due to content change
            "unchanged"  → existing row, content identical (last_seen_at touched)
        """
        new_hash = self.compute_content_hash()

        sql = """
        INSERT INTO player_transition_raw (
            season_id_ext, season_label, firstname, lastname, date_born,
            year_born, club_from, club_to, transition_date,
            data_source_id, content_hash, last_seen_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT (firstname, lastname, date_born, transition_date)
        DO UPDATE SET
            season_id_ext = CASE
                WHEN player_transition_raw.content_hash IS NULL OR player_transition_raw.content_hash <> excluded.content_hash
                THEN excluded.season_id_ext ELSE player_transition_raw.season_id_ext END,
            season_label = CASE
                WHEN player_transition_raw.content_hash IS NULL OR player_transition_raw.content_hash <> excluded.content_hash
                THEN excluded.season_label ELSE player_transition_raw.season_label END,
            year_born = CASE
                WHEN player_transition_raw.content_hash IS NULL OR player_transition_raw.content_hash <> excluded.content_hash
                THEN excluded.year_born ELSE player_transition_raw.year_born END,
            club_from = CASE
                WHEN player_transition_raw.content_hash IS NULL OR player_transition_raw.content_hash <> excluded.content_hash
                THEN excluded.club_from ELSE player_transition_raw.club_from END,
            club_to = CASE
                WHEN player_transition_raw.content_hash IS NULL OR player_transition_raw.content_hash <> excluded.content_hash
                THEN excluded.club_to ELSE player_transition_raw.club_to END,
            transition_date = CASE
                WHEN player_transition_raw.content_hash IS NULL OR player_transition_raw.content_hash <> excluded.content_hash
                THEN excluded.transition_date ELSE player_transition_raw.transition_date END,
            content_hash = CASE
                WHEN player_transition_raw.content_hash IS NULL OR player_transition_raw.content_hash <> excluded.content_hash
                THEN excluded.content_hash ELSE player_transition_raw.content_hash END,
            row_updated = CASE
                WHEN player_transition_raw.content_hash IS NULL OR player_transition_raw.content_hash <> excluded.content_hash
                THEN CURRENT_TIMESTAMP ELSE player_transition_raw.row_updated END
        WHERE player_transition_raw.content_hash IS NULL OR player_transition_raw.content_hash <> excluded.content_hash
        RETURNING row_id;
        """

        vals = (
            self.season_id_ext, self.season_label, self.firstname, self.lastname, self.date_born,
            self.year_born, self.club_from, self.club_to, self.transition_date,
            self.data_source_id, new_hash
        )

        cursor.execute(sql, vals)
        row = cursor.fetchone()  # <-- must consume RETURNING

        if row:
            self.row_id = row[0]
            # Distinguish inserted vs updated:
            if cursor.lastrowid == self.row_id:
                return "inserted"
            return "updated"

        # Unchanged → just touch last_seen_at
        touch_sql = """
        UPDATE player_transition_raw
        SET last_seen_at = CURRENT_TIMESTAMP
        WHERE firstname = ? AND lastname = ? AND date_born = ? AND transition_date = ?
        RETURNING row_id;
        """
        cursor.execute(
            touch_sql,
            (self.firstname, self.lastname, self.date_born, self.transition_date),
        )
        touched = cursor.fetchone()
        if touched:
            self.row_id = touched[0]
        return "unchanged"

