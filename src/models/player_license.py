# src/models/player_license.py

from datetime import datetime
from dataclasses import dataclass
import logging
from typing import Optional, List, Dict

@dataclass
class PlayerLicense:
    player_id: int
    club_id: int
    season_id: int
    license_id: int
    valid_from: datetime.date
    valid_to: datetime.date
    row_id: Optional[int] = None

    @staticmethod
    def from_dict(data: dict):
        return PlayerLicense(
            player_id=data["player_id"],
            club_id=data["club_id"],
            season_id=data["season_id"],
            license_id=data["license_id"],
            valid_from=data["valid_from"],
            valid_to=data["valid_to"],
            row_id=data.get("row_id")
        )
    
    @staticmethod
    def get_by_player_id(cursor, player_id: int, season_id: Optional[int] = None, club_id: Optional[int] = None) -> List['PlayerLicense']:
        """Retrieve PlayerLicense instances for a player, optionally filtered by season_id and club_id."""
        try:
            query = """
                SELECT player_id, club_id, season_id, license_id, valid_from, valid_to
                FROM player_license
                WHERE player_id = ?
            """
            params = [player_id]
            if season_id is not None:
                query += " AND season_id = ?"
                params.append(season_id)
            if club_id is not None:
                query += " AND club_id = ?"
                params.append(club_id)
            cursor.execute(query, params)
            rows = cursor.fetchall()
            return [PlayerLicense.from_dict({
                "player_id": row[0],
                "club_id": row[1],
                "season_id": row[2],
                "license_id": row[3],
                "valid_from": row[4],
                "valid_to": row[5]
            }) for row in rows]
        except Exception as e:
            logging.error(f"Error retrieving licenses for player_id {player_id}: {e}")
            return []
        
    def validate(self, cursor) -> dict:
        """Validate a single PlayerLicense instance."""
        try:
            # Check if player_id exists in player table
            cursor.execute("SELECT 1 FROM player WHERE player_id = ?", (self.player_id,))
            if not cursor.fetchone():
                return {
                    "status": "failed",
                    "row_id": self.row_id,
                    "reason": f"Foreign key violation: player_id {self.player_id} does not exist in player table"
                }

            # Check if club_id exists in club table
            cursor.execute("SELECT 1 FROM club WHERE club_id = ?", (self.club_id,))
            if not cursor.fetchone():
                return {
                    "status": "failed",
                    "row_id": self.row_id,
                    "reason": f"Foreign key violation: club_id {self.club_id} does not exist in club table"
                }

            # Check if season_id exists in season table
            cursor.execute("SELECT 1 FROM season WHERE season_id = ?", (self.season_id,))
            if not cursor.fetchone():
                return {
                    "status": "failed",
                    "row_id": self.row_id,
                    "reason": f"Foreign key violation: season_id {self.season_id} does not exist in season table"
                }

            # Check if license_id exists in license table
            cursor.execute("SELECT 1 FROM license WHERE license_id = ?", (self.license_id,))
            if not cursor.fetchone():
                return {
                    "status": "failed",
                    "row_id": self.row_id,
                    "reason": f"Foreign key violation: license_id {self.license_id} does not exist in license table"
                }

            # Check if the record already exists
            cursor.execute("""
                SELECT 1 FROM player_license 
                WHERE player_id = ? AND license_id = ? AND season_id = ? AND club_id = ?
            """, (self.player_id, self.license_id, self.season_id, self.club_id))
            if cursor.fetchone():
                return {
                    "status": "skipped",
                    "row_id": self.row_id,
                    "reason": "Player license already exists in database"
                }

            # Check for overlapping licenses
            cursor.execute("""
                SELECT 1 FROM player_license 
                WHERE player_id = ? AND license_id = ? AND season_id = ? 
                AND ((valid_from <= ? AND valid_to >= ?) OR (valid_from <= ? AND valid_to >= ?))
            """, (self.player_id, self.license_id, self.season_id, self.valid_from, self.valid_from, self.valid_to, self.valid_to))
            if cursor.fetchone():
                return {
                    "status": "success",
                    "row_id": self.row_id,
                    "reason": "Warning! Player already has a license of the same type with overlapping dates"
                }

            return {
                "status": "success",
                "row_id": self.row_id,
                "reason": "Player license validated successfully"
            }
        except Exception as e:
            logging.error(f"Error validating license for player_id {self.player_id}: {e}")
            return {
                "status": "failed",
                "row_id": self.row_id,
                "reason": f"Database error: {str(e)}"
            }        

    @staticmethod
    def batch_save_to_db(cursor, validated_licenses: List['PlayerLicense']) -> List[dict]:
        """Batch insert validated PlayerLicense objects, returning results for all licenses."""
        if not validated_licenses:
            return []

        results = []
        try:
            batch = [(l.player_id, l.club_id, l.season_id, l.license_id, l.valid_from, l.valid_to, l.row_id) for l in validated_licenses]
            cursor.executemany("""
                INSERT OR IGNORE INTO player_license (player_id, club_id, season_id, license_id, valid_from, valid_to)
                VALUES (?, ?, ?, ?, ?, ?)
            """, [(row[0], row[1], row[2], row[3], row[4], row[5]) for row in batch])
            inserted_count = cursor.rowcount
            logging.info(f"Batch inserted {inserted_count} licenses (skipped {len(validated_licenses) - inserted_count} duplicates)")

            # Return results for all licenses
            for i, license in enumerate(validated_licenses):
                status = "success" if i < inserted_count else "skipped"
                reason = "Inserted player license" if i < inserted_count else "Player license already exists in database"
                results.append({
                    "status": status,
                    "row_id": license.row_id,
                    "reason": reason
                })

            return results
        except Exception as e:
            logging.error(f"Error batch inserting licenses: {e}")
            return [{"status": "failed", "row_id": l.row_id, "reason": f"Database error: {str(e)}"} for l in validated_licenses]
        
    @staticmethod
    def validate_batch(cursor, licenses: List['PlayerLicense']) -> List[dict]:
        """Batch validate multiple PlayerLicense objects, including date range checks."""
        if not licenses:
            return []

        results = []
        try:
            # Bulk fetch valid IDs
            player_ids = set(l.player_id for l in licenses if l.player_id)
            if player_ids:
                cursor.execute(f"SELECT player_id FROM player WHERE player_id IN ({','.join(['?']*len(player_ids))})", list(player_ids))
                valid_players = set(row[0] for row in cursor.fetchall())
            else:
                valid_players = set()

            club_ids = set(l.club_id for l in licenses if l.club_id)
            if club_ids:
                cursor.execute(f"SELECT club_id FROM club WHERE club_id IN ({','.join(['?']*len(club_ids))})", list(club_ids))
                valid_clubs = set(row[0] for row in cursor.fetchall())
            else:
                valid_clubs = set()

            season_ids = set(l.season_id for l in licenses if l.season_id)
            if season_ids:
                cursor.execute(f"SELECT season_id, season_start_date, season_end_date FROM season WHERE season_id IN ({','.join(['?']*len(season_ids))})", list(season_ids))
                season_dates = {row[0]: (row[1], row[2]) for row in cursor.fetchall()}
                valid_seasons = set(season_dates.keys())
            else:
                season_dates = {}
                valid_seasons = set()

            license_ids = set(l.license_id for l in licenses if l.license_id)
            if license_ids:
                cursor.execute(f"SELECT license_id FROM license WHERE license_id IN ({','.join(['?']*len(license_ids))})", list(license_ids))
                valid_license_ids = set(row[0] for row in cursor.fetchall())
            else:
                valid_license_ids = set()

            # Check existing licenses in chunks to avoid SQL limits
            existing_licenses = set()
            chunk_size = 500  # SQLite max bindings ~999, so chunk safely
            for i in range(0, len(licenses), chunk_size):
                chunk = licenses[i:i + chunk_size]
                if chunk:
                    cursor.execute(f"""
                        SELECT player_id, club_id, season_id, license_id
                        FROM player_license
                        WHERE (player_id, club_id, season_id, license_id) IN ({','.join(['(?,?,?,?)']*len(chunk))})
                    """, [item for l in chunk for item in (l.player_id, l.club_id, l.season_id, l.license_id)])
                    existing_licenses.update((row[0], row[1], row[2], row[3]) for row in cursor.fetchall())

            # Check overlapping licenses in chunks
            overlapping_licenses = {}
            for i in range(0, len(licenses), chunk_size):
                chunk = licenses[i:i + chunk_size]
                if chunk:
                    cursor.execute(f"""
                        SELECT player_id, season_id, license_id, valid_from, valid_to
                        FROM player_license
                        WHERE (player_id, season_id, license_id) IN ({','.join(['(?,?,?)']*len(chunk))})
                    """, [item for l in chunk for item in (l.player_id, l.season_id, l.license_id)])
                    for row in cursor.fetchall():
                        player_id, season_id, license_id, valid_from, valid_to = row
                        overlapping_licenses[(player_id, season_id, license_id)] = (valid_from, valid_to)

            for license in licenses:
                key = (license.player_id, license.club_id, license.season_id, license.license_id)
                overlap_key = (license.player_id, license.season_id, license.license_id)

                # Check for null/zero IDs (from cache misses)
                if not license.player_id or license.player_id == 0:
                    results.append({"status": "failed", "row_id": license.row_id, "reason": f"Invalid player_id: {license.player_id}"})
                    continue
                if not license.club_id or license.club_id == 0:
                    results.append({"status": "failed", "row_id": license.row_id, "reason": f"Invalid club_id: {license.club_id}"})
                    continue
                if not license.season_id or license.season_id == 0:
                    results.append({"status": "failed", "row_id": license.row_id, "reason": f"Invalid season_id: {license.season_id}"})
                    continue
                if not license.license_id or license.license_id == 0:
                    results.append({"status": "failed", "row_id": license.row_id, "reason": f"Invalid license_id: {license.license_id}"})
                    continue

                # Foreign key checks
                if license.player_id not in valid_players:
                    results.append({"status": "failed", "row_id": license.row_id, "reason": f"Foreign key violation: player_id {license.player_id} does not exist in player table"})
                    continue
                if license.club_id not in valid_clubs:
                    results.append({"status": "failed", "row_id": license.row_id, "reason": f"Foreign key violation: club_id {license.club_id} does not exist in club table"})
                    continue
                if license.season_id not in valid_seasons:
                    results.append({"status": "failed", "row_id": license.row_id, "reason": f"Foreign key violation: season_id {license.season_id} does not exist in season table"})
                    continue
                if license.license_id not in valid_license_ids:
                    results.append({"status": "failed", "row_id": license.row_id, "reason": f"Foreign key violation: license_id {license.license_id} does not exist in license table"})
                    continue

                # Existence check
                if key in existing_licenses:
                    results.append({"status": "skipped", "row_id": license.row_id, "reason": "Player license already exists in database"})
                    continue

                # Date range check
                if not (license.valid_from and license.valid_to and license.season_id in season_dates):
                    results.append({"status": "failed", "row_id": license.row_id, "reason": "Invalid or missing date fields"})
                    continue
                season_start, season_end = season_dates[license.season_id]
                if license.valid_from > season_end:
                    # Adjust valid_from to season_end if slightly beyond (e.g., 2025-07-12 -> 2025-06-30)
                    if (license.valid_from - season_end).days <= 30:  # Allow 30-day leeway
                        license.valid_from = season_end
                        logging.debug(f"Adjusted valid_from to {season_end} for row_id {license.row_id}")
                    else:
                        results.append({"status": "failed", "row_id": license.row_id, 
                                    "reason": f"Valid from date {license.valid_from} is outside season range {season_start} - {season_end}"})
                        continue
                if license.valid_from < season_start:
                    results.append({"status": "failed", "row_id": license.row_id, 
                                "reason": f"Valid from date {license.valid_from} is outside season range {season_start} - {season_end}"})
                    continue
                if license.valid_from == season_end:
                    results.append({"status": "skipped", "row_id": license.row_id, 
                                "reason": "Valid from date equals season end date"})
                    continue

                # Overlapping license check
                if overlap_key in overlapping_licenses:
                    existing_from, existing_to = overlapping_licenses[overlap_key]
                    if (existing_from <= license.valid_from <= existing_to or 
                        existing_from <= license.valid_to <= existing_to or
                        license.valid_from <= existing_from <= license.valid_to or
                        license.valid_from <= existing_to <= license.valid_to):
                        results.append({"status": "success", "row_id": license.row_id, 
                                    "reason": "Warning! Player already has a license of the same type with overlapping dates"})
                    else:
                        results.append({"status": "success", "row_id": license.row_id, "reason": "Player license validated successfully"})
                else:
                    results.append({"status": "success", "row_id": license.row_id, "reason": "Player license validated successfully"})

            return results
        except Exception as e:
            logging.error(f"Error batch validating licenses: {e}")
            return [{"status": "failed", "row_id": l.row_id, "reason": f"Database error: {str(e)}"} for l in licenses]   

    def save_to_db(self, cursor):
        try:
            # Check if player_id exists in player table
            cursor.execute("SELECT 1 FROM player WHERE player_id = ?", (self.player_id,))
            if not cursor.fetchone():
                return {
                    "status": "failed",
                    "player_id": self.player_id,
                    "reason": f"Foreign key violation: player_id {self.player_id} does not exist in player table"
                }

            # Check if club_id exists in club table
            cursor.execute("SELECT 1 FROM club WHERE club_id = ?", (self.club_id,))
            if not cursor.fetchone():
                return {
                    "status": "failed",
                    "player_id": self.player_id,
                    "reason": f"Foreign key violation: club_id {self.club_id} does not exist in club table"
                }

            # Check if season_id exists in season table
            cursor.execute("SELECT 1 FROM season WHERE season_id = ?", (self.season_id,))
            if not cursor.fetchone():
                return {
                    "status": "failed",
                    "player_id": self.player_id,
                    "reason": f"Foreign key violation: season_id {self.season_id} does not exist in season table"
                }

            # Check if license_id exists in license table
            cursor.execute("SELECT 1 FROM license WHERE license_id = ?", (self.license_id,))
            if not cursor.fetchone():
                return {
                    "status": "failed",
                    "player_id": self.player_id,
                    "reason": f"Foreign key violation: license_id {self.license_id} does not exist in license table"
                }

            # Check if the record already exists
            cursor.execute("""
                SELECT 1 FROM player_license 
                WHERE player_id = ? AND license_id = ? AND season_id = ? AND club_id = ?
            """, (self.player_id, self.license_id, self.season_id, self.club_id))
            if cursor.fetchone():
                return {
                    "status": "skipped",
                    "player_id": self.player_id,
                    "reason": "Player license already exists in database"
                }

            # Check if the player already has a license of the same type with overlapping dates
            cursor.execute("""
                SELECT 1 FROM player_license 
                WHERE player_id = ? AND license_id = ? AND season_id = ? 
                AND ((valid_from <= ? AND valid_to >= ?) OR (valid_from <= ? AND valid_to >= ?))
            """, (self.player_id, self.license_id, self.season_id, self.valid_from, self.valid_from, self.valid_to, self.valid_to))

            overlapping_license = 1 if cursor.fetchone() else 0

            # Insert the player license
            cursor.execute("""
                INSERT INTO player_license (
                    player_id, club_id, valid_from, valid_to, license_id, season_id
                )
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                self.player_id, self.club_id, self.valid_from, self.valid_to,
                self.license_id, self.season_id
            ))

            if overlapping_license == 1:
                logging.warning(f"Player player_id: {self.player_id} already has a license of the same type with overlapping dates. Club: {self.club_id}, Season: {self.season_id}, License: {self.license_id}, Dates: {self.valid_from} - {self.valid_to}")
                return {
                        "status": "success",
                        "player_id": self.player_id,
                        "reason": "Warning! Player already has a license of the same type with overlapping dates"
                    }

            return {
                "status": "success",
                "player_id": self.player_id,
                "reason": "Player license inserted successfully"
            }

        except Exception as e:
            return {
                "status": "failed",
                "player_id": self.player_id,
                "reason": f"Database error: {str(e)}"
            }

    @staticmethod
    def cache_name_club_map(cursor):
        """
        Returns dict keyed by (firstname_lower, lastname_lower, club_id)
        Value = list of dicts with player_id and license validity periods
        """
        cursor.execute("""
            SELECT p.player_id, LOWER(p.firstname), LOWER(p.lastname),
                   pl.club_id, pl.valid_from, pl.valid_to
            FROM player p
            JOIN player_license pl ON p.player_id = pl.player_id
        """)
        cache = {}
        for player_id, firstname, lastname, club_id, valid_from, valid_to in cursor.fetchall():
            key = (firstname, lastname, club_id)
            cache.setdefault(key, []).append({
                "player_id": player_id,
                "valid_from": valid_from,
                "valid_to": valid_to
            })
        return cache

    @staticmethod
    def cache_find_by_name_club_date(licenses_cache, firstname, lastname, club_id, tournament_date, fallback_to_latest=False):
        """
        Strict lookup: find player_id with a valid license for tournament_date.
        Optional fallback: return the most recent license if no valid one is found.
        """
        key = (firstname.lower(), lastname.lower(), club_id)
        if key not in licenses_cache:
            return None

        # Step 1: Strict match
        valid_candidates = [
            license["player_id"]
            for license in licenses_cache[key]
            if license["valid_from"] <= tournament_date <= license["valid_to"]
        ]
        if len(valid_candidates) == 1:
            return valid_candidates[0]
        elif len(valid_candidates) > 1:
            logging.warning(f"Multiple valid licenses for {firstname} {lastname} at club_id={club_id} on {tournament_date}")
            return valid_candidates[0]

        # Step 2: Optional fallback to most recent license
        if fallback_to_latest:
            most_recent = max(licenses_cache[key], key=lambda x: x["valid_to"], default=None)
            if most_recent:
                logging.info(
                    f"ℹFallback: using most recent license for {firstname} {lastname} at club_id={club_id} "
                    f"(valid_to={most_recent['valid_to']})"
                )
                return most_recent["player_id"]

        return None


