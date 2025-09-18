# src/models/player_license.py

from datetime import date
from dataclasses import dataclass
import logging
from typing import Optional, List, Tuple, Dict, Any, Set, Iterable
from collections import defaultdict
import sqlite3
from models.cache_mixin import CacheMixin
from models.season import Season
from utils import normalize_key

@dataclass
class PlayerLicense(CacheMixin):
    player_id:                  int
    club_id:                    int
    valid_from:                 date
    valid_to:                   date
    license_id:                 int
    season_id:                  int
    row_created:                Optional[date] = None
    row_updated:                Optional[date] = None

    @staticmethod
    def from_dict(data: dict) -> "PlayerLicense":
        return PlayerLicense(
            player_id           = data["player_id"],
            club_id             = data["club_id"],
            season_id           = data["season_id"],
            license_id          = data["license_id"],
            valid_from          = data["valid_from"],
            valid_to            = data["valid_to"],
            row_created         = data.get("row_created"),
            row_updated         = data.get("row_updated")
        )
    
    def validate(self, cursor: sqlite3.Cursor) -> Tuple[bool, str]:
        """
        Validate a single PlayerLicense instance, focusing on date ranges, duplicates, and overlaps.
        Assumes player_id, club_id, and license_id are valid (checked in resolver).
        Season_map can be passed to avoid DB queries for season validation.
        Returns (is_valid, error_message)
        """

        # Check required fields
        if not all([self.player_id, self.club_id, self.season_id, self.license_id, self.valid_from, self.valid_to]):
            return False, "Missing required field(s)"
        
        # Check valid_to >= valid_from
        if self.valid_to < self.valid_from:
            return False, f"Invalid date range: valid_to {self.valid_to} before valid_from {self.valid_from}"

        # Season validation
        cursor.execute("SELECT start_date, end_date FROM season WHERE season_id = ?", (self.season_id,))
        season_row = cursor.fetchone()
        if not season_row:
            return False, f"Invalid season_id {self.season_id}"
        sd, ed = season_row

        # Check valid_from and valid_to within season bounds
        if not (sd <= self.valid_from <= ed):
            return False, f"Valid_from {self.valid_from} outside season bounds {sd} to {ed}"
        if not (sd <= self.valid_to <= ed):
            return False, f"Valid_to {self.valid_to} outside season bounds {sd} to {ed}"

        # Check DB duplicate
        cursor.execute("""
            SELECT 1 FROM player_license
            WHERE player_id = ? AND club_id = ? AND season_id = ? AND license_id = ?
        """, (self.player_id, self.club_id, self.season_id, self.license_id))
        if cursor.fetchone():
            return False, "License already exists"

        # Overlap check (same player, license, season)
        cursor.execute("""
            SELECT valid_from, valid_to FROM player_license
            WHERE player_id = ? AND license_id = ? AND season_id = ?
        """, (self.player_id, self.license_id, self.season_id))
        for vf, vt in cursor.fetchall():
            if isinstance(vf, str):
                vf = date.fromisoformat(vf)
            if isinstance(vt, str):
                vt = date.fromisoformat(vt)
            if not (self.valid_to < vf or self.valid_from > vt):
                return False, "Overlaps existing license period"

        return True, ""
    

    def upsert(self, cursor: sqlite3.Cursor) -> Optional[str]:
        """
        Upsert a single PlayerLicense with change detection.
        Returns one of: "inserted", "updated", "unchanged", or None (invalid).
        """

        sql = """
        INSERT INTO player_license
        (player_id, club_id, season_id, license_id, valid_from, valid_to)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT (player_id, license_id, season_id, club_id)
        DO UPDATE SET
            valid_from = CASE
                WHEN player_license.valid_from != excluded.valid_from OR player_license.valid_to != excluded.valid_to
                THEN excluded.valid_from ELSE player_license.valid_from END,
            valid_to = CASE
                WHEN player_license.valid_from != excluded.valid_from OR player_license.valid_to != excluded.valid_to
                THEN excluded.valid_to ELSE player_license.valid_to END,
            row_updated = CASE
                WHEN player_license.valid_from != excluded.valid_from OR player_license.valid_to != excluded.valid_to
                THEN CURRENT_TIMESTAMP ELSE player_license.row_updated END
        RETURNING player_id;
        """
        vals = (
            self.player_id, self.club_id, self.season_id, self.license_id,
            self.valid_from, self.valid_to
        )
        cursor.execute(sql, vals)
        row = cursor.fetchone()
        if row:
            # Either inserted or updated-with-change
            # Heuristic: INSERT sets lastrowid
            if cursor.lastrowid:
                return "inserted"
            return "updated"

        # Unchanged (conflict but no update needed)
        return "unchanged"
    
    @classmethod
    def cache_name_club_map(cls, cursor) -> Dict[Tuple[str, int], List[Dict[str, Any]]]:
        sql = """
            SELECT 
                pl.player_id,
                pl.club_id,
                pl.valid_from,
                pl.valid_to,
                pl.license_id,
                pl.season_id,
                p.firstname,
                p.lastname
            FROM player_license pl
            JOIN player p ON pl.player_id = p.player_id
        """
        rows = cls.cached_query(cursor, sql)

        license_map: Dict[Tuple[str, int], List[Dict[str, Any]]] = defaultdict(list)
        for row in rows:
            full_norm = normalize_key(f"{row['firstname'] or ''} {row['lastname'] or ''}").strip()
            key = (full_norm, row['club_id'])
            license_map[key].append({
                "player_id":    row['player_id'],
                "club_id":      row['club_id'],
                "license_id":   row['license_id'],
                "valid_from":   row['valid_from'],
                "valid_to":     row['valid_to'],
                "season_id":    row['season_id']
            })
        return license_map

    @classmethod
    def cache_all(cls, cursor) -> Dict[int, Set[Tuple[int, int]]]:
        """
        Load all player_license rows into memory.
        Returns a dict mapping player_id → set of (club_id, season_id).
        """
        license_map: Dict[int, Set[Tuple[int, int]]] = defaultdict(set)
        cursor.execute(
            "SELECT player_id, club_id, season_id FROM player_license"
        )
        for pid, cid, sid in cursor.fetchall():
            license_map[pid].add((cid, sid))
        logging.info(f"Cached licenses for {len(license_map)} players")
        return license_map

    @staticmethod
    def has_license(license_map: Dict[int, Set[Tuple[int, int]]], player_id: int, club_id: int, seasons: Iterable[int]) -> bool:
        """
        Check if the given player_id has a license in club_id for any of the specified seasons.
        """
        licenses = license_map.get(player_id, set())
        return any((club_id, sid) in licenses for sid in seasons)
    
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
        
    @staticmethod
    def batch_insert(
        cursor, 
        licenses: List["PlayerLicense"], 
        logger
    ) -> List[Dict[str, Any]]:
        """
        Batch upsert PlayerLicense objects in safe-sized chunks.
        Updates valid_from, valid_to, and row_updated on conflict if content changes.
        Returns one dict per input license summarizing skip/insert/update/fail.
        """
        if not licenses:
            return []

        to_upsert: List[tuple] = []
        results: List[Dict[str, Any]] = []

        # Prepare upsert data
        for lic in licenses:
            key = (lic.player_id, lic.club_id, lic.season_id, lic.license_id)
            content_hash = lic.compute_content_hash()
            item_key = f"(player_id: {lic.player_id}, club_id: {lic.club_id}, season_id: {lic.season_id}, license_id: {lic.license_id})"
            to_upsert.append((
                lic.player_id,
                lic.club_id,
                lic.season_id,
                lic.license_id,
                lic.valid_from,
                lic.valid_to,
                content_hash
            ))
            results.append({
                "status": "pending",
                "key": item_key,
                "reason": "Will upsert"
            })

        # Nothing to upsert?
        if not to_upsert:
            return results

        # Chunk size calculation
        MAX_VARS = 999
        COLS_PER_ROW = 7  # Updated for content_hash
        chunk_size = MAX_VARS // COLS_PER_ROW  # ~142

        insert_sql = """
            INSERT INTO player_license
            (player_id, club_id, season_id, license_id, valid_from, valid_to, row_updated)
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT (player_id, license_id, season_id, club_id)
            DO UPDATE SET
                valid_from = CASE
                    WHEN player_license.row_updated IS NULL OR excluded.valid_from != player_license.valid_from OR excluded.valid_to != player_license.valid_to
                    THEN excluded.valid_from ELSE player_license.valid_from END,
                valid_to = CASE
                    WHEN player_license.row_updated IS NULL OR excluded.valid_from != player_license.valid_from OR excluded.valid_to != player_license.valid_to
                    THEN excluded.valid_to ELSE player_license.valid_to END,
                row_updated = CASE
                    WHEN player_license.row_updated IS NULL OR excluded.valid_from != player_license.valid_from OR excluded.valid_to != player_license.valid_to
                    THEN CURRENT_TIMESTAMP ELSE player_license.row_updated END
            RETURNING player_id, club_id, season_id, license_id
        """

        # Execute chunks
        inserted_count = 0
        updated_count = 0
        for start in range(0, len(to_upsert), chunk_size):
            chunk = to_upsert[start : start + chunk_size]
            try:
                cursor.executemany(insert_sql, chunk)
                for row in cursor.fetchall():
                    inserted_count += 1 if cursor.lastrowid else 0
                    updated_count += 1 if not cursor.lastrowid else 0
                for i in range(start, start + len(chunk)):
                    results[i]["status"] = "success"
                    results[i]["reason"] = "Inserted" if cursor.lastrowid else "Updated"
                    logger.success(results[i]["key"], results[i]["reason"])
            except sqlite3.Error as e:
                logging.error(f"Chunk upsert error: {e}")
                for i in range(start, start + len(chunk)):
                    results[i]["status"] = "failed"
                    results[i]["reason"] = str(e)
                    logger.failed(results[i]["key"], "Player license upsert failed")
        
        logging.info(f"Batch upsert completed: {inserted_count} inserted, {updated_count} updated")
        return results
        
    # @staticmethod
    # def batch_insert(
    #     cursor, 
    #     licenses: List["PlayerLicense"], 
    #     logger
    # ) -> List[Dict[str, Any]]:
    #     """
    #     Batch-insert PlayerLicense objects in safe-sized chunks to avoid exceeding
    #     SQLite’s default variable limit (~999). Returns one dict per input license
    #     summarizing skip/insert/fail.
    #     Aligns with Player.save_to_db pattern but for batch.
    #     """
    #     # Assume all licenses are validated as new; no need to re-fetch existing

    #     to_insert: List[tuple] = []
    #     results: List[Dict[str, Any]] = []

    #     # Prepare insert data
    #     for lic in licenses:
    #         key = (lic.player_id, lic.club_id, lic.season_id, lic.license_id)
    #         item_key = f"(player_id: {lic.player_id}, club_id: {lic.club_id}, season_id: {lic.season_id}, license_id: {lic.license_id})"
    #         to_insert.append((
    #             lic.player_id,
    #             lic.club_id,
    #             lic.season_id,
    #             lic.license_id,
    #             lic.valid_from,
    #             lic.valid_to
    #         ))
    #         results.append({
    #             "status": "pending",
    #             "key": item_key,
    #             "reason": "Will insert"
    #         })

    #     # Nothing to insert?
    #     if not to_insert:
    #         return results

    #     # Chunk size calculation
    #     MAX_VARS = 999
    #     COLS_PER_ROW = 6
    #     chunk_size = MAX_VARS // COLS_PER_ROW  # ~166

    #     insert_sql = """
    #         INSERT OR IGNORE INTO player_license
    #         (player_id, club_id, season_id, license_id, valid_from, valid_to)
    #         VALUES (?, ?, ?, ?, ?, ?)
    #     """

    #     # Execute chunks
    #     inserted_count = 0
    #     for start in range(0, len(to_insert), chunk_size):
    #         chunk = to_insert[start : start + chunk_size]
    #         try:
    #             cursor.executemany(insert_sql, chunk)
    #             inserted_count += cursor.rowcount
    #         except sqlite3.Error as e:
    #             logging.error(f"Chunk insert error: {e}")
    #             for i in range(start, start + len(chunk)):
    #                 results[i]["status"] = "failed"
    #                 results[i]["reason"] = str(e)
    #                 logger.failed(results[i]["key"], "Player license insert failed")
    #         else:
    #             for i in range(start, start + len(chunk)):
    #                 results[i]["status"] = "success"
    #                 results[i]["reason"] = "Inserted"
    #                 logger.success(results[i]["key"], "Player license inserted successfully")

    #     logging.info(f"Batch insert completed: {inserted_count} new licenses")
    #     return results

    @staticmethod
    def batch_validate(
        cursor, 
        licenses: List['PlayerLicense'], 
        logger
    ) -> List[dict]:         
        """Batch validate multiple PlayerLicense objects, including date range checks."""
        if not licenses:
            return []

        MAX_VARS = 999

        def chunk_iterable(iterable, size):
            it = list(iterable)
            for i in range(0, len(it), size):
                yield it[i : i + size]

        # Prepare sets of all IDs
        player_ids = {l.player_id for l in licenses if l.player_id}
        club_ids = {l.club_id for l in licenses if l.club_id}
        season_ids = {l.season_id for l in licenses if l.season_id}
        license_ids = {l.license_id for l in licenses if l.license_id}

        # 1) Chunked fetch of valid foreign keys
        valid_players = set()
        for chunk in chunk_iterable(player_ids, MAX_VARS):
            placeholders = ",".join("?" * len(chunk))
            cursor.execute(f"SELECT player_id FROM player WHERE player_id IN ({placeholders})", chunk)
            valid_players.update(r[0] for r in cursor.fetchall())

        valid_clubs = set()
        for chunk in chunk_iterable(club_ids, MAX_VARS):
            placeholders = ",".join("?" * len(chunk))
            cursor.execute(f"SELECT club_id FROM club WHERE club_id IN ({placeholders})", chunk)
            valid_clubs.update(r[0] for r in cursor.fetchall())

        # Fetch ALL seasons for reassignment flexibility
        valid_seasons = set()
        season_dates = {}
        cursor.execute("SELECT season_id, start_date, end_date FROM season")
        for sid, sd, ed in cursor.fetchall():
            valid_seasons.add(sid)
            season_dates[sid] = (sd, ed)

        valid_license_ids = set()
        for chunk in chunk_iterable(license_ids, MAX_VARS):
            placeholders = ",".join("?" * len(chunk))
            cursor.execute(f"SELECT license_id FROM license WHERE license_id IN ({placeholders})", chunk)
            valid_license_ids.update(r[0] for r in cursor.fetchall())

        # --- Fetch existing keys and periods broadly (covers reassignment) ---
        existing_keys = set()
        existing_periods = {}  # (pid, lid) -> list[(sid, vf, vt)]

        player_ids = {l.player_id for l in licenses if l.player_id}
        if player_ids:
            for chunk in chunk_iterable(player_ids, MAX_VARS):
                q = f"""
                    SELECT player_id, club_id, season_id, license_id
                    FROM player_license
                    WHERE player_id IN ({','.join('?'*len(chunk))})
                """
                cursor.execute(q, chunk)
                existing_keys.update(tuple(r) for r in cursor.fetchall())

            # Overlap periods grouped by (player_id, license_id)
            pairs = {(l.player_id, l.license_id) for l in licenses if l.player_id and l.license_id}
            pairs = list(pairs)
            for c in chunk_iterable(pairs, max(1, MAX_VARS // 2)):
                placeholders = ",".join("(?,?)" for _ in c)
                binds = [x for pair in c for x in pair]
                q = f"""
                    SELECT player_id, season_id, license_id, valid_from, valid_to
                    FROM player_license
                    WHERE (player_id, license_id) IN (VALUES {placeholders})
                """
                cursor.execute(q, binds)
                for pid, sid, lid, vf, vt in cursor.fetchall():
                    existing_periods.setdefault((pid, lid), []).append((sid, vf, vt))

        # 4) Per-license validation
        results = []
        for lic in licenses:
            # key = (lic.row_id, lic.player_id, lic.club_id, lic.season_id, lic.license_id)
            # ov_key = (lic.player_id, lic.season_id, lic.license_id)
            item_key = f"(row_id: {lic.row_id}, player_id: {lic.player_id}, club_id: {lic.club_id}, season_id: {lic.season_id}, license_id: {lic.license_id})"

            # Required fields
            if not all([lic.player_id, lic.club_id, lic.season_id, lic.license_id]):
                reason = "Missing required field"
                results.append({"status": "failed", "row_id": lic.row_id, "reason": reason})
                logger.failed(item_key, reason)
                continue

            # FK validation
            if lic.player_id not in valid_players:
                reason = f"Invalid player_id {lic.player_id}"
                results.append({"status": "failed", "row_id": lic.row_id, "reason": reason})
                logger.failed(item_key, reason)
                continue
            if lic.club_id not in valid_clubs:
                reason = f"Invalid club_id {lic.club_id}"
                results.append({"status": "failed", "row_id": lic.row_id, "reason": reason})
                logger.failed(item_key, reason)
                continue
            if lic.season_id not in valid_seasons:
                reason = f"Invalid season_id {lic.season_id}"
                results.append({"status": "failed", "row_id": lic.row_id, "reason": reason})
                logger.failed(item_key, reason)
                continue
            if lic.license_id not in valid_license_ids:
                reason = f"Invalid license_id {lic.license_id}"
                results.append({"status": "failed", "row_id": lic.row_id, "reason": reason})
                logger.failed(item_key, reason)
                continue

            # --- Season normalization & reassignment ---
            sd, ed = season_dates.get(lic.season_id, (None, None))

            def find_season_for(d):
                for sid_, (s, e) in season_dates.items():
                    if s <= d <= e:
                        return sid_
                return None

            if lic.season_id not in season_dates or not (sd <= lic.valid_from <= ed):
                new_sid = find_season_for(lic.valid_from)
                if new_sid is None:
                    reason = "Valid_from outside all seasons"
                    results.append({"status": "failed", "row_id": lic.row_id, "reason": reason})
                    logger.failed(item_key, reason)
                    continue
                if lic.season_id != new_sid:
                    logger.warning(item_key, f"Reassigned season_id")
                    lic.season_id = new_sid
                sd, ed = season_dates[new_sid]

            # if lic.valid_to is None:
            #     lic.valid_to = ed

            # Final normalized keys
            final_key = (lic.player_id, lic.club_id, lic.season_id, lic.license_id)
            pid_lid_key = (lic.player_id, lic.license_id)

            # Check DB duplicate after reassignment
            if final_key in existing_keys:
                reason = "License already exists"
                results.append({"status": "skipped", "row_id": lic.row_id, "reason": reason})
                logger.skipped(item_key, reason)
                continue

            # Overlap check (same player, license) for normalized season
            new_start, new_end = lic.valid_from, lic.valid_to
            overlapped = False
            for sid_, vf, vt in existing_periods.get(pid_lid_key, []):
                if sid_ != lic.season_id:
                    continue
                if isinstance(vf, str):
                    from datetime import date; vf = date.fromisoformat(vf)
                if isinstance(vt, str):
                    from datetime import date; vt = date.fromisoformat(vt)
                if not (new_end < vf or new_start > vt):
                    reason = "Overlaps existing license period"
                    results.append({"status": "failed", "row_id": lic.row_id, "reason": reason})
                    logger.failed(item_key, reason)
                    overlapped = True
                    break
            if overlapped:
                continue

            # Success
            results.append({"status": "success", "row_id": lic.row_id, "reason": "Valid"})

        return results

    @staticmethod
    def cache_find_by_name_club_date(licenses_cache, firstname, lastname, club_id, tournament_date, fallback_to_latest=False):
        """
        Strict lookup: find player_id with a valid license for tournament_date.
        Optional fallback: return the most recent license if no valid one is found.
        """
        fn_norm = normalize_key(firstname)
        ln_norm = normalize_key(lastname)
        key = (fn_norm, ln_norm, club_id)
        if key not in licenses_cache:
            return None

        # Strict match on date
        valid_pids = {
            lic["player_id"] for lic in licenses_cache[key]
            if lic["valid_from"] <= tournament_date <= lic["valid_to"]
        }
        if len(valid_pids) == 1:
            return next(iter(valid_pids))
        elif len(valid_pids) > 1:
            logging.warning(f"Ambiguous valid licenses for {firstname} {lastname} at club {club_id} on {tournament_date}")
            return next(iter(valid_pids))  # Arbitrary first

        if fallback_to_latest:
            most_recent = max(licenses_cache[key], key=lambda lic: lic["valid_to"], default=None)
            if most_recent:
                logging.info(f"Fallback to latest license for {firstname} {lastname} at club {club_id} (valid_to={most_recent['valid_to']})")
                return most_recent["player_id"]

        logging.warning(f"No valid license for {firstname} {lastname} at club {club_id} on {tournament_date}")
        return None

    @staticmethod
    def find_player_id(
        cursor, 
        licenses_cache, 
        raw_name, 
        club_id, 
        tournament_date,
        fallback_to_latest=True, 
        fuzzy_threshold=0.85
    ):
        """
        1. Try each (firstname, lastname) split—strict cache lookup
        2. Fuzzy match among licensed players in the club
        Note: Removed alias lookup as it's handled in Player class.
        """
        parts = raw_name.split()
        if len(parts) < 2:
            return None

        # Generate possible (fn, ln) splits
        for i in range(1, len(parts)):
            fn = " ".join(parts[:i])
            ln = " ".join(parts[i:])
            pid = PlayerLicense.cache_find_by_name_club_date(
                licenses_cache, fn, ln, club_id, tournament_date, fallback_to_latest
            )
            if pid:
                logging.info(f"Matched split: '{raw_name}' → fn='{fn}', ln='{ln}' pid={pid}")
                return pid

        # Fuzzy match (if needed, implement with difflib or similar if imported)
        # For now, skip as not implemented in original
        return None