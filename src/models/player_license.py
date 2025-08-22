# src/models/player_license.py

from datetime import date
from dataclasses import dataclass
import logging
from typing import Optional, List, Tuple, Dict, Any, Set, Iterable
from collections import defaultdict
import sqlite3
from models.cache_mixin import CacheMixin
from utils import normalize_key

@dataclass
class PlayerLicense(CacheMixin):
    player_id:    int
    club_id:      int
    season_id:    int
    license_id:   int
    valid_from:   date
    valid_to:     date
    row_id:       Optional[int] = None

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
    
    @classmethod
    def cache_name_club_map(cls, cursor) -> Dict[Tuple[str, str, int], List[Dict[str, Any]]]:
        """
        Build a (first_name_norm, last_name_norm, club_id) to list of license dicts map using cached query.
        Note: Returns list of dicts per key for licenses, as there might be multiple per name/club.
        """
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

        license_map: Dict[Tuple[str, str, int], List[Dict[str, Any]]] = {}
        for row in rows:
            fn_norm = normalize_key(row['firstname'])
            ln_norm = normalize_key(row['lastname'])
            key = (fn_norm, ln_norm, row['club_id'])
            license_map.setdefault(key, []).append({
                "player_id": row['player_id'],
                "club_id": row['club_id'],
                "license_number": row['license_id'],
                "valid_from": row['valid_from'],
                "valid_to": row['valid_to']
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
    def has_license(
        license_map: Dict[int, Set[Tuple[int, int]]],
        player_id: int,
        club_id: int,
        seasons: Iterable[int]
    ) -> bool:
        """
        Check if the given player_id has a license in club_id for any of the specified seasons.
        """
        licenses = license_map.get(player_id, ())
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
    def batch_save_to_db(cursor, licenses, logger: List["PlayerLicense"]) -> List[Dict[str, Any]]:
        """
        Batch-insert PlayerLicense objects in safe-sized chunks so as not to exceed
        SQLite’s default variable limit (~999). Returns one dict per input license
        summarizing skip/insert/fail.
        """
        # 1) Load existing keys to skip duplicates
        cursor.execute("""
            SELECT player_id, club_id, season_id, license_id
              FROM player_license
        """)
        existing = {tuple(r) for r in cursor.fetchall()}

        to_insert: List[tuple]          = []
        insert_positions: List[int]     = []
        results: List[Dict[str, Any]]   = []

        # 2) Build both lists in lockstep
        for idx, lic in enumerate(licenses):
            key = (lic.player_id, lic.club_id, lic.season_id, lic.license_id)
            if key in existing:
                results.append({
                    "status": "skipped",
                    "key": str(key),
                    "reason": "Already exists"
                })
            else:
                results.append({
                    "status": "pending",
                    "key": str(key),
                    "reason": "Will insert"
                })
                to_insert.append((
                    lic.player_id,
                    lic.club_id,
                    lic.season_id,
                    lic.license_id,
                    lic.valid_from,
                    lic.valid_to
                ))
                insert_positions.append(idx)

        # 3) Nothing new?
        if not to_insert:
            return results

        # 4) Figure out chunk size (SQLite default max vars ≈999, 6 columns per row)
        MAX_VARS     = 999
        COLS_PER_ROW = 6
        chunk_size   = MAX_VARS // COLS_PER_ROW  # = 166

        insert_sql = """
            INSERT OR IGNORE INTO player_license
              (player_id, club_id, season_id, license_id, valid_from, valid_to)
            VALUES (?, ?, ?, ?, ?, ?)
        """

        # 5) Execute each chunk separately
        for start in range(0, len(to_insert), chunk_size):
            chunk      = to_insert[start : start + chunk_size]
            positions  = insert_positions[start : start + chunk_size]
            key_item = (lic.player_id, lic.club_id, lic.season_id, lic.license_id, lic.valid_from, lic.valid_to)
            try:
                cursor.executemany(insert_sql, chunk)
            except sqlite3.Error as e:
                logging.error(f"Chunk insert error rows {start}-{start+len(chunk)-1}: {e}")
                for pos in positions:
                    results[pos].update(status="failed", reason=str(e))
                    logger.failed(key_item, f"Player license insert failed")
            else:
                for pos in positions:
                    results[pos].update(status="success", reason="Inserted")
                    logger.success(key_item, f"Player license inserted successfully")

        return results
    
    @staticmethod
    def validate_batch(cursor, licenses, logger: List['PlayerLicense']) -> List[dict]:         
        try:

            """Batch validate multiple PlayerLicense objects, including date range checks."""
            if not licenses:
                return []

            MAX_VARS = 999
            def chunked(ids):
                """Yield successive chunks of at most MAX_VARS items."""
                it = list(ids)
                for i in range(0, len(it), MAX_VARS):
                    yield it[i : i + MAX_VARS]

            # Prepare sets of all IDs we need to check
            player_ids  = {l.player_id  for l in licenses if l.player_id}
            club_ids    = {l.club_id    for l in licenses if l.club_id}
            season_ids  = {l.season_id  for l in licenses if l.season_id}
            license_ids = {l.license_id for l in licenses if l.license_id}

            # 1) Chunked fetch of valid foreign keys
            valid_players  = set()
            valid_clubs    = set()
            valid_seasons  = set()
            season_dates   = {}
            valid_license_ids = set()

            # players
            for chunk in chunked(player_ids):
                placeholders = ",".join("?" * len(chunk))
                cursor.execute(
                    f"SELECT player_id FROM player WHERE player_id IN ({placeholders})",
                    chunk
                )
                valid_players.update(r[0] for r in cursor.fetchall())

            # clubs
            for chunk in chunked(club_ids):
                placeholders = ",".join("?" * len(chunk))
                cursor.execute(
                    f"SELECT club_id FROM club WHERE club_id IN ({placeholders})",
                    chunk
                )
                valid_clubs.update(r[0] for r in cursor.fetchall())

            # seasons + dates
            for chunk in chunked(season_ids):
                placeholders = ",".join("?" * len(chunk))
                cursor.execute(
                    f"SELECT season_id, start_date, end_date \
                    FROM season WHERE season_id IN ({placeholders})",
                    chunk
                )
                for sid, sd, ed in cursor.fetchall():
                    valid_seasons.add(sid)
                    season_dates[sid] = (sd, ed)

            # licenses
            for chunk in chunked(license_ids):
                placeholders = ",".join("?" * len(chunk))
                cursor.execute(
                    f"SELECT license_id FROM license WHERE license_id IN ({placeholders})",
                    chunk
                )
                valid_license_ids.update(r[0] for r in cursor.fetchall())

            # 2) Chunked fetch of existing player_license keys
            existing_licenses = set()
            players_with_licenses = {k[0] for k in existing_licenses}  # Set of player_ids with at least one existing license
            all_lics = licenses
            cols_per_row = 4
            for i in range(0, len(all_lics), MAX_VARS // cols_per_row):
                slice_ = all_lics[i : i + (MAX_VARS // cols_per_row)]
                placeholders = ",".join("(?,?,?,?)" for _ in slice_)
                binds = []
                for lic in slice_:
                    binds.extend((lic.player_id, lic.club_id, lic.season_id, lic.license_id))
                cursor.execute(
                    f"SELECT player_id, club_id, season_id, license_id \
                    FROM player_license WHERE (player_id,club_id,season_id,license_id) IN ({placeholders})",
                    binds
                )
                existing_licenses.update(tuple(r) for r in cursor.fetchall())

            # 3) Chunked fetch of overlap periods
            overlapping_licenses = {}
            cols_per_row = 3
            for i in range(0, len(all_lics), MAX_VARS // cols_per_row):
                slice_ = all_lics[i : i + (MAX_VARS // cols_per_row)]
                placeholders = ",".join("(?,?,?)" for _ in slice_)
                binds = []
                for lic in slice_:
                    binds.extend((lic.player_id, lic.season_id, lic.license_id))
                cursor.execute(
                    f"SELECT player_id, season_id, license_id, valid_from, valid_to \
                    FROM player_license WHERE (player_id,season_id,license_id) IN ({placeholders})",
                    binds
                )
                for pid, sid, lid, vf, vt in cursor.fetchall():
                    overlapping_licenses[(pid, sid, lid)] = (vf, vt)

            # 4) Per-license validation
            results = []
            for lic in licenses:
                key = (lic.player_id, lic.club_id, lic.season_id, lic.license_id)
                ov_key = (lic.player_id, lic.season_id, lic.license_id)
                item_key = f"Player_id: {lic.player_id}, Club_id: {lic.club_id}, Season_id: {lic.season_id}, License_id: {lic.license_id})"

                # null/zero checks
                if not lic.player_id:
                    # results.append({"status":"failed","row_id":lic.row_id,"reason":f"Invalid player_id: {lic.player_id}"})
                    results.append({"status":"failed","row_id":lic.row_id,"reason":f"Invalid player_id: {lic.player_id}"})
                    logger.failed(item_key, f"Invalid player_id: {lic.player_id}")
                    continue
                if not lic.club_id:
                    results.append({"status":"failed","row_id":lic.row_id,"reason":f"Invalid club_id: {lic.club_id}"})
                    logger.failed(item_key, f"Invalid club_id: {lic.club_id}")
                    continue
                if not lic.season_id:
                    results.append({"status":"failed","row_id":lic.row_id,"reason":f"Invalid season_id: {lic.season_id}"})
                    logger.failed(item_key, f"Invalid season_id: {lic.season_id}")
                    continue
                if not lic.license_id:
                    results.append({"status":"failed","row_id":lic.row_id,"reason":f"Invalid license_id: {lic.license_id}"})
                    logger.failed(item_key, f"Invalid license_id: {lic.license_id}")
                    continue

                # FK checks
                if lic.player_id not in valid_players:
                    results.append({"status":"failed","row_id":lic.row_id,"reason":f"Foreign key violation: player_id {lic.player_id}"})
                    logger.failed(item_key, f"Foreign key violation: player_id {lic.player_id}")
                    continue
                if lic.club_id not in valid_clubs:
                    results.append({"status":"failed","row_id":lic.row_id,"reason":f"Foreign key violation: club_id {lic.club_id}"})
                    logger.failed(item_key, f"Foreign key violation: club_id {lic.club_id}")
                    continue
                if lic.season_id not in valid_seasons:
                    results.append({"status":"failed","row_id":lic.row_id,"reason":f"Foreign key violation: season_id {lic.season_id}"})
                    logger.failed(item_key, f"Foreign key violation: season_id {lic.season_id}")
                    continue
                if lic.license_id not in valid_license_ids:
                    results.append({"status":"failed","row_id":lic.row_id,"reason":f"Foreign key violation: license_id {lic.license_id}"})
                    logger.failed(item_key, f"Foreign key violation: license_id {lic.license_id}")
                    continue

                # Already exists?
                if key in existing_licenses:
                    results.append({"status":"skipped","row_id":lic.row_id,"reason":f"Player license already exists"})
                    logger.skipped(item_key, f"Player license already exists")
                    continue

                # Date‐range check
                sd, ed = season_dates.get(lic.season_id, (None,None))
                if not sd or not ed:
                    results.append({"status":"failed","row_id":lic.row_id,"reason":f"Season date missing"})
                    logger.failed(item_key, f"Season date missing")
                    continue
                if lic.valid_from < sd or lic.valid_from > ed:
                    # allow up to 30d fudge beyond season_end
                    if abs((lic.valid_from - ed).days) <= 30:
                        lic.valid_from = ed
                    else:
                        results.append({"status":"failed","row_id":lic.row_id,"reason":f"Valid from {lic.valid_from} outside {sd}–{ed}"})
                        logger.failed(item_key, f"Valid from outside season range")
                        continue
                if lic.valid_from == ed:
                    logger.warning(item_key, "Valid from date equals season end date")
                    # results.append({"status":"skipped",
                    #                 "row_id":lic.row_id,
                    #                 "reason":"Valid from date equals season end date"})
                    # continue

                # Overlap check
                if ov_key in overlapping_licenses:
                    of, ot = overlapping_licenses[ov_key]
                    # overlap if NOT (new_end < of OR new_start > ot)
                    new_start, new_end = lic.valid_from, lic.valid_to
                    if not (new_end < of or new_start > ot):
                        logger.failed(item_key, "Overlaps existing license")
                        continue
                    
                # Everything OK
                results.append({"status":"success","row_id":lic.row_id,"reason": "Valid player license"})

            return results

        except Exception as e:
            logging.error(f"Error batch validating licenses: {e}")
            # fail all on error
            return [{"status":"failed","row_id":l.row_id,"reason":f"Database error: {e}"} for l in licenses]
        


    @staticmethod
    def cache_find_by_name_club_date(licenses_cache, firstname, lastname, club_id, tournament_date, fallback_to_latest=False):
        """
        Strict lookup: find player_id with a valid license for tournament_date.
        Optional fallback: return the most recent license if no valid one is found.
        """
        key = (firstname, lastname, club_id)
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
                    f"Fallback: using most recent license for {firstname} {lastname} at club_id={club_id} "
                    f"(valid_to={most_recent['valid_to']})"
                )
                return most_recent["player_id"]
            
        logging.warning(f"No valid license found for {firstname} {lastname} at club_id={club_id} on {tournament_date}")

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
        1. Try each (lastname, firstname) split—strict cache lookup
        2. Fuzzy match among licensed players in the club
        # CHANGE: Removed step 2 (Exact alias lookup) from the description, as aliases are now merged into ext_ids and handled upstream in Player class caches. This simplifies the method and avoids redundant DB queries.
        """
        parts = raw_name.split()
        candidates = []

        # Build every possible split point (any number of firstnames and lastnames)
        for i in range(1, len(parts)):
            lastname  = " ".join(parts[:i])
            firstname = " ".join(parts[i:])
            candidates.append((lastname, firstname))

        # 1 Strict date‐valid cache lookup (with fallback)
        for ln, fn in candidates:
            pid = PlayerLicense.cache_find_by_name_club_date(
                licenses_cache, fn, ln, club_id, tournament_date,
                fallback_to_latest=fallback_to_latest
            )
            if pid:
                logging.info(f"Matched strict/fallback: '{raw_name}' → fn='{fn}', ln='{ln}'")
                return pid

        # CHANGE: Removed the entire block for "2 Exact alias lookup". Original code had:
        # for ln, fn in candidates:
        #     cursor.execute("""
        #         SELECT pa.player_id, p.firstname, p.lastname
        #           FROM player_alias pa
        #           JOIN player       p ON pa.player_id = p.player_id
        #          WHERE pa.firstname = ? AND pa.lastname = ?
        #     """, (fn, ln))
        #     row = cursor.fetchone()
        #     if row:
        #         alias_pid, alias_fn, alias_ln = row
        #         pid = PlayerLicense.cache_find_by_name_club_date(
        #             licenses_cache, alias_fn, alias_ln, club_id, tournament_date,
        #             fallback_to_latest=fallback_to_latest
        #         )
        #         if pid:
        #             logging.info(f"Matched alias: '{raw_name}' → '{alias_fn} {alias_ln}'")
        #             return pid
        # RATIONALE: With the merge of player_alias into player_id_ext, alias resolution is now part of the Player class (e.g., via cache_id_ext_map and cache_name_year_map). Lookups here should rely on the pre-cached Player data, not query player_alias (which no longer exists). This prevents errors and improves performance.

        # # 3 Fuzzy fallback among all players licensed at this club
        # cursor.execute("""
        #     SELECT DISTINCT p.firstname, p.lastname, p.player_id
        #       FROM player p
        #       JOIN player_license pl ON p.player_id = pl.player_id
        #      WHERE pl.club_id = ?
        # """, (club_id,))
        # rows = cursor.fetchall()

        # target = raw_name.lower()
        # best_ratio, best_pid, best_name = 0.0, None, None
        # for db_fn, db_ln, db_pid in rows:
        #     db_name = f"{db_fn} {db_ln}".lower()
        #     ratio = difflib.SequenceMatcher(None, target, db_name).ratio()
        #     if ratio > best_ratio:
        #         best_ratio, best_pid, best_name = ratio, db_pid, f"{db_fn} {db_ln}"

        # if best_ratio >= fuzzy_threshold:
        #     logging.info(f"Fuzzy matched '{raw_name}' → '{best_name}' (score={best_ratio:.2f})")
        #     return best_pid

        logging.warning(f"No match for '{raw_name}' at club_id={club_id}")
        return None
    

    # def validate(self, cursor) -> dict:
    #     """Validate a single PlayerLicense instance."""
    #     try:
    #         # Check if player_id exists in player table
    #         cursor.execute("SELECT 1 FROM player WHERE player_id = ?", (self.player_id,))
    #         if not cursor.fetchone():
    #             return {
    #                 "status": "failed",
    #                 "row_id": self.row_id,
    #                 "reason": f"Foreign key violation: player_id {self.player_id} does not exist in player table"
    #             }

    #         # Check if club_id exists in club table
    #         cursor.execute("SELECT 1 FROM club WHERE club_id = ?", (self.club_id,))
    #         if not cursor.fetchone():
    #             return {
    #                 "status": "failed",
    #                 "row_id": self.row_id,
    #                 "reason": f"Foreign key violation: club_id {self.club_id} does not exist in club table"
    #             }

    #         # Check if season_id exists in season table
    #         cursor.execute("SELECT 1 FROM season WHERE season_id = ?", (self.season_id,))
    #         if not cursor.fetchone():
    #             return {
    #                 "status": "failed",
    #                 "row_id": self.row_id,
    #                 "reason": f"Foreign key violation: season_id {self.season_id} does not exist in season table"
    #             }

    #         # Check if license_id exists in license table
    #         cursor.execute("SELECT 1 FROM license WHERE license_id = ?", (self.license_id,))
    #         if not cursor.fetchone():
    #             return {
    #                 "status": "failed",
    #                 "row_id": self.row_id,
    #                 "reason": f"Foreign key violation: license_id {self.license_id} does not exist in license table"
    #             }

    #         # Check if the record already exists
    #         cursor.execute("""
    #             SELECT 1 FROM player_license 
    #             WHERE player_id = ? AND license_id = ? AND season_id = ? AND club_id = ?
    #         """, (self.player_id, self.license_id, self.season_id, self.club_id))
    #         if cursor.fetchone():
    #             return {
    #                 "status": "skipped",
    #                 "row_id": self.row_id,
    #                 "reason": "Player license already exists in database"
    #             }

    #         # Check for overlapping licenses
    #         cursor.execute("""
    #             SELECT 1 FROM player_license 
    #             WHERE player_id = ? AND license_id = ? AND season_id = ? 
    #             AND ((valid_from <= ? AND valid_to >= ?) OR (valid_from <= ? AND valid_to >= ?))
    #         """, (self.player_id, self.license_id, self.season_id, self.valid_from, self.valid_from, self.valid_to, self.valid_to))
    #         if cursor.fetchone():
    #             return {
    #                 "status": "success",
    #                 "row_id": self.row_id,
    #                 "reason": "Warning! Player already has a license of the same type with overlapping dates"
    #             }

    #         return {
    #             "status": "success",
    #             "row_id": self.row_id,
    #             "reason": "Player license validated successfully"
    #         }
    #     except Exception as e:
    #         logging.error(f"Error validating license for player_id {self.player_id}: {e}")
    #         return {
    #             "status": "failed",
    #             "row_id": self.row_id,
    #             "reason": f"Database error: {str(e)}"
    #         }        

    # def save_to_db(self, cursor, logger):
    #     try:
    #         item_key = f"{self.firstname} {self.lastname} (Player_id: {self.player_id}, Club_id: {self.club_id}, Season_id: {self.season_id}, License_id: {self.license_id})"
    #         # Check if player_id exists in player table
    #         cursor.execute("SELECT 1 FROM player WHERE player_id = ?", (self.player_id,))
    #         if not cursor.fetchone():
    #             logger.failed(item_key, f"Foreign key violation: player_id does not exist in player table")
    #             return {
    #                 "status": "failed",
    #                 "player_id": self.player_id,
    #                 "reason": f"Foreign key violation: player_id {self.player_id} does not exist in player table"
    #             }

    #         # Check if club_id exists in club table
    #         cursor.execute("SELECT 1 FROM club WHERE club_id = ?", (self.club_id,))
    #         if not cursor.fetchone():
    #             logger.failed(item_key, f"Foreign key violation: club_id does not exist in club table")
    #             return {
    #                 "status": "failed",
    #                 "player_id": self.player_id,
    #                 "reason": f"Foreign key violation: club_id {self.club_id} does not exist in club table"
    #             }

    #         # Check if season_id exists in season table
    #         cursor.execute("SELECT 1 FROM season WHERE season_id = ?", (self.season_id,))
    #         if not cursor.fetchone():
    #             logger.failed(item_key, f"Foreign key violation: season_id does not exist in season table")
    #             return {
    #                 "status": "failed",
    #                 "player_id": self.player_id,
    #                 "reason": f"Foreign key violation: season_id {self.season_id} does not exist in season table"
    #             }

    #         # Check if license_id exists in license table
    #         cursor.execute("SELECT 1 FROM license WHERE license_id = ?", (self.license_id,))
    #         if not cursor.fetchone():
    #             logger.failed(item_key, f"Foreign key violation: license_id does not exist in license table")
    #             return {
    #                 "status": "failed",
    #                 "player_id": self.player_id,
    #                 "reason": f"Foreign key violation: license_id {self.license_id} does not exist in license table"
    #             }

    #         # Check if the record already exists
    #         cursor.execute("""
    #             SELECT 1 FROM player_license 
    #             WHERE player_id = ? AND license_id = ? AND season_id = ? AND club_id = ?
    #         """, (self.player_id, self.license_id, self.season_id, self.club_id))
    #         if cursor.fetchone():
    #             logger.failed(item_key, f"Player license already exists in database")
    #             return {
    #                 "status": "skipped",
    #                 "player_id": self.player_id,
    #                 "reason": "Player license already exists in database"
    #             }

    #         # Check if the player already has a license of the same type with overlapping dates
    #         cursor.execute("""
    #             SELECT 1 FROM player_license 
    #             WHERE player_id = ? AND license_id = ? AND season_id = ? 
    #             AND ((valid_from <= ? AND valid_to >= ?) OR (valid_from <= ? AND valid_to >= ?))
    #         """, (self.player_id, self.license_id, self.season_id, self.valid_from, self.valid_from, self.valid_to, self.valid_to))

    #         overlapping_license = 1 if cursor.fetchone() else 0

    #         # Insert the player license
    #         cursor.execute("""
    #             INSERT INTO player_license (
    #                 player_id, club_id, valid_from, valid_to, license_id, season_id
    #             )
    #             VALUES (?, ?, ?, ?, ?, ?)
    #         """, (
    #             self.player_id, self.club_id, self.valid_from, self.valid_to,
    #             self.license_id, self.season_id
    #         ))

    #         if overlapping_license == 1:
    #             # logging.warning(f"Player player_id: {self.player_id} already has a license of the same type with overlapping dates. Club: {self.club_id}, Season: {self.season_id}, License: {self.license_id}, Dates: {self.valid_from} - {self.valid_to}")
    #             logger.warning(item_key, "Player already has a license of the same type with overlapping dates")
    #             logger.success(item_key, "Player license inserted successfully, but with overlapping dates")
    #             return {
    #                     "status": "success",
    #                     "player_id": self.player_id,
    #                     "reason": "Warning! Player already has a license of the same type with overlapping dates"
    #                 }

    #         logger.success(item_key, "Player license inserted successfully")
    #         logging.info(item_key, "Player license inserted successfully")
    #         return {
    #             "status": "success",
    #             "player_id": self.player_id,
    #             "reason": "Player license inserted successfully"
    #         }

    #     except Exception as e:
    #         return {
    #             "status": "failed",
    #             "player_id": self.player_id,
    #             "reason": f"Database error: {str(e)}"
    #         }