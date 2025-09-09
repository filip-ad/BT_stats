from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from typing import List, Optional, Dict, Any, Tuple
import sqlite3
from models.tournament import Tournament
from utils import OperationLogger, parse_date


@dataclass
class TournamentClass:
    tournament_class_id:            Optional[int] = None
    tournament_class_id_ext:        Optional[str] = None
    tournament_id:                  int = None
    tournament_class_type_id:       Optional[int] = None
    tournament_class_structure_id:  Optional[int] = None
    startdate:                      Optional[date] = None
    longname:                       str = None
    shortname:                      str = None
    gender:                         Optional[str] = None
    max_rank:                       Optional[int] = None
    max_age:                        Optional[int] = None
    url:                            Optional[str] = None
    data_source_id:                 int = 1
    is_valid:                       bool = True

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "TournamentClass":
        """Instantiate from a dict (keys matching column names)."""
        return TournamentClass(
            tournament_class_id             = d.get("tournament_class_id"),
            tournament_class_id_ext         = d.get("tournament_class_id_ext"),
            tournament_id                   = d["tournament_id"],
            tournament_class_type_id        = d.get("tournament_class_type_id"),
            tournament_class_structure_id   = d.get("tournament_class_structure_id"),
            startdate                       = parse_date(d.get("startdate"), context="TournamentClass.from_dict"),
            longname                        = d.get("longname", ""),
            shortname                       = d.get("shortname", ""),
            gender                          = d.get("gender"),
            max_rank                        = d.get("max_rank"),
            max_age                         = d.get("max_age"),
            url                             = d.get("url"),
            data_source_id                  = d.get("data_source_id", 1),
            is_valid                        = d.get("is_valid", True),
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
        if not self.tournament_id:
            missing.append("tournament_id")
        if self.tournament_class_type_id is None or self.tournament_class_type_id == 9:
            missing.append("tournament_class_type_id (unknown)")
        if (
            self.tournament_class_structure_id is None
            or self.tournament_class_structure_id == 9
        ):
            missing.append("tournament_class_structure_id (unknown)")

        if missing:
            self.is_valid = False
            return False, f"Missing/invalid fields: {', '.join(missing)}"

        self.is_valid = True
        return True, ""

    def upsert(self, cursor: sqlite3.Cursor) -> Optional[str]:
        """
        Upsert tournament class data based on (tournament_class_id_ext, data_source_id)
        or (tournament_id, shortname, startdate).
        Returns "inserted" or "updated" on success, None on no change.
        """
        action = None
        tournament_class_id = None

        if self.tournament_class_id_ext is not None:
            cursor.execute(
                "SELECT tournament_class_id FROM tournament_class WHERE tournament_class_id_ext = ? AND data_source_id = ?;",
                (self.tournament_class_id_ext, self.data_source_id),
            )
            row = cursor.fetchone()
            if row:
                tournament_class_id = row[0]
                # Prepare values for UPDATE
                vals = (
                    self.tournament_id,
                    self.tournament_class_type_id,
                    self.tournament_class_structure_id,
                    self.startdate,
                    self.longname or None,
                    self.shortname or None,
                    self.gender or None,
                    self.max_rank,
                    self.max_age,
                    self.url or None,
                    self.is_valid,
                    tournament_class_id,
                )
                cursor.execute(
                    """
                    UPDATE tournament_class
                    SET tournament_id                 = ?,
                        tournament_class_type_id      = ?,
                        tournament_class_structure_id = ?,
                        startdate                     = ?,
                        longname                      = ?,
                        shortname                     = ?,
                        gender                        = ?,
                        max_rank                      = ?,
                        max_age                       = ?,
                        url                           = ?,
                        is_valid                      = ?,
                        row_updated                   = CURRENT_TIMESTAMP
                    WHERE tournament_class_id = ?
                    RETURNING tournament_class_id;
                    """,
                    vals,
                )
                self.tournament_class_id = cursor.fetchone()[0]
                action = "updated"

        if (
            action is None
            and self.tournament_id
            and self.shortname
            and self.startdate
        ):
            cursor.execute(
                "SELECT tournament_class_id FROM tournament_class WHERE tournament_id = ? AND shortname = ? AND startdate = ?;",
                (self.tournament_id, self.shortname, self.startdate),
            )
            row = cursor.fetchone()
            if row:
                tournament_class_id = row[0]
                # Prepare values for UPDATE with fallback key
                vals = (
                    self.tournament_class_id_ext or None,
                    self.tournament_id,
                    self.tournament_class_type_id,
                    self.tournament_class_structure_id,
                    self.startdate,
                    self.longname or None,
                    self.shortname or None,
                    self.gender or None,
                    self.max_rank,
                    self.max_age,
                    self.url or None,
                    self.data_source_id,
                    self.is_valid,
                    tournament_class_id,
                )
                cursor.execute(
                    """
                    UPDATE tournament_class
                    SET tournament_class_id_ext       = ?,
                        tournament_id                 = ?,
                        tournament_class_type_id      = ?,
                        tournament_class_structure_id = ?,
                        startdate                     = ?,
                        longname                      = ?,
                        shortname                     = ?,
                        gender                        = ?,
                        max_rank                      = ?,
                        max_age                       = ?,
                        url                           = ?,
                        data_source_id                = ?,
                        is_valid                      = ?,
                        row_updated                   = CURRENT_TIMESTAMP
                    WHERE tournament_class_id = ?
                    RETURNING tournament_class_id;
                    """,
                    vals,
                )
                self.tournament_class_id = cursor.fetchone()[0]
                action = "updated"

        if action is None:
            # INSERT (only if we have enough data)
            vals = (
                self.tournament_class_id_ext or None,
                self.tournament_id,
                self.tournament_class_type_id,
                self.tournament_class_structure_id,
                self.startdate,
                self.longname or None,
                self.shortname or None,
                self.gender or None,
                self.max_rank,
                self.max_age,
                self.url or None,
                self.data_source_id,
                self.is_valid,
            )
            cursor.execute(
                """
                INSERT INTO tournament_class (
                    tournament_class_id_ext,
                    tournament_id,
                    tournament_class_type_id,
                    tournament_class_structure_id,
                    startdate,
                    longname,
                    shortname,
                    gender,
                    max_rank,
                    max_age,
                    url,
                    data_source_id,
                    is_valid
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING tournament_class_id;
                """,
                vals,
            )
            self.tournament_class_id = cursor.fetchone()[0]
            action = "inserted"

        return action

    def get_final_stage(self) -> Optional[int]:
            """
            Determine the final stage based on tournament_class_structure_id.
            Returns the stage number for final positions PDF or None if unknown.
            """
            if self.tournament_class_structure_id == 1:  # Groups_and_KO
                return 6
            elif self.tournament_class_structure_id == 2:  # Groups_only
                return 4
            elif self.tournament_class_structure_id == 3:  # KO_only
                return 6
            else:  # 9 (Unknown) or None
                return None

    @classmethod
    def get_filtered_classes(
        cls,
        cursor: sqlite3.Cursor,
        class_id_exts: Optional[List[str]] = None,  # External class IDs
        tournament_id_exts: Optional[List[str]] = None,  # External tournament IDs
        data_source_id: Optional[int] = None,
        cutoff_date: Optional[date] = None,
        require_ended: bool = True,
        allowed_type_ids: Optional[List[int]] = None,
        allowed_structure_ids: Optional[List[int]] = None,  # Filter by structure_id
        max_classes: Optional[int] = None,
        order: Optional[str] = None,
        cache_key: Optional[str] = None  # e.g., 'id', 'tournament_id'; None for direct SQL
    ) -> List['TournamentClass']:
        """Load and filter tournament classes based on config settings.
        
        Accepts either class_id_exts or tournament_id_exts (with data_source_id), converts to internal IDs,
        and applies other filters. If neither ext list is provided, uses only other filters.
        Raises error if both ext lists are provided or if data_source_id missing when exts given.
        If cache_key is provided, queries all classes, builds an in-memory cache dict using the specified key,
        and filters/looks up in memory. If cache_key is None, builds a dynamic SQL query with filters
        for efficiency (direct fetch of matching rows with filters).
        Note: cache_key options adjusted to internal keys (e.g., 'id' for tournament_class_id, 'tournament_id').
        """
        if class_id_exts is not None and tournament_id_exts is not None:
            raise ValueError("Cannot provide both class_id_exts and tournament_id_exts")
        if (class_id_exts is not None or tournament_id_exts is not None) and data_source_id is None:
            raise ValueError("data_source_id required when providing class_id_exts or tournament_id_exts")

        class_ids: Optional[List[int]] = None
        tournament_ids: Optional[List[int]] = None
        if class_id_exts is not None:
            class_ids = cls.get_internal_class_ids(cursor, class_id_exts, data_source_id)
        elif tournament_id_exts is not None:
            tournament_ids = Tournament.get_internal_tournament_ids(cursor, tournament_id_exts, data_source_id)

        classes: List['TournamentClass']

        if cache_key is not None:
            # Query all classes
            all_query = """
                SELECT tc.*, t.tournament_status_id AS tournament_status_id
                FROM tournament_class tc
                JOIN tournament t ON tc.tournament_id = t.tournament_id
            """
            cursor.execute(all_query)
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            all_rows = [dict(zip(columns, row)) for row in rows]
            all_classes = [cls.from_dict(r) for r in all_rows]  # Fixed from_row to from_dict

            # Build cache dict based on cache_key
            classes_by_key: Any = {}
            is_grouped = False
            if cache_key == 'tournament_id':
                classes_by_key = defaultdict(list)
                for tc in all_classes:
                    classes_by_key[tc.tournament_id].append(tc)
                is_grouped = True
            elif cache_key == 'id':
                for tc in all_classes:
                    classes_by_key[tc.tournament_class_id] = tc
            else:
                raise ValueError(f"Unknown cache_key: {cache_key}")

            # Lookup if specific IDs match the cache_key, else filter all_classes
            if class_ids is not None and cache_key == 'id':
                classes = [classes_by_key.get(cid) for cid in class_ids if cid in classes_by_key]
            elif tournament_ids is not None and cache_key == 'tournament_id':
                classes = [tc for tid in tournament_ids for tc in classes_by_key.get(tid, [])]
            elif class_ids is not None and cache_key != 'id':
                raise ValueError(f"class_ids derived but cache_key '{cache_key}' does not match 'id'")
            elif tournament_ids is not None and cache_key != 'tournament_id':
                raise ValueError(f"tournament_ids derived but cache_key '{cache_key}' does not match 'tournament_id'")
            else:
                # Filter all_classes in memory
                classes = [
                    tc for tc in all_classes
                    if (class_ids is None or tc.tournament_class_id in class_ids)
                    and (tournament_ids is None or tc.tournament_id in tournament_ids)
                    and (not require_ended or tc.tournament_status_id == 3)
                    and (allowed_type_ids is None or tc.tournament_class_type_id in allowed_type_ids)
                    and (allowed_structure_ids is None or tc.tournament_class_structure_id in allowed_structure_ids)
                    and (cutoff_date is None or tc.startdate >= cutoff_date)  # Fixed to use startdate
                ]

            # Apply sorting in memory
            order = (order or "").lower()
            if order == "newest":
                classes.sort(key=lambda tc: tc.startdate or date.min, reverse=True)
            elif order == "oldest":
                classes.sort(key=lambda tc: tc.startdate or date.min)

            # Apply max limit
            if max_classes is not None and max_classes > 0:
                classes = classes[:max_classes]

        else:
            # Direct dynamic SQL query
            query = """
                SELECT tc.*, t.tournament_status_id AS tournament_status_id
                FROM tournament_class tc
                JOIN tournament t ON tc.tournament_id = t.tournament_id
                WHERE 1=1
            """
            params: List[Any] = []

            if require_ended:
                query += " AND t.tournament_status_id = 3"

            if class_ids is not None:
                placeholders = ', '.join(['?'] * len(class_ids))
                query += f" AND tc.tournament_class_id IN ({placeholders})"
                params.extend(class_ids)

            if tournament_ids is not None:
                placeholders = ', '.join(['?'] * len(tournament_ids))
                query += f" AND tc.tournament_id IN ({placeholders})"
                params.extend(tournament_ids)

            if allowed_type_ids is not None:
                placeholders = ', '.join(['?'] * len(allowed_type_ids))
                query += f" AND tc.tournament_class_type_id IN ({placeholders})"
                params.extend(allowed_type_ids)

            if allowed_structure_ids is not None:
                placeholders = ', '.join(['?'] * len(allowed_structure_ids))
                query += f" AND tc.tournament_class_structure_id IN ({placeholders})"
                params.extend(allowed_structure_ids)

            if cutoff_date is not None:
                query += " AND tc.startdate >= ?"
                params.append(cutoff_date)

            # Apply ordering
            order = (order or "").lower()
            if order == "newest":
                query += " ORDER BY tc.startdate DESC"
            elif order == "oldest":
                query += " ORDER BY tc.startdate ASC"

            # Apply max limit
            if max_classes is not None and max_classes > 0:
                query += " LIMIT ?"
                params.append(max_classes)

            # Execute and instantiate
            cursor.execute(query, params)
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            row_dicts = [dict(zip(columns, row)) for row in rows]
            classes = [cls.from_dict(rd) for rd in row_dicts]

        return classes

# # src/models/tournament_class.py

# from __future__ import annotations

# from collections import defaultdict
# from dataclasses import dataclass
# import datetime
# from typing import Optional, List, Dict, Any, Tuple, Set
# import logging
# import sqlite3
# from models.cache_mixin import CacheMixin
# from models.tournament import Tournament
# from utils import OperationLogger, parse_date

# @dataclass
# class TournamentClass(CacheMixin):
#     tournament_class_id:            Optional[int]  = None           # Canonical ID for class
#     tournament_class_id_ext:        Optional[str]  = None           # External ID from ondata.se or other source
#     tournament_id:                  int = None                      # Foreign key to parent tournament
#     tournament_class_type_id:       Optional[int]  = None           # Type of class (e.g., "singles", "doubles")
#     tournament_class_structure_id:  Optional[int]  = None           # Foreign key to tournament structure (e.g., "knockout", "round-robin")
#     date:                           Optional[datetime.date] = None  # Date of the class
#     longname:                       str = None                      # Full description of the class
#     shortname:                      str = None                      # Short description of the class
#     gender:                         Optional[str]  = None           # Gender category (e.g., "male", "female")
#     max_rank:                       Optional[int]  = None           # Maximum rank allowed in the class
#     max_age:                        Optional[int]  = None           # Maximum age allowed in the class
#     url:                            Optional[str]  = None           # URL for the class
#     data_source_id:                 int = 1                         # Data source ID (default 1 for 'ondata')
#     is_valid:                       bool = False                    # Validity flag for the class

#     @staticmethod
#     def from_dict(d: Dict[str, Any]) -> "TournamentClass":
#         """Instantiate from a scraped dict (keys matching column names)."""
#         return TournamentClass(
#             tournament_class_id             = d.get("tournament_class_id"),
#             tournament_class_id_ext         = d.get("tournament_class_id_ext"),
#             tournament_id                   = d["tournament_id"],
#             tournament_class_type_id        = d.get("tournament_class_type_id"),
#             tournament_class_structure_id   = d.get("tournament_class_structure_id"),
#             date                            = parse_date(d.get("date"), context="TournamentClass.from_dict"),
#             longname                        = d.get("longname", ""),
#             shortname                       = d.get("shortname", ""),
#             gender                          = d.get("gender"),
#             max_rank                        = d.get("max_rank"),
#             max_age                         = d.get("max_age"),
#             url                             = d.get("url"),
#             data_source_id                  = d.get("data_source_id", 1),
#             is_valid                        = d.get("is_valid", False)
#         )
    
#     @classmethod
#     def from_row(
#         cls, 
#         row: Dict[str, Any]
#     ) -> 'TournamentClass':
#         """Instantiate from a database row dict, including extra fields like tournament_is_valid."""
#         return cls.from_dict(row)  # Assumes from_dict handles extra keys like tournament_is_valid
    
#     @classmethod
#     def get_internal_class_ids(
#         cls,
#         cursor: sqlite3.Cursor,
#         ext_ids: List[str],
#         data_source_id: int = 1
#     ) -> List[int]:
#         """Convert list of external class IDs (with data_source_id) to internal tournament_class_ids."""
#         if not ext_ids:
#             return []

#         placeholders = ', '.join(['?'] * len(ext_ids))
#         sql = f"""
#             SELECT tournament_class_id 
#             FROM tournament_class 
#             WHERE tournament_class_id_ext IN ({placeholders}) AND data_source_id = ?
#         """
#         params = ext_ids + [data_source_id]
#         cursor.execute(sql, params)
#         return [row[0] for row in cursor.fetchall()]
    
#     @classmethod
#     def get_valid_singles_after_cutoff(
#         cls, 
#         cursor: sqlite3.Cursor, 
#         cutoff_date: date
#     ) -> List[TournamentClass]:
#         sql = """
#             SELECT * FROM tournament_class
#             WHERE tournament_class_type_id = 1  -- Singles
#             AND date >= ?
#             AND is_valid = 1
#             ORDER BY date ASC;
#         """
#         results = cls.cached_query(cursor, sql, (cutoff_date,), cache_key_extra="get_valid_singles_after_cutoff")
#         return [cls.from_dict(res) for res in results]

#     @classmethod
#     def get_filtered_classes(
#         cls,
#         cursor:                 sqlite3.Cursor,
#         class_id_exts:          Optional[List[str]] = None,  # External class IDs
#         tournament_id_exts:     Optional[List[str]] = None,  # External tournament IDs
#         data_source_id:         Optional[int] = None,
#         cutoff_date:            Optional[date] = None,
#         require_ended:          bool = True,
#         allowed_type_ids:       Optional[List[int]] = None,
#         allowed_structure_ids:  Optional[List[int]] = None,  # NEW: Filter by structure_id (e.g., 1 for Groups_and_KO, 3 for KO_only)
#         max_classes:            Optional[int] = None,
#         order:                  Optional[str] = None,
#         cache_key:              Optional[str] = None  # e.g., 'id', 'tournament_id'; None for direct SQL query
#     ) -> List['TournamentClass']:
#         """Load and filter tournament classes based on config settings.
        
#         Accepts either class_id_exts or tournament_id_exts (with data_source_id), converts to internal IDs,
#         and applies other filters. If neither ext list is provided, uses only other filters.
#         Raises error if both ext lists are provided or if data_source_id missing when exts given.
#         If cache_key is provided, queries all classes, builds an in-memory cache dict using the specified key,
#         and filters/looks up in memory. If cache_key is None, builds a dynamic SQL query with filters
#         for efficiency (direct fetch of matching rows with filters).
#         Note: cache_key options adjusted to internal keys (e.g., 'id' for tournament_class_id, 'tournament_id').
#         """
#         if class_id_exts is not None and tournament_id_exts is not None:
#             raise ValueError("Cannot provide both class_id_exts and tournament_id_exts")
#         if (class_id_exts is not None or tournament_id_exts is not None) and data_source_id is None:
#             raise ValueError("data_source_id required when providing class_id_exts or tournament_id_exts")

#         class_ids: Optional[List[int]] = None
#         tournament_ids: Optional[List[int]] = None
#         if class_id_exts is not None:
#             class_ids = cls.get_internal_class_ids(cursor, class_id_exts, data_source_id)
#         elif tournament_id_exts is not None:
#             tournament_ids = Tournament.get_internal_tournament_ids(cursor, tournament_id_exts, data_source_id)

#         classes: List['TournamentClass']

#         if cache_key is not None:
#             # Query all classes (self-contained, no nested method calls)
#             all_query = """
#                 SELECT tc.*, t.tournament_status_id AS tournament_status_id
#                 FROM tournament_class tc
#                 JOIN tournament t ON tc.tournament_id = t.tournament_id
#             """
#             cursor.execute(all_query)
#             rows = cursor.fetchall()
#             columns = [col[0] for col in cursor.description]
#             all_rows = [dict(zip(columns, row)) for row in rows]
#             all_classes = [cls.from_row(r) for r in all_rows]

#             # Build cache dict based on cache_key
#             classes_by_key: Any = {}
#             is_grouped = False
#             if cache_key == 'tournament_id':
#                 classes_by_key = defaultdict(list)
#                 for tc in all_classes:
#                     classes_by_key[tc.tournament_id].append(tc)
#                 is_grouped = True
#             elif cache_key == 'id':
#                 for tc in all_classes:
#                     classes_by_key[tc.tournament_class_id] = tc  # Use internal id
#             else:
#                 raise ValueError(f"Unknown cache_key: {cache_key}")

#             # Lookup if specific IDs match the cache_key, else filter all_classes
#             if class_ids is not None and cache_key == 'id':
#                 classes = [classes_by_key.get(cid) for cid in class_ids if cid in classes_by_key]
#             elif tournament_ids is not None and cache_key == 'tournament_id':
#                 classes = [tc for tid in tournament_ids for tc in classes_by_key.get(tid, [])]
#             elif class_ids is not None and cache_key != 'id':
#                 raise ValueError(f"class_ids derived but cache_key '{cache_key}' does not match 'id'")
#             elif tournament_ids is not None and cache_key != 'tournament_id':
#                 raise ValueError(f"tournament_ids derived but cache_key '{cache_key}' does not match 'tournament_id'")
#             else:
#                 # No specific lookup: filter all_classes in memory
#                 classes = [
#                     tc for tc in all_classes
#                     if (class_ids is None or tc.tournament_class_id in class_ids)
#                     and (tournament_ids is None or tc.tournament_id in tournament_ids)
#                     and (not require_ended or tc.tournament_status_id == 3)
#                     and (allowed_type_ids is None or tc.tournament_class_type_id in allowed_type_ids)
#                     and (allowed_structure_ids is None or tc.tournament_class_structure_id in allowed_structure_ids)  # NEW: In-memory filter for structure_id
#                     and (cutoff_date is None or tc.date >= cutoff_date)
#                 ]

#             # Apply sorting in memory
#             order = (order or "").lower()
#             if order == "newest":
#                 classes.sort(key=lambda tc: tc.date or datetime.date.min, reverse=True)
#             elif order == "oldest":
#                 classes.sort(key=lambda tc: tc.date or datetime.date.min)

#             # Apply max limit
#             if max_classes is not None and max_classes > 0:
#                 classes = classes[:max_classes]

#         else:
#             # Direct dynamic SQL query (efficient, no in-memory filtering)
#             query = """
#                 SELECT tc.*, t.tournament_status_id AS tournament_status_id
#                 FROM tournament_class tc
#                 JOIN tournament t ON tc.tournament_id = t.tournament_id
#                 WHERE 1=1
#             """
#             params: List[Any] = []

#             if require_ended:
#                 query += " AND t.tournament_status_id = 3"

#             if class_ids is not None:
#                 placeholders = ', '.join(['?'] * len(class_ids))
#                 query += f" AND tc.tournament_class_id IN ({placeholders})"
#                 params.extend(class_ids)

#             if tournament_ids is not None:
#                 placeholders = ', '.join(['?'] * len(tournament_ids))
#                 query += f" AND tc.tournament_id IN ({placeholders})"
#                 params.extend(tournament_ids)

#             if allowed_type_ids is not None:
#                 placeholders = ', '.join(['?'] * len(allowed_type_ids))
#                 query += f" AND tc.tournament_class_type_id IN ({placeholders})"
#                 params.extend(allowed_type_ids)

#             if allowed_structure_ids is not None:  # NEW: Add to SQL WHERE
#                 placeholders = ', '.join(['?'] * len(allowed_structure_ids))
#                 query += f" AND tc.tournament_class_structure_id IN ({placeholders})"
#                 params.extend(allowed_structure_ids)

#             if cutoff_date is not None:
#                 query += " AND tc.date >= ?"
#                 params.append(cutoff_date)

#             # Apply ordering
#             order = (order or "").lower()
#             if order == "newest":
#                 query += " ORDER BY tc.date DESC"
#             elif order == "oldest":
#                 query += " ORDER BY tc.date ASC"

#             # Apply max limit
#             if max_classes is not None and max_classes > 0:
#                 query += " LIMIT ?"
#                 params.append(max_classes)

#             # Execute and instantiate
#             cursor.execute(query, params)
#             rows = cursor.fetchall()
#             columns = [col[0] for col in cursor.description]
#             row_dicts = [dict(zip(columns, row)) for row in rows]
#             classes = [cls.from_row(rd) for rd in row_dicts]

#         return classes
    
#     @classmethod
#     def cache_by_id_ext(cls, cursor: sqlite3.Cursor) -> Dict[int, 'TournamentClass']:
#         """Cache TournamentClass instances by tournament_class_id_ext."""
#         sql = """
#             SELECT tournament_class_id, tournament_class_id_ext, tournament_id, tournament_class_type_id,
#                 tournament_class_structure_id, date, longname, shortname, gender, max_rank, max_age,
#                 url, data_source_id, is_valid
#             FROM tournament_class
#             WHERE tournament_class_id_ext IS NOT NULL
#         """
#         rows = cls.cached_query(cursor, sql, cache_key_extra="cache_by_id_ext")
#         result = {}
#         for row in rows:
#             tc = cls.from_dict(row)
#             result[tc.tournament_class_id_ext] = tc
#         return result
    
#     @staticmethod
#     def get_by_ext_ids(cursor, ext_ids: List[int]) -> List['TournamentClass']:
#         """
#         Fetch TournamentClass instances by a list of tournament_class_id_ext.
#         Returns list of matching classes.
#         """
#         if not ext_ids:
#             return []

#         placeholders = ','.join('?' for _ in ext_ids)
#         sql = f"""
#             SELECT tournament_class_id, tournament_class_id_ext, tournament_id, tournament_class_type_id, tournament_class_structure_id,
#                    date, longname, shortname, gender, max_rank, max_age, url, data_source_id
#             FROM tournament_class
#             WHERE tournament_class_id_ext IN ({placeholders})
#         """
#         cursor.execute(sql, ext_ids)
#         rows = cursor.fetchall()
#         classes = []
#         for row in rows:
#             data = {
#                 'tournament_class_id':              row[0],
#                 'tournament_class_id_ext':          row[1],
#                 'tournament_id':                    row[2],
#                 'tournament_class_type_id':         row[3],
#                 'tournament_class_structure_id':    row[4],
#                 'date':                             row[5],
#                 'longname':                         row[6],
#                 'shortname':                        row[7],
#                 'gender':                           row[8],
#                 'max_rank':                         row[9],
#                 'max_age':                          row[10],
#                 'url':                              row[11],
#                 'data_source_id':                   row[12]
#             }
#             classes.append(TournamentClass.from_dict(data))
        
#         if len(classes) != len(ext_ids):
#             missing = set(ext_ids) - {c.tournament_class_id_ext for c in classes}
#             logging.warning(f"Missing classes for ext_ids: {missing}")
        
#         return classes

#     def upsert(
#             self, 
#             cursor, 
#             logger: OperationLogger, 
#             item_key: str
#         ):
#         """
#         Deterministic upsert for tournament_class.

#         Rules:
#         1) Prefer matching by (tournament_class_id_ext, data_source_id)
#         2) Else match by (tournament_id, shortname, date)
#         3) If a match is found -> UPDATE; else -> INSERT
#         4) If both match different rows -> log failure (manual merge)
#         """
#         vals = (
#             self.tournament_class_id_ext, self.tournament_id, self.tournament_class_type_id,
#             self.tournament_class_structure_id, self.date, self.longname, self.shortname,
#             self.gender, self.max_rank, self.max_age, self.url, self.data_source_id, self.is_valid
#         )

#         # 1) Try primary key (ext + data source)
#         primary_id = None
#         if self.tournament_class_id_ext and self.data_source_id:
#             cursor.execute(
#                 "SELECT tournament_class_id FROM tournament_class "
#                 "WHERE tournament_class_id_ext = ? AND data_source_id = ?;",
#                 (self.tournament_class_id_ext, self.data_source_id)
#             )
            
#             row = cursor.fetchone()
#             if row:
#                 primary_id = row[0]

#         # 2) Try fallback key (tournament_id, shortname, date)
#         fallback_id = None
#         cursor.execute(
#             "SELECT tournament_class_id FROM tournament_class "
#             "WHERE tournament_id = ? AND shortname = ? AND date = ?;",
#             (self.tournament_id, self.shortname, self.date)
#         )
#         row = cursor.fetchone()
#         if row:
#             fallback_id = row[0]


#         # 3) Conflict: they point to different rows
#         if primary_id and fallback_id and primary_id != fallback_id:
#             logger.failed(
#                 item_key,
#                 (f"Conflicting classes: ext={self.tournament_class_id_ext}/ds={self.data_source_id} → id {primary_id}, "
#                 f"(tournament_id, shortname, date)=({self.tournament_id}, {self.shortname}, {self.date}) → id {fallback_id}. "
#                 "Manual merge required.")
#             )
#             self.tournament_class_id = primary_id  # pick a stable id to proceed with in memory
#             return

#         target_id = primary_id or fallback_id

#         if target_id:
#             # UPDATE existing row (attach ext/ds if missing, update other fields)
#             cursor.execute(
#                 """
#                 UPDATE tournament_class
#                 SET tournament_class_id_ext       = COALESCE(?, tournament_class_id_ext),
#                     tournament_id                 = ?,
#                     tournament_class_type_id      = ?,
#                     tournament_class_structure_id = ?,
#                     date                          = ?,
#                     longname                      = ?,
#                     shortname                     = ?,
#                     gender                        = ?,
#                     max_rank                      = ?,
#                     max_age                       = ?,
#                     url                           = ?,
#                     data_source_id                = COALESCE(?, data_source_id),
#                     is_valid                      = ?,
#                     row_updated                   = CURRENT_TIMESTAMP
#                 WHERE tournament_class_id = ?
#                 RETURNING tournament_class_id;
#                 """,
#                 (*vals, target_id)
#             )
#             self.tournament_class_id = cursor.fetchone()[0]
#             basis = "id_ext+data_source" if primary_id else "fallback: tournament_id, shortname, date"
#             logger.success(item_key, f"Tournament class successfully updated ({basis})")
#             return

#         # INSERT new row
#         try:
#             cursor.execute(
#                 """
#                 INSERT INTO tournament_class (
#                     tournament_class_id_ext, tournament_id, tournament_class_type_id,
#                     tournament_class_structure_id, date, longname, shortname, gender,
#                     max_rank, max_age, url, data_source_id, is_valid
#                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
#                 RETURNING tournament_class_id;
#                 """,
#                 vals
#             )
#             self.tournament_class_id = cursor.fetchone()[0]
#             logger.success(item_key, f"Tournament class created (id {self.tournament_class_id} {self.shortname} {self.date})")

#             # debug
#             logging.info(f"Created: {self}")

#             return
#         except sqlite3.IntegrityError:
#             # Rare race: row appeared between our checks and INSERT.
#             # Retry as UPDATE against whichever key now resolves.
#             cursor.execute(
#                 "SELECT tournament_class_id FROM tournament_class "
#                 "WHERE tournament_class_id_ext = ? AND data_source_id = ?;",
#                 (self.tournament_class_id_ext, self.data_source_id)
#             )
#             row = cursor.fetchone()
#             if not row:
#                 cursor.execute(
#                     "SELECT tournament_class_id FROM tournament_class "
#                     "WHERE tournament_id = ? AND shortname = ? AND date = ?;",
#                     (self.tournament_id, self.shortname, self.date)
#                 )
#                 row = cursor.fetchone()

#             if row:
#                 target_id = row[0]
#                 cursor.execute(
#                     """
#                     UPDATE tournament_class
#                     SET tournament_class_id_ext       = COALESCE(?, tournament_class_id_ext),
#                         tournament_id                 = ?,
#                         tournament_class_type_id      = ?,
#                         tournament_class_structure_id = ?,
#                         date                          = ?,
#                         longname                      = ?,
#                         shortname                     = ?,
#                         gender                        = ?,
#                         max_rank                      = ?,
#                         max_age                       = ?,
#                         url                           = ?,
#                         data_source_id                = COALESCE(?, data_source_id),
#                         is_valid                      = ?,
#                         row_updated                   = CURRENT_TIMESTAMP
#                     WHERE tournament_class_id = ?
#                     RETURNING tournament_class_id;
#                     """,
#                     (*vals, target_id)
#                 )
#                 self.tournament_class_id = cursor.fetchone()[0]
#                 logger.success(item_key, f"Tournament class updated after race")
#                 return
#             raise


#     def validate(
#         self, 
#         logger: OperationLogger, 
#         item_key: str
#     ) -> Dict[str, str]:
#         """
#         Validate TournamentClass fields, set the valid flag, log to OperationLogger.
#         Returns dict with status and reason.
#         """

#         if not (self.shortname and self.date and self.tournament_id):
#             reason = "Missing required fields (shortname, date, tournament_id)"
#             logger.failed(item_key, reason)
#             self.is_valid = False
#             return {"status": "failed", "reason": reason}

#         # Check for inferred fields (e.g., type_id, structure_id)        
#         if self.tournament_class_structure_id == 9:
#             reason = "Tournament class structure (eg. groups only, KO only) now known, may need special handling"
#             logger.warning(item_key, reason)
#             self.is_valid = False
#             return {"status": "failed", "reason": reason}
        
#         if self.tournament_class_type_id == 9:
#             reason = "Tournament class type (eg. singles, doubles etc) unknown, may need special handling"
#             logger.warning(item_key, reason)
#             self.is_valid = False
#             return {"status": "failed", "reason": reason}

#         # Warnings (non-fatal, but could set valid=False if strict)
#         if not self.tournament_class_id_ext:
#             logger.warning(item_key, "No valid external ID (likely upcoming)")
#         if not self.longname:
#             logger.warning(item_key, "Missing longname")

#         self.is_valid = True

#         return {
#             "status": "success",
#             "reason": "Vdsfsdfdsfd"
#         }