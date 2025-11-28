from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from typing import List, Optional, Dict, Any, Tuple
import sqlite3
from models.tournament import Tournament
from models.cache_mixin import CacheMixin
from utils import parse_date


@dataclass
class TournamentClass(CacheMixin):
    """
    Represents a tournament class (e.g., "P12", "H2", "D1").
    
    Parent-Child Relationship (added 2025-11-28):
        B-playoff classes (e.g., "P12~B") have a parent relationship to their
        main class ("P12"). The parent class contains entries for players who
        didn't advance from the group stage but still appear in B-playoff matches.
        
        - tournament_class_id_parent: FK to parent class (NULL for main classes)
        - Helper methods: get_by_id(), get_parent_class(), get_sibling_classes()
        
        Detection: Classes with "~B" suffix in shortname are detected as children.
        See: resolve_tournament_classes.py second pass for parent detection logic.
    """
    tournament_class_id:            Optional[int] = None
    tournament_class_id_ext:        Optional[str] = None
    tournament_id:                  int = None
    tournament_class_type_id:       Optional[int] = None
    tournament_class_structure_id:  Optional[int] = None
    tournament_class_id_parent:     Optional[int] = None  # FK to parent class (e.g., P12 is parent of P12~B)
    ko_tree_size:                   Optional[int] = None
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
            tournament_class_id_parent      = d.get("tournament_class_id_parent"),
            ko_tree_size                    = d.get("ko_tree_size"),
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

    @classmethod
    def set_tree_size(
        cls,
        cursor: sqlite3.Cursor,
        tournament_class_id_ext: str,
        tree_size: int,
        data_source_id: int = 1,
    ) -> None:
        """Persist a corrected KO tree size for a class id ext."""
        if not tournament_class_id_ext or not tree_size:
            return
        cursor.execute(
            """
            UPDATE tournament_class
            SET ko_tree_size = ?, row_updated = CURRENT_TIMESTAMP
            WHERE tournament_class_id_ext = ? AND data_source_id = ?;
            """,
            (tree_size, tournament_class_id_ext, data_source_id),
        )
        # Clear cached SELECTs for this model so future reads see the update.
        cls.clear_cache()
    
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
                    self.tournament_class_id_parent,
                    self.ko_tree_size,
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
                        tournament_class_id_parent    = ?,
                        ko_tree_size                  = ?,
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
                    self.tournament_class_id_parent,
                    self.ko_tree_size,
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
                        tournament_class_id_parent    = ?,
                        ko_tree_size                  = ?,
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
                self.tournament_class_id_parent,
                self.ko_tree_size,
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
                    tournament_class_id_parent,
                    ko_tree_size,
                    startdate,
                    longname,
                    shortname,
                    gender,
                    max_rank,
                    max_age,
                    url,
                    data_source_id,
                    is_valid
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            elif self.tournament_class_structure_id == 4:  # Groups_and_Groups
                return 4
            else:  # 9 (Unknown) or None
                return None
            
    # Used by get_filtered_classes       
    @classmethod
    def get_internal_class_ids(
        cls,
        cursor: sqlite3.Cursor,
        ext_ids: List[str],
        data_source_id: int = 1
    ) -> List[int]:
        """Convert list of external class IDs (with data_source_id) to internal tournament_class_ids."""
        if not ext_ids:
            return []

        placeholders = ', '.join(['?'] * len(ext_ids))
        sql = f"""
            SELECT tournament_class_id 
            FROM tournament_class 
            WHERE tournament_class_id_ext IN ({placeholders}) AND data_source_id = ?
        """
        params = ext_ids + [data_source_id]
        cursor.execute(sql, params)
        return [row[0] for row in cursor.fetchall()]
        
    @classmethod
    def get_by_ext_id(cls, cursor: sqlite3.Cursor, tournament_class_id_ext: str) -> Optional['TournamentClass']:
        """Fetch TournamentClass by external ID with default caching."""
        rows = cls.cached_query(cursor, "SELECT * FROM tournament_class WHERE tournament_class_id_ext = ?", (tournament_class_id_ext,), cache_key_extra=f"tc_by_ext:{tournament_class_id_ext}")
        if not rows:
            return None
        return cls.from_dict(rows[0])

    @classmethod
    def get_by_id(cls, cursor: sqlite3.Cursor, tournament_class_id: int) -> Optional['TournamentClass']:
        """Fetch TournamentClass by internal ID."""
        rows = cls.cached_query(
            cursor, 
            "SELECT * FROM tournament_class WHERE tournament_class_id = ?", 
            (tournament_class_id,), 
            cache_key_extra=f"tc_by_id:{tournament_class_id}"
        )
        if not rows:
            return None
        return cls.from_dict(rows[0])

    def get_parent_class(self, cursor: sqlite3.Cursor) -> Optional['TournamentClass']:
        """
        Get the parent class if tournament_class_id_parent is set.
        For B-playoff classes like 'P12~B', this returns the main class 'P12'.
        """
        if not self.tournament_class_id_parent:
            return None
        return TournamentClass.get_by_id(cursor, self.tournament_class_id_parent)

    def get_sibling_classes(self, cursor: sqlite3.Cursor) -> List['TournamentClass']:
        """
        Get sibling classes that share the same parent (excluding self).
        If this class has no parent, returns empty list.
        If this IS the parent, returns all child classes.
        """
        siblings: List['TournamentClass'] = []
        
        if self.tournament_class_id_parent:
            # This is a child class - get other children of same parent
            rows = self.__class__.cached_query(
                cursor,
                """
                SELECT * FROM tournament_class 
                WHERE tournament_class_id_parent = ? 
                  AND tournament_class_id != ?
                """,
                (self.tournament_class_id_parent, self.tournament_class_id),
                cache_key_extra=f"tc_siblings_of_child:{self.tournament_class_id}"
            )
            siblings = [self.__class__.from_dict(r) for r in rows]
            
            # Also include the parent itself as a "sibling" for lookup purposes
            parent = self.get_parent_class(cursor)
            if parent:
                siblings.insert(0, parent)
        else:
            # This might be a parent class - get all children
            rows = self.__class__.cached_query(
                cursor,
                """
                SELECT * FROM tournament_class 
                WHERE tournament_class_id_parent = ?
                """,
                (self.tournament_class_id,),
                cache_key_extra=f"tc_children_of:{self.tournament_class_id}"
            )
            siblings = [self.__class__.from_dict(r) for r in rows]
        
        return siblings

    @classmethod
    def get_classes_in_tournament(
        cls, 
        cursor: sqlite3.Cursor, 
        tournament_id: int
    ) -> List['TournamentClass']:
        """Get all classes belonging to a tournament."""
        rows = cls.cached_query(
            cursor,
            "SELECT * FROM tournament_class WHERE tournament_id = ?",
            (tournament_id,),
            cache_key_extra=f"tc_by_tournament:{tournament_id}"
        )
        return [cls.from_dict(r) for r in rows]

    @classmethod
    def set_parent_class(
        cls,
        cursor: sqlite3.Cursor,
        tournament_class_id: int,
        parent_class_id: int,
    ) -> None:
        """Set the parent class for a tournament class (used during resolution)."""
        cursor.execute(
            """
            UPDATE tournament_class
            SET tournament_class_id_parent = ?, row_updated = CURRENT_TIMESTAMP
            WHERE tournament_class_id = ?
            """,
            (parent_class_id, tournament_class_id),
        )
        cls.clear_cache()
        
    @classmethod
    def get_filtered_classes(
        cls,
        cursor:                     sqlite3.Cursor,
        class_id_exts:              Optional[List[str]] = None,  # External class IDs
        tournament_id_exts:         Optional[List[str]] = None,  # External tournament IDs
        data_source_id:             Optional[int] = None,
        cutoff_date:                Optional[date] = None,
        require_ended:              bool = True,
        allowed_type_ids:           Optional[List[int]] = None,
        allowed_structure_ids:      Optional[List[int]] = None,  # Filter by structure_id
        max_classes:                Optional[int] = None,
        order:                      Optional[str] = None,
        cache_key:                  Optional[str] = None  # e.g., 'id', 'tournament_id'; None for direct SQL
    ) -> List['TournamentClass']:
        """Load and filter tournament classes based on config settings.
        
        Accepts either class_id_exts or tournament_id_exts (with data_source_id), converts to internal IDs,
        and applies other filters. If neither ext list is provided, uses only other filters.
        Raises error if both ext lists are provided or if data_source_id missing when exts given.
        If cache_key is provided, queries all classes, builds an in-memory cache dict using the specified key,
        and filters/looks up in memory. If cache_key is None, builds a dynamic SQL query with filters
        for efficiency (direct fetch of matching rows with filters).
        Note: cache_key options adjusted to internal keys (e.g., 'id' for tournament_class_id, 'tournament_id').
        
        Updated: If class_id_exts or tournament_id_exts are provided, the cutoff_date filter is ignored (overridden)
        to allow processing specific classes regardless of date.
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

        # Determine if we should apply cutoff_date (only if no specific IDs provided)
        apply_cutoff = cutoff_date is not None and class_ids is None and tournament_ids is None

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
                from collections import defaultdict
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
                    and (not apply_cutoff or tc.startdate >= cutoff_date)  # Apply cutoff only if needed
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

            if apply_cutoff:
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
    
    # @classmethod
    # def get_filtered_classes(
    #     cls,
    #     cursor:                     sqlite3.Cursor,
    #     class_id_exts:              Optional[List[str]] = None,  # External class IDs
    #     tournament_id_exts:         Optional[List[str]] = None,  # External tournament IDs
    #     data_source_id:             Optional[int] = None,
    #     cutoff_date:                Optional[date] = None,
    #     require_ended:              bool = True,
    #     allowed_type_ids:           Optional[List[int]] = None,
    #     allowed_structure_ids:      Optional[List[int]] = None,  # Filter by structure_id
    #     max_classes:                Optional[int] = None,
    #     order:                      Optional[str] = None,
    #     cache_key:                  Optional[str] = None  # e.g., 'id', 'tournament_id'; None for direct SQL
    # ) -> List['TournamentClass']:
    #     """Load and filter tournament classes based on config settings.
        
    #     Accepts either class_id_exts or tournament_id_exts (with data_source_id), converts to internal IDs,
    #     and applies other filters. If neither ext list is provided, uses only other filters.
    #     Raises error if both ext lists are provided or if data_source_id missing when exts given.
    #     If cache_key is provided, queries all classes, builds an in-memory cache dict using the specified key,
    #     and filters/looks up in memory. If cache_key is None, builds a dynamic SQL query with filters
    #     for efficiency (direct fetch of matching rows with filters).
    #     Note: cache_key options adjusted to internal keys (e.g., 'id' for tournament_class_id, 'tournament_id').
    #     """
    #     if class_id_exts is not None and tournament_id_exts is not None:
    #         raise ValueError("Cannot provide both class_id_exts and tournament_id_exts")
    #     if (class_id_exts is not None or tournament_id_exts is not None) and data_source_id is None:
    #         raise ValueError("data_source_id required when providing class_id_exts or tournament_id_exts")

    #     class_ids: Optional[List[int]] = None
    #     tournament_ids: Optional[List[int]] = None
    #     if class_id_exts is not None:
    #         class_ids = cls.get_internal_class_ids(cursor, class_id_exts, data_source_id)
    #     elif tournament_id_exts is not None:
    #         tournament_ids = Tournament.get_internal_tournament_ids(cursor, tournament_id_exts, data_source_id)

    #     classes: List['TournamentClass']

    #     if cache_key is not None:
    #         # Query all classes
    #         all_query = """
    #             SELECT tc.*, t.tournament_status_id AS tournament_status_id
    #             FROM tournament_class tc
    #             JOIN tournament t ON tc.tournament_id = t.tournament_id
    #         """
    #         cursor.execute(all_query)
    #         rows = cursor.fetchall()
    #         columns = [col[0] for col in cursor.description]
    #         all_rows = [dict(zip(columns, row)) for row in rows]
    #         all_classes = [cls.from_dict(r) for r in all_rows]  # Fixed from_row to from_dict

    #         # Build cache dict based on cache_key
    #         classes_by_key: Any = {}
    #         is_grouped = False
    #         if cache_key == 'tournament_id':
    #             classes_by_key = defaultdict(list)
    #             for tc in all_classes:
    #                 classes_by_key[tc.tournament_id].append(tc)
    #             is_grouped = True
    #         elif cache_key == 'id':
    #             for tc in all_classes:
    #                 classes_by_key[tc.tournament_class_id] = tc
    #         else:
    #             raise ValueError(f"Unknown cache_key: {cache_key}")

    #         # Lookup if specific IDs match the cache_key, else filter all_classes
    #         if class_ids is not None and cache_key == 'id':
    #             classes = [classes_by_key.get(cid) for cid in class_ids if cid in classes_by_key]
    #         elif tournament_ids is not None and cache_key == 'tournament_id':
    #             classes = [tc for tid in tournament_ids for tc in classes_by_key.get(tid, [])]
    #         elif class_ids is not None and cache_key != 'id':
    #             raise ValueError(f"class_ids derived but cache_key '{cache_key}' does not match 'id'")
    #         elif tournament_ids is not None and cache_key != 'tournament_id':
    #             raise ValueError(f"tournament_ids derived but cache_key '{cache_key}' does not match 'tournament_id'")
    #         else:
    #             # Filter all_classes in memory
    #             classes = [
    #                 tc for tc in all_classes
    #                 if (class_ids is None or tc.tournament_class_id in class_ids)
    #                 and (tournament_ids is None or tc.tournament_id in tournament_ids)
    #                 and (not require_ended or tc.tournament_status_id == 3)
    #                 and (allowed_type_ids is None or tc.tournament_class_type_id in allowed_type_ids)
    #                 and (allowed_structure_ids is None or tc.tournament_class_structure_id in allowed_structure_ids)
    #                 and (cutoff_date is None or tc.startdate >= cutoff_date)  # Fixed to use startdate
    #             ]

    #         # Apply sorting in memory
    #         order = (order or "").lower()
    #         if order == "newest":
    #             classes.sort(key=lambda tc: tc.startdate or date.min, reverse=True)
    #         elif order == "oldest":
    #             classes.sort(key=lambda tc: tc.startdate or date.min)

    #         # Apply max limit
    #         if max_classes is not None and max_classes > 0:
    #             classes = classes[:max_classes]

    #     else:
    #         # Direct dynamic SQL query
    #         query = """
    #             SELECT tc.*, t.tournament_status_id AS tournament_status_id
    #             FROM tournament_class tc
    #             JOIN tournament t ON tc.tournament_id = t.tournament_id
    #             WHERE 1=1
    #         """
    #         params: List[Any] = []

    #         if require_ended:
    #             query += " AND t.tournament_status_id = 3"

    #         if class_ids is not None:
    #             placeholders = ', '.join(['?'] * len(class_ids))
    #             query += f" AND tc.tournament_class_id IN ({placeholders})"
    #             params.extend(class_ids)

    #         if tournament_ids is not None:
    #             placeholders = ', '.join(['?'] * len(tournament_ids))
    #             query += f" AND tc.tournament_id IN ({placeholders})"
    #             params.extend(tournament_ids)

    #         if allowed_type_ids is not None:
    #             placeholders = ', '.join(['?'] * len(allowed_type_ids))
    #             query += f" AND tc.tournament_class_type_id IN ({placeholders})"
    #             params.extend(allowed_type_ids)

    #         if allowed_structure_ids is not None:
    #             placeholders = ', '.join(['?'] * len(allowed_structure_ids))
    #             query += f" AND tc.tournament_class_structure_id IN ({placeholders})"
    #             params.extend(allowed_structure_ids)

    #         if cutoff_date is not None:
    #             query += " AND tc.startdate >= ?"
    #             params.append(cutoff_date)

    #         # Apply ordering
    #         order = (order or "").lower()
    #         if order == "newest":
    #             query += " ORDER BY tc.startdate DESC"
    #         elif order == "oldest":
    #             query += " ORDER BY tc.startdate ASC"

    #         # Apply max limit
    #         if max_classes is not None and max_classes > 0:
    #             query += " LIMIT ?"
    #             params.append(max_classes)

    #         # Execute and instantiate
    #         cursor.execute(query, params)
    #         rows = cursor.fetchall()
    #         columns = [col[0] for col in cursor.description]
    #         row_dicts = [dict(zip(columns, row)) for row in rows]
    #         classes = [cls.from_dict(rd) for rd in row_dicts]

    #     return classes
