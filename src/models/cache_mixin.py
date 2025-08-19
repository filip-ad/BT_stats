# src/models/cache_mixin.py

from typing import Any, Dict, List, Optional, Tuple
import hashlib
import sqlite3


class CacheMixin:
    """
    Generic caching mixin for database queries.
    Mixin providing simple in-memory caching for SELECT queries.

    Each subclass receives its own cache dictionary to avoid sharing
    cached results across different model classes.
    """

    _cache: Dict[Tuple, Any]

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls._cache = {}

    @classmethod
    def cached_query(
        cls,
        cursor: sqlite3.Cursor,
        sql: str,
        params: Tuple = (),
        cache_key_extra: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Execute and cache a SELECT query.

        Parameters are hashed to build a cache key.  If the key is found
        in the per-class cache, the cached result is returned.
        """

        sql_hash = hashlib.md5(sql.encode()).hexdigest()
        params_hash = hashlib.md5(str(params).encode()).hexdigest()
        extra_hash = hashlib.md5(cache_key_extra.encode()).hexdigest() if cache_key_extra else ""
        cache_key = (sql_hash, params_hash, extra_hash)

        if cache_key in cls._cache:
            return cls._cache[cache_key]

        cursor.execute(sql, params)
        columns = [col[0] for col in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]

        cls._cache[cache_key] = results
        return results

    @classmethod
    def clear_cache(cls) -> None:
        """Clear the cache for this subclass."""

        cls._cache.clear()