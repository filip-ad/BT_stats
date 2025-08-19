# src/models/base.py

from typing import List, Dict, Any, Tuple, Optional
import sqlite3
import hashlib  # For cache key hashing

class BaseModel:
    _cache: Dict[Tuple, Any] = {}  # Class-level cache: (query_hash, params_hash, extra_hash) -> result

    @classmethod
    def cached_query(
        cls, 
        cursor: sqlite3.Cursor, 
        sql: str, 
        params: Tuple = (), 
        cache_key_extra: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """General caching for SELECT queries. Returns list of dicts from rows."""
        
        # Generate unique cache key
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
    def clear_cache(cls):
        """Clear model cache (e.g., after updates)."""
        cls._cache.clear()