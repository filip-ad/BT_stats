# src/models/ranking_group.py

from dataclasses import dataclass
from typing import Dict

@dataclass
class RankingGroup:
    ranking_group_id:       int
    gender:                 str
    min_rank:               int
    max_rank:               int
    class_description:      str
    class_short:            str

    @staticmethod
    def cache_map(cursor) -> Dict[str, int]:
        """
        Return a dict mapping class_short -> ranking_group_id.
        """
        cursor.execute("SELECT class_short, ranking_group_id FROM ranking_group")
        return {row[0]: row[1] for row in cursor.fetchall()}
