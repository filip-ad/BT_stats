# src/models/tournament_group.py

from dataclasses import dataclass
from typing import Optional, List, Dict
import logging

@dataclass
class TournamentGroup:
    group_id: Optional[int] = None
    tournament_class_id: Optional[int] = None
    name: Optional[str] = None
    sort_order: Optional[int] = None

    @staticmethod
    def from_dict(d: Dict) -> "TournamentGroup":
        return TournamentGroup(
            group_id=d.get("group_id"),
            tournament_class_id=d.get("tournament_class_id"),
            name=d.get("name"),
            sort_order=d.get("sort_order")
        )

    @staticmethod
    def get_by_name(cursor, tournament_class_id: int, name: str) -> Optional["TournamentGroup"]:
        try:
            cursor.execute("""
                SELECT group_id, tournament_class_id, name, sort_order
                  FROM tournament_group
                 WHERE tournament_class_id = ? AND name = ?
            """, (tournament_class_id, name))
            row = cursor.fetchone()
            if row:
                return TournamentGroup(row[0], row[1], row[2], row[3])
            return None
        except Exception as e:
            logging.error(f"Error get_by_name({tournament_class_id}, {name}): {e}")
            return None

    def upsert(self, cursor) -> "TournamentGroup":
        """
        INSERT or UPDATE (by unique (tournament_class_id, name)).
        Returns self with group_id set.
        """
        try:
            cursor.execute("""
                INSERT INTO tournament_group (tournament_class_id, name, sort_order)
                VALUES (?, ?, ?)
                ON CONFLICT(tournament_class_id, name)
                DO UPDATE SET sort_order=excluded.sort_order
            """, (self.tournament_class_id, self.name, self.sort_order))
            # fetch id
            cursor.execute("""
                SELECT group_id FROM tournament_group
                 WHERE tournament_class_id=? AND name=?
            """, (self.tournament_class_id, self.name))
            row = cursor.fetchone()
            self.group_id = row[0] if row else None
            return self
        except Exception as e:
            logging.error(f"Error upserting tournament_group tc={self.tournament_class_id} name='{self.name}': {e}")
            return self

    def add_member(self, cursor, participant_id: int, seed_in_group: Optional[int] = None) -> bool:
        """
        INSERT OR IGNORE a member into tournament_group_member.
        """
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO tournament_group_member (group_id, participant_id, seed_in_group)
                VALUES (?, ?, ?)
            """, (self.group_id, participant_id, seed_in_group))
            return cursor.rowcount == 1
        except Exception as e:
            logging.error(f"Error adding member pid={participant_id} to group_id={self.group_id}: {e}")
            return False

    @staticmethod
    def clear_for_class(cursor, tournament_class_id: int) -> Dict[str, int]:
        """
        Remove all data tied to pools for a class (games → sides → matches → members → groups).
        Assumes stage table has code='GROUP' with a stage_id.
        """
        cleared = {"games": 0, "sides": 0, "matches": 0, "members": 0, "groups": 0}
        try:
            # games
            cursor.execute("""
                DELETE FROM game
                 WHERE match_id IN (
                   SELECT match_id FROM match
                    WHERE tournament_class_id = ?
                      AND stage_id = (SELECT stage_id FROM stage WHERE code='GROUP')
                 )
            """, (tournament_class_id,))
            cleared["games"] = cursor.rowcount or 0

            # sides
            cursor.execute("""
                DELETE FROM match_side_participant
                 WHERE match_id IN (
                   SELECT match_id FROM match
                    WHERE tournament_class_id = ?
                      AND stage_id = (SELECT stage_id FROM stage WHERE code='GROUP')
                 )
            """, (tournament_class_id,))
            cleared["sides"] = cursor.rowcount or 0

            # matches
            cursor.execute("""
                DELETE FROM match
                 WHERE tournament_class_id = ?
                   AND stage_id = (SELECT stage_id FROM stage WHERE code='GROUP')
            """, (tournament_class_id,))
            cleared["matches"] = cursor.rowcount or 0

            # members
            cursor.execute("""
                DELETE FROM tournament_group_member
                 WHERE group_id IN (
                   SELECT group_id FROM tournament_group
                    WHERE tournament_class_id = ?
                 )
            """, (tournament_class_id,))
            cleared["members"] = cursor.rowcount or 0

            # groups
            cursor.execute("""
                DELETE FROM tournament_group
                 WHERE tournament_class_id = ?
            """, (tournament_class_id,))
            cleared["groups"] = cursor.rowcount or 0

        except Exception as e:
            logging.error(f"Error clearing pools for class {tournament_class_id}: {e}")
        return cleared
