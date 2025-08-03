# src/models/tournament_class.py

from dataclasses import dataclass
import datetime
from typing import Optional, List, Dict, Any, Tuple
import logging
import sqlite3
from utils import parse_date

@dataclass
class TournamentClass:
    tournament_class_id: Optional[int]  = None
    tournament_id:       int            = 0
    date:                Optional[datetime.date] = None
    class_description:   str            = ""
    class_short:         str            = ""
    gender:              Optional[str]  = None
    max_rank:            Optional[int]  = None
    max_age:             Optional[int]  = None
    players_url:         Optional[str]  = None
    groups_url:          Optional[str]  = None
    group_games_url:     Optional[str]  = None
    group_results_url:   Optional[str]  = None
    knockout_url:        Optional[str]  = None
    final_results_url:   Optional[str]  = None

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "TournamentClass":
        """Instantiate from a scraped dict (keys matching column names)."""
        return TournamentClass(
            tournament_class_id = d.get("tournament_class_id"),
            tournament_id       = d["tournament_id"],
            date                = parse_date(d.get("date"), context="TournamentClass.from_dict"),
            class_description   = d.get("class_description", ""),
            class_short         = d.get("class_short", ""),
            gender              = d.get("gender"),
            max_rank            = d.get("max_rank"),
            max_age             = d.get("max_age"),
            players_url         = d.get("players_url"),
            groups_url          = d.get("groups_url"),
            group_games_url     = d.get("group_games_url"),
            group_results_url   = d.get("group_results_url"),
            knockout_url        = d.get("knockout_url"),
            final_results_url   = d.get("final_results_url"),
        )

    @staticmethod
    def cache_existing(cursor) -> Dict[Tuple[int,str], int]:
        """
        Load all existing (tournament_id, class_short) → tournament_class_id into a dict.
        This lets us skip INSERTs for duplicates in bulk.
        """
        cursor.execute("""
            SELECT tournament_class_id, tournament_id, class_short
              FROM tournament_class
        """)
        cache: Dict[Tuple[int,str], int] = {}
        for tc_id, t_id, short in cursor.fetchall():
            cache[(t_id, short)] = tc_id
        logging.info(f"Cached {len(cache)} existing tournament_class rows")
        return cache

    def save_to_db(self, cursor) -> Dict[str, Any]:
        """
        Save *this* instance if it doesn’t already exist (by tournament_id+class_short).
        Returns a result dict for reporting.
        """
        if not (self.tournament_id and self.class_short and self.date):
            return {
                "status": "failed",
                "key": f"{self.tournament_id}/{self.class_short}",
                "reason": "Missing required fields"
            }

        exists = TournamentClass.get_by_key(cursor, self.tournament_id, self.class_short)
        if exists:
            self.tournament_class_id = exists.tournament_class_id
            return {
                "status": "skipped",
                "key": f"{self.tournament_id}/{self.class_short}",
                "reason": "Tournament class already exists"
            }

        try:
            cursor.execute("""
                INSERT INTO tournament_class
                  (tournament_id, date, class_description, class_short,
                   gender, max_rank, max_age,
                   players_url, groups_url,
                   group_games_url, group_results_url,
                   knockout_url, final_results_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                self.tournament_id,
                self.date,
                self.class_description,
                self.class_short,
                self.gender,
                self.max_rank,
                self.max_age,
                self.players_url,
                self.groups_url,
                self.group_games_url,
                self.group_results_url,
                self.knockout_url,
                self.final_results_url
            ))
            self.tournament_class_id = cursor.lastrowid
            return {
                "status": "success",
                "key": f"{self.tournament_id}/{self.class_short}",
                "reason": "Inserted"
            }
        except sqlite3.Error as e:
            logging.error(f"Error inserting tournament_class {self.class_short}: {e}")
            return {
                "status": "failed",
                "key": f"{self.tournament_id}/{self.class_short}",
                "reason": str(e)
            }

    @staticmethod
    def get_by_key(cursor, tournament_id: int, class_short: str) -> Optional["TournamentClass"]:
        """Fetch a single TournamentClass by its natural key."""
        cursor.execute("""
            SELECT tournament_class_id, tournament_id, date,
                   class_description, class_short, gender,
                   max_rank, max_age,
                   players_url, groups_url,
                   group_games_url, group_results_url,
                   knockout_url, final_results_url
              FROM tournament_class
             WHERE tournament_id = ? AND class_short = ?
        """, (tournament_id, class_short))
        row = cursor.fetchone()
        if not row:
            return None
        return TournamentClass.from_dict({
            "tournament_class_id": row[0],
            "tournament_id":       row[1],
            "date":                row[2],
            "class_description":   row[3],
            "class_short":         row[4],
            "gender":              row[5],
            "max_rank":            row[6],
            "max_age":             row[7],
            "players_url":         row[8],
            "groups_url":          row[9],
            "group_games_url":     row[10],
            "group_results_url":   row[11],
            "knockout_url":        row[12],
            "final_results_url":   row[13]
        })

    @staticmethod
    def batch_save(cursor, items: List["TournamentClass"]) -> List[Dict[str, Any]]:
        """
        Insert a whole list of TournamentClass objects in one go.
        Uses an in-memory cache to skip existing keys, then executemany.
        """
        cache = TournamentClass.cache_existing(cursor)
        to_insert: List[Tuple[Any,...]] = []
        results: List[Dict[str,Any]] = []

        for tc in items:
            key = (tc.tournament_id, tc.class_short)
            if key in cache:
                results.append({
                    "status": "skipped",
                    "key": f"{tc.tournament_id}/{tc.class_short}",
                    "reason": "Already exists"
                })
                continue

            # collect values for executemany
            to_insert.append((
                tc.tournament_id,
                tc.date,
                tc.class_description,
                tc.class_short,
                tc.gender,
                tc.max_rank,
                tc.max_age,
                tc.players_url,
                tc.groups_url,
                tc.group_games_url,
                tc.group_results_url,
                tc.knockout_url,
                tc.final_results_url
            ))
            results.append({
                "status": "success",
                "key": f"{tc.tournament_id}/{tc.class_short}",
                "reason": "Class inserted successfully"
            })

        if to_insert:
            try:
                cursor.executemany("""
                    INSERT INTO tournament_class
                      (tournament_id, date, class_description, class_short,
                       gender, max_rank, max_age,
                       players_url, groups_url,
                       group_games_url, group_results_url,
                       knockout_url, final_results_url)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, to_insert)

            except sqlite3.Error as e:
                logging.error(f"Batch insert error: {e}")
                # mark all pending as failed
                for r in results:
                    if r["reason"] == "Class inserted successfully":
                        r.update(status="failed", reason=str(e))

        return results
