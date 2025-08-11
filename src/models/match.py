from dataclasses import dataclass, field
from typing import Optional, List, Dict, Tuple
import logging
from models.tournament_stage import TournamentStage

@dataclass
class Match:
    match_id:               Optional[int] = None
    tournament_class_id:    Optional[int] = None    # XOR with fixture_id
    fixture_id:             Optional[int] = None    # XOR with tournament_class_id
    stage_code:             Optional[str] = None    # 'GROUP','R16','...','LEAGUE'; optional for league
    group_id:               Optional[int] = None
    best_of:                Optional[int] = None
    date:                   Optional[str] = None
    score_summary:          Optional[str] = None
    notes:                  Optional[str] = None

    sides_participants:     List[Tuple[int, int]] = field(default_factory=list)                             # [(side, participant_id)]
    sides_players:          List[Tuple[int, int, Optional[int]]] = field(default_factory=list)              # [(side, player_id, club_id)]
    games:                  List[Tuple[int, Optional[int], Optional[int]]] = field(default_factory=list)

    @staticmethod
    def from_dict(d: Dict) -> "Match":
        return Match(
            match_id=d.get("match_id"),
            tournament_class_id=d.get("tournament_class_id"),
            fixture_id=d.get("fixture_id"),
            stage_code=d.get("stage_code"),
            group_id=d.get("group_id"),
            best_of=d.get("best_of"),
            date=d.get("date"),
            score_summary=d.get("score_summary"),
            notes=d.get("notes"),
            sides_participants=d.get("sides_participants", []),
            sides_players=d.get("sides_players", []),
            games=d.get("games", []),
        )

    def add_side_participant(
            self, 
            side: int, 
            participant_id: int
        ): 
        self.sides_participants.append((side, participant_id))

    def add_side_player(
            self, 
            side: int, 
            player_id: int, 
            club_id: Optional[int] = None
        ): 
        self.sides_players.append((side, player_id, club_id))

    def add_game(
            self, 
            game_number: int, 
            s1: Optional[int], 
            s2: Optional[int]
        ): 
        self.games.append((game_number, s1, s2))

    def _validate(
            self, 
            cursor
        ) -> Optional[str]:
        has_tc = self.tournament_class_id is not None
        has_fx = self.fixture_id is not None
        if has_tc == has_fx:
            return "Exactly one of tournament_class_id or fixture_id must be set."

        # League default
        if has_fx and not self.stage_code:
            self.stage_code = "LEAGUE"

        if has_tc and not self.stage_code:
            return "stage_code is required for tournament matches."

        if self.stage_code != "GROUP" and self.group_id is not None:
            return "group_id may only be set when stage_code='GROUP'."

        if has_tc:
            if not self.sides_participants or self.sides_players:
                return "Tournament match must use participant sides only."
            if len(self.sides_participants) < 2:
                return "Tournament match requires two participant sides."
        else:
            if not self.sides_players or self.sides_participants:
                return "League match must use player sides only."
            if len(self.sides_players) < 2:
                return "League match requires two player sides."

        # Stage must exist
        stage_id = TournamentStage.id_by_code(cursor, self.stage_code)
        if stage_id is None:
            return f"Unknown stage_code '{self.stage_code}'."
        return None

    def save_to_db(
            self, 
            cursor
        ) -> Dict:
        err = self._validate(cursor)
        if err:
            return {"status": "failed", "reason": err}

        stage_id = TournamentStage.id_by_code(cursor, self.stage_code)
        try:
            cursor.execute("""
                INSERT INTO match (tournament_class_id, fixture_id, stage_id, group_id, best_of, date, score_summary, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (self.tournament_class_id, self.fixture_id, stage_id, self.group_id, self.best_of, self.date, self.score_summary, self.notes))
            self.match_id = cursor.lastrowid

            if self.tournament_class_id is not None:
                for side, pid in self.sides_participants:
                    cursor.execute("""
                        INSERT INTO match_side_participant (match_id, side, participant_id)
                        VALUES (?, ?, ?)
                    """, (self.match_id, side, pid))
            else:
                for side, player_id, club_id in self.sides_players:
                    cursor.execute("""
                        INSERT INTO match_side_player (match_id, side, player_id, club_id)
                        VALUES (?, ?, ?, ?)
                    """, (self.match_id, side, player_id, club_id))

            for game_no, s1, s2 in sorted(self.games, key=lambda g: g[0]):
                winner = 1 if (s1 is not None and s2 is not None and s1 > s2) else (2 if (s1 is not None and s2 is not None) else 1)
                cursor.execute("""
                    INSERT INTO game (match_id, game_number, side1_points, side2_points, winner_side)
                    VALUES (?, ?, ?, ?, ?)
                """, (self.match_id, game_no, s1, s2, winner))

            return {"status": "success", "match_id": self.match_id, "reason": f"Inserted {len(self.games)} games"}
        except Exception as e:
            logging.error(f"Error inserting match: {e}")
            return {"status": "failed", "reason": f"Insertion error: {e}"}

    @staticmethod
    def get_by_id(cursor, match_id: int) -> Optional["Match"]:
        try:
            cursor.execute("""
                SELECT m.match_id, m.tournament_class_id, m.fixture_id, s.code, m.group_id,
                       m.best_of, m.date, m.score_summary, m.notes
                  FROM match m
                  JOIN stage s ON s.stage_id = m.stage_id
                 WHERE m.match_id = ?
            """, (match_id,))
            row = cursor.fetchone()
            if not row: return None
            m = Match(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8])

            if m.tournament_class_id is not None:
                cursor.execute("SELECT side, participant_id FROM match_side_participant WHERE match_id=? ORDER BY side", (match_id,))
                m.sides_participants = [(r[0], r[1]) for r in cursor.fetchall()]
            else:
                cursor.execute("SELECT side, player_id, club_id FROM match_side_player WHERE match_id=? ORDER BY side", (match_id,))
                m.sides_players = [(r[0], r[1], r[2]) for r in cursor.fetchall()]

            cursor.execute("SELECT game_number, side1_points, side2_points FROM game WHERE match_id=? ORDER BY game_number", (match_id,))
            m.games = [(r[0], r[1], r[2]) for r in cursor.fetchall()]
            return m
        except Exception as e:
            logging.error(f"Error loading match {match_id}: {e}")
            return None
