import logging
from typing import List, Dict, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import date
from models.club import Club
from models.player import Player
from models.player_license import PlayerLicense
from models.player_transition import PlayerTransition
from utils import normalize_key

@dataclass
class PlayerParticipant:
    tournament_class_id:            Optional[int] = None
    tournament_participant_id_ext:  Optional[str] = None
    fullname_raw:                   Optional[str] = None
    club_name_raw:                  Optional[str] = None
    seed:                           Optional[int] = None
    final_position:                 Optional[int] = None

    # Filled during save
    participant_id:                 Optional[int] = None
    player_id:                      Optional[int] = None
    club_id:                        Optional[int] = None

    @staticmethod
    def from_dict(data: dict) -> "PlayerParticipant":
        return PlayerParticipant(
            tournament_class_id=data["tournament_class_id"],
            tournament_participant_id_ext=data.get("tournament_participant_id_ext"),
            fullname_raw=data["fullname_raw"],
            club_name_raw=data["club_name_raw"],
            seed=data.get("seed"),                 
            final_position=data.get("final_position"),
        )
    
    @staticmethod
    def remove_for_class(cursor, tournament_class_id: int) -> int:
        cursor.execute(
            "DELETE FROM player_participant WHERE tournament_class_id = ?", 
            (tournament_class_id,)
        )
        return cursor.rowcount

    def save_to_db(
        self,
        cursor,
        class_date: date,
        club_map: Dict[str, Club],                                                  # normalized club name → Club
        license_name_club_map: Dict[Tuple[str, str, int], List[Dict[str, Any]]],    # (firstname, lastname, club_id) → license-rows
        player_name_map: Dict[str, List[int]],                                      # normalized "firstname lastname" → [player_id,...]
        cache_unverified_name_map: Dict[str, int],                                  # cleaned fullname_raw → player_id (for unverified)
    ) -> dict:
        """
        High-level orchestration of club and player matching, with staged fallbacks.
        Returns a dict with status, key, reason, match_type, category, and optional warnings.
        """
        warnings: List[str] = []

        # 1) Club lookup
        club = self.find_club(cursor, club_map, warnings)
        if not club:
            return {
                "status": "failed",
                "key":    f"{self.tournament_class_id}_{self.fullname_raw}",
                "reason": "Club not found (tried exact + prefix)",
                "warnings": warnings
            }
        self.club_id = club.club_id

        if self.club_id == 9999:
            warnings.append("Using 'Unknown club' as fallback")

        # 2) Player matching pipeline
        for strategy in [
            self.match_by_name,
            self.match_by_name_with_license,
            self.match_by_transition,
            self.match_by_any_season_license,
            self.match_by_name_substring_license
        ]:
            outcome = strategy(
                cursor, 
                class_date, 
                license_name_club_map, 
                player_name_map,
                cache_unverified_name_map,
                warnings
            )
            if isinstance(outcome, dict):
                return outcome  # final failure or raw insertion
            if outcome is not None:
                pid, match_type, warnings = outcome
                self.player_id = pid
                break
        else:
            # No strategy found a pid: fallback to unverified
            return self.fallback_unverified(cursor, warnings, cache_unverified_name_map)

        # 3) Insert canonical participant
        return self.insert_participant(cursor, match_type, warnings)

    def find_club(
        self,
        cursor,
        club_map,
        warnings: List[str]
    ) -> Optional[Club]:
        '''
        Description
        '''
        norm = Club._normalize(self.club_name_raw)
        club = club_map.get(norm)
        # prefix fallback
        if not club and len(norm) >= 5:
            prefix_keys = [k for k in club_map if k.startswith(norm)]
            if len(prefix_keys) == 1:
                club = club_map[prefix_keys[0]]
                warnings.append("Club name matched by prefix")
                all_aliases = [a['alias'] for a in club.aliases]
                logging.info(
                    f"Prefix‐matched '{self.club_name_raw}' → {club.shortname}, aliases={all_aliases}"
                )
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO club_name_prefix_match
                        (tournament_class_id, club_raw_name, matched_club_id, matched_club_aliases)
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        self.tournament_class_id,
                        self.club_name_raw,
                        club.club_id,
                        ",".join(all_aliases)
                    )
                )
        if not club:
            club = Club.get_by_id(cursor, 9999)  # Fallback to "Unknown club"
            logging.warning(
                f"Club not found for '{self.club_name_raw}' (norm: {norm}). Using 'Unknown club'."
            )
            cursor.execute(
                """
                INSERT OR IGNORE INTO club_missing (club_name_raw, club_name_norm)
                VALUES (?, ?)
                """,
                (self.club_name_raw, norm)
            )
        return club

    def get_name_candidates(
        self, 
        player_name_map,
        ) -> List[int]:
        '''
        Description
        '''
        clean = normalize_key(self.fullname_raw)
        parts = clean.split()
        keys = [
            normalize_key(f"{fn} {ln}")
            for i in range(1, len(parts))
            for ln in [" ".join(parts[:i])]
            for fn in [" ".join(parts[i:])]
        ]
        matches = set()
        for k in keys:
            matches.update(player_name_map.get(k, []))
        return list(matches)

    def match_by_name(
        self,
        cursor,
        class_date: date,
        license_name_club_map,
        player_name_map,
        cache_unverified_name_map,
        warnings
    ):
        # 1) Find all candidate player_ids by exact normalized name
        pids = self.get_name_candidates(player_name_map)
        if len(pids) != 1:
            return None

        pid = pids[0]
        warnings: List[str] = []

        # 2) Look up all license‐rows for this club in your cache
        valid = False
        for (fn, ln, cid), rows in license_name_club_map.items():
            if cid != self.club_id:
                continue
            for lic in rows:
                if lic["player_id"] != pid:
                    continue
                # compare dates as date objects
                if lic["valid_from"] <= class_date <= lic["valid_to"]:
                    valid = True
                    break
            if valid:
                break

        # 3) If we never saw a valid‐today license, warn
        if not valid:
            warnings.append(
                "Player did not have a valid license in the club on the day of the tournament"
            )

        return pid, "unique_name", warnings


    def match_by_name_with_license(
        self,
        cursor,
        class_date: date,
        license_name_club_map,
        player_name_map,
        cache_unverified_name_map,
        warnings
    ):
        pids = self.get_name_candidates(player_name_map)
        if len(pids) <= 1:
            return None
        placeholders = ",".join("?" for _ in pids)
        sql = f"""
            SELECT DISTINCT player_id FROM player_license
             WHERE player_id IN ({placeholders})
               AND club_id = ?
               AND date(valid_from) <= date(?)
               AND date(valid_to)   >= date(?)
        """
        params = [*pids, self.club_id, class_date.isoformat(), class_date.isoformat()]
        cursor.execute(sql, params)
        valid = [r[0] for r in cursor.fetchall()]
        if len(valid) == 1:
            return valid[0], "license", []
        if len(valid) > 1:
            return {
                "status":       "failed",
                "key":          f"{self.tournament_class_id}_{normalize_key(self.fullname_raw)}",
                "reason":       "Ambiguous player candidates with valid licenses",
                "category":     "",
                "match_type":   ""
            }
        cursor.execute(
            f"SELECT DISTINCT player_id FROM player_license WHERE player_id IN ({placeholders}) AND club_id = ?",
            [*pids, self.club_id]
        )
        expired = [r[0] for r in cursor.fetchall()]
        if len(expired) == 1:
            return expired[0], "expired_license", ["Matched via prior season license (expired before class_date)"]
        if len(expired) > 1:
            return {
                "status":       "failed",
                "key":          f"{self.tournament_class_id}_{normalize_key(self.fullname_raw)}",
                "reason":       "Ambiguous player candidates with expired licenses",
                "category":     "",
                "match_type":   ""
            }
        return None

    def match_by_transition(
        self,
        cursor,
        class_date: date,
        license_name_club_map,
        player_name_map,
        cache_unverified_name_map,
        warnings
    ):
        pids = self.get_name_candidates(player_name_map)
        if not pids:
            return None
        placeholders = ",".join("?" for _ in pids)
        sql = f"""
            SELECT player_id FROM player_transition
             WHERE (club_id_to = ? OR club_id_from = ?)
               AND date(transition_date) <= date(?)
               AND player_id IN ({placeholders})
        """
        params = [self.club_id, self.club_id, class_date.isoformat(), *pids]
        cursor.execute(sql, params)
        trans = [r[0] for r in cursor.fetchall()]
        if len(trans) == 1:
            return trans[0], "transition", []
        if len(trans) > 1:
            return {
                "status":       "failed",
                "key":          f"{self.tournament_class_id}_{self.fullname_raw}",
                "reason":       "Ambiguous transition matches",
                "category":     "None",
                "match_type":   ""
            }
        return None

    def match_by_any_season_license(
            self, 
            cursor, 
            *_args
        ):
        cursor.execute(
            "SELECT DISTINCT player_id FROM player_license WHERE club_id = ?",
            (self.club_id,)
        )
        all_l = [r[0] for r in cursor.fetchall()]
        if len(all_l) == 1:
            pid = all_l[0]
            cursor.execute(
                """
                INSERT OR IGNORE INTO player_participant
                  (tournament_class_id, player_id, club_id)
                VALUES (?, ?, ?)
                """,
                (self.tournament_class_id, pid, self.club_id)
            )
            status = "success" if cursor.rowcount == 1 else "skipped"
            reason = (
                "Participant added successfully (matching canonical player)"
                if status == "success"
                else "Participant already exists (matching canonical player)"
            )
            return {
                "status":     status,
                "key":        f"{self.tournament_class_id}_{pid}",
                "reason":     reason,
                "match_type": "license_fallback",
                "category":   "canonical"
            }
        return None

    def match_by_name_substring_license(
        self,
        cursor,
        class_date: date,
        license_name_club_map,
        player_name_map,
        cache_unverified_name_map,
        warnings
    ):
        clean = normalize_key(self.fullname_raw)
        raw_parts = clean.split()

        # need at least 2 names to substring match
        if len(raw_parts) > 2:
            return None
        
        first_tok, last_tok = raw_parts[0], raw_parts[-1]

        # 1) find all license-rows for this club whose normalized key has both tokens
        candidates = []
        for (fn, ln, cid), rows in license_name_club_map.items():
            if cid != self.club_id:
                continue
            # only consider candidates whose own name has >=3 tokens
            candidate_key = normalize_key(f"{fn} {ln}")
            cand_parts = candidate_key.split()
            if len(cand_parts) < 3:
                continue

            # and where both raw tokens appear somewhere in their name
            if first_tok in candidate_key and last_tok in candidate_key:
                candidates.extend(rows)

        if not candidates:
            return None

        # 2) split into valid vs expired
        valid_ids = set()
        expired_ids = set()
        for row in candidates:
            pid = row["player_id"]
            vf, vt = row["valid_from"], row["valid_to"]
            if vf <= class_date <= vt:
                valid_ids.add(pid)
            else:
                expired_ids.add(pid)

        # 3) exactly one valid → success
        if len(valid_ids) == 1:
            pid = next(iter(valid_ids))
            logging.info(
                f"Matched {self.fullname_raw} → player_id={pid} (valid license)"
            )
            return pid, "license_name_fallback", ["Matched via name-substring against 3+ token candidate"]

        # 4) ambiguous valids → failure
        if len(valid_ids) > 1:
            candidates_list = []
            for pid in valid_ids:
                player = Player.get_by_id(cursor, pid)
                name = f"{player.firstname} {player.lastname}" if player else None
                candidates_list.append({"player_id": pid, **({"name": name} if name else {})})
            return {
                "status":     "failed",
                "key":        f"{self.tournament_class_id}_{clean}",
                "reason":     "Ambiguous substring + license matches",
                "candidates": candidates_list,
                "category":   "",
                "match_type": ""
            }

        # 5) single expired → fallback
        if len(expired_ids) == 1:
            pid = next(iter(expired_ids))
            return pid, "expired_license", ["Matched via prior-season license (expired)"]

        # 6) ambiguous expired → failure
        if len(expired_ids) > 1:
            return {
                "status":     "failed",
                "key":        f"{self.tournament_class_id}_{clean}",
                "reason":     "Ambiguous substring + license matches (expired only)",
                "candidates": [{"player_id": pid} for pid in expired_ids],
                "category":   "",
                "match_type": ""
            }

        return None

    def fallback_unverified(
                self, 
                cursor, 
                warnings: List[str],
                cache_unverified_name_map
            ) -> dict:
            clean = " ".join(self.fullname_raw.strip().split())
            existing = cache_unverified_name_map.get(clean)
            if existing is not None:
                # use existing unverified player
                self.player_id = existing
                match_type = "unverified_fallback_existing"
            else:
                # create a new unverified player
                cursor.execute(
                    """
                    INSERT INTO player (fullname_raw, is_verified)
                    VALUES (?, FALSE)
                    """,
                    (clean,)
                )
                new_id = cursor.lastrowid
                self.player_id = new_id
                cache_unverified_name_map[clean] = new_id
                match_type = "unverified_fallback_new"
            # insert into participant    
            cursor.execute(
                """
                INSERT OR IGNORE INTO player_participant (
                    tournament_class_id, 
                    tournament_participant_id_ext,
                    player_id, 
                    club_id,
                    seed
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    self.tournament_class_id,
                    self.tournament_participant_id_ext,
                    self.player_id,
                    self.club_id,
                    self.seed
                )
            )
            inserted = cursor.rowcount == 1
            if match_type == "unverified_fallback_new" and inserted:
                reason = "Participant added successfully (new unverified player)"
            elif match_type == "unverified_fallback_existing" and inserted:
                reason = "Participant added successfully (existing unverified player)"
            else:
                reason = "Participant already exists (unverified player)"

            if "new" in match_type: 
                warnings.append("Could not match with verified player, new unverified player inserted")
            else:
                warnings.append("Could not match with verified player, existing unverified player used")

            return {
                "status":     "success" if inserted else "skipped",
                "key":        f"{self.tournament_class_id}_{self.player_id}",
                "reason":     reason,
                "match_type": match_type,
                "category":   "unverified",
                "warnings":   warnings
            }
    
    def insert_participant(
        self,
        cursor,
        match_type: str,
        warnings: List[str]
    ) -> dict:
        cursor.execute(
            """
            INSERT OR IGNORE INTO player_participant (
                tournament_class_id, 
                tournament_participant_id_ext,
                player_id, 
                club_id,
                seed
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                self.tournament_class_id,
                self.tournament_participant_id_ext,
                self.player_id,
                self.club_id,
                self.seed
            )
        )
        inserted = cursor.rowcount == 1
        if inserted:
            reason = "Participant added successfully (matching canonical player)"
        else:
            reason = "Participant already exists (matching canonical player)"

        return {
            "status":     "success" if inserted else "skipped",
            "key":        f"{self.tournament_class_id}_{self.player_id}",
            "reason":     reason,
            "match_type": match_type,
            "category":   "canonical",
            "warnings":   warnings
        }
    
    @staticmethod
    def clear_final_positions_for_class(cursor, tournament_class_id: int) -> int:
        """
        Set final_position=NULL for all participants in the given class.
        Returns number of rows affected.
        """
        cursor.execute("""
            UPDATE player_participant
            SET final_position = NULL
            WHERE tournament_class_id = ?
        """, (tournament_class_id,))
        return cursor.rowcount
    
    @classmethod
    def upd_final_position_by_participant(
        cls,
        cursor,
        tournament_class_id: int,
        fullname_raw: str,
        club_raw: str,
        position: int,
        player_name_map: dict,           # from Player.cache_name_map()
        unverified_name_map: dict,       # from Player.cache_unverified_name_map()
        class_part_by_player: dict,      # {player_id: (participant_id, club_id)} for THIS class
        club_map: dict                   # from Club.cache_name_map()
    ) -> bool:
        """
        Resolve player by name using Player caches (verified & unverified),
        then map player_id → participant_id for this class, optionally
        disambiguating with club.
        """
        # 1) candidate player_ids from caches
        clean = normalize_key(fullname_raw)
        parts = clean.split()

        candidates = set()

        # direct unverified match (fullname_raw)
        if clean in unverified_name_map:
            candidates.add(unverified_name_map[clean])

        # verified: try all split points in BOTH orders (FN LN and LN FN)
        for i in range(1, len(parts)):
            ln = " ".join(parts[:i])
            fn = " ".join(parts[i:])
            key_fn_ln = normalize_key(f"{fn} {ln}")
            key_ln_fn = normalize_key(f"{ln} {fn}")
            candidates.update(player_name_map.get(key_fn_ln, []))
            candidates.update(player_name_map.get(key_ln_fn, []))

        if not candidates:
            logging.warning(f"[positions] No player candidates for '{fullname_raw}'")
            return False

        # 2) keep only candidates present in this class
        present = [(pid, class_part_by_player[pid]) for pid in candidates if pid in class_part_by_player]
        if not present:
            logging.warning(f"[positions] No participant in class {tournament_class_id} for '{fullname_raw}'")
            return False

        # 3) if multiple, use club to disambiguate
        if len(present) > 1:
            from models.club import Club
            club_obj = club_map.get(Club._normalize(club_raw))
            if club_obj:
                wanted_cid = club_obj.club_id
                present = [(pid, pc) for pid, pc in present if pc[1] == wanted_cid]

        if len(present) != 1:
            logging.warning(f"[positions] Ambiguous/absent after club check for '{fullname_raw}' / '{club_raw}' in class {tournament_class_id}")
            return False

        participant_id = present[0][1][0]

        # 4) update
        cursor.execute(
            "UPDATE player_participant SET final_position = ? WHERE participant_id = ?",
            (position, participant_id),
        )
        return cursor.rowcount == 1

    @staticmethod
    def cache_by_class_id(
        cursor
    ) -> dict:
        """
        Returns:
            tournament_class_id -> { (normalized_name, club_id) : participant_id }
        Includes both verified and unverified players.
        """
        from utils import normalize_key

        data = {}
        cursor.execute("""
            SELECT pp.tournament_class_id,
                pp.participant_id,
                p.firstname,
                p.lastname,
                p.fullname_raw,
                pp.club_id
            FROM player_participant pp
            JOIN player p ON p.player_id = pp.player_id
        """)
        for class_id, pid, fn, ln, fullname_raw, cid in cursor.fetchall():
            if fn and ln:  # verified
                full_name = f"{fn} {ln}".strip()
            else:          # unverified
                full_name = fullname_raw or ""
            norm_name = normalize_key(full_name)
            data.setdefault(class_id, {})[(norm_name, cid)] = pid

        return data
    
    @staticmethod
    def cache_by_class_id_player(cursor) -> Dict[int, Dict[int, Tuple[int, int]]]:
        """
        Returns:
            class_id -> { player_id : (participant_id, club_id) }
        """
        data: Dict[int, Dict[int, Tuple[int, int]]] = {}
        cursor.execute("""
            SELECT pp.tournament_class_id,
                   pp.participant_id,
                   pp.player_id,
                   pp.club_id
              FROM player_participant pp
        """)
        for class_id, part_id, player_id, club_id in cursor.fetchall():
            if class_id not in data:
                data[class_id] = {}
            data[class_id][player_id] = (part_id, club_id)
        logging.info(f"Cached participant map by class (player_id→(participant_id,club_id)) for {len(data)} classes")
        return data
  
    @staticmethod
    def cache_by_class_name_fast(cursor):
        """
        Returns:
        class_id -> {
            "by_name_club": Dict[(name_key, club_id), participant_id],
            "by_name_only": Dict[name_key, List[participant_id]]
        }
        """
        from utils import normalize_key
        data = {}
        cursor.execute("""
            SELECT pp.tournament_class_id,
                pp.participant_id,
                pp.club_id,
                COALESCE(TRIM(p.firstname || ' ' || p.lastname), p.fullname_raw) AS nm
            FROM player_participant pp
            JOIN player p ON p.player_id = pp.player_id
        """)
        for class_id, pid, cid, nm in cursor.fetchall():
            name_key = normalize_key(nm or "")
            bucket = data.setdefault(class_id, {"by_name_club": {}, "by_name_only": {}})
            bucket["by_name_club"][(name_key, cid)] = pid
            bucket["by_name_only"].setdefault(name_key, []).append(pid)
        return data