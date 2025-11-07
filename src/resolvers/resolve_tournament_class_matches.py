# src/resolvers/resolve_tournament_class_matches.py
# Updated to remove group/member post-processing, as now handled in entries resolver

from models.tournament_class import TournamentClass
from models.tournament_class_match_raw import TournamentClassMatchRaw
from models.tournament_class_entry import TournamentClassEntry
from models.tournament_class_player import TournamentClassPlayer
from models.match import Match
from models.game import Game
from models.match_side import MatchSide
from models.match_player import MatchPlayer
from models.tournament_class_match import TournamentClassMatch
from models.tournament_class_group import TournamentClassGroup
from models.club import Club
from models.player import Player
from utils import OperationLogger, normalize_key, name_keys_for_lookup_all_splits, parse_date
from typing import List, Dict, Optional, Tuple, Any
import sqlite3
from datetime import date
from config import RESOLVE_MATCHES_CUTOFF_DATE
import re

def resolve_tournament_class_matches(cursor: sqlite3.Cursor, run_id=None) -> None:
    """Resolve raw matches into match-related tables."""

    logger = OperationLogger(
        verbosity       = 2,
        print_output    = False,
        log_to_db       = True,
        cursor          = cursor,
        object_type     = "match",
        run_type        = "resolve",
        run_id          = run_id
    )

    raw_rows = TournamentClassMatchRaw.get_all(cursor)
    if not raw_rows:
        logger.skipped({}, "No raw match data to resolve")
        return

    cutoff_date: date | None = parse_date(RESOLVE_MATCHES_CUTOFF_DATE) if RESOLVE_MATCHES_CUTOFF_DATE else None
    if cutoff_date:
        filtered_classes = TournamentClass.get_filtered_classes(
            cursor,
            cutoff_date       = cutoff_date,
            require_ended     = False,
            allowed_type_ids  = [1],  # singles for initial
            order             = "newest"
        )
        allowed_class_exts = {tc.tournament_class_id_ext for tc in filtered_classes if tc.tournament_class_id_ext}
        if allowed_class_exts:
            raw_rows = [r for r in raw_rows if r.tournament_class_id_ext in allowed_class_exts]
        logger.info(f"Resolving matches for classes since {cutoff_date} ({len(raw_rows)} raw matches)...")
    else:
        logger.info("Resolving tournament class matches...", to_console=True)

    # Group raw by class_ext
    groups: Dict[str, List[TournamentClassMatchRaw]] = {}
    for row in raw_rows:
        class_ext = row.tournament_class_id_ext
        if class_ext:
            groups.setdefault(class_ext, []).append(row)

    logger.info({}, f"Classes with raw match rows: {len(groups)}")

    for idx, (class_ext, class_raws) in enumerate(groups.items(), start=1):
        logger_keys = {
            'tournament_class_id_ext': class_ext,
            'match_id_ext': None,
            'group_id_ext': None,
            's1_fullname_raw': None,
            's2_fullname_raw': None,
        }

        logger.inc_processed()

        try:

            tc = TournamentClass.get_by_ext_id(cursor, class_ext)
            if not tc:
                logger.failed(logger_keys, "No matching tournament_class found")
                continue

            tournament_class_id = tc.tournament_class_id
            match_date = tc.startdate if tc.startdate else None

            # ── per-class stats ────────────────────────────────────────────────
            removed_count      = TournamentClassMatch.remove_for_class(cursor, tournament_class_id)
            raws_count         = len(class_raws)
            inserted_count     = 0
            failed_count       = 0
            garbage_count      = 0
            doubles_count      = 0
            no_participants    = 0
            unmatched_sides    = 0

            # Build participant cache/index for this class
            entry_index = build_entry_index_for_class(cursor, tournament_class_id)
            if not entry_index.get("entries"):
                # we’ll still walk raws to count doubles/garbage, but resolution will fail
                pass

            # Group raws by stage and group (for group_id update after insert)
            stage_group_matches: Dict[int, Dict[str, List[TournamentClassMatchRaw]]] = {}
            for raw in class_raws:
                stage_id  = raw.tournament_class_stage_id
                group_ext = raw.group_id_ext or ""
                stage_group_matches.setdefault(stage_id, {}).setdefault(group_ext, []).append(raw)

            # ── process each raw ───────────────────────────────────────────────
            for raw in class_raws:
                logger_keys.update({
                    'match_id_ext':      raw.match_id_ext,
                    'group_id_ext':      raw.group_id_ext,
                    's1_fullname_raw':   raw.s1_fullname_raw,
                    's2_fullname_raw':   raw.s2_fullname_raw,
                })

                # Garbage?
                if is_garbage_match(raw):
                    garbage_count += 1
                    continue

                # Doubles?
                if any('/' in (s or '') for s in (raw.s1_player_id_ext, raw.s2_player_id_ext, raw.s1_fullname_raw, raw.s2_fullname_raw, raw.s1_clubname_raw, raw.s2_clubname_raw)):
                    doubles_count += 1
                    continue

                # No participants cached?
                if not entry_index.get("entries"):
                    no_participants += 1
                    failed_count += 1
                    continue

                # Already exists guard (paranoia, since we cleared)
                if TournamentClassMatch.exists(cursor, tournament_class_id, raw.match_id_ext):
                    # shouldn’t happen after remove_for_class, but don’t insert again
                    continue

                # Resolve sides (always pick best candidate within the class)
                side1 = resolve_side(
                    raw.s1_player_id_ext, raw.s1_fullname_raw, raw.s1_clubname_raw,
                    entry_index, cursor, logger, logger_keys, side=1, group_desc_hint=raw.group_id_ext
                )
                side2 = resolve_side(
                    raw.s2_player_id_ext, raw.s2_fullname_raw, raw.s2_clubname_raw,
                    entry_index, cursor, logger, logger_keys, side=2, group_desc_hint=raw.group_id_ext
                )

                if not side1 or not side2:
                    unmatched_sides += 1
                    failed_count += 1
                    continue

                entry_id1, players1, clubs1 = side1
                entry_id2, players2, clubs2 = side2

                # Parse scores
                games, winner_side, walkover_side = parse_scores(raw.game_point_tokens, raw.best_of)

                # Create match
                match = Match(best_of=raw.best_of, date=match_date, winner_side=winner_side, walkover_side=walkover_side)
                is_valid, msg = match.validate()
                if not is_valid:
                    failed_count += 1
                    continue

                match_id = match.insert(cursor)

                # Games
                for g in games:
                    g.match_id = match_id
                    g.insert(cursor)

                # Sides
                MatchSide(match_id=match_id, side_no=1, represented_entry_id=entry_id1).insert(cursor)
                MatchSide(match_id=match_id, side_no=2, represented_entry_id=entry_id2).insert(cursor)

                # Players on each side
                insert_match_players(match_id, 1, players1, clubs1, cursor)
                insert_match_players(match_id, 2, players2, clubs2, cursor)

                # tournament_class_match (group set after loop)
                tcm = TournamentClassMatch(
                    tournament_class_id=tournament_class_id,
                    match_id=match_id,
                    tournament_class_match_id_ext=raw.match_id_ext,
                    tournament_class_stage_id=raw.tournament_class_stage_id,
                    stage_round_no=None,
                    draw_pos=None
                )
                tcm.insert(cursor)

                inserted_count += 1

            # ── post-process: fill tcm.group_id for group stage ───────────────
            for stage_id, group_matches in stage_group_matches.items():
                if stage_id != 1:  # 1 = GROUP
                    continue
                for group_ext, gmatches in group_matches.items():
                    if not group_ext:
                        continue
                    tcg = TournamentClassGroup.get_by_description(cursor, tournament_class_id, group_ext)
                    if not tcg:
                        continue
                    group_id = tcg.tournament_class_group_id
                    for raw in gmatches:
                        cursor.execute("""
                            UPDATE tournament_class_match
                            SET tournament_class_group_id = ?
                            WHERE tournament_class_id = ? AND tournament_class_match_id_ext = ?;
                        """, (group_id, tournament_class_id, raw.match_id_ext)) 
            
            # ── single compact line per class ─────────────────────────────────
            # logger.info(
            #     {'tournament_class_id': tournament_class_id, 'tournament_class_id_ext': class_ext},
            #     f"[{idx}/{len(groups)}] Class resolved: "
            #     f"removed={removed_count}, raws={raws_count}, inserted={inserted_count}, "
            #     f"failed={failed_count}, garbage={garbage_count}, doubles={doubles_count}, "
            #     f"no_participants={no_participants}, unmatched_sides={unmatched_sides}",
            #     to_console=True
            # )
            
            # Status icon: ✅ if perfect, ❌ if failures
            status_icon = "✅" if failed_count == 0 else "❌"

            logger.info(
                {'tournament_class_id': tournament_class_id, 'tournament_class_id_ext': class_ext},
                f"{status_icon} [{idx}/{len(groups)}] Class resolved: "
                f"removed={removed_count}, raws={raws_count}, inserted={inserted_count}, "
                f"failed={failed_count}, garbage={garbage_count}, doubles={doubles_count}, "
                f"no_participants={no_participants}, unmatched_sides={unmatched_sides}",
                to_console=True
            )

            if failed_count == 0:
                logger.success(logger_keys, "Class matches resolved successfully")
            else: 
                logger.failed(logger_keys, f"Class matches resolved with failures")

        except Exception as e:
            logger.failed(logger_keys, f"Exception: {str(e)}")

    logger.summarize()

# Helper functions

# --- pure-python helpers (no SQL here) ---

def build_entry_index_for_class(cursor: sqlite3.Cursor, tournament_class_id: int) -> Dict[str, Any]:
    rows = TournamentClassEntry.fetch_participants_for_class(cursor, tournament_class_id)
    return build_entry_index(rows)

def _norm(s: Optional[str]) -> Optional[str]:
    return normalize_key(s) if s else None

def _name_keys(fullname: Optional[str]) -> Dict[str, str]:
    """
    Build multiple normalized keys for matching:
    - first_last: 'firstname lastname'
    - last_first: 'lastname firstname' (handles 'Lennebratt Nils' vs 'Nils Lennebratt')
    - loose: unordered 'first|last' for last-ditch tie-breaks
    """
    clean = normalize_key(fullname or "")
    parts = clean.split()
    if not parts:
        return {}
    first = parts[0]
    last  = parts[-1]
    return {
        "first_last": f"{first} {last}",
        "last_first": f"{last} {first}",
        "loose": "|".join(sorted([first, last])),
    }

def build_entry_index(participant_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Index participants of this class for robust name/club/group matching.
    Creates a unified index (by_name_any) that maps BOTH 'first last' and
    'last first' to the same rows, so raw names in either orientation can match.
    """
    by_ext, by_first_last, by_last_first, by_loose, by_name_any = {}, {}, {}, {}, {}
    entries = []
    for r in participant_rows:
        ctx = {
            "entry_id":    r["entry_id"],
            "player_id":   r["player_id"],
            "player_name": r["player_name"] or "",
            "club_id":     r["club_id"],
            "club_key":    _norm(r["club_shortname"]),
            "group_desc":  r.get("group_desc") or "",
            "tpid_ext":    r.get("tpid_ext") or "",
        }
        keys = _name_keys(ctx["player_name"])
        ctx["keys"] = keys
        entries.append(ctx)

        if ctx["tpid_ext"]:
            by_ext[ctx["tpid_ext"]] = ctx

        if "first_last" in keys:
            k = keys["first_last"]
            by_first_last.setdefault(k, []).append(ctx)
            by_name_any.setdefault(k, []).append(ctx)

        if "last_first" in keys:
            k = keys["last_first"]
            by_last_first.setdefault(k, []).append(ctx)
            by_name_any.setdefault(k, []).append(ctx)

        if "loose" in keys:
            by_loose.setdefault(keys["loose"], []).append(ctx)

    return {
        "by_ext": by_ext,
        "by_first_last": by_first_last,
        "by_last_first": by_last_first,
        "by_loose": by_loose,
        "by_name_any": by_name_any,      # NEW: both orientations map here
        "entries": entries,
    }


def resolve_side(
    player_id_ext: Optional[str],
    fullname_raw: Optional[str],
    clubname_raw: Optional[str],
    entry_index: Dict[str, Any],
    cursor: sqlite3.Cursor,
    logger: OperationLogger,
    logger_keys: Dict,
    side: int,
    group_desc_hint: Optional[str] = None
) -> Optional[Tuple[int, List[int], List[int]]]:
    """
    Resolve a match side to (entry_id, [player_id], [club_id]) using ONLY participants
    of the current class (entry_index). Extremely permissive: if the side is singles,
    we will always choose the best candidate (unless there are zero participants).
    """

    # ---- 0) quick outs -------------------------------------------------------
    if not fullname_raw:
        logger.warning(logger_keys, f"No fullname for side {side}")
        return None

    # Doubles: skip (not implemented yet)
    if '/' in (player_id_ext or '') or '/' in (fullname_raw or '') or '/' in (clubname_raw or ''):
        logger.warning(logger_keys, "Doubles not implemented, skipping")
        return None

    # Normalize inputs
    raw_name  = normalize_key(fullname_raw)
    raw_club  = _norm(clubname_raw)
    raw_group = _norm(group_desc_hint)

    # ---- 1) hard key: tournament_player_id_ext -------------------------------
    by_ext = entry_index.get("by_ext", {})
    if player_id_ext and player_id_ext in by_ext:
        e = by_ext[player_id_ext]
        return e["entry_id"], [e["player_id"]], [e["club_id"]]

    # ---- 2) candidate set = all entries in this class ------------------------
    entries = entry_index.get("entries", [])
    if not entries:
        logger.warning(logger_keys, f"No participants cached for class; cannot resolve side {side}")
        return None

    # Precompute raw tokens and initials
    def _tokens(name: str) -> List[str]:
        # split on spaces, drop empties
        return [t for t in name.split() if t]

    def _initials(tokens: List[str]) -> List[str]:
        # single-letter initials from tokens (e.g., "f", "h")
        return [t[0] for t in tokens if t]

    raw_tokens   = _tokens(raw_name)
    raw_initials = _initials(raw_tokens)
    raw_set      = set(raw_tokens)

    # For substring/loose matches, also build a collapsed unordered key
    def _loose_key(tokens: List[str]) -> str:
        return "|".join(sorted(tokens))

    raw_loose = _loose_key(raw_tokens)

    # ---- 3) scoring model ----------------------------------------------------
    # We score each candidate and pick the highest.
    # Components (weights chosen to be decisive but sensible):
    #  +8  exact full string match (any orientation)
    #  +6  loose unordered all-tokens match
    #  +4  token overlap count (bounded)
    #  +3  initials pattern fits (when raw contains initials or candidate contains initials)
    #  +2  group matches the hint
    #  +1  club matches the hint
    #  +1  any single-token exact match (lastname-only or firstname-only)
    #  +1  substring match of any token (prefix/suffix/contains)
    #
    # Tie-breakers (in order):
    #   1) has group match
    #   2) has club match
    #   3) shorter name-length distance to raw (proxy for edit distance)
    #   4) lowest entry_id (stable deterministic)
    #
    # NOTE: We deliberately allow returning a single "best" even if several are close.

    def score_candidate(cand: Dict[str, Any]) -> Tuple[int, Dict[str, int]]:
        cname         = normalize_key(cand["player_name"] or "")
        ctokens       = _tokens(cname)
        cset          = set(ctokens)
        cinitials     = _initials(ctokens)
        cand_loose    = _loose_key(ctokens)
        cgroup        = _norm(cand.get("group_desc") or "")
        cclub         = cand.get("club_key")
        entry_id      = cand["entry_id"]

        score = 0
        flags = {
            "group_match": 0,
            "club_match": 0,
        }

        # exact any-orientation (first last OR last first) == raw string
        # Check both raw and flipped raw
        raw_flipped = " ".join(list(reversed(raw_tokens)))
        if cname == raw_name or cname == raw_flipped:
            score += 8

        # loose: all tokens match ignoring order
        if cand_loose == raw_loose:
            score += 6

        # token overlap (cap at 4 to avoid dominating)
        overlap = len(raw_set & cset)
        score += min(overlap, 4)

        # initials logic
        # Case A: raw contains initials (e.g., "f hejdebäck")
        raw_has_initials = any(len(t) == 1 for t in raw_tokens)
        if raw_has_initials:
            # Candidate first letter(s) should fit raw initials at the right positions
            # We'll accept "F Hejdebäck" vs "Filip Hejdebäck", etc.
            # Count matches for available positions
            match_init = 0
            for rt, ct in zip(raw_tokens, ctokens):
                if len(rt) == 1 and ct and ct[0] == rt:
                    match_init += 1
            score += 3 if match_init > 0 else 0

        # Case B: candidate has initials (rare in your curated data, but safe)
        cand_has_initials = any(len(t) == 1 for t in ctokens)
        if cand_has_initials and raw_tokens:
            match_init = 0
            for ct, rt in zip(ctokens, raw_tokens):
                if len(ct) == 1 and rt and rt[0] == ct:
                    match_init += 1
            score += 3 if match_init > 0 else 0

        # group & club hints
        if raw_group and cgroup and cgroup == raw_group:
            score += 2
            flags["group_match"] = 1
        if raw_club and cclub and cclub == raw_club:
            score += 1
            flags["club_match"] = 1

        # single-token exact (lastname-only or firstname-only)
        if len(raw_tokens) == 1 and raw_tokens[0] in cset:
            score += 1
        elif len(ctokens) == 1 and ctokens[0] in raw_set:
            score += 1

        # substring match for any token: raw token contained in candidate token or vice versa
        # (handles mild OCR splits or hyphenation)
        substr_hit = False
        for rt in raw_tokens:
            for ct in ctokens:
                if rt and ct and (rt in ct or ct in rt):
                    substr_hit = True
                    break
            if substr_hit:
                break
        if substr_hit:
            score += 1

        return score, flags

    # compute scores
    best = None  # (score, flags, name_len_delta, entry_id, cand)
    raw_len = len(raw_name)
    for cand in entries:
        s, flags = score_candidate(cand)
        # tie-breakers
        c_name = normalize_key(cand["player_name"] or "")
        name_len_delta = abs(len(c_name) - raw_len)
        key = (
            s,
            flags["group_match"],
            flags["club_match"],
            -name_len_delta,            # prefer closer length
            -len(c_name),               # slight bias toward longer exactness when equal
            -cand["entry_id"],          # we’ll invert later to pick smallest entry_id
        )
        if best is None or key > best[0]:
            best = (key, s, flags, name_len_delta, cand)

    if not best:
        logger.warning(logger_keys, f"No candidates available for side {side}")
        return None

    # Pick the candidate (invert the entry_id tiebreak back to original)
    _, final_score, flags, _, cand = best
    chosen = cand

    return chosen["entry_id"], [chosen["player_id"]], [chosen["club_id"]]

def parse_scores(tokens: Optional[str], best_of: Optional[int]) -> Tuple[List[Game], Optional[int], Optional[int]]:
    # Q1: Table tennis, so points to 11, win by 2 assumed in calculation
    # Q3: Parse as margins, positive for side1 win, negative for side2; calc points as max(11, abs(m) + 2) for winner
    # Q11: Detect WO:S1 or WO:S2 for walkover
    if not tokens:
        return [], None, None

    if tokens.upper().startswith("WO:S"):
        wo_side = int(tokens[-1])
        return [], None, wo_side

    try:
        margins = [int(t.strip()) for t in tokens.split(',') if t.strip()]
    except ValueError:
        return [], None, None

    games = []
    side1_wins = 0
    side2_wins = 0
    for i, m in enumerate(margins, start=1):
        if m > 0:
            p1 = max(11, m + 2) if abs(m) > 9 else 11  # Adjust for deuce
            p2 = p1 - m
            side1_wins += 1
        else:
            p2 = max(11, abs(m) + 2) if abs(m) > 9 else 11
            p1 = p2 + m  # m negative
            side2_wins += 1
        games.append(Game(game_no=i, points_side1=p1, points_side2=p2))

    req_wins = (best_of // 2) + 1 if best_of else (len(margins) // 2) + 1
    if side1_wins >= req_wins:
        winner = 1
    elif side2_wins >= req_wins:
        winner = 2
    else:
        winner = None

    return games, winner, None

def insert_match_players(match_id: int, side_no: int, players: List[int], clubs: List[int], cursor: sqlite3.Cursor) -> None:
    for order, (p_id, c_id) in enumerate(zip(players, clubs), start=1):
        mp = MatchPlayer(match_id=match_id, side_no=side_no, player_id=p_id, player_order=order, club_id=c_id)
        mp.insert(cursor)

def is_garbage_match(raw: TournamentClassMatchRaw) -> bool:
    # Q14: Garbage patterns like "Sets: 5", "WO WO", "Vakant Vakant"; skip if both sides garbage
    garbage = {"sets:", "poång:", "diff:", "wo", "vakant", "wo wo", "vakant vakant"}
    s1_lower = (raw.s1_fullname_raw or "").lower()
    s2_lower = (raw.s2_fullname_raw or "").lower()
    return s1_lower in garbage and s2_lower in garbage

def is_garbage_name(name: str) -> bool:
    garbage = {"wo", "vakant"}
    return name.lower() in garbage

def extract_group_sort_order(group_ext: str) -> Optional[int]:
    # Q7: Proposal - extract number from "Pool X" -> X
    match = re.search(r'\d+', group_ext)
    return int(match.group()) if match else None