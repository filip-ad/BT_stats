# src/resolvers/resolve_tournament_class_matches.py

from models.tournament_class import TournamentClass
from models.tournament_class_match_raw import TournamentClassMatchRaw
from models.tournament_class_entry import TournamentClassEntry
from models.match import Match
from models.game import Game
from models.match_side import MatchSide
from models.match_player import MatchPlayer
from models.tournament_class_match import TournamentClassMatch
from models.tournament_class_group import TournamentClassGroup
from utils import OperationLogger, normalize_key, parse_date
from typing import List, Dict, Optional, Tuple, Any
import sqlite3
from datetime import date
import re
from config import (
    SCRAPE_PARTICIPANTS_MAX_CLASSES,
    SCRAPE_PARTICIPANTS_CLASS_ID_EXTS,
    SCRAPE_PARTICIPANTS_TNMT_ID_EXTS,
    SCRAPE_PARTICIPANTS_ORDER,
    SCRAPE_PARTICIPANTS_CUTOFF_DATE,
    RESOLVE_MATCHES_CUTOFF_DATE,
    PLACEHOLDER_PLAYER_ID,
    PLACEHOLDER_PLAYER_NAME,
    PLACEHOLDER_CLUB_ID,
)

# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS =['30834']  # Edge case, with duplicate matches in 2-page PDF - https://resultat.ondata.se/ViewClassPDF.php?classID=30834&stage=3
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['30284']
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['557']
# RESOLVE_MATCHES_CUTOFF_DATE = '2000-01-01'

debug = False

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
            cutoff_date             = cutoff_date,
            class_id_exts           = SCRAPE_PARTICIPANTS_CLASS_ID_EXTS,
            data_source_id          = 1 if (SCRAPE_PARTICIPANTS_CLASS_ID_EXTS or SCRAPE_PARTICIPANTS_TNMT_ID_EXTS) else None,
            require_ended           = False,
            allowed_structure_ids   = [1,2,3,4],
            allowed_type_ids        = [1],  # singles for initial
            order                   = "newest"
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

            if debug:
                logger.info(f"Found raw match for class_ext={class_ext}: row_id={row.row_id}, s1='{row.s1_fullname_raw}', s2='{row.s2_fullname_raw}', tokens='{row.game_point_tokens}', match_id_ext='{row.match_id_ext}'")

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
                logger.failed(logger_keys.copy(), "No matching tournament_class found")
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
                    entry_index, cursor, logger, logger_keys, side=1,
                    tournament_class_id=tournament_class_id, group_desc_hint=raw.group_id_ext
                )
                side2 = resolve_side(
                    raw.s2_player_id_ext, raw.s2_fullname_raw, raw.s2_clubname_raw,
                    entry_index, cursor, logger, logger_keys, side=2,
                    tournament_class_id=tournament_class_id, group_desc_hint=raw.group_id_ext
                )

                if not side1 or not side2:
                    unmatched_sides += 1
                    failed_count += 1
                    continue

                entry_id1, players1, clubs1 = side1
                entry_id2, players2, clubs2 = side2

                # Parse scores

                games, winner_side, walkover_side = parse_scores(raw.game_point_tokens, raw.best_of)
                side1_placeholder = players1 and players1[0] == PLACEHOLDER_PLAYER_ID
                side2_placeholder = players2 and players2[0] == PLACEHOLDER_PLAYER_ID

                # Backfill missing winner info when WO tokens specify the forfeiting side.
                if walkover_side in (1, 2) and winner_side is None:
                    winner_side = 2 if walkover_side == 1 else 1

                tokens_upper = (raw.game_point_tokens or "").strip().upper()
                # Some PDFs only output "WO" with no :S1/:S2 marker. If exactly one side
                # is the placeholder entry we infer the walkover direction automatically.
                if (
                    walkover_side is None
                    and winner_side is None
                    and tokens_upper == "WO"
                    and (side1_placeholder ^ side2_placeholder)
                ):
                    walkover_side = 1 if side1_placeholder else 2
                    winner_side = 2 if walkover_side == 1 else 1

                if debug:
                    logger.info(logger_keys.copy(),f"Parsed games for match_id_ext={raw.match_id_ext}: side1={raw.s1_fullname_raw}, side2={raw.s2_fullname_raw}, winner_side={winner_side}, walkover_side={walkover_side}, games={games}")

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
                if stage_id not in (1, 11):  # 1 = GROUP, 11 = GROUP_STG2
                    continue
                for group_ext, gmatches in group_matches.items():
                    if not group_ext:
                        continue
                    tcg = TournamentClassGroup.get_by_description(cursor, tournament_class_id, group_ext)
                    if not tcg:
                        tcg = TournamentClassGroup(
                            tournament_class_id=tournament_class_id,
                            description=group_ext,
                            sort_order=extract_group_sort_order(group_ext) if group_ext else None,
                        )
                        tcg.upsert(cursor)
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
    tournament_class_id: int,
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

    # Replace placeholder names ("Vakant", "WO") with the Unknown Player entry.
    # This lets us keep WO rows while still producing valid match_side rows.
    if _is_placeholder(fullname_raw):
        return ensure_placeholder_participant(cursor, tournament_class_id, entry_index)

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
    # Table tennis scoring: games to 11, win by 2
    # Tokens represent loser's score: positive for side2 loss (side1 win), negative for side1 loss (side2 win)
    # If |token| > 9, winner's score = |token| + 2 (deuce)
    # Detect WO:S1 or WO:S2 for walkover
    if not tokens:
        return [], None, None

    stripped = tokens.strip()
    upper = stripped.upper()
    if upper.startswith("WO"):
        wo_side = None
        if upper.startswith("WO:S"):
            side_token = upper.split(":")[-1]
            side_token = re.sub(r"\D", "", side_token) or side_token
            try:
                wo_side = int(side_token[-1])
            except (ValueError, IndexError):
                wo_side = None
        winner = None
        if wo_side in (1, 2):
            winner = 2 if wo_side == 1 else 1
        return [], winner, wo_side

    try:
        scores = [int(t.strip()) for t in stripped.split(',') if t.strip()]
    except ValueError:
        return [], None, None
    games = []
    side1_wins = 0
    side2_wins = 0
    for i, loser_score in enumerate(scores, start=1):
        abs_loser = abs(loser_score)
        if abs_loser > 9:
            winner_points = abs_loser + 2
        else:
            winner_points = 11
        if loser_score > 0:
            # Side1 wins, side2 has loser_score
            p1 = winner_points
            p2 = loser_score
            side1_wins += 1
        else:
            # Side2 wins, side1 has abs_loser
            p1 = abs_loser
            p2 = winner_points
            side2_wins += 1
        games.append(Game(game_no=i, points_side1=p1, points_side2=p2))
    req_wins = (best_of // 2) + 1 if best_of else (len(scores) // 2) + 1
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

_PLACEHOLDER_RE = re.compile(r"\b(vakant|wo)\b", re.IGNORECASE)

def _is_placeholder(name: Optional[str]) -> bool:
    return bool(name and _PLACEHOLDER_RE.search(name))

def ensure_placeholder_participant(
    cursor: sqlite3.Cursor,
    tournament_class_id: int,
    entry_index: Dict[str, Any],
) -> Tuple[int, List[int], List[int]]:
    """
    Ensure the placeholder player (Unknown Player) has a tournament_class_entry
    in this class so matches can reference a valid entry_id.

    We lazily insert a synthetic entry/tournament_class_player the first time we
    encounter a Vakant/WO side in this class, then reuse it for subsequent matches.
    """
    cache = entry_index.setdefault("_placeholder_cache", {})
    cached_entry_id = cache.get("entry_id")
    if cached_entry_id:
        return cached_entry_id, [PLACEHOLDER_PLAYER_ID], [PLACEHOLDER_CLUB_ID]

    cursor.execute("""
        SELECT e.tournament_class_entry_id
        FROM tournament_class_entry e
        JOIN tournament_class_player tp ON tp.tournament_class_entry_id = e.tournament_class_entry_id
        WHERE e.tournament_class_id = ? AND tp.player_id = ?
        LIMIT 1
    """, (tournament_class_id, PLACEHOLDER_PLAYER_ID))
    row = cursor.fetchone()
    if row:
        entry_id = row[0]
    else:
        entry_group_id = _allocate_placeholder_entry_group(cursor, tournament_class_id)
        entry_ext = f"placeholder-{PLACEHOLDER_PLAYER_ID}-{tournament_class_id}"
        cursor.execute("""
            INSERT INTO tournament_class_entry (
                tournament_class_entry_id_ext,
                tournament_class_entry_group_id_int,
                tournament_class_id,
                seed,
                final_position
            ) VALUES (?, ?, ?, NULL, NULL)
        """, (entry_ext, entry_group_id, tournament_class_id))
        entry_id = cursor.lastrowid

        cursor.execute("""
            INSERT OR IGNORE INTO tournament_class_player (
                tournament_class_entry_id,
                tournament_player_id_ext,
                player_id,
                club_id
            ) VALUES (?, NULL, ?, ?)
        """, (entry_id, PLACEHOLDER_PLAYER_ID, PLACEHOLDER_CLUB_ID))

    placeholder_entry = {
        "entry_id": entry_id,
        "player_id": PLACEHOLDER_PLAYER_ID,
        "player_name": PLACEHOLDER_PLAYER_NAME,
        "club_id": PLACEHOLDER_CLUB_ID,
        "club_shortname": None,
        "group_desc": "",
        "tpid_ext": None,
        "club_key": None,
        "keys": _name_keys(PLACEHOLDER_PLAYER_NAME),
    }
    entry_index.setdefault("entries", []).append(placeholder_entry)
    keys = placeholder_entry.get("keys") or {}
    entry_index.setdefault("by_ext", {})
    entry_index.setdefault("by_first_last", {})
    entry_index.setdefault("by_last_first", {})
    entry_index.setdefault("by_loose", {})
    entry_index.setdefault("by_name_any", {})
    if "first_last" in keys:
        entry_index["by_first_last"].setdefault(keys["first_last"], []).append(placeholder_entry)
        entry_index["by_name_any"].setdefault(keys["first_last"], []).append(placeholder_entry)
    if "last_first" in keys:
        entry_index["by_last_first"].setdefault(keys["last_first"], []).append(placeholder_entry)
        entry_index["by_name_any"].setdefault(keys["last_first"], []).append(placeholder_entry)
    if "loose" in keys:
        entry_index["by_loose"].setdefault(keys["loose"], []).append(placeholder_entry)

    cache["entry_id"] = entry_id
    return entry_id, [PLACEHOLDER_PLAYER_ID], [PLACEHOLDER_CLUB_ID]

def _allocate_placeholder_entry_group(cursor: sqlite3.Cursor, tournament_class_id: int) -> int:
    """Pick a unique negative group_id slot for placeholder entries to avoid collisions."""
    cursor.execute("""
        SELECT MIN(tournament_class_entry_group_id_int)
        FROM tournament_class_entry
        WHERE tournament_class_id = ?
    """, (tournament_class_id,))
    row = cursor.fetchone()
    min_val = row[0] if row else None
    if min_val is None:
        return -1
    return min_val - 1

def is_garbage_match(raw: TournamentClassMatchRaw) -> bool:
    # Skip rows where both sides are placeholders such as "Vakant" / "WO"
    return _is_placeholder(raw.s1_fullname_raw) and _is_placeholder(raw.s2_fullname_raw)

def is_garbage_name(name: str) -> bool:
    return _is_placeholder(name)

def extract_group_sort_order(group_ext: str) -> Optional[int]:
    # Q7: Proposal - extract number from "Pool X" -> X
    match = re.search(r'\d+', group_ext)
    return int(match.group()) if match else None
