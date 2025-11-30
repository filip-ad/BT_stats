# src/resolvers/resolve_tournament_class_matches.py
"""
Resolver for tournament class matches.

This module handles the resolution of raw match data into structured match records,
including player identification, score parsing, and match creation.

Key Features:
- Name-based player matching with fuzzy scoring (exact, loose, token overlap)
- Hard key matching via tournament_player_id_ext
- Sibling/Parent class resolution for B-playoff classes (added 2025-11-28)

Sibling Resolution:
    B-playoff classes (e.g., "P12~B") contain players who didn't advance from the
    main class ("P12") group stage. These players appear in B-playoff KO matches
    but are NOT in the B-playoff entry list (they're in the parent class entries).
    
    When a player cannot be matched in the current class entries, we check if the
    class has a parent (tournament_class_id_parent). If so, we search the parent
    class entries and create a "synthetic" entry in the B-class for that player.
    
    See: _try_sibling_resolution(), create_synthetic_entry_from_sibling()
"""

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
# RESOLVE_MATCHES_CUTOFF_DATE = '2025-11-01'

SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['30921']

# TO-DO: Fix the doubles counter, does it do anything at all right now? Just searching for '/' is not enough.

debug = True

def _format_table(headers: List[str], rows: List[List[str]]) -> List[str]:
    if not rows:
        return []
    col_widths = [
        max(len(headers[i]), max(len(r[i]) for r in rows)) for i in range(len(headers))
    ]
    header_line = " | ".join(headers[i].ljust(col_widths[i]) for i in range(len(headers)))
    separator = "-+-".join("-" * col_widths[i] for i in range(len(headers)))
    lines = [separator, header_line, separator]
    for r in rows:
        lines.append(" | ".join(r[i].ljust(col_widths[i]) for i in range(len(headers))))
    lines.append(separator)
    return lines

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
            no_participants    = 0
            unmatched_sides    = 0

            # Build participant cache/index for this class
            entry_index = build_entry_index_for_class(cursor, tournament_class_id)
            if not entry_index.get("entries"):
                # we'll still walk raws to count doubles/garbage, but resolution will fail
                pass

            # Build parent/sibling entry index if this is a B-playoff class
            # This allows us to find players who are in the parent class but not in the B-class entry list
            parent_entry_index: Optional[Dict[str, Any]] = None
            parent_class_shortname: Optional[str] = None
            if tc.tournament_class_id_parent:
                parent_class = TournamentClass.get_by_id(cursor, tc.tournament_class_id_parent)
                if parent_class:
                    parent_entry_index = build_entry_index_for_class(cursor, parent_class.tournament_class_id)
                    parent_class_shortname = parent_class.shortname
                    if debug and parent_entry_index.get("entries"):
                        logger.info(
                            logger_keys.copy(),
                            f"Built parent entry index from '{parent_class.shortname}' with {len(parent_entry_index.get('entries', []))} entries"
                        )

            # Group raws by stage and group (for group_id update after insert)
            stage_group_matches: Dict[int, Dict[str, List[TournamentClassMatchRaw]]] = {}
            for raw in class_raws:
                stage_id  = raw.tournament_class_stage_id
                group_ext = raw.group_id_ext or ""
                stage_group_matches.setdefault(stage_id, {}).setdefault(group_ext, []).append(raw)

            # ── process each raw ───────────────────────────────────────────────
            debug_rows: List[List[str]] = [] if debug else []
            headers = [
                "Stage","P1 id","P1 name","P1 club","VS","P2 id","P2 name","P2 club","Winner","Tokens/BYE","Resolved","Matched","Issue"
            ]
            
            # Cache for stage descriptions
            stage_desc_cache: Dict[int, str] = {}
            def get_stage_desc(stage_id: Optional[int]) -> str:
                if stage_id is None:
                    return "?"
                if stage_id not in stage_desc_cache:
                    cursor.execute("SELECT shortname FROM tournament_class_stage WHERE tournament_class_stage_id = ?", (stage_id,))
                    row = cursor.fetchone()
                    stage_desc_cache[stage_id] = row[0] if row else "?"
                return stage_desc_cache[stage_id]
            
            # Track resolved matches for KO sanity check (stage_id -> round_no -> set of player_ids)
            ko_round_players: Dict[int, Dict[Optional[int], Dict[int, str]]] = {}
            for raw in class_raws:
                logger_keys.update({
                    'match_id_ext':      raw.match_id_ext,
                    'group_id_ext':      raw.group_id_ext,
                    's1_fullname_raw':   raw.s1_fullname_raw,
                    's2_fullname_raw':   raw.s2_fullname_raw,
                })

                # Get stage description for debug output
                stage_desc = get_stage_desc(raw.tournament_class_stage_id) if raw.tournament_class_stage_id else "?"
                if raw.group_id_ext:
                    stage_desc = f"{stage_desc}:{raw.group_id_ext}"

                # Prepare debug row context (raw values)
                raw_p1_id  = str(raw.s1_player_id_ext or "")
                raw_p1_nm  = raw.s1_fullname_raw or ""
                raw_p1_clb = raw.s1_clubname_raw or ""
                raw_p2_id  = str(raw.s2_player_id_ext or "")
                raw_p2_nm  = raw.s2_fullname_raw or ""
                raw_p2_clb = raw.s2_clubname_raw or ""
                tokens = (raw.game_point_tokens or "").strip()
                early_games, early_winner, early_wo = parse_scores(tokens, raw.best_of)
                winner_text = "Unknown"
                if early_winner == 1:
                    winner_text = raw_p1_nm or "Unknown"
                elif early_winner == 2:
                    winner_text = raw_p2_nm or "Unknown"
                tokens_text = tokens if tokens else ""
                # Check if player_id_ext exists in the participant index
                by_ext_index = entry_index.get("by_ext", {})
                method_s1 = ("placeholder" if _is_placeholder(raw_p1_nm) else ("by_ext" if (raw.s1_player_id_ext and raw.s1_player_id_ext in by_ext_index) else "name_score"))
                method_s2 = ("placeholder" if _is_placeholder(raw_p2_nm) else ("by_ext" if (raw.s2_player_id_ext and raw.s2_player_id_ext in by_ext_index) else "name_score"))
                method_summary = f"S1:{method_s1}; S2:{method_s2}"
                issue_msg = ""
                resolved_ok = False

                # Garbage?
                if is_garbage_match(raw):
                    garbage_count += 1
                    if debug:
                        debug_rows.append([stage_desc,raw_p1_id,raw_p1_nm,raw_p1_clb,"vs",raw_p2_id,raw_p2_nm,raw_p2_clb,winner_text,tokens_text,"N",method_summary,"garbage (both placeholders)"])
                    continue

                # Previously: skipped rows with '/' (assumed doubles). Disabled.

                # No participants cached?
                if not entry_index.get("entries"):
                    no_participants += 1
                    failed_count += 1
                    if debug:
                        debug_rows.append([stage_desc,raw_p1_id,raw_p1_nm,raw_p1_clb,"vs",raw_p2_id,raw_p2_nm,raw_p2_clb,winner_text,tokens_text,"N",method_summary,"no participants cached for class"])
                    continue

                # Already exists guard (paranoia, since we cleared)
                if TournamentClassMatch.exists(cursor, tournament_class_id, raw.match_id_ext):
                    # shouldn't happen after remove_for_class, but don't insert again
                    continue

                # Resolve sides (always pick best candidate within the class)
                # If parent_entry_index is available (B-playoff class), we'll fallback to it
                side1 = resolve_side(
                    raw.s1_player_id_ext, raw.s1_fullname_raw, raw.s1_clubname_raw,
                    entry_index, cursor, logger, logger_keys, side=1,
                    tournament_class_id=tournament_class_id, group_desc_hint=raw.group_id_ext,
                    parent_entry_index=parent_entry_index, parent_class_shortname=parent_class_shortname
                )
                side2 = resolve_side(
                    raw.s2_player_id_ext, raw.s2_fullname_raw, raw.s2_clubname_raw,
                    entry_index, cursor, logger, logger_keys, side=2,
                    tournament_class_id=tournament_class_id, group_desc_hint=raw.group_id_ext,
                    parent_entry_index=parent_entry_index, parent_class_shortname=parent_class_shortname
                )

                if not side1 or not side2:
                    unmatched_sides += 1
                    failed_count += 1
                    if debug:
                        parts = []
                        if not side1:
                            parts.append("S1 unmatched")
                        if not side2:
                            parts.append("S2 unmatched")
                        issue_msg = ", ".join(parts) or "unmatched side(s)"
                        debug_rows.append([stage_desc,raw_p1_id,raw_p1_nm,raw_p1_clb,"vs",raw_p2_id,raw_p2_nm,raw_p2_clb,winner_text,tokens_text,"N",method_summary,issue_msg])
                    continue

                entry_id1, players1, clubs1, resolve_info1 = side1
                entry_id2, players2, clubs2, resolve_info2 = side2
                
                # Get resolved player names for debug output
                resolved_p1_name = resolve_info1.get("player_name", "?")
                resolved_p2_name = resolve_info2.get("player_name", "?")
                resolved_p1_id = str(resolve_info1.get("tpid_ext", ""))
                resolved_p2_id = str(resolve_info2.get("tpid_ext", ""))
                resolved_p1_club = resolve_info1.get("club_key", "") or ""
                resolved_p2_club = resolve_info2.get("club_key", "") or ""
                
                # Update method summary with actual method and scores used
                actual_method1 = resolve_info1.get("method", method_s1)
                actual_method2 = resolve_info2.get("method", method_s2)
                score1 = resolve_info1.get("score")
                score2 = resolve_info2.get("score")
                
                if actual_method1 == "name_score":
                    method_s1 = f"name({score1 if score1 is not None else '?'})"
                elif actual_method1 == "sibling":
                    parent1 = resolve_info1.get("parent_class", "?")
                    method_s1 = f"sibling({score1 if score1 is not None else ''}←{parent1})"
                elif actual_method1 in ("by_ext", "placeholder"):
                    method_s1 = actual_method1
                    
                if actual_method2 == "name_score":
                    method_s2 = f"name({score2 if score2 is not None else '?'})"
                elif actual_method2 == "sibling":
                    parent2 = resolve_info2.get("parent_class", "?")
                    method_s2 = f"sibling({score2 if score2 is not None else ''}←{parent2})"
                elif actual_method2 in ("by_ext", "placeholder"):
                    method_s2 = actual_method2
                    
                method_summary = f"S1:{method_s1}; S2:{method_s2}"
                
                # Sanity check: same player cannot be on both sides
                if players1 and players2 and set(players1) & set(players2):
                    failed_count += 1
                    issue_msg = f"SAME PLAYER BOTH SIDES: {resolved_p1_name}"
                    if debug:
                        debug_rows.append([stage_desc,resolved_p1_id,resolved_p1_name,resolved_p1_club,"vs",resolved_p2_id,resolved_p2_name,resolved_p2_club,winner_text,tokens_text,"N",method_summary,issue_msg])
                    logger.warning(logger_keys, issue_msg)
                    continue

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
                    if debug:
                        debug_rows.append([stage_desc,resolved_p1_id,resolved_p1_name,resolved_p1_club,"vs",resolved_p2_id,resolved_p2_name,resolved_p2_club,winner_text,tokens_text,"N",method_summary,f"invalid match: {msg}"])
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
                
                # Track for KO sanity check
                stage_id = raw.tournament_class_stage_id
                round_no = None  # Could be populated from draw analysis later
                if stage_id and stage_id not in (1, 11):  # Not GROUP stages
                    ko_round_players.setdefault(stage_id, {}).setdefault(round_no, {})
                    for pid in players1:
                        if pid != PLACEHOLDER_PLAYER_ID:
                            ko_round_players[stage_id][round_no][pid] = resolved_p1_name
                    for pid in players2:
                        if pid != PLACEHOLDER_PLAYER_ID:
                            ko_round_players[stage_id][round_no][pid] = resolved_p2_name

                inserted_count += 1
                resolved_ok = True
                if debug:
                    debug_rows.append([stage_desc,resolved_p1_id,resolved_p1_name,resolved_p1_club,"vs",resolved_p2_id,resolved_p2_name,resolved_p2_club,winner_text,tokens_text,"Y",method_summary,""])

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
            
            # ── KO sanity check: detect players in multiple matches of same stage ──
            # ko_round_players is: {stage_id: {round_no: {player_id: player_name}}}
            # In KO, each stage should have unique players (no player can appear twice in same stage)
            ko_duplicates_found = 0
            for stage_id, rounds in ko_round_players.items():
                # For each stage, collect ALL player appearances
                stage_player_counts: Dict[int, List[str]] = {}  # player_id -> list of appearances
                for round_no, players in rounds.items():
                    for pid, pname in players.items():
                        stage_player_counts.setdefault(pid, []).append(pname)
                
                # Check for duplicates
                for pid, appearances in stage_player_counts.items():
                    if len(appearances) > 1:
                        ko_duplicates_found += 1
                        stage_name = get_stage_desc(stage_id)
                        logger.warning(
                            {'tournament_class_id': tournament_class_id, 'tournament_class_id_ext': class_ext},
                            f"KO SANITY FAIL: Player '{appearances[0]}' (id={pid}) appears in {len(appearances)} matches of stage {stage_name}"
                        )
            
            if ko_duplicates_found > 0:
                logger.warning(
                    {'tournament_class_id': tournament_class_id, 'tournament_class_id_ext': class_ext},
                    f"Found {ko_duplicates_found} player(s) appearing in multiple matches of same KO stage - data may be corrupted"
                )
            
            # Status icon: ✅ if perfect, ❌ if failures
            status_icon = "✅" if failed_count == 0 else "❌"

            # Print debug table per class (mirrors scraper style plus extra columns)
            if debug and debug_rows:
                title = f"Class {class_ext} — Raw matches and resolution"
                table_lines = _format_table(headers, debug_rows)
                if table_lines:
                    logger.info({'tournament_class_id': tournament_class_id, 'tournament_class_id_ext': class_ext}, title, to_console=True)
                    logger.info({"tournament_class_id_ext": class_ext}, "\n".join(table_lines), to_console=True)

            logger.info(
                {'tournament_class_id': tournament_class_id, 'tournament_class_id_ext': class_ext},
                f"{status_icon} [{idx}/{len(groups)}] Class resolved: "
                f"removed={removed_count}, raws={raws_count}, inserted={inserted_count}, "
                f"failed={failed_count}, garbage={garbage_count}, "
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
    group_desc_hint: Optional[str] = None,
    parent_entry_index: Optional[Dict[str, Any]] = None,
    parent_class_shortname: Optional[str] = None
) -> Optional[Tuple[int, List[int], List[int], Dict[str, Any]]]:
    """
    Resolve a match side to (entry_id, [player_id], [club_id], resolve_info) using ONLY 
    participants of the current class (entry_index). 
    
    If resolution fails (player not found or score too low) and parent_entry_index is provided,
    falls back to searching the parent class entries. If found there, creates a synthetic
    entry in the current class so the player appears in both.
    
    resolve_info contains:
      - method: 'by_ext', 'name_score', 'sibling', or 'placeholder'
      - score: integer score if name_score/sibling, None otherwise
      - player_name: resolved player name
      - tpid_ext: tournament player id ext
      - club_key: normalized club name
      - parent_class: (only for sibling) the shortname of the parent class
    
    Returns None if no match found or score too low.
    """

    # ---- 0) quick outs -------------------------------------------------------
    if not fullname_raw:
        logger.warning(logger_keys, f"No fullname for side {side}")
        return None

    # Doubles detection disabled: allow names or ids containing '/'.

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
        resolve_info = {
            "method": "by_ext",
            "score": None,
            "player_name": e["player_name"],
            "tpid_ext": e.get("tpid_ext", ""),
            "club_key": e.get("club_key", ""),
        }
        return e["entry_id"], [e["player_id"]], [e["club_id"]], resolve_info

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
        # ---- Sibling/Parent class fallback --------------------------------
        # For B-playoff classes, try searching the parent class entries
        if parent_entry_index:
            sibling_result = _try_sibling_resolution(
                player_id_ext, fullname_raw, clubname_raw, 
                parent_entry_index, cursor, logger, logger_keys, side,
                tournament_class_id, entry_index, parent_class_shortname
            )
            if sibling_result:
                return sibling_result
        return None

    # Pick the candidate (invert the entry_id tiebreak back to original)
    _, final_score, flags, _, cand = best
    chosen = cand
    
    # Minimum score threshold for name-based matching
    # Score of 6+ means loose match (all tokens match ignoring order)
    # Score of 8+ means exact match
    # Score < 4 means very weak match (only substring or single token)
    MIN_NAME_SCORE = 4
    
    if final_score < MIN_NAME_SCORE:
        logger.warning(logger_keys, f"Side {side}: Best match '{chosen['player_name']}' has score {final_score} < {MIN_NAME_SCORE} (too low for '{fullname_raw}')")
        
        # ---- Sibling/Parent class fallback --------------------------------
        # For B-playoff classes, try searching the parent class entries
        if parent_entry_index:
            sibling_result = _try_sibling_resolution(
                player_id_ext, fullname_raw, clubname_raw, 
                parent_entry_index, cursor, logger, logger_keys, side,
                tournament_class_id, entry_index, parent_class_shortname
            )
            if sibling_result:
                return sibling_result
        
        return None
    
    resolve_info = {
        "method": "name_score",
        "score": final_score,
        "player_name": chosen["player_name"],
        "tpid_ext": chosen.get("tpid_ext", ""),
        "club_key": chosen.get("club_key", ""),
    }

    return chosen["entry_id"], [chosen["player_id"]], [chosen["club_id"]], resolve_info


def _try_sibling_resolution(
    player_id_ext: Optional[str],
    fullname_raw: str,
    clubname_raw: Optional[str],
    parent_entry_index: Dict[str, Any],
    cursor: sqlite3.Cursor,
    logger: OperationLogger,
    logger_keys: Dict,
    side: int,
    tournament_class_id: int,
    entry_index: Dict[str, Any],
    parent_class_shortname: Optional[str],
) -> Optional[Tuple[int, List[int], List[int], Dict[str, Any]]]:
    """
    Try to resolve a player from the parent/sibling class entries.
    
    This handles the B-playoff edge case where players appear in KO matches
    but aren't in the B-class entry list (they're in the parent class).
    
    Resolution attempts:
    1. First try by_ext match (tournament_player_id_ext) in parent entries
    2. If no ext match, try name-based scoring against parent entries
    
    If a match is found with score >= MIN_NAME_SCORE (4), creates a synthetic
    entry in the current class via create_synthetic_entry_from_sibling().
    
    Args:
        player_id_ext: The external player ID from the match data
        fullname_raw: Raw player name from match data
        clubname_raw: Raw club name (optional)
        parent_entry_index: Entry index built from parent class entries
        cursor: Database cursor
        logger: Operation logger
        logger_keys: Current logging context
        side: Match side number (1 or 2)
        tournament_class_id: Current (child/B-class) tournament class ID
        entry_index: Entry index for current class (for caching synthetic entries)
        parent_class_shortname: Parent class shortname for logging
    
    Returns:
        Tuple of (entry_id, player_ids, club_ids, resolve_info) or None if not found
    
    Added: 2025-11-28 to handle B-playoff class resolution
    """
    raw_name = normalize_key(fullname_raw)
    raw_club = _norm(clubname_raw)
    
    # ---- Try by_ext match in parent ----
    if player_id_ext:
        ext_key = player_id_ext  # ext keys are not normalized, use as-is
        parent_entry = parent_entry_index.get("by_ext", {}).get(ext_key)
        if parent_entry:
            logger.info(logger_keys, f"Side {side}: Found '{fullname_raw}' by ext '{ext_key}' in parent class {parent_class_shortname}")
            return create_synthetic_entry_from_sibling(
                cursor, tournament_class_id, entry_index, parent_entry,
                parent_class_shortname, logger, logger_keys
            )
    
    # ---- Try name scoring in parent ----
    parent_entries = parent_entry_index.get("entries", [])
    if not parent_entries:
        return None
    
    candidates = []
    for ent in parent_entries:
        ent_keys = ent.get("keys") or {}
        score = 0
        
        # First+Last match
        if ent_keys.get("first_last") == raw_name or ent_keys.get("last_first") == raw_name:
            score = 8
        # Loose match (sorted tokens)
        elif ent_keys.get("loose") == normalize_key(" ".join(sorted(raw_name.split()))):
            score = 6
        else:
            # Token matching
            raw_tokens = set(raw_name.split())
            ent_tokens = set(ent_keys.get("loose", "").split())
            if raw_tokens and ent_tokens:
                overlap = raw_tokens & ent_tokens
                if overlap:
                    coverage = len(overlap) / max(len(raw_tokens), len(ent_tokens))
                    score = int(coverage * 6)
        
        if score >= 4:  # MIN_NAME_SCORE threshold
            candidates.append((score, ent))
    
    if not candidates:
        return None
    
    # Pick best match
    candidates.sort(key=lambda x: x[0], reverse=True)
    best_score, parent_entry = candidates[0]
    
    logger.info(logger_keys, f"Side {side}: Found '{fullname_raw}' as '{parent_entry['player_name']}' (score={best_score}) in parent class {parent_class_shortname}")
    
    # Store the score in sibling cache for resolve_info
    entry_index.setdefault("_sibling_cache", {})
    cache_key = f"player_{parent_entry['player_id']}"
    entry_index["_sibling_cache"][cache_key] = {"entry_id": None, "score": best_score}  # entry_id will be set by create_synthetic
    
    result = create_synthetic_entry_from_sibling(
        cursor, tournament_class_id, entry_index, parent_entry,
        parent_class_shortname, logger, logger_keys
    )
    
    # Update resolve_info with the score
    if result:
        entry_id, player_ids, club_ids, resolve_info = result
        resolve_info["score"] = best_score
        return entry_id, player_ids, club_ids, resolve_info
    
    return result


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
    placeholder_resolve_info = {
        "method": "placeholder",
        "score": None,
        "player_name": PLACEHOLDER_PLAYER_NAME,
        "tpid_ext": "",
        "club_key": "",
    }
    
    cache = entry_index.setdefault("_placeholder_cache", {})
    cached_entry_id = cache.get("entry_id")
    if cached_entry_id:
        return cached_entry_id, [PLACEHOLDER_PLAYER_ID], [PLACEHOLDER_CLUB_ID], placeholder_resolve_info

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
        cursor.execute("""
            INSERT INTO tournament_class_entry (
                tournament_class_entry_id_ext,
                tournament_class_entry_group_id_int,
                tournament_class_id,
                seed,
                final_position
            ) VALUES (NULL, ?, ?, NULL, NULL)
        """, (entry_group_id, tournament_class_id))
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
    return entry_id, [PLACEHOLDER_PLAYER_ID], [PLACEHOLDER_CLUB_ID], placeholder_resolve_info


def create_synthetic_entry_from_sibling(
    cursor: sqlite3.Cursor,
    tournament_class_id: int,
    entry_index: Dict[str, Any],
    parent_entry: Dict[str, Any],
    parent_class_shortname: Optional[str],
    logger: OperationLogger,
    logger_keys: Dict,
) -> Tuple[int, List[int], List[int], Dict[str, Any]]:
    """
    Create a synthetic tournament_class_entry in the current (B-playoff) class
    based on a player found in the parent class entries.
    
    This handles the scenario where players from the main class (e.g., "P12") appear
    in B-playoff KO matches but aren't in the B-class entry list. We create a new
    entry record linking the player to the B-class so match resolution succeeds.
    
    The synthetic entry:
    - Uses a negative group ID (allocated via _allocate_placeholder_entry_group)
    - Sets tournament_class_entry_id_ext to NULL (player identified via tournament_player_id_ext)
    - Links to the same player_id/club_id from the parent class entry
    - Is cached in entry_index["_sibling_cache"] to avoid duplicate inserts
    
    Args:
        cursor: Database cursor
        tournament_class_id: The B-class tournament_class_id to create entry in
        entry_index: Entry index for caching (modified in place)
        parent_entry: Entry dict from parent class with player_id, club_id, etc.
        parent_class_shortname: Parent class name for logging/resolve_info
        logger: Operation logger
        logger_keys: Current logging context
    
    Returns:
        Tuple of (entry_id, [player_id], [club_id], resolve_info)
    
    Added: 2025-11-28 to handle B-playoff class resolution
    """
    player_id = parent_entry["player_id"]
    player_name = parent_entry["player_name"]
    club_id = parent_entry["club_id"]
    tpid_ext = parent_entry.get("tpid_ext") or ""
    club_key = parent_entry.get("club_key") or ""
    
    # Check if we already created a synthetic entry for this player (cached)
    sibling_cache = entry_index.setdefault("_sibling_cache", {})
    cache_key = f"player_{player_id}"
    if cache_key in sibling_cache:
        cached = sibling_cache[cache_key]
        resolve_info = {
            "method": "sibling",
            "score": cached.get("score"),
            "player_name": player_name,
            "tpid_ext": tpid_ext,
            "club_key": club_key,
            "parent_class": parent_class_shortname,
        }
        return cached["entry_id"], [player_id], [club_id], resolve_info
    
    # Check if entry already exists for this player
    cursor.execute("""
        SELECT e.tournament_class_entry_id
        FROM tournament_class_entry e
        JOIN tournament_class_player tp ON tp.tournament_class_entry_id = e.tournament_class_entry_id
        WHERE e.tournament_class_id = ? AND tp.player_id = ?
        LIMIT 1
    """, (tournament_class_id, player_id))
    row = cursor.fetchone()
    
    if row:
        entry_id = row[0]
    else:
        # Allocate a new negative group ID for synthetic entries
        entry_group_id = _allocate_placeholder_entry_group(cursor, tournament_class_id)
        
        # Use NULL for entry_id_ext (like regular entries) - the player is identified
        # by tournament_player_id_ext which is set at tournament level, not class level
        cursor.execute("""
            INSERT INTO tournament_class_entry (
                tournament_class_entry_id_ext,
                tournament_class_entry_group_id_int,
                tournament_class_id,
                seed,
                final_position
            ) VALUES (NULL, ?, ?, NULL, NULL)
        """, (entry_group_id, tournament_class_id))
        entry_id = cursor.lastrowid
        
        cursor.execute("""
            INSERT OR IGNORE INTO tournament_class_player (
                tournament_class_entry_id,
                tournament_player_id_ext,
                player_id,
                club_id
            ) VALUES (?, ?, ?, ?)
        """, (entry_id, tpid_ext or None, player_id, club_id))
        
        logger.info(logger_keys, f"Created synthetic entry for player '{player_name}' (id={player_id}) from parent class {parent_class_shortname}")
    
    # Add to entry_index so future matches can find this player
    synthetic_entry = {
        "entry_id": entry_id,
        "player_id": player_id,
        "player_name": player_name,
        "club_id": club_id,
        "club_shortname": parent_entry.get("club_shortname"),
        "group_desc": "",
        "tpid_ext": tpid_ext,
        "club_key": club_key,
        "keys": parent_entry.get("keys") or _name_keys(player_name),
    }
    entry_index.setdefault("entries", []).append(synthetic_entry)
    
    # Add to lookup indices
    keys = synthetic_entry.get("keys") or {}
    entry_index.setdefault("by_ext", {})
    entry_index.setdefault("by_first_last", {})
    entry_index.setdefault("by_last_first", {})
    entry_index.setdefault("by_loose", {})
    entry_index.setdefault("by_name_any", {})
    
    if tpid_ext:
        entry_index["by_ext"].setdefault(tpid_ext, []).append(synthetic_entry)
    if "first_last" in keys:
        entry_index["by_first_last"].setdefault(keys["first_last"], []).append(synthetic_entry)
        entry_index["by_name_any"].setdefault(keys["first_last"], []).append(synthetic_entry)
    if "last_first" in keys:
        entry_index["by_last_first"].setdefault(keys["last_first"], []).append(synthetic_entry)
        entry_index["by_name_any"].setdefault(keys["last_first"], []).append(synthetic_entry)
    if "loose" in keys:
        entry_index["by_loose"].setdefault(keys["loose"], []).append(synthetic_entry)
    
    # Cache for reuse
    sibling_cache[cache_key] = {"entry_id": entry_id, "score": None}
    
    resolve_info = {
        "method": "sibling",
        "score": None,
        "player_name": player_name,
        "tpid_ext": tpid_ext,
        "club_key": club_key,
        "parent_class": parent_class_shortname,
    }
    
    return entry_id, [player_id], [club_id], resolve_info


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
