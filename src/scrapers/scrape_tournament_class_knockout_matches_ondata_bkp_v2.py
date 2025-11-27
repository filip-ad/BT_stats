# src/scrapers/scrape_tournament_class_knockout_matches_ondata.py

from __future__ import annotations

from datetime import date
import hashlib
import io
import re
import unicodedata
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Set, Tuple

import pdfplumber

"""
Utility script for parsing knockout brackets from resultat.ondata.se PDFs.

Why this file exists
--------------------
This is a stand-alone, auditable sandbox used to iterate on KO-bracket parsing
before landing changes in `scrapers/scrape_tournament_class_knockout_matches_ondata.py`.
It deliberately keeps the parsing logic verbose and linear so each heuristic is
easy to inspect, tweak, and back out if it hurts other formats.

What the parser does (high level)
---------------------------------
1) Collects all players from the left-most column (ids, names, clubs, y-centers).
2) Extracts winner labels and score tokens column by column, clustered by x-band
   to infer rounds (RO128/64/32/16/8/4/2 + qualifiers and small double-WO layouts).
3) Builds `Match` objects round by round, using y-alignment and alias matching to
   attach winners/scores, and applies WO markers/tokens when present.
4) Runs validation passes (missing scores, duplicate players, stray score tokens,
   walkovers with scores, round-size continuity) and emits debug tables.

Key formats and edge cases covered here
---------------------------------------
- Standard RO8/16/32/64/128 brackets (single page and the split two-page RO128).
- Brackets with qualifiers above the KO tree.
- Dubbel-WO/Double-WO patterns in both tiny brackets (≤4 players) and sparse
  large trees where RO64/RO128 have BYEs plus Dubbel-WO holes.
- Walkover markers as text ("WO"/"wo") and embedded WO tokens near scores.

Important guardrails for contributors (incl. AI agents)
-------------------------------------------------------
- Be very careful not to change anything that impacts other formats; prefer
  localized fixes guarded by explicit flags/heuristics.
- Always test in the venv (e.g., `source ../.venv/bin/activate` and run the
  scraper for the target `SCRAPE_PARTICIPANTS_CLASS_ID_EXTS`).
- Keep the verbose debug prints/checks intact; they are the safety net when
  adding heuristics for new PDFs.
- When in doubt, bias toward not consuming tokens/winners rather than forcing
  assignments that could corrupt progression.
"""

# -------------------------------------------------------------------
# Project imports
# -------------------------------------------------------------------
from config import (
    SCRAPE_PARTICIPANTS_MAX_CLASSES,
    SCRAPE_PARTICIPANTS_CLASS_ID_EXTS,
    SCRAPE_PARTICIPANTS_TNMT_ID_EXTS,
    SCRAPE_PARTICIPANTS_ORDER,
    SCRAPE_PARTICIPANTS_CUTOFF_DATE,
)
from utils import (
    parse_date,
    OperationLogger,
    _download_pdf_ondata_by_tournament_class_and_stage,
    sanitize_name,
    normalize_key,
)
from models.tournament import Tournament
from models.tournament_class import TournamentClass
from models.tournament_class_match_raw import TournamentClassMatchRaw

# TESTCASES DIFFERENT BRACKET SIZES (WORKING):
# ==================================================================================================
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['29622']             # RO8 test
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['30021']             # RO16 test
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['29625']             # RO32 test -- 19 matches

# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['25395']             # RO16 but missing ko_tree_size
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['23772']             # RO32 - Two players with same name but different clubs / entry_ids. RO16 match 2,3,4 winners incorrectly identified. Can be due to missing player ID exts.
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ["125"]               # RO16 without player ID:s

# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['29837']             # "Double walkover" in small bracket (4 players)

# TESTCASES WITH QUALIFICATION MATCHES (WORKING):
# ==================================================================================================
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['29866']             # Qualification + RO16 test -- 16 matches 
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['26156']             # Qualifier 1 match, no player id exts
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['23822']             # Qualifier 2 matches, has player id exts 
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['26159']             # Qualifier 3 matches, no player id exts
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['21441']             # Qualifier 6 matches, 2 rows of 3 matches, with player id exts
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['135']                # 3 qual matches in a straight line

# TESTCASES WITH BRACKET SIZE 64 (WORKING):
# ==================================================================================================
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['1006']              # RO64 test with player ID:s -- 47 matches
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['121']               # RO64 bracket -- 28 RO64 matches, 16 RO32 matches, 8 RO16 matches, 4 QF, 2 SF, 1 Final, total 59 matches - with player id exts

# TESTCASES WITH BRACKET SIZE 128 (NOT WORKING):
# ==================================================================================================
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['6955']              # RO128, with player ID:s
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['31041']             # RO128, with player id exts
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['31032']             # RO128, with player id exts
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['30527']             # RO128, with player id exts -- 98 matches

# TESTCASES DUBBEL-WO (WORKING):
# ==================================================================================================
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['29834']             # Dubbel-WO + WO 
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['30796']             # Dubbel-WO + WO
SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['110']               # RO128 with some Dubbel-WO matches

# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['113']               # RO64 with separate qual matches on page 2

# NOT WORKING TESTCASES:
# ==================================================================================================
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['29631']


# Map url -> md5 hash so repeated runs can detect PDF changes.
LAST_PDF_HASHES: Dict[str, str] = {}

DEBUG_OUTPUT: bool = True


def _debug_print(message: str) -> None:
    if DEBUG_OUTPUT:
        print(message)

    # -------------------------------------------------------------------
    # Parser core (KO bracket extraction)
    # -------------------------------------------------------------------
WINNER_LABEL_PATTERN = re.compile(
    r"^(?:(\d{1,3})\s+)?([\wÅÄÖåäö.\-]+(?:\s+[\wÅÄÖåäö.\-]+)*)$",
    re.UNICODE,
)
# Phrases that show up as instructions/titles; should never be treated as winners.
WINNER_INSTRUCTION_PHRASES = {
    "segraren är understruken",
    "segraren ar understruken",
    "winner is underlined",
    "software license may only be used at tournaments arranged by",
    "coordinated by tt coordinator",
    "junior boys singles",
    "swedish junior",
    "btk safir",
    "nybegynner",
    "knock-out stage",
}
# Matches a sequence of score tokens followed by a winner label inside the
# same text blob, e.g. ``"5, 8, 6, 11 169 Augustsson A"``.
COMBINED_SCORE_LABEL_RE = re.compile(
    r"^((?:-?\d+\s*,\s*)+-?\d+)\s+(.+)$"
)
# Heuristic x-bands for the earliest (RO64) column (legacy helpers for very wide PDFs)
R64_WINNERS_X   = (195, 250) # was (170, 210)
R64_SCORES_X    = (250, 305) # was (210, 260)

@dataclass
class Player:
    full_name: str
    club: str
    short: str
    center: float
    x: Optional[float]
    x1: Optional[float]
    player_id_ext: Optional[str]
    player_suffix_id: Optional[str]
    aliases: Set[str] = field(default_factory=set)

@dataclass
class ScoreEntry:
    scores: Tuple[int, ...]
    center: float
    x: float

@dataclass
class WinnerEntry:
    short: str
    center: float
    x: float
    player_id_ext: Optional[str]
    is_double_wo: bool = False

@dataclass
class Match:
    players: List[Player]
    winner: Optional[Player]
    scores: Optional[Tuple[int, ...]]
    center: float
    walkover: bool = False
    walkover_forfeiter: Optional[Player] = None


def scrape_tournament_class_knockout_matches_ondata(cursor, run_id=None):
    """Parse KO bracket PDFs from OnData (stage=5) and persist raw match rows."""

    logger = OperationLogger(
        verbosity       = 2,
        print_output    = False,
        log_to_db       = True,
        cursor          = cursor,
        object_type     = "tournament_class_match_raw",
        run_type        = "scrape",
        run_id          = run_id,
    )

    cutoff_date     = parse_date(SCRAPE_PARTICIPANTS_CUTOFF_DATE) if SCRAPE_PARTICIPANTS_CUTOFF_DATE else None
    classes         = TournamentClass.get_filtered_classes(
        cursor                  = cursor,
        class_id_exts           = SCRAPE_PARTICIPANTS_CLASS_ID_EXTS,
        tournament_id_exts      = SCRAPE_PARTICIPANTS_TNMT_ID_EXTS,
        data_source_id          = 1 if (SCRAPE_PARTICIPANTS_CLASS_ID_EXTS or SCRAPE_PARTICIPANTS_TNMT_ID_EXTS) else None,
        cutoff_date             = cutoff_date,
        require_ended           = False,
        allowed_structure_ids   = [1, 3],   # Groups+KO or KO-only
        allowed_type_ids        = [1],      # singles
        max_classes             = SCRAPE_PARTICIPANTS_MAX_CLASSES,
        order                   = SCRAPE_PARTICIPANTS_ORDER,
    )

    tournament_ids  = [tc.tournament_id for tc in classes if tc.tournament_id is not None]
    tid_to_ext      = Tournament.get_id_ext_map_by_id(cursor, tournament_ids)

    logger.info(f"Scraping tournament class KO matches for {len(classes)} classes from OnData")

    total_seen = total_inserted = total_skipped = 0

    for idx, tclass in enumerate(classes, 1):
        tid_ext = tid_to_ext.get(tclass.tournament_id)
        cid_ext = tclass.tournament_class_id_ext

        logger_keys = {
            "class_idx":                                f"{idx}/{len(classes)}",
            "tournament":                               tclass.shortname or tclass.longname or "N/A",
            "tournament_id":                            str(tclass.tournament_id or "None"),
            "tournament_id_ext":                        str(tid_ext or "None"),
            "tournament_class_id":                      str(tclass.tournament_class_id or "None"),
            "tournament_class_id_ext":                  str(cid_ext or "None"),
            "date":                                     str(getattr(tclass, "startdate", None) or "None"),
            "stage":                                    5,
            "missing_players":                          "",
            "tokens_not_attached":                      "",
            "missing_score_for_match":                  "",
            "duplicate_players_in_first_round":         "",
            "inconsistent_best_of_in_round":            "",
            "misc_validation_issues":                   "",
            "round_name":                               "",
            "players":                                  ""
        }

        
        if DEBUG_OUTPUT:
            header_line = f"===== {tclass.shortname or tclass.longname or 'N/A'} [cid_ext = '{cid_ext or 'None'}'] [tid_ext = '{tid_ext or 'None'}'] ====="
            _debug_print("\n" + header_line)
            # _debug_print(f"Tournament ID ext: {tid_ext or 'None'}")

        if not cid_ext:
            logger.failed(logger_keys.copy(), "No tournament_class_id_ext available for class")
            continue

        # Force refresh if the tournament ended within the last 90 days
        today = date.today()
        ref_date = (tclass.startdate or today)
        force_refresh = False
        if ref_date:
            try:
                ref_date = ref_date.date() if hasattr(ref_date, "date") else ref_date
                if (today - ref_date).days <= 90:
                    force_refresh = True
            except Exception:
                pass

        # Currently disable force refresh
        force_refresh = False

        pdf_path, downloaded, msg = _download_pdf_ondata_by_tournament_class_and_stage(
            tournament_id_ext=tid_ext or "",
            class_id_ext=cid_ext or "",
            stage=5,
            force_download=force_refresh,
        )
        if msg and DEBUG_OUTPUT:
            _debug_print(msg)
            _debug_print(f"URL: https://resultat.ondata.se/ViewClassPDF.php?tournamentID={tid_ext}&classID={cid_ext}&stage=5")

        if not pdf_path:
            logger.failed(logger_keys.copy(), "No valid KO PDF (stage=5) for class")
            continue

        try:
            with open(pdf_path, "rb") as handle:
                pdf_bytes = handle.read()

            pdf_hash_key = f"{cid_ext or ''}:{tid_ext or ''}:stage5"
            words = _extract_words(pdf_bytes, pdf_hash_key)
            pdf_pages_words: List[List[dict]] = []
            pdf_page_boxes: List[Tuple[float, float]] = []
            wo_words_all: List[dict] = []
            try:
                with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf_doc:
                    pdf_pages_words = [
                        page.extract_words(keep_blank_chars=True) for page in pdf_doc.pages
                    ]
                    pdf_page_boxes = [(page.width, page.height) for page in pdf_doc.pages]
            except Exception:
                pdf_pages_words = [words]
                pdf_page_boxes = []

            # Special case: some RO128 brackets are split across two PDF pages (two halves).
            split_built = False
            is_two_page_split_128 = (
                len(pdf_pages_words) >= 2
                and (tclass.ko_tree_size or 0) >= 128
            )
            if len(pdf_pages_words) >= 2 and not is_two_page_split_128:
                # Heuristic: many winners/players across two pages -> likely a split RO128 even if ko_tree_size is missing.
                page2_words = pdf_pages_words[1]
                page2_players = _extract_players(page2_words)
                page2_winners = _deduplicate_winner_entries(_extract_winner_entries(page2_words, (0, 10000)))
                if (len(page2_players) + len(_extract_players(words)) > 64) and (len(page2_winners) + len(_deduplicate_winner_entries(_extract_winner_entries(words, (0, 10000)))) > 70):
                    is_two_page_split_128 = True

            if is_two_page_split_128:
                # Parse each half separately as a 64-tree, then stitch in the final from page 1.
                half_rounds: List[List[Match]] = []
                half_champions: List[Player] = []
                combined_score_pool: List[ScoreEntry] = []
                combined_winners_page: List[WinnerEntry] = []
                combined_players: List[Player] = []
                wo_words_all: List[dict] = []

                for page_words in pdf_pages_words[:2]:
                    wo_words_page = _filter_wo_words(page_words)
                    markers_page = _extract_wo_markers(page_words, -1e9, 1e9)
                    rounds_half, players_half, scores_half, winners_half = _parse_single_page_bracket(
                        page_words,
                        tree_size=64,
                        logger=logger,
                        logger_keys=logger_keys.copy(),
                        strict_winner_matching=True,
                    )
                    score_bands_half = _cluster_columns([s.x for s in scores_half])
                    if rounds_half:
                        _apply_walkovers_from_words(
                            rounds_half,
                            wo_words_page,
                            players_half,
                            override_scored=True,
                            scores_hint=scores_half,
                            score_pool=scores_half,
                            score_bands=score_bands_half,
                        )
                        _apply_walkovers_to_rounds(
                            rounds_half,
                            markers_page,
                            scores_hint=scores_half,
                            override_scored=True,
                            score_pool=scores_half,
                            score_bands=score_bands_half,
                        )
                    wo_words_all.extend(wo_words_page)
                    if rounds_half:
                        if len(rounds_half) < 6 and rounds_half and rounds_half[-1] and len(rounds_half[-1]) >= 2:
                            last_round = rounds_half[-1]
                            participants = []
                            for m in last_round[:2]:
                                if m.winner:
                                    participants.append(m.winner)
                                elif m.players:
                                    participants.append(m.players[0])
                            if len(participants) == 2:
                                center = sum(m.center for m in last_round[:2]) / 2.0
                                score_guess = _assign_nearest_score(center, list(scores_half), tolerance=60.0)
                                rounds_half.append([Match(players=participants, winner=participants[0], scores=score_guess, center=center)])
                        half_rounds.append(rounds_half)
                        champion: Optional[Player] = None
                        if winners_half:
                            try:
                                max_x_w = max(w.x for w in winners_half)
                                band = [w for w in winners_half if max_x_w - w.x <= 1.0]
                                counts: Dict[str, int] = {}
                                for w in band:
                                    counts[w.short] = counts.get(w.short, 0) + 1
                                target_short = max(counts.items(), key=lambda kv: kv[1])[0] if counts else None
                                if target_short:
                                    target_entries = [w for w in band if w.short == target_short]
                                    target_entries.sort(key=lambda w: w.center)
                                    for cand in target_entries:
                                        try:
                                            champion = _match_short_to_full(
                                                cand.short,
                                                cand.center,
                                                players_half,
                                                cand.player_id_ext,
                                            )
                                            break
                                        except ValueError:
                                            continue
                            except Exception:
                                champion = None
                        if champion is None and rounds_half and rounds_half[-1] and rounds_half[-1][0].winner:
                            champion = rounds_half[-1][0].winner
                        if champion:
                            half_champions.append(champion)
                    combined_score_pool.extend(scores_half)
                    combined_winners_page.extend(winners_half)
                    combined_players.extend(players_half)

                if half_rounds and half_champions:
                    # Extract final winner/score from first page (bottom right area)
                    page1_scores = sorted(_extract_score_entries(pdf_pages_words[0], (0, 10000)), key=lambda s: (s.x, s.center))
                    page1_winners = _deduplicate_winner_entries(_extract_winner_entries(pdf_pages_words[0], (0, 10000)))
                    page1_size = pdf_page_boxes[0] if pdf_page_boxes else None
                    if page1_scores:
                        combined_score_pool.extend(page1_scores)
                    combined_winners_page.extend(page1_winners)
                    combined_players = _deduplicate_players(combined_players)
                    page1_winners = _filter_winners_to_player_band(page1_winners, combined_players)

                    final_center_y = sum(p.center for p in half_champions) / len(half_champions)
                    final_winner_entry = None
                    if page1_winners:
                        try:
                            max_x_final = max(w.x for w in page1_winners)
                            candidates = [w for w in page1_winners if max_x_final - w.x <= 1.0]
                            if candidates:
                                # Prefer a candidate that matches one of the half champions
                                champion_keys = {_player_key(p) for p in half_champions}
                                matched = []
                                for cand in candidates:
                                    try:
                                        player = _match_short_to_full(cand.short, cand.center, combined_players, cand.player_id_ext)
                                        if _player_key(player) in champion_keys:
                                            matched.append((cand, player))
                                    except ValueError:
                                        continue
                                if matched:
                                    final_winner_entry = max(matched, key=lambda tpl: tpl[0].center)[0]
                                else:
                                    final_winner_entry = min(candidates, key=lambda w: abs(w.center - final_center_y))
                            else:
                                final_winner_entry = _assign_nearest_winner(final_center_y, page1_winners, tolerance=120.0)
                        except Exception:
                            final_winner_entry = _assign_nearest_winner(final_center_y, page1_winners, tolerance=120.0)
                    final_scores = None
                    if page1_scores:
                        bottom_right_candidate = _score_entry_closest_to_bottom_right(
                            page1_scores,
                            page_size=page1_size,
                            page_words=pdf_pages_words[0] if pdf_pages_words else None,
                        )
                        if bottom_right_candidate:
                            final_scores = bottom_right_candidate.scores
                        if final_scores is None:
                            try:
                                max_x_score = max(s.x for s in page1_scores)
                                score_candidates = [s for s in page1_scores if max_x_score - s.x <= 12.0]
                                if score_candidates:
                                    # Pick the bottom-most entry in the right-most column (final box sits bottom right).
                                    final_scores = max(score_candidates, key=lambda s: (s.x, s.center)).scores
                            except Exception:
                                pass
                    if final_scores is None:
                        final_scores = _assign_nearest_score(final_center_y, combined_score_pool, tolerance=120.0)
                    final_winner: Optional[Player] = None
                    if final_winner_entry is not None:
                        try:
                            final_winner = _match_short_to_full(final_winner_entry.short, final_winner_entry.center, combined_players, final_winner_entry.player_id_ext)
                        except ValueError:
                            final_winner = None
                    if final_winner is None:
                        final_winner = half_champions[0] if half_champions else None

                    final_match = Match(players=half_champions, winner=final_winner, scores=final_scores, center=final_center_y)

                    # Merge halves: build combined rounds per stage
                    merged_rounds: List[List[Match]] = []
                    max_depth = max(len(r) for r in half_rounds)
                    for depth in range(max_depth):
                        merged: List[Match] = []
                        for hr in half_rounds:
                            if depth < len(hr):
                                merged.extend(hr[depth])
                        merged_rounds.append(merged)
                    merged_rounds.append([final_match])

                    all_rounds = merged_rounds
                    players = combined_players
                    all_scores_page = combined_score_pool
                    all_winners_page = combined_winners_page
                    score_bands_page = _cluster_columns([s.x for s in all_scores_page])
                    winner_bands_page = _cluster_columns([w.x for w in all_winners_page])
                    tree_size = 128
                    split_built = True
                else:
                    # Fallback to normal single-page flow if something went wrong
                    players = _extract_players(words)
            else:
                players = _extract_players(words)
            if split_built:
                # Skip standard single-page extraction; we already built combined rounds.
                double_wo_small_bracket = False
                qual_header = None
            else:
                qual_header = _find_qualification_header(words)
                if qual_header:
                    qual_center = (float(qual_header["top"]) + float(qual_header["bottom"])) / 2
                    players = [p for p in players if p.center < qual_center + 5]

                all_scores_page = sorted(_extract_score_entries(words, (0, 10000)), key=lambda s: (s.x, s.center))
                all_winners_page = _deduplicate_winner_entries(_extract_winner_entries(words, (0, 10000)))
                all_winners_page = _filter_winners_to_player_band(all_winners_page, players)
                all_winners_page.sort(key=lambda w: (w.x, w.center))
                score_bands_page = _cluster_columns([s.x for s in all_scores_page])
                winner_bands_page = _cluster_columns([w.x for w in all_winners_page])
                wo_words_all = _filter_wo_words(words)
            wo_markers_all = _extract_wo_markers(words, -1e9, 1e9)
            double_wo_small_bracket = _contains_double_wo(words) and len(players) <= 4

            total_winners = len(all_winners_page)
            if total_winners == 0:
                logger.warning(logger_keys.copy(), "No winner labels detected on page")
                continue

            tree_size = int(tclass.ko_tree_size or 0)
            fallback_tree_size_used = False
            if tree_size < 2:
                fallback_tree_size_used = True
                tree_size = 2
                while tree_size - 1 < total_winners and tree_size <= 512:
                    tree_size *= 2
            # If the bracket is a tiny Dubbel-WO collapse (<=2 players), force a 2-slot tree so validation expectations match.
            if double_wo_small_bracket and len(players) <= 2:
                tree_size = 2
                fallback_tree_size_used = True
            if double_wo_small_bracket and tree_size > 4:
                target = 1
                while target < max(len(players), 2):
                    target *= 2
                tree_size = max(4, target)
                fallback_tree_size_used = True
            validation_tree_size = tree_size
            if split_built:
                validation_tree_size = max(validation_tree_size, 128)

            all_rounds: List[List[Match]] = []
            score_entries_pool: List[ScoreEntry] = []
            round_winner_entries: List[List[WinnerEntry]] = []

            if split_built:
                all_rounds = merged_rounds
                score_entries_pool = list(combined_score_pool)
                round_winner_entries = []
            else:
                # Group winners by x-bands (merge/split when large brackets collapse columns)
                if tree_size >= 64:
                    winners_by_round = _allocate_winners_by_round(
                        all_winners_page,
                        winner_bands_page,
                        tree_size,
                        use_alignment=(tree_size < 64),
                    )
                else:
                    winners_by_round: List[List[WinnerEntry]] = []
                    for band in winner_bands_page:
                        chunk = [w for w in all_winners_page if band[0] <= w.x <= band[1]]
                        chunk.sort(key=lambda w: w.center)
                        if chunk:
                            winners_by_round.append(chunk)
                    if double_wo_small_bracket and winners_by_round:
                        flattened = [
                            w
                            for chunk in winners_by_round
                            for w in chunk
                            if w.short.strip()
                            and not any(instr in w.short.lower() for instr in WINNER_INSTRUCTION_PHRASES)
                        ]
                        flattened.sort(key=lambda w: w.center)
                        if len(flattened) >= 2:
                            first_round = flattened[:2]
                            next_round = [flattened[-1]] if len(flattened) >= 3 else flattened[1:]
                            winners_by_round = [first_round, next_round]

                if double_wo_small_bracket:
                    all_rounds, score_entries_pool = _build_double_wo_bracket(
                        players,
                        all_winners_page,
                        all_scores_page,
                        wo_markers_all,
                    )
                else:
                    previous_round: Optional[List[Match]] = None
                    available_score_bands = list(score_bands_page)
                    score_entries_pool = list(all_scores_page)
                    carry_winners: List[WinnerEntry] = []
                    for ridx, winner_chunk in enumerate(winners_by_round):
                        combined_winners = []
                        if carry_winners:
                            combined_winners.extend(carry_winners)
                            carry_winners = []
                        combined_winners.extend(winner_chunk)
                        if not combined_winners:
                            continue
                        round_winner_entries.append(list(combined_winners))
                        win_min = min(w.x for w in combined_winners)
                        win_max = max(w.x for w in combined_winners)
                        winner_band = (win_min, win_max)
                        score_band = _find_closest_score_band(available_score_bands, winner_band)
                        band_was_available = False
                        rounds_left = len(winners_by_round) - (ridx + 1)
                        if score_band in available_score_bands and rounds_left > 2:
                            available_score_bands.remove(score_band)
                            band_was_available = True
                        score_min = (score_band[0] - 1.0) if score_band[0] is not None else None
                        score_max = (score_band[1] + 1.0) if score_band[1] is not None else None
                        score_window = (score_min, score_max)
                        scores_for_round = [
                            entry
                            for entry in score_entries_pool
                            if (score_min is None or entry.x >= score_min)
                            and (score_max is None or entry.x <= score_max)
                        ]
                        original_scores = list(scores_for_round)
                        tolerance_step = ridx
                        if previous_round is None:
                            current_round, leftover_scores = _build_first_round(
                                players,
                                combined_winners,
                                scores_for_round,
                                score_window,
                                tree_size=tree_size
                            )
                            if ridx + 1 < len(winners_by_round):
                                _apply_advancers_from_next_round(
                                    current_round,
                                    winners_by_round[ridx + 1],
                                    players,
                                )
                            remaining_ids = {id(entry) for entry in leftover_scores}
                            consumed = [entry for entry in original_scores if id(entry) not in remaining_ids]
                        else:
                            current_round, remaining_winners, leftover_scores = _build_next_round(
                                previous_round,
                                combined_winners,
                                scores_for_round,
                                players,
                                score_window,
                                winner_tolerance=24.0 + 4.0 * tolerance_step,
                                score_tolerance=28.0 + 4.0 * tolerance_step,
                            )
                            _fill_missing_winners(previous_round, current_round)
                            if ridx + 1 < len(winners_by_round):
                                _apply_advancers_from_next_round(
                                    current_round,
                                    winners_by_round[ridx + 1],
                                    players,
                                )
                            if remaining_winners:
                                carry_winners.extend(remaining_winners)
                            remaining_ids = {id(entry) for entry in leftover_scores}
                            consumed = [entry for entry in original_scores if id(entry) not in remaining_ids]
                        if current_round:
                            all_rounds.append(current_round)
                            previous_round = current_round
                        if not consumed and band_was_available:
                            available_score_bands.insert(0, score_band)
                        if consumed:
                            consumed_ids = {id(entry) for entry in consumed}
                            score_entries_pool = [
                                entry for entry in score_entries_pool if id(entry) not in consumed_ids
                            ]

            if all_rounds and len(all_rounds[-1]) > 1:
                semifinals = all_rounds[-1]
                final_center_y = sum(m.center for m in semifinals) / len(semifinals)
                final_winner_candidates = [
                    w
                    for w in all_winners_page
                    if winner_bands_page and w.x >= winner_bands_page[-1][1] - 1.0
                ]
                final_winner_entry = _assign_nearest_winner(final_center_y, final_winner_candidates, tolerance=45.0)
                if final_winner_entry is None and final_winner_candidates:
                    final_winner_entry = min(final_winner_candidates, key=lambda w: abs(w.center - final_center_y))
                final_scores_candidates = [
                    s
                    for s in all_scores_page
                    if score_bands_page and s.x >= score_bands_page[-1][0] - 1.0
                ]
                final_scores = _pop_score_aligned(
                    final_scores_candidates,
                    final_center_y,
                    40.0,
                )
                if final_scores is None:
                    final_scores = _assign_nearest_score(final_center_y, final_scores_candidates, tolerance=40.0)
                final_participants = [m.winner for m in semifinals if m.winner]
                final_winner: Optional[Player] = None
                if final_winner_entry is not None:
                    try:
                        final_winner = _match_short_to_full(
                            final_winner_entry.short,
                            final_winner_entry.center,
                            players,
                            final_winner_entry.player_id_ext,
                        )
                    except ValueError:
                        final_winner = None
                if final_winner is None and final_participants:
                    final_winner = final_participants[0]
                final_match = Match(
                    players=final_participants,
                    winner=final_winner,
                    scores=final_scores,
                    center=final_center_y,
                )
                _fill_missing_winners(semifinals, [final_match])
                all_rounds.append([final_match])
                round_winner_entries.append(final_winner_candidates)

            if len(all_rounds) >= 2 and round_winner_entries:
                max_winner_x = max(entry.x for chunk in round_winner_entries for entry in chunk)
                final_entries = [
                    entry
                    for chunk in round_winner_entries
                    for entry in chunk
                    if entry.x >= max_winner_x - 0.6
                ]
                final_players_set: Set[Tuple[Optional[str], str]] = set()
                for entry in final_entries:
                    try:
                        player = _match_short_to_full(
                            entry.short,
                            entry.center,
                            players,
                            entry.player_id_ext,
                        )
                    except ValueError:
                        continue
                    final_players_set.add(_player_key(player))
                if final_players_set and len(all_rounds[-2]) > 0:
                    semifinal_round = all_rounds[-2]
                    for match in semifinal_round:
                        if match.winner and _player_key(match.winner) in final_players_set:
                            continue
                        for participant in match.players:
                            if _player_key(participant) in final_players_set:
                                match.winner = participant
                                break

            # Guard against duplicate finals (two consecutive 1-match rounds).
            _dedupe_final_rounds(all_rounds, score_entries_pool, all_scores_page)

            if score_entries_pool and all_rounds:
                final_round = all_rounds[-1]
                if final_round:
                    match = final_round[-1]
                    if match.scores is None and score_entries_pool:
                        assigned = _assign_nearest_score(match.center, score_entries_pool, tolerance=80.0)
                        if assigned is not None:
                            match.scores = assigned
            if score_entries_pool and all_rounds:
                for matches in reversed(all_rounds):
                    for match in matches:
                        if match.scores is not None or len(match.players) < 2:
                            continue
                        assigned = _assign_nearest_score(match.center, score_entries_pool, tolerance=80.0)
                        if assigned is not None:
                            match.scores = assigned
                            if not score_entries_pool:
                                break
                    if not score_entries_pool:
                        break

            for ridx in range(len(all_rounds) - 2, -1, -1):
                _fill_missing_winners(all_rounds[ridx], all_rounds[ridx + 1])

            if len(all_rounds) >= 2 and len(all_rounds[-1]) == 1:
                semifinal_round = all_rounds[-2]
                finalists = [match.winner for match in semifinal_round if match.winner]
                if len(finalists) == 2:
                    all_rounds[-1][0].players = finalists

            if split_built:
                qualification = []
            else:
                qualification = _extract_qualification_matches(words)
                if len(pdf_pages_words) > 1:
                    for extra_page_words in pdf_pages_words[1:]:
                        extra_q = _extract_qualification_matches(extra_page_words)
                        if extra_q:
                            qualification.extend(extra_q)
                if qualification and all_rounds:
                    _assign_qualification_winners_by_presence(qualification, all_rounds[0])
                if qualification:
                    _fill_walkover_forfeiter(qualification)
                if qualification:
                    for match in qualification:
                        if match.scores is None:
                            continue
                        for idx, entry in enumerate(score_entries_pool):
                            if entry.scores == match.scores:
                                score_entries_pool.pop(idx)
                                break
            if not split_built and wo_words_all:
                _apply_walkovers_from_words(
                    all_rounds,
                    wo_words_all,
                    players,
                    scores_hint=all_scores_page,
                    score_pool=score_entries_pool,
                    score_bands=score_bands_page,
                )
            # Apply WO markers to KO rounds before validation/insertion
            if not split_built:
                _apply_walkovers_to_rounds(
                    all_rounds,
                    wo_markers_all,
                    scores_hint=all_scores_page,
                    override_scored=True,
                    score_pool=score_entries_pool,
                    score_bands=score_bands_page,
                )
            _fill_walkover_forfeiter([m for r in all_rounds for m in r])
            _propagate_round_participants(all_rounds)
            _align_winners_to_advancers(all_rounds)
            _align_winners_to_future_advancers(all_rounds)
            _propagate_round_participants(all_rounds)

            for matches in all_rounds:
                _ensure_winner_first(matches)
            if qualification:
                _ensure_winner_first(qualification)

            _force_walkover_for_unscored_small_bracket(
                all_rounds,
                tree_size=validation_tree_size,
                wo_tokens_present=bool(wo_markers_all or wo_words_all),
            )
            _reassign_scores_small_bracket(
                all_rounds,
                scores=list(all_scores_page),
                tree_size=validation_tree_size,
            )
            # Try to reattach any leftover score tokens (e.g., freed when a match became WO)
            if score_entries_pool:
                for matches in all_rounds:
                    for match in matches:
                        if match.scores is not None or len(match.players) < 2 or getattr(match, "walkover", False):
                            continue
                        assigned = _assign_nearest_score(match.center, score_entries_pool, tolerance=60.0)
                        if assigned is not None:
                            match.scores = assigned

            if all_rounds:
                _strip_scores_from_walkovers([m for r in all_rounds for m in r])
            if qualification:
                _strip_scores_from_walkovers(qualification)

            _run_structural_bracket_checks(
                all_rounds,
                logger=logger,
                logger_keys=logger_keys.copy(),
            )

            _validate_bracket(
                pdf_hash_key,
                tclass,
                all_rounds,
                players,
                validation_tree_size,
                list(score_entries_pool),
                fallback_tree_size_used,
                qualification,
                logger=logger,
                logger_keys=logger_keys.copy(),
            )

            debug_lines: List[str] = []
            if qualification:
                debug_lines.extend(_label_round("Qualification", qualification))
                debug_lines.append("")
            wo_count = sum(
                1
                for round_matches in all_rounds
                for m in round_matches
                if getattr(m, "walkover", False)
            )
            if qualification:
                wo_count += sum(
                    1 for m in qualification if getattr(m, "walkover", False)
                )
            debug_lines.append(f"WO matches detected: {wo_count}")
            for matches in all_rounds:
                ro_size = len(matches) * 2
                if ro_size > 8:
                    name = f"RO{ro_size}"
                elif ro_size == 8:
                    name = "RO8/QF"
                elif ro_size == 4:
                    name = "RO4/SF"
                elif ro_size == 2:
                    name = "RO2/Final"
                else:
                    name = "Final"
                debug_lines.extend(_label_round(name, matches))
                debug_lines.append("")
            if DEBUG_OUTPUT and debug_lines:
                wo_count = sum(
                    1
                    for round_matches in all_rounds
                    for m in round_matches
                    if getattr(m, "walkover", False)
                )
                if qualification:
                    wo_count += sum(
                        1 for m in qualification if getattr(m, "walkover", False)
                    )
                debug_lines.append(f"WO matches detected: {wo_count}")
                _debug_print("\n".join(debug_lines))

            round_payloads: List[Tuple[int, Sequence[Match]]] = []
            for matches in all_rounds:
                if not matches:
                    continue
                stage_id = _stage_id_for_match_count(len(matches))
                if stage_id is None:
                    logger.warning(
                        logger_keys.copy(),
                        "Unable to map match count to stage id; defaulting to QF",
                    )
                    stage_id = 6
                round_payloads.append((stage_id, matches))
            if qualification:
                round_payloads.append((10, qualification))

            stage_ids_to_clear = {
                stage_id for stage_id, match_list in round_payloads if stage_id is not None and match_list
            }
            if not stage_ids_to_clear:
                stage_ids_to_clear = set(_DEFAULT_KO_STAGE_IDS)

            removed_total = 0
            for stage_id in sorted(stage_ids_to_clear):
                removed_total += TournamentClassMatchRaw.remove_for_class(
                    cursor,
                    tournament_class_id_ext=cid_ext,
                    data_source_id=1,
                    tournament_class_stage_id=stage_id,
                )

            class_seen = class_inserted = class_skipped = 0
            for stage_id, match_list in round_payloads:
                seen, inserted, skipped = _insert_matches_for_stage(
                    cursor,
                    match_list,
                    stage_id,
                    tid_ext,
                    cid_ext,
                    logger=logger,
                    logger_keys=logger_keys.copy(),
                )
                class_seen += seen
                class_inserted += inserted
                class_skipped += skipped

            total_seen += class_seen
            total_inserted += class_inserted
            total_skipped += class_skipped

            parsed_size = validation_tree_size if all_rounds else 0
            stored = tclass.ko_tree_size
            try:
                if split_built and parsed_size and (not stored or stored < parsed_size):
                    TournamentClass.set_tree_size(cursor, cid_ext, parsed_size, data_source_id=1)
                    stored = parsed_size
                    logger.info(logger_keys.copy(), f"Tournament class tree size adjusted to {parsed_size}", to_console=True)
            except Exception:
                pass
            logger.info(
                logger_keys.copy(),
                f"Removed: {removed_total}   Inserted: {class_inserted}   Skipped: {class_skipped} -- Tree size stored / parsed: {stored} / {parsed_size}",
            )

        except Exception as exc:
            logger.failed(logger_keys.copy(), f"KO PDF parsing failed: {exc}")
            continue

    logger.info(
        f"Scraping completed. Inserted: {total_inserted}, Skipped: {total_skipped}, Matches seen: {total_seen}"
    )
    logger.summarize()



def _extract_words(pdf_bytes: bytes, hash_key: str) -> List[dict]:
    """Extract words from the first PDF page and retain a content hash."""

    LAST_PDF_HASHES[hash_key] = hashlib.md5(pdf_bytes).hexdigest()
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        page = pdf.pages[0]
        return page.extract_words(keep_blank_chars=True)


def _contains_double_wo(words: Sequence[dict]) -> bool:
    for w in words:
        txt = w.get("text", "").lower()
        if "dubbel-wo" in txt or "double-wo" in txt or "dubbel wo" in txt:
            return True
    return False


def _is_double_wo_label(text: str) -> bool:
    normalized = normalize_key(text, preserve_diacritics=True, preserve_nordic=True)
    collapsed = re.sub(r"[^\w]", "", normalized)
    return collapsed in {"dubbelwo", "doublewo"}


def _parse_single_page_bracket(
    words: Sequence[dict],
    tree_size: int,
    *,
    logger: OperationLogger,
    logger_keys: Dict[str, str],
    strict_winner_matching: bool = False,
) -> Tuple[List[List[Match]], List[Player], List[ScoreEntry], List[WinnerEntry]]:
    """Lightweight wrapper to parse one page and return rounds without DB writes."""
    players = _extract_players(words)
    qual_header = _find_qualification_header(words)
    if qual_header:
        qual_center = (float(qual_header["top"]) + float(qual_header["bottom"])) / 2
        players = [p for p in players if p.center < qual_center + 5]

    all_scores_page = sorted(_extract_score_entries(words, (0, 10000)), key=lambda s: (s.x, s.center))
    all_winners_page = _deduplicate_winner_entries(_extract_winner_entries(words, (0, 10000)))
    all_winners_page = _filter_winners_to_player_band(all_winners_page, players)
    all_winners_page.sort(key=lambda w: (w.x, w.center))
    if strict_winner_matching:
        filtered_winners: List[WinnerEntry] = []
        for entry in all_winners_page:
            if getattr(entry, "is_double_wo", False):
                filtered_winners.append(entry)
                continue
            try:
                _match_short_to_full(entry.short, entry.center, players, entry.player_id_ext)
            except ValueError:
                continue
            filtered_winners.append(entry)
        all_winners_page = filtered_winners
    score_bands_page = _cluster_columns([s.x for s in all_scores_page])
    winner_bands_page = _cluster_columns([w.x for w in all_winners_page])

    total_winners = len(all_winners_page)
    if total_winners == 0:
        logger.warning(logger_keys.copy(), "No winner labels detected on page")
        return [], players, all_scores_page, all_winners_page

    if tree_size < 2:
        tree_size = 2
        while tree_size - 1 < total_winners and tree_size <= 512:
            tree_size *= 2

    if tree_size >= 64:
        winners_by_round = _allocate_winners_by_round(
            all_winners_page,
            winner_bands_page,
            tree_size,
            use_alignment=(tree_size < 64),
        )
    else:
        winners_by_round = []
        for band in winner_bands_page:
            chunk = [w for w in all_winners_page if band[0] <= w.x <= band[1]]
            chunk.sort(key=lambda w: w.center)
            if chunk:
                winners_by_round.append(chunk)

    all_rounds: List[List[Match]] = []
    previous_round: Optional[List[Match]] = None
    available_score_bands = list(score_bands_page)
    score_entries_pool = list(all_scores_page)
    round_winner_entries: List[List[WinnerEntry]] = []
    carry_winners: List[WinnerEntry] = []

    for ridx, winner_chunk in enumerate(winners_by_round):
        combined_winners = []
        if carry_winners:
            combined_winners.extend(carry_winners)
            carry_winners = []
        combined_winners.extend(winner_chunk)
        if not combined_winners:
            continue
        round_winner_entries.append(list(combined_winners))
        win_min = min(w.x for w in combined_winners)
        win_max = max(w.x for w in combined_winners)
        winner_band = (win_min, win_max)
        score_band = _find_closest_score_band(available_score_bands, winner_band)
        band_was_available = False
        rounds_left = len(winners_by_round) - (ridx + 1)
        if score_band in available_score_bands and rounds_left > 2:
            available_score_bands.remove(score_band)
            band_was_available = True
        score_min = (score_band[0] - 1.0) if score_band[0] is not None else None
        score_max = (score_band[1] + 1.0) if score_band[1] is not None else None
        score_window = (score_min, score_max)
        scores_for_round = [
            entry
            for entry in score_entries_pool
            if (score_min is None or entry.x >= score_min)
            and (score_max is None or entry.x <= score_max)
        ]
        original_scores = list(scores_for_round)
        tolerance_step = ridx
        if previous_round is None:
            current_round, leftover_scores = _build_first_round(
                players,
                combined_winners,
                scores_for_round,
                score_window,
                tree_size=tree_size
            )
            if ridx + 1 < len(winners_by_round):
                _apply_advancers_from_next_round(
                    current_round,
                    winners_by_round[ridx + 1],
                    players,
                )
            remaining_ids = {id(entry) for entry in leftover_scores}
            consumed = [entry for entry in original_scores if id(entry) not in remaining_ids]
        else:
            current_round, remaining_winners, leftover_scores = _build_next_round(
                previous_round,
                combined_winners,
                scores_for_round,
                players,
                score_window,
                winner_tolerance=24.0 + 4.0 * tolerance_step,
                score_tolerance=28.0 + 4.0 * tolerance_step,
                strict_participant_matching=strict_winner_matching,
            )
            _fill_missing_winners(previous_round, current_round)
            if ridx + 1 < len(winners_by_round):
                _apply_advancers_from_next_round(
                    current_round,
                    winners_by_round[ridx + 1],
                    players,
                )
            if remaining_winners:
                carry_winners.extend(remaining_winners)
            remaining_ids = {id(entry) for entry in leftover_scores}
            consumed = [entry for entry in original_scores if id(entry) not in remaining_ids]
        if current_round:
            all_rounds.append(current_round)
            previous_round = current_round
        if not consumed and band_was_available:
            available_score_bands.insert(0, score_band)
        if consumed:
            consumed_ids = {id(entry) for entry in consumed}
            score_entries_pool = [
                entry for entry in score_entries_pool if id(entry) not in consumed_ids
            ]

    for ridx in range(len(all_rounds) - 2, -1, -1):
        _fill_missing_winners(all_rounds[ridx], all_rounds[ridx + 1])

    if len(all_rounds) >= 2 and len(all_rounds[-1]) == 1:
        semifinal_round = all_rounds[-2]
        finalists = [match.winner for match in semifinal_round if match.winner]
        if len(finalists) == 2:
            all_rounds[-1][0].players = finalists

    for matches in all_rounds:
        _ensure_winner_first(matches)

    return all_rounds, players, all_scores_page, all_winners_page


def _player_key(player: Player) -> Tuple[Optional[str], str, Optional[str], str]:
    return (player.player_id_ext, player.full_name, player.player_suffix_id, player.club)


def _to_center(word: dict) -> float:
    return (float(word["top"]) + float(word["bottom"])) / 2


def _make_short(name: str) -> str:
    parts = name.split()
    if len(parts) < 2:
        return name
    return f"{parts[0]} {parts[1][0]}"


DRAW_PREFIX_RE = re.compile(r"^\s*\d+\s*[>.\)\-]\s*")


def _strip_draw_prefix(text: str) -> str:
    """Remove leading draw index markers (e.g. '1>' or '2)')."""
    return DRAW_PREFIX_RE.sub("", text.strip())


def _split_score_and_label(text: str) -> Tuple[Optional[str], str]:
    cleaned = text.replace(" ", " ")
    cleaned = _strip_draw_prefix(cleaned)
    match = COMBINED_SCORE_LABEL_RE.match(cleaned)
    if match:
        score_text, label_text = match.groups()
        return score_text.strip(), label_text.strip()
    return None, cleaned.strip()


def _build_name_aliases(full_name: str) -> Set[str]:
    """Generate alias set for robust matching, including initials and splits."""
    if not full_name.strip():
        return set()
    full_name = unicodedata.normalize('NFC', full_name)
    normalized = normalize_key(full_name, preserve_diacritics=True, preserve_nordic=True)
    tokens = normalized.split()
    aliases: Set[str] = {normalized}  # Full normalized

    # Local version of name_keys_for_lookup_all_splits to preserve diacritics
    if len(tokens) > 1:
        for i in range(1, len(tokens)):
            prefix = " ".join(tokens[:i])
            suffix = " ".join(tokens[i:])
            fn_ln = f"{prefix} {suffix}"
            ln_fn = f"{suffix} {prefix}"
            aliases.add(fn_ln)
            if fn_ln != ln_fn:
                aliases.add(ln_fn)

    if len(tokens) <= 1:
        return aliases

    # First-initial + surname (e.g., "T Ander")
    first_initial = tokens[0][0]
    surname_only = " ".join(tokens[1:])
    if surname_only:
        aliases.add(f"{first_initial} {surname_only}")
        aliases.add(f"{first_initial}{surname_only}")

    # For each possible surname prefix length (assume first 1+ tokens as surname group)
    for surname_start in range(1, len(tokens) + 1):
        surname_tokens = tokens[:surname_start]
        firstname_tokens = tokens[surname_start:]
        if not firstname_tokens:
            continue

        # Surname group as-is
        surname = " ".join(surname_tokens)
        aliases.add(surname)

        # Initials for firstnames
        initials = " ".join(t[0] for t in firstname_tokens if t)
        if initials:
            aliases.add(f"{surname} {initials}")
            # Concat initials if single-letter
            if all(len(t) == 1 for t in firstname_tokens):
                concat_initials = "".join(firstname_tokens)
                aliases.add(f"{surname} {concat_initials}")

        # Reversed order (firstname initials + surname)
        if len(firstname_tokens) > 1:
            fn_initials = " ".join(t[0] for t in firstname_tokens[:-1]) + f" {firstname_tokens[-1]}"
            aliases.add(f"{fn_initials} {surname}")

    # Handle hyphenated: treat as single token but also split
    hyphen_tokens = re.split(r"[\s-]", normalized)
    if len(hyphen_tokens) > len(tokens):
        for i in range(1, len(hyphen_tokens)):
            prefix = " ".join(hyphen_tokens[:i])
            suffix = " ".join(hyphen_tokens[i:])
            aliases.add(f"{prefix} {suffix}")
            aliases.add(f"{suffix} {prefix}")

    return aliases


def _extract_players(words: Sequence[dict]) -> List[Player]:
    players: List[Player] = []
    for word in words:
        if float(word["x0"]) < 200 and "," in word["text"]:
            raw_text = _strip_draw_prefix(word["text"])
            name_segment = raw_text.split(",", 1)[0]
            if not re.search(r"[A-Za-zÅÄÖåäö]", name_segment):
                continue
            match = re.match(
                r"\s*(?:(\d{1,3})\s+)?([^,(]+?(?:\s+[^,(]+?)*)(?:\s*\(([^)]+)\))?,\s*(.+)",
                raw_text,
            )
            if not match:
                continue
            player_id_ext, raw_name, player_suffix_id, raw_club = match.groups()
            if player_id_ext:
                player_id_ext = player_id_ext.strip()
            full_name = unicodedata.normalize('NFC', raw_name.strip())
            club = unicodedata.normalize('NFC', raw_club.strip())
            player = Player(
                full_name=full_name,
                club=club,
                short=_make_short(full_name),
                center=_to_center(word),
                x=float(word["x0"]),
                x1=float(word["x1"]),
                player_id_ext=player_id_ext,
                player_suffix_id=player_suffix_id.strip() if player_suffix_id else None,
            )
            player.aliases = _build_name_aliases(full_name)
            players.append(player)
    players.sort(key=lambda p: p.center)
    return players


def _extract_score_entries(words: Sequence[dict], x_range: Tuple[float, float]) -> List[ScoreEntry]:
    start, stop = x_range
    entries: List[ScoreEntry] = []
    for word in words:
        x0 = float(word["x0"])
        if not (start <= x0 <= stop):
            continue
        raw = word["text"].strip().replace("−", "-")
        if not raw:
            continue
        score_text, _ = _split_score_and_label(raw)
        target = score_text or _strip_draw_prefix(raw)
        if not target:
            continue
        if not score_text and re.search(r"[A-Za-zÅÄÖåäö]", target):
            continue
        m = re.fullmatch(r"-?\d+(?:\s*,\s*-?\d+)+", target)
        if not m:
            continue
        nums = [int(tok) for tok in re.findall(r"-?\d+", m.group(0))]
        if not nums:
            continue
        entries.append(ScoreEntry(scores=tuple(nums), center=_to_center(word), x=x0))
    entries.sort(key=lambda e: e.center)
    return entries


def _extract_winner_entries(words: Sequence[dict], x_range: Tuple[float, float]) -> List[WinnerEntry]:
    start, stop = x_range
    winners: List[WinnerEntry] = []
    for word in words:
        x0 = float(word["x0"])
        if not (start <= x0 <= stop):
            continue
        text = word["text"].replace(" ", " ").strip()
        _score_text, text = _split_score_and_label(text)
        if not text:
            continue
        wo_prefix = re.match(r"^wo\s+(.+)$", text, flags=re.IGNORECASE)
        if wo_prefix:
            candidate = wo_prefix.group(1).strip()
            if candidate and re.search(r"[A-Za-zÅÄÖåäö]", candidate):
                text = candidate
        if any(
            token in text
            for token in (
                "Slutspel",
                "Höstpool",
                "program",
                "Kvalifikation",
                "Kvalificering",
                "Qualification",
                "Qualifying",
                # Qual headers handled elsewhere
                "Vinderen",
                "Mesterskaberne",
                "Senior Elite",
            )
        ):
            continue
        normalized_text = text.lower()
        if any(instr in normalized_text for instr in WINNER_INSTRUCTION_PHRASES):
            continue
        m = WINNER_LABEL_PATTERN.match(text)
        if not m:
            alt = re.match(r"^(\d{1,3})\s+([\wÅÄÖåäö.\-]+)$", text)
            if not alt:
                continue
            m = alt
        player_id_ext, label = m.groups()
        if any(ch.isdigit() for ch in label):
            continue
        label = unicodedata.normalize('NFC', label.strip())
        is_double_wo = _is_double_wo_label(label)
        if " " not in label and not player_id_ext and not is_double_wo:
            continue
        winners.append(
            WinnerEntry(
                short=label,
                center=_to_center(word),
                x=x0,
                player_id_ext=player_id_ext,
                is_double_wo=is_double_wo,
            )
        )
    winners.sort(key=lambda w: w.center)
    return winners


def _match_short_to_full(
    short: str,
    center: float,
    players: Sequence[Player],
    player_id_ext: Optional[str] = None,
) -> Player:
    short = unicodedata.normalize('NFC', short)
    normalized = normalize_key(short, preserve_diacritics=True, preserve_nordic=True)
    if not normalized.strip():
        raise ValueError(f"Empty label {short!r}")

    # Existing fast paths
    if player_id_ext:
        id_candidates = [p for p in players if p.player_id_ext == player_id_ext]
        if id_candidates:
            return min(id_candidates, key=lambda p: abs(p.center - center))

    candidates = [p for p in players if p.short == short]
    if candidates:
        return min(candidates, key=lambda p: abs(p.center - center))

    alt_short = _make_short(short)
    if alt_short != short:
        candidates = [p for p in players if p.short == alt_short]
        if candidates:
            return min(candidates, key=lambda p: abs(p.center - center))

    prefix_matches = [p for p in players if p.full_name.startswith(short)]
    if prefix_matches:
        return min(prefix_matches, key=lambda p: abs(p.center - center))

    # New alias matching
    alias_matches: List[Tuple[float, Player]] = []
    for p in players:
        if normalized in p.aliases:
            delta = abs(p.center - center)
            alias_matches.append((delta, p))

    if alias_matches:
        alias_matches.sort(key=lambda t: t[0])
        best = alias_matches[0][1]
        if DEBUG_OUTPUT and len(alias_matches) > 1:
            _debug_print(f"Alias match ambiguity for {short!r}: resolved to {best.full_name} via proximity")
        return best

    raise ValueError(f"No player matches label {short!r}")


def _assign_nearest_score(
    center: float,
    pool: List[ScoreEntry],
    tolerance: float = 20.0,
) -> Optional[Tuple[int, ...]]:
    if not pool:
        return None
    best_index = None
    best_delta = None
    best_len = None
    for idx, entry in enumerate(pool):
        delta = abs(entry.center - center)
        length = len(entry.scores)
        if best_delta is None or delta < best_delta:
            best_delta = delta
            best_index = idx
            best_len = length
        elif best_delta is not None and delta == best_delta:
            if best_len is None or length < best_len:
                best_index = idx
                best_len = length
    if best_delta is None or best_delta > tolerance or best_index is None:
        return None
    entry = pool.pop(best_index)
    return entry.scores


def _score_entry_closest_to_bottom_right(
    entries: Sequence[ScoreEntry],
    *,
    page_size: Optional[Tuple[float, float]] = None,
    page_words: Optional[Sequence[dict]] = None,
) -> Optional[ScoreEntry]:
    """Pick the score entry closest to the bottom-right corner of the page."""
    if not entries:
        return None
    target_x = target_y = None
    if page_size and len(page_size) == 2:
        target_x, target_y = page_size
    if (target_x is None or target_y is None) and page_words:
        try:
            target_x = max(float(w.get("x1", w.get("x0", 0.0))) for w in page_words)
            target_y = max(float(w.get("bottom", w.get("top", 0.0))) for w in page_words)
        except Exception:
            target_x = target_y = None
    if target_x is None or target_y is None:
        target_x = max(entry.x for entry in entries)
        target_y = max(entry.center for entry in entries)
    return min(
        entries,
        key=lambda entry: (target_x - entry.x) ** 2 + (target_y - entry.center) ** 2,
    )


def _pop_score_aligned(
    pool: List[ScoreEntry],
    center: float,
    tolerance: float,
    *,
    min_x: Optional[float] = None,
    max_x: Optional[float] = None,
    target_x: Optional[float] = None,
) -> Optional[Tuple[int, ...]]:
    if not pool:
        return None
    best_idx: Optional[int] = None
    best_delta: Optional[float] = None
    best_xdelta: Optional[float] = None
    best_len: Optional[int] = None
    for idx, entry in enumerate(pool):
        if min_x is not None and entry.x < min_x:
            continue
        if max_x is not None and entry.x > max_x:
            continue
        delta = abs(entry.center - center)
        xdelta = abs(entry.x - target_x) if target_x is not None else 0.0
        length = len(entry.scores)
        if best_delta is None or delta < best_delta:
            best_delta = delta
            best_idx = idx
            best_xdelta = xdelta
            best_len = length
        elif best_delta is not None and delta == best_delta:
            if best_xdelta is None or xdelta < best_xdelta:
                best_idx = idx
                best_xdelta = xdelta
                best_len = length
            elif best_xdelta is not None and xdelta == best_xdelta:
                if best_len is None or length < best_len:
                    best_idx = idx
                    best_len = length
    if best_idx is None or best_delta is None or best_delta > tolerance:
        return None
    return pool.pop(best_idx).scores


def _format_player(player: Optional[Player]) -> str:
    if not player:
        return "Unknown"
    prefix = f"[{player.player_id_ext}] " if player.player_id_ext else ""
    suffix = f"[{player.player_suffix_id}]" if player.player_suffix_id else ""
    return f"{prefix}{player.full_name}{suffix}, {player.club}"


def _assign_nearest_winner(
    center: float, pool: List[WinnerEntry], tolerance: float = 20.0
) -> Optional[WinnerEntry]:
    if not pool:
        return None
    best_index: Optional[int] = None
    best_delta: Optional[float] = None
    best_x: Optional[float] = None
    for idx, entry in enumerate(pool):
        delta = abs(entry.center - center)
        if best_delta is None or delta < best_delta:
            best_delta = delta
            best_index = idx
            best_x = entry.x
        elif best_delta is not None and delta == best_delta:
            # Tie-break: prefer right-most label when y-distance is identical
            if best_x is None or entry.x > best_x:
                best_index = idx
                best_x = entry.x
    if best_delta is None or best_delta > tolerance or best_index is None:
        return None
    return pool.pop(best_index)


def _pop_matching_winner(
    center: float,
    participants: Sequence[Player],
    pool: List[WinnerEntry],
    tolerance: float,
) -> Optional[WinnerEntry]:
    best_idx: Optional[int] = None
    best_delta: Optional[float] = None
    best_x: Optional[float] = None
    for idx, entry in enumerate(pool):
        try:
            matched = _match_short_to_full(
                entry.short,
                entry.center,
                participants,
                entry.player_id_ext,
            )
        except ValueError:
            continue
        if matched not in participants:
            continue
        delta = abs(entry.center - center)
        if best_delta is None or delta < best_delta:
            best_delta = delta
            best_idx = idx
            best_x = entry.x
        elif best_delta is not None and delta == best_delta:
            if best_x is None or entry.x > best_x:
                best_idx = idx
                best_x = entry.x
    if best_idx is not None:
        return pool.pop(best_idx)
    return _assign_nearest_winner(center, pool, tolerance)


def _pop_matching_winner_strict(
    center: float,
    participants: Sequence[Player],
    pool: List[WinnerEntry],
    tolerance: float,
) -> Optional[WinnerEntry]:
    """
    Variant of _pop_matching_winner that only returns labels that match participants.

    If no matching label is found, we leave the pool untouched so later matches
    can still use the remaining winner entries (useful when brackets have many
    BYEs or missing underlined labels).
    """
    best_idx: Optional[int] = None
    best_delta: Optional[float] = None
    best_x: Optional[float] = None
    for idx, entry in enumerate(pool):
        try:
            matched = _match_short_to_full(
                entry.short,
                entry.center,
                participants,
                entry.player_id_ext,
            )
        except ValueError:
            continue
        if matched not in participants:
            continue
        delta = abs(entry.center - center)
        if best_delta is None or delta < best_delta:
            best_delta = delta
            best_idx = idx
            best_x = entry.x
        elif best_delta is not None and delta == best_delta:
            if best_x is None or entry.x > best_x:
                best_idx = idx
                best_x = entry.x
    if best_idx is not None:
        return pool.pop(best_idx)
    return None


def _build_first_round(
    players: Sequence[Player],
    winners: Sequence[WinnerEntry],
    scores: Sequence[ScoreEntry],
    score_window: Tuple[Optional[float], Optional[float]],
    tree_size: int,
) -> Tuple[List[Match], List[ScoreEntry]]:
    score_min, score_max = score_window
    remaining_scores = list(scores)
    matches: List[Match] = []

    sorted_players = sorted(players, key=lambda p: p.center)
    sorted_winners = sorted(winners, key=lambda w: w.center)

    expected_matches = tree_size // 2
    if len(sorted_winners) < expected_matches:
        num_missing = expected_matches - len(sorted_winners)
        if len(sorted_winners) > 0:
            min_w = sorted_winners[0].center
            max_w = sorted_winners[-1].center
            min_p = sorted_players[0].center if sorted_players else min_w - 50
            max_p = sorted_players[-1].center if sorted_players else max_w + 50
            if len(sorted_winners) > 1:
                avg_delta = (max_w - min_w) / (len(sorted_winners) - 1)
            else:
                avg_delta = 20.0 if not sorted_players else (max_p - min_p) / (len(sorted_players) - 1)

            gaps = []
            gap_before = min_w - min_p
            if gap_before > avg_delta * 0.5:
                gaps.append(('before', gap_before, min_p, min_w))

            gap_after = max_p - max_w
            if gap_after > avg_delta * 0.5:
                gaps.append(('after', gap_after, max_w, max_p))

            for j in range(len(sorted_winners) - 1):
                delta = sorted_winners[j + 1].center - sorted_winners[j].center
                if delta > avg_delta * 1.5:
                    gaps.append(('between', delta, sorted_winners[j].center, sorted_winners[j + 1].center))

            gaps.sort(key=lambda g: -g[1])

            for k in range(min(num_missing, len(gaps))):
                pos_type, _, start, end = gaps[k]
                if pos_type == 'before':
                    insert_center = start
                elif pos_type == 'after':
                    insert_center = end
                else:
                    insert_center = (start + end) / 2
                virtual = WinnerEntry(short='', center=insert_center, x=sorted_winners[0].x if sorted_winners else 0, player_id_ext=None)
                sorted_winners.append(virtual)

        sorted_winners.sort(key=lambda w: w.center)

    used_indices: Set[int] = set()

    for idx, winner_entry in enumerate(sorted_winners):
        participants: List[Player] = []

        lower_bound = (
            (sorted_winners[idx - 1].center + winner_entry.center) / 2.0 if idx > 0 else float("-inf")
        )
        upper_bound = (
            (winner_entry.center + sorted_winners[idx + 1].center) / 2.0
            if idx + 1 < len(sorted_winners)
            else float("inf")
        )

        for p_idx, player in enumerate(sorted_players):
            if p_idx in used_indices:
                continue
            if player.center < lower_bound:
                continue
            if player.center >= upper_bound:
                if len(participants) >= 2:
                    break
                continue
            participants.append(player)
            used_indices.add(p_idx)
            if len(participants) == 2:
                break

        allow_rescue = not getattr(winner_entry, "is_double_wo", False)

        if len(participants) == 0 and allow_rescue:
            closests = sorted(
                (
                    (abs(player.center - winner_entry.center), p_idx, player)
                    for p_idx, player in enumerate(sorted_players)
                    if p_idx not in used_indices
                ),
                key=lambda tpl: tpl[0],
            )
            for _, p_idx, player in closests:
                if len(participants) == 2:
                    break
                participants.append(player)
                used_indices.add(p_idx)
        elif len(participants) == 1 and allow_rescue:
            closests = sorted(
                (
                    (abs(player.center - winner_entry.center), p_idx, player)
                    for p_idx, player in enumerate(sorted_players)
                    if p_idx not in used_indices
                ),
                key=lambda tpl: tpl[0],
            )
            for delta, p_idx, player in closests:
                if delta > 18.0:
                    break
                if not (lower_bound <= player.center < upper_bound):
                    continue
                participants.append(player)
                used_indices.add(p_idx)
                break

        if getattr(winner_entry, "is_double_wo", False):
            winner: Optional[Player] = None
            if len(participants) == 1:
                winner = participants[0]
            elif len(participants) >= 2:
                winner = None
            matches.append(
                Match(
                    players=list(participants),
                    winner=winner,
                    scores=None,
                    center=winner_entry.center,
                    walkover=True,
                )
            )
            continue

        winner: Optional[Player] = None
        try:
            winner = _match_short_to_full(
                winner_entry.short,
                winner_entry.center,
                participants or players,
                winner_entry.player_id_ext,
            )
        except ValueError:
            try:
                candidate = _match_short_to_full(
                    winner_entry.short,
                    winner_entry.center,
                    players,
                    winner_entry.player_id_ext,
                )
            except ValueError:
                candidate = None
            if candidate and candidate in participants:
                winner = candidate
            elif candidate and len(participants) == 1:
                winner = candidate
            elif candidate and len(participants) == 0:
                # Fill BYE/Dubbel-WO gaps by seeding the labeled player.
                participants.append(candidate)
                winner = candidate
            else:
                if winner_entry.short.strip():
                    _debug_print(f"Failed to match winner label {winner_entry.short!r} at y={winner_entry.center}")
                winner = None

        if winner is None and len(participants) == 1:
            winner = participants[0]

        match_scores: Optional[Tuple[int, ...]] = None
        if len(participants) >= 2 and winner_entry.short != '':
            target_x = None
            if score_min is not None and score_max is not None:
                target_x = (score_min + score_max) / 2.0
            match_scores = _pop_score_aligned(
                remaining_scores,
                winner_entry.center,
                tolerance=30.0,
                min_x=score_min,
                max_x=score_max,
                target_x=target_x,
            )
            if match_scores is None:
                match_scores = _assign_nearest_score(
                    winner_entry.center,
                    remaining_scores,
                    tolerance=30.0,
                )

        matches.append(
            Match(
                players=participants,
                winner=winner,
                scores=match_scores,
                center=winner_entry.center,
            )
        )

    matches.sort(key=lambda m: m.center)
    return matches, remaining_scores


def _build_next_round(
    previous_round: Sequence[Match],
    winners: Sequence[WinnerEntry],
    scores: List[ScoreEntry],
    players: Sequence[Player],
    score_window: Tuple[Optional[float], Optional[float]],
    winner_tolerance: float = 18.0,
    score_tolerance: float = 20.0,
    strict_participant_matching: bool = False,
) -> Tuple[List[Match], List[WinnerEntry], List[ScoreEntry]]:
    score_min, score_max = score_window
    remaining_scores = list(scores)
    remaining_winners = sorted(winners, key=lambda w: w.center)
    matches: List[Match] = []

    ordered_prev = sorted(previous_round, key=lambda m: m.center)
    pair_count = (len(ordered_prev) + 1) // 2

    def _advancer(match: Optional[Match]) -> Optional[Player]:
        if match is None:
            return None
        if getattr(match, "walkover", False) and match.winner is None:
            return None
        if match.winner:
            return match.winner
        if match.players:
            return match.players[0]
        return None

    for idx in range(pair_count):
        first_match = ordered_prev[2 * idx]
        second_match = ordered_prev[2 * idx + 1] if 2 * idx + 1 < len(ordered_prev) else None

        participants: List[Player] = []
        centers: List[float] = []

        primary = _advancer(first_match)
        if primary:
            participants.append(primary)
        if first_match.players:
            centers.append(first_match.center)

        if second_match:
            secondary = _advancer(second_match)
            if secondary:
                participants.append(secondary)
            if second_match.players:
                centers.append(second_match.center)

        if not participants:
            continue

        center = sum(centers) / len(centers) if centers else participants[0].center

        if strict_participant_matching:
            winner_entry = _pop_matching_winner_strict(center, participants, remaining_winners, winner_tolerance)
        else:
            winner_entry = _pop_matching_winner(center, participants, remaining_winners, winner_tolerance)
            if winner_entry is None and remaining_winners:
                winner_entry = min(remaining_winners, key=lambda w: abs(w.center - center))
                remaining_winners.remove(winner_entry)

        winner: Optional[Player] = None
        if winner_entry is not None:
            try:
                winner = _match_short_to_full(
                    winner_entry.short,
                    winner_entry.center,
                    participants,
                    winner_entry.player_id_ext,
                )
            except ValueError:
                try:
                    candidate = _match_short_to_full(
                        winner_entry.short,
                        winner_entry.center,
                        players,
                        winner_entry.player_id_ext,
                    )
                    if candidate in participants:
                        winner = candidate
                    elif len(participants) == 1:
                        winner = candidate
                    elif candidate and len(participants) == 0:
                        participants.append(candidate)
                        winner = candidate
                    else:
                        if DEBUG_OUTPUT:
                            _debug_print(f"Matched {winner_entry.short!r} to non-participant {candidate.full_name if candidate else 'None'}")
                        winner = None
                except ValueError:
                    winner = None

        if winner is None and participants and (winner_entry is None or not winner_entry.short.strip()):
            winner = participants[0]

        match_scores = _pop_score_aligned(
            remaining_scores,
            center,
            score_tolerance,
            min_x=score_min,
            max_x=score_max,
            target_x=(score_min + score_max) / 2.0 if score_min is not None and score_max is not None else None,
        )
        if match_scores is None:
            match_scores = _assign_nearest_score(center, remaining_scores, tolerance=score_tolerance)

        matches.append(Match(players=participants, winner=winner, scores=match_scores, center=center))

    matches.sort(key=lambda m: m.center)
    return matches, remaining_winners, remaining_scores


def _fill_missing_winners(previous_round: Sequence[Match], next_round: Sequence[Match]) -> None:
    advancing_players: List[Player] = []
    for match in next_round:
        advancing_players.extend(player for player in match.players if player)
        if match.winner and match.winner not in advancing_players:
            advancing_players.append(match.winner)

    advancing_keys = {_player_key(p) for p in advancing_players if p}

    for match in previous_round:
        if not match.players:
            continue
        if match.winner and _player_key(match.winner) in advancing_keys:
            continue
        for player in match.players:
            if _player_key(player) in advancing_keys:
                match.winner = player
                break
        else:
            # If exactly one participant appears in the next round and the current winner is not advancing, switch winner.
            present = [p for p in match.players if _player_key(p) in advancing_keys]
            if present and match.winner and _player_key(match.winner) not in advancing_keys and len(present) == 1:
                match.winner = present[0]


def _propagate_round_participants(all_rounds: Sequence[Sequence[Match]]) -> None:
    """Ensure each round's participants come from winners of the previous round."""
    for ridx in range(1, len(all_rounds)):
        prev_round = all_rounds[ridx - 1]
        curr_round = all_rounds[ridx]
        for midx, match in enumerate(curr_round):
            expected: List[Player] = []
            if 2 * midx < len(prev_round):
                winner = prev_round[2 * midx].winner
                if winner:
                    expected.append(winner)
            if 2 * midx + 1 < len(prev_round):
                winner = prev_round[2 * midx + 1].winner
                if winner:
                    expected.append(winner)
            if not expected:
                continue
            if len(match.players) != len(expected) or any(
                _player_key(a) != _player_key(b) for a, b in zip(match.players, expected)
            ):
                match.players = list(expected)
                if match.winner and _player_key(match.winner) not in {_player_key(p) for p in expected}:
                    match.winner = expected[0]


def _align_winners_to_advancers(all_rounds: Sequence[Sequence[Match]]) -> None:
    """
    If a match winner does not show up in the next round but an opponent does,
    flip the winner to the advancing player. Helps when winner labels were mis-attached.
    """
    for ridx in range(len(all_rounds) - 1):
        next_advancers = {_player_key(p) for m in all_rounds[ridx + 1] for p in m.players if p}
        for match in all_rounds[ridx]:
            if len(match.players) < 2 or not next_advancers:
                continue
            current_winner_key = _player_key(match.winner) if match.winner else None
            if current_winner_key in next_advancers:
                continue
            for p in match.players:
                if _player_key(p) in next_advancers:
                    match.winner = p
                    break


def _align_winners_to_future_advancers(all_rounds: Sequence[Sequence[Match]]) -> None:
    """
    If a match winner never appears in later rounds but an opponent does, flip to that opponent.
    This catches cases where winner labels were wrong but the bracket progression is visible.
    """
    total_rounds = len(all_rounds)
    if total_rounds <= 1:
        return
    for ridx in range(total_rounds - 1):
        future_players = {
            _player_key(p)
            for later_rounds in all_rounds[ridx + 1 :]
            for m in later_rounds
            for p in m.players
            if p
        }
        if not future_players:
            continue
        for match in all_rounds[ridx]:
            if len(match.players) < 2:
                continue
            winner_key = _player_key(match.winner) if match.winner else None
            if winner_key in future_players:
                continue
            for p in match.players:
                if _player_key(p) in future_players:
                    match.winner = p
                    break


def _run_structural_bracket_checks(
    rounds: Sequence[Sequence[Match]],
    *,
    logger: OperationLogger,
    logger_keys: Dict[str, str],
) -> None:
    """
    Post-parse structural sanity checks on the final bracket.

    The checks are intentionally simple and deterministic so they can be tweaked
    without touching the core parsing logic:
    - Round size continuity: current round winners must feed the next round.
    - Winner progression: every winner (except the final) must appear exactly once in the next round.
    - Appearance bounds: no player should appear more times than the number of rounds.
    - Walkover integrity: WO matches must not retain score tokens.
    """
    if not rounds:
        return

    total_rounds = len(rounds)

    # 1) Round size continuity
    for ridx in range(1, total_rounds):
        prev = rounds[ridx - 1]
        curr = rounds[ridx]
        expected = max(len(prev) // 2, 1)
        if len(curr) != expected:
            logger.warning(
                logger_keys.copy(),
                f"Structural check: round {ridx} has {len(curr)} matches, expected {expected}",
            )

    # 2) Winner progression
    for ridx in range(total_rounds - 1):
        winners = {_player_key(m.winner) for m in rounds[ridx] if m.winner}
        next_participants = {
            _player_key(p) for m in rounds[ridx + 1] for p in m.players if p
        }
        missing = winners - next_participants
        stray = next_participants - winners
        if missing:
            sample = ", ".join(name for _, name, *_ in list(missing)[:3])
            logger.warning(
                logger_keys.copy(),
                f"Structural check: winners missing in next round ({sample})",
            )
        if stray:
            sample = ", ".join(name for _, name, *_ in list(stray)[:3])
            logger.warning(
                logger_keys.copy(),
                f"Structural check: next round has participants without wins ({sample})",
            )

    # 3) Appearance bounds
    counts: Dict[Tuple[Optional[str], str, Optional[str], str], int] = {}
    for match in (m for r in rounds for m in r):
        for p in match.players:
            key = _player_key(p)
            counts[key] = counts.get(key, 0) + 1
    max_allowed = total_rounds
    offenders = [key[1] for key, count in counts.items() if count > max_allowed]
    if offenders:
        sample = ", ".join(offenders[:3])
        logger.warning(
            logger_keys.copy(),
            f"Structural check: player appears too many times ({sample})",
        )

    # 4) WO integrity
    for match in (m for r in rounds for m in r):
        if getattr(match, "walkover", False) and match.scores is not None:
            logger.warning(
                logger_keys.copy(),
                "Structural check: walkover match retains score tokens",
            )


def _strip_scores_from_walkovers(matches: Sequence[Match]) -> None:
    """Walkovers should not carry score tokens; drop any that slipped through."""
    for match in matches:
        if getattr(match, "walkover", False) and match.scores is not None:
            match.scores = None


def _force_walkover_for_unscored_small_bracket(
    rounds: Sequence[Sequence[Match]],
    *,
    tree_size: int,
    wo_tokens_present: bool,
) -> None:
    """
    For very small brackets, unscored two-player matches are almost always walkovers.
    If the PDF contains any WO tokens, mark such matches as WO to avoid missing-score noise.
    """
    if tree_size > 16 or not wo_tokens_present:
        return
    for ridx, round_matches in enumerate(rounds):
        if ridx != 0:
            continue
        for match in round_matches:
            if getattr(match, "walkover", False):
                continue
            if len(match.players) != 2:
                continue
            if match.scores is not None:
                continue
            match.walkover = True
            if match.winner is None:
                match.winner = match.players[0]
            if match.winner:
                forfeiter = match.players[1] if _player_key(match.winner) == _player_key(match.players[0]) else match.players[0]
                match.walkover_forfeiter = forfeiter


def _reassign_scores_small_bracket(
    rounds: Sequence[Sequence[Match]],
    scores: Sequence[ScoreEntry],
    *,
    tree_size: int,
    tolerance: float = 50.0,
) -> None:
    """
    For small trees, re-run a simple nearest-by-center score assignment after WOs.
    This can fix cases where early WO handling consumed scores that should belong
    to other matches (common in sparse/brackets with many BYEs).
    """
    if tree_size > 16 and len(scores) > 32:
        return
    pool = list(scores)
    pool.sort(key=lambda s: s.center)
    for matches in rounds:
        for match in matches:
            if getattr(match, "walkover", False) or len(match.players) != 2:
                continue
            assigned = _assign_nearest_score(match.center, pool, tolerance=tolerance)
            if assigned is not None:
                match.scores = assigned


def _apply_advancers_from_next_round(
    current_round: Sequence[Match],
    next_round_entries: Sequence[WinnerEntry],
    players: Sequence[Player],
) -> None:
    """
    Ensure winners in the current round reflect who appears in the next round column.

    This is especially helpful when multiple players share a name or when a PDF
    omits/merges winner labels, since the next-round entries represent the
    true advancers.
    """
    if not current_round or not next_round_entries:
        return

    for entry in next_round_entries:
        if not entry.short.strip():
            continue
        try:
            advancer = _match_short_to_full(
                entry.short,
                entry.center,
                players,
                entry.player_id_ext,
            )
        except ValueError:
            continue
        adv_key = _player_key(advancer)
        for match in current_round:
            for participant in match.players:
                if _player_key(participant) == adv_key:
                    if match.winner is None or _player_key(match.winner) != adv_key:
                        match.winner = participant
                    break
            else:
                continue
            break


def _ensure_winner_first(matches: Sequence[Match]) -> None:
    for match in matches:
        if match.winner is None or len(match.players) < 2:
            continue
        winner_key = _player_key(match.winner)
        for idx, player in enumerate(match.players):
            if _player_key(player) == winner_key:
                if idx == 0:
                    break
                player_to_front = match.players.pop(idx)
                match.players.insert(0, player_to_front)
                break


def _dedupe_final_rounds(
    all_rounds: List[List[Match]],
    score_pool: List[ScoreEntry],
    all_scores_page: Sequence[ScoreEntry],
) -> None:
    """
    Some layouts can produce two consecutive 1-match rounds (duplicate finals).
    Prefer the stronger final and merge any missing info (players/winner/scores),
    then drop the extra round. Guarded to avoid affecting normal layouts.
    """
    if len(all_rounds) < 3:
        return
    if len(all_rounds[-1]) != 1 or len(all_rounds[-2]) != 1:
        return

    primary = all_rounds[-2][0]
    duplicate = all_rounds[-1][0]

    def _score_tuple(match: Match) -> Tuple[int, int, int]:
        has_two_players = 1 if len(match.players) >= 2 else 0
        has_scores = 1 if match.scores is not None else 0
        has_winner = 1 if match.winner is not None else 0
        return (has_two_players, has_scores, has_winner)

    primary_score = _score_tuple(primary)
    duplicate_score = _score_tuple(duplicate)
    keep, drop = (primary, duplicate)
    if duplicate_score > primary_score:
        keep, drop = (duplicate, primary)

    # Merge missing data from the dropped final into the kept one.
    if len(keep.players) < 2 and len(drop.players) >= 2:
        keep.players = list(drop.players)
    if keep.winner is None and drop.winner is not None:
        keep.winner = drop.winner
    if keep.scores is None and drop.scores is not None:
        keep.scores = drop.scores
    if getattr(drop, "walkover", False) and not getattr(keep, "walkover", False):
        keep.walkover = drop.walkover
        keep.walkover_forfeiter = drop.walkover_forfeiter

    # If the surviving final still lacks scores, try to attach the rightmost leftover score token.
    if keep.scores is None and score_pool:
        candidate = max(score_pool, key=lambda s: (s.x, s.center))
        keep.scores = candidate.scores
        try:
            score_pool.remove(candidate)
        except ValueError:
            pass

    all_rounds[-2] = [keep]
    all_rounds.pop()


def _label_round(name: str, matches: Sequence[Match]) -> List[str]:
    lines: List[str] = [f"{name}:"]
    headers = [
        "P1 id",
        "P1 name",
        "P1 club",
        "VS",
        "P2 id",
        "P2 name",
        "P2 club",
        "Winner",
        "Tokens/BYE",
    ]
    rows: List[List[str]] = []

    def player_cells(player: Optional[Player]) -> Tuple[str, str, str]:
        if not player:
            return ("", "Unknown", "")
        pid = player.player_id_ext or ""
        name = player.full_name
        if player.player_suffix_id:
            name = f"{name} [{player.player_suffix_id}]"
        club = player.club or ""
        return (pid, name, club)

    for match in matches:
        wo_token = _walkover_token(match)
        left = match.players[0] if match.players else None
        right = match.players[1] if len(match.players) > 1 else None
        p1_id, p1_name, p1_club = player_cells(left)
        p2_id, p2_name, p2_club = player_cells(right)
        winner_text = _format_player(match.winner) if match.winner else "Unknown"
        tokens_text = ""

        if len(match.players) < 2 and match.players:
            tokens_text = "BYE"
            if wo_token:
                tokens_text = f"{tokens_text} ({wo_token})"
            winner_text = _format_player(match.players[0])
            p2_id = p2_name = p2_club = ""
        elif len(match.players) < 2:
            tokens_text = "Unknown"
        else:
            if wo_token:
                tokens_text = wo_token
            elif match.scores is not None:
                csv = _scores_to_csv(match.scores)
                tokens_text = f"({csv})" if csv else ""

        rows.append(
            [
                p1_id,
                p1_name,
                p1_club,
                "vs",
                p2_id,
                p2_name,
                p2_club,
                winner_text,
                tokens_text,
            ]
        )

    if not rows:
        return lines

    col_widths = [
        max(len(header), max(len(row[idx]) for row in rows))
        for idx, header in enumerate(headers)
    ]
    header_line = " | ".join(header.ljust(col_widths[idx]) for idx, header in enumerate(headers))
    separator = "-+-".join("-" * col_widths[idx] for idx in range(len(headers)))
    lines.append(separator)
    lines.append(header_line)
    lines.append(separator)
    for row in rows:
        lines.append(" | ".join(row[idx].ljust(col_widths[idx]) for idx in range(len(headers))))
    lines.append(separator)
    return lines


def _cluster_columns(xs: List[float], max_gap: float = 25.0) -> List[Tuple[float, float]]:
    if not xs:
        return []
    xs = sorted(xs)
    bands: List[Tuple[float, float]] = []
    start = prev = xs[0]
    for x in xs[1:]:
        if x - prev > max_gap:
            bands.append((start, prev))
            start = x
        prev = x
    bands.append((start, prev))
    return bands


def _expected_winner_counts(tree_size: int) -> List[int]:
    counts: List[int] = []
    size = max(tree_size, 0)
    while size >= 2:
        size //= 2
        counts.append(size)
    return counts


def _allocate_winners_by_round(
    all_winners_page: Sequence[WinnerEntry],
    winner_bands_page: Sequence[Tuple[float, float]],
    tree_size: int,
    use_alignment: bool = True,
) -> List[List[WinnerEntry]]:
    """Merge/split winner columns to match expected round sizes for large brackets."""
    band_entries: List[List[WinnerEntry]] = []
    for band in winner_bands_page:
        chunk = [w for w in all_winners_page if band[0] <= w.x <= band[1]]
        chunk.sort(key=lambda w: w.center)
        if chunk:
            band_entries.append(chunk)

    winners_by_round: List[List[WinnerEntry]] = []
    band_idx = 0
    for expected in _expected_winner_counts(tree_size):
        current: List[WinnerEntry] = []
        while len(current) < expected and band_idx < len(band_entries):
            entries = band_entries[band_idx]
            need = expected - len(current)
            if len(entries) >= expected and winners_by_round and (use_alignment or len(entries) > expected):
                targets: List[float] = []
                prev_round = winners_by_round[-1]
                for i in range(0, len(prev_round), 2):
                    pair = prev_round[i:i + 2]
                    if len(pair) == 2:
                        targets.append((pair[0].center + pair[1].center) / 2.0)
                if targets:
                    candidates = list(entries)
                    chosen: List[WinnerEntry] = []
                    for center in targets[:expected]:
                        if not candidates:
                            break
                        best = min(candidates, key=lambda w: abs(w.center - center))
                        chosen.append(best)
                        candidates.remove(best)
                    if len(chosen) == expected:
                        chosen.sort(key=lambda w: w.center)
                        current.extend(chosen)
                        band_entries[band_idx] = candidates
                        continue
            if len(entries) <= need:
                current.extend(entries)
                band_idx += 1
            else:
                current.extend(entries[:need])
                band_entries[band_idx] = entries[need:]
        winners_by_round.append(current)
    return winners_by_round


def _deduplicate_winner_entries(entries: Sequence[WinnerEntry], center_tolerance: float = 6.0) -> List[WinnerEntry]:
    deduped: List[WinnerEntry] = []
    for entry in entries:
        if any(
            existing.short == entry.short and abs(existing.center - entry.center) < center_tolerance
            for existing in deduped
        ):
            continue
        deduped.append(entry)
    return deduped


def _filter_winners_to_player_band(
    winners: Sequence[WinnerEntry],
    players: Sequence[Player],
    *,
    margin_factor: float = 0.75,
    min_margin: float = 8.0,
    max_margin: float = 28.0,
) -> List[WinnerEntry]:
    """
    Drop winner labels that sit far above or below the player column (titles/footers).
    This guards against page headers being misinterpreted as winners (e.g. "Knock-Out Stage").
    """
    if not winners or not players:
        return list(winners)

    centers = sorted(p.center for p in players)
    gaps = [
        centers[idx + 1] - centers[idx]
        for idx in range(len(centers) - 1)
        if centers[idx + 1] > centers[idx]
    ]
    if gaps:
        typical_gap = sorted(gaps)[len(gaps) // 2]
    elif len(centers) > 1:
        typical_gap = abs(centers[-1] - centers[0]) / max(len(centers) - 1, 1)
    else:
        typical_gap = 18.0

    margin = typical_gap * margin_factor
    margin = max(min_margin, min(max_margin, margin))

    lower = centers[0] - margin
    upper = centers[-1] + margin
    return [w for w in winners if lower <= w.center <= upper]


def _deduplicate_players(players: Sequence[Player]) -> List[Player]:
    seen: Set[Tuple[Optional[str], str, Optional[str], str]] = set()
    out: List[Player] = []
    for p in players:
        key = _player_key(p)
        if key in seen:
            continue
        seen.add(key)
        out.append(p)
    out.sort(key=lambda p: p.center)
    return out


_WO_WORD_RE = re.compile(r"^\s*wo\b", re.IGNORECASE)


def _filter_wo_words(words: Sequence[dict]) -> List[dict]:
    """Return only explicit WO tokens, ignoring unrelated substrings like 'Harwood'."""
    filtered: List[dict] = []
    for w in words:
        txt = w.get("text", "")
        if not txt:
            continue
        if _WO_WORD_RE.match(txt):
            filtered.append(w)
    return filtered


def _walkover_bounds_for_match(match: Match) -> Tuple[Optional[float], Optional[float]]:
    xs = []
    xends = []
    for p in match.players:
        if p.x is not None:
            xs.append(p.x)
        if getattr(p, "x1", None) is not None:
            xends.append(float(p.x1))  # type: ignore[arg-type]
    if not xs and not xends:
        return None, None
    match_min = min(xs) if xs else None
    match_max = max(xends) if xends else (max(xs) if xs else None)
    return match_min, match_max


def _nearest_band_index(bands: Sequence[Tuple[float, float]], x_center: float) -> Optional[int]:
    if not bands:
        return None
    best_idx = None
    best_delta = None
    for idx, (start, stop) in enumerate(bands):
        band_center = (start + stop) / 2.0
        delta = abs(x_center - band_center)
        if best_delta is None or delta < best_delta:
            best_delta = delta
            best_idx = idx
    return best_idx


def _apply_walkovers_from_words(
    rounds: Sequence[Sequence[Match]],
    wo_words: Sequence[dict],
    players: Sequence[Player],
    *,
    y_tolerance: float = 18.0,
    x_tolerance: float = 320.0,
    override_scored: bool = False,
    scores_hint: Optional[Sequence[ScoreEntry]] = None,
    score_pool: Optional[List[ScoreEntry]] = None,
    score_bands: Optional[Sequence[Tuple[float, float]]] = None,
    round_offset: int = 0,
) -> None:
    """
    Attach WO markers found anywhere on the page to the nearest matches.
    If the WO text includes a player id ext, we use it to set the winner/forfeiter.
    """
    if not wo_words:
        return
    flat_matches: List[Tuple[int, Match, Tuple[Optional[float], Optional[float]]]] = []
    for ridx, r in enumerate(rounds):
        for m in r:
            bounds = _walkover_bounds_for_match(m)
            flat_matches.append((ridx, m, bounds))

    centers = sorted(m.center for _, m, _ in flat_matches if m.players)
    y_spacing = None
    if len(centers) >= 2:
        gaps = [b - a for a, b in zip(centers, centers[1:]) if b > a]
        if gaps:
            gaps.sort()
            y_spacing = gaps[len(gaps) // 2]
    dynamic_y_tol = y_tolerance
    if y_spacing is not None:
        dynamic_y_tol = max(y_tolerance, min(28.0, y_spacing * 1.2))

    for w in wo_words:
        txt = w.get("text", "")
        y = (float(w.get("top", 0)) + float(w.get("bottom", 0))) / 2.0
        x0 = float(w.get("x0", 0.0))
        x1 = float(w.get("x1", x0))
        x_center = (x0 + x1) / 2.0
        target_round = _nearest_band_index(score_bands, x_center) if score_bands is not None else None
        if target_round is not None:
            target_round += round_offset
        pid = None
        m_pid = re.search(r"\b(\d{1,3})\b", txt)
        if m_pid:
            pid = m_pid.group(1)
        txt_lower = txt.strip().lower()
        allow_override_scored = override_scored or txt_lower.startswith("wo")

        def _attempt(target: Optional[int]) -> Optional[Match]:
            best_match = None
            best_delta = None
            for ridx, match, bounds in flat_matches:
                if target is not None and ridx != target:
                    continue
                if not match.players or len(match.players) < 2:
                    continue
                if match.scores is not None and pid is None and not allow_override_scored:
                    # Do not override scored matches unless the WO explicitly names a player id.
                    continue
                delta_y = abs(match.center - y)
                if delta_y > dynamic_y_tol:
                    continue
                if scores_hint and pid is None and match.scores is None and not allow_override_scored:
                    left, right = bounds
                    nearby_scores = 0
                    for entry in scores_hint:
                        if abs(entry.center - match.center) > dynamic_y_tol:
                            continue
                        if left is not None and right is not None:
                            if entry.x < left - x_tolerance or entry.x > right + x_tolerance:
                                continue
                        nearby_scores += 1
                    if nearby_scores >= 2 and txt_lower != "wo":
                        return None  # Defer to score tokens to avoid mis-tagging.
                left, right = bounds
                if left is not None and right is not None:
                    if x0 < left - x_tolerance and x1 < left - x_tolerance:
                        continue
                    if x0 > right + x_tolerance and x1 > right + x_tolerance:
                        continue
                if best_match is None or delta_y < best_delta:  # type: ignore[operator-not-supported]
                    best_match = match
                    best_delta = delta_y
            return best_match

        best = _attempt(target_round)
        if best is None and target_round is not None:
            # Fallback: retry without round gating (WO labels sometimes sit between score columns).
            best = _attempt(None)
        if not best:
            continue
        if best.scores is not None and not allow_override_scored and not pid:
            continue
        best.walkover = True
        if best.scores is not None and score_pool is not None:
            score_pool.append(ScoreEntry(scores=best.scores, center=best.center, x=x_center))
        best.scores = None
        if pid and len(best.players) == 2:
            winner = None
            for p in best.players:
                if p.player_id_ext == pid:
                    winner = p
                    break
            if winner:
                best.winner = winner
                forfeiter = best.players[1] if _player_key(winner) == _player_key(best.players[0]) else best.players[0]
                best.walkover_forfeiter = forfeiter


def _apply_walkovers_to_rounds(
    rounds: Sequence[Sequence[Match]],
    wo_markers: Sequence[Tuple[float, float, float]],
    *,
    y_tolerance: float = 18.0,
    x_tolerance: float = 320.0,
    scores_hint: Optional[Sequence[ScoreEntry]] = None,
    override_scored: bool = False,
    score_pool: Optional[List[ScoreEntry]] = None,
    score_bands: Optional[Sequence[Tuple[float, float]]] = None,
    round_offset: int = 0,
) -> None:
    if not wo_markers:
        return
    flat_matches: List[Tuple[int, Match, Tuple[Optional[float], Optional[float]]]] = []
    for ridx, r in enumerate(rounds):
        for m in r:
            flat_matches.append((ridx, m, _walkover_bounds_for_match(m)))

    centers = sorted(m.center for _, m, _ in flat_matches if m.players)
    y_spacing = None
    if len(centers) >= 2:
        gaps = [b - a for a, b in zip(centers, centers[1:]) if b > a]
        if gaps:
            gaps.sort()
            y_spacing = gaps[len(gaps) // 2]
    dynamic_y_tol = y_tolerance
    if y_spacing is not None:
        dynamic_y_tol = max(y_tolerance, min(28.0, y_spacing * 1.2))

    for marker_y, marker_x0, marker_x1 in wo_markers:
        x_center = (marker_x0 + marker_x1) / 2.0
        target_round = _nearest_band_index(score_bands, x_center) if score_bands is not None else None
        if target_round is not None:
            target_round += round_offset
        best = None
        best_delta = None
        skip_due_to_scores = False
        def _scan(target: Optional[int]) -> Tuple[Optional[Match], bool, Optional[float]]:
            local_best = None
            local_delta = None
            local_skip = False
            for ridx, match, bounds in flat_matches:
                if target is not None and ridx != target:
                    continue
                if not match.players or len(match.players) < 2:
                    continue
                if match.scores is not None:
                    # Do not override scored matches using bare WO markers.
                    continue
                delta_y = abs(match.center - marker_y)
                if delta_y > dynamic_y_tol:
                    continue
                if scores_hint and match.scores is None and not override_scored:
                    left, right = bounds
                    nearby_scores = 0
                    for entry in scores_hint:
                        if abs(entry.center - match.center) > dynamic_y_tol:
                            continue
                        if left is not None and right is not None:
                            if entry.x < left - x_tolerance or entry.x > right + x_tolerance:
                                continue
                        nearby_scores += 1
                    if nearby_scores >= 2:
                        local_skip = True
                        break
                if match.scores is not None and not override_scored:
                    continue
                left, right = bounds
                if left is not None and right is not None:
                    if marker_x0 < left - x_tolerance and marker_x1 < left - x_tolerance:
                        continue
                    if marker_x0 > right + x_tolerance and marker_x1 > right + x_tolerance:
                        continue
                if local_best is None or (local_delta is not None and delta_y < local_delta) or local_delta is None:
                    local_best = match
                    local_delta = delta_y
            return local_best, local_skip, local_delta

        best, skip_due_to_scores, best_delta = _scan(target_round)
        if best is None and not skip_due_to_scores and target_round is not None:
            best, skip_due_to_scores, best_delta = _scan(None)
        if skip_due_to_scores or best is None:
            continue
        best.walkover = True
        best.scores = None
        if best.scores is not None and score_pool is not None:
            score_pool.append(ScoreEntry(scores=best.scores, center=best.center, x=(marker_x0 + marker_x1) / 2.0))
        if best.winner is None and best.players:
            best.winner = best.players[0]
        if best.winner and len(best.players) == 2:
            forfeiter = best.players[1] if _player_key(best.winner) == _player_key(best.players[0]) else best.players[0]
            best.walkover_forfeiter = forfeiter


def _find_closest_score_band(score_bands: List[Tuple[float, float]], winner_band: Tuple[float, float]) -> Tuple[float, float]:
    candidates = [sb for sb in score_bands if sb[0] > winner_band[0]]
    if not candidates:
        return (winner_band[1], winner_band[1] + 50)
    return min(candidates, key=lambda sb: sb[0] - winner_band[1])


def _round_display_name(match_count: int) -> str:
    if match_count <= 0:
        return "Round"
    size = match_count * 2
    if match_count == 1:
        return "Final"
    if size > 8:
        return f"RO{size}"
    if size == 8:
        return "RO8/QF"
    if size == 4:
        return "RO4/SF"
    if size == 2:
        return "RO2/Final"
    return "Round"


_STAGE_ID_BY_MATCH_COUNT = {
    64: 2,
    32: 3,
    16: 4,
    8: 5,
    4: 6,
    2: 7,
    1: 8,
}

_DEFAULT_KO_STAGE_IDS = {2, 3, 4, 5, 6, 7, 8, 10}


def _stage_id_for_match_count(match_count: int) -> Optional[int]:
    if match_count <= 0:
        return None
    return _STAGE_ID_BY_MATCH_COUNT.get(match_count)


def _scores_to_csv(scores: Optional[Tuple[int, ...]]) -> Optional[str]:
    if not scores:
        return None
    return ", ".join(str(value) for value in scores)


def _walkover_token(match: Match) -> Optional[str]:
    if not getattr(match, "walkover", False):
        return None
    side = None
    forfeiter = getattr(match, "walkover_forfeiter", None)
    if forfeiter and match.players:
        if _player_key(forfeiter) == _player_key(match.players[0]):
            side = 1
        elif len(match.players) > 1 and _player_key(forfeiter) == _player_key(match.players[1]):
            side = 2
    return f"WO:S{side}" if side else "WO"


def _format_name_with_club(player: Player) -> str:
    club = player.club.strip() if player.club else None
    return player.full_name if not club else f"{player.full_name}, {club}"


def _insert_matches_for_stage(
    cursor,
    matches: Sequence[Match],
    stage_id: int,
    tid_ext: Optional[str],
    cid_ext: Optional[str],
    *,
    logger: OperationLogger,
    logger_keys: Dict[str, str],
) -> Tuple[int, int, int]:
    seen = inserted = skipped = 0
    for match in matches:
        if len(match.players) < 2:
            continue

        seen += 1
        p1, p2 = match.players[0], match.players[1]
        tokens_csv = _scores_to_csv(match.scores)
        best_of = _infer_best_of_from_scores(match.scores or ())

        if getattr(match, "walkover", False):
            side = None
            forfeiter = getattr(match, "walkover_forfeiter", None)
            if forfeiter:
                if _player_key(forfeiter) == _player_key(p1):
                    side = 1
                elif _player_key(forfeiter) == _player_key(p2):
                    side = 2
            tokens_csv = f"WO:S{side}" if side else "WO"
            best_of = None

        raw_line_text = (
            f"{(p1.player_id_ext or '').strip()} {_format_name_with_club(p1)} - "
            f"{(p2.player_id_ext or '').strip()} {_format_name_with_club(p2)}"
        ).strip()
        if tokens_csv:
            raw_line_text = f"{raw_line_text} {tokens_csv}".strip()

        row = TournamentClassMatchRaw(
            tournament_id_ext=tid_ext or "",
            tournament_class_id_ext=cid_ext or "",
            group_id_ext=None,
            match_id_ext=None,
            s1_player_id_ext=p1.player_id_ext,
            s2_player_id_ext=p2.player_id_ext,
            s1_fullname_raw=p1.full_name,
            s2_fullname_raw=p2.full_name,
            s1_clubname_raw=p1.club,
            s2_clubname_raw=p2.club,
            game_point_tokens=tokens_csv or None,
            best_of=best_of,
            raw_line_text=raw_line_text or None,
            tournament_class_stage_id=stage_id,
            data_source_id=1,
        )

        match_keys = logger_keys.copy()
        match_keys.update({
            "match_id_ext": row.match_id_ext or "None",
            "round_stage_id": str(stage_id),
        })

        is_valid, error_message = row.validate()
        if not is_valid:
            skipped += 1
            logger.failed(match_keys, f"Validation failed: {error_message}")
            continue

        try:
            row.compute_hash()
            row.insert(cursor)
            inserted += 1
            logger.success(match_keys, "Raw KO match saved")
            if hasattr(logger, "inc_processed"):
                logger.inc_processed()
        except Exception as exc:
            skipped += 1
            logger.failed(match_keys, f"Insert failed: {exc}")

    return seen, inserted, skipped


def _infer_best_of_from_scores(scores: Sequence[int]) -> Optional[int]:
    if not scores:
        return None
    p1_wins = sum(1 for value in scores if value >= 0)
    p2_wins = sum(1 for value in scores if value < 0)
    wins = max(p1_wins, p2_wins)
    if wins == 0:
        return None
    return 2 * wins - 1


def _validate_bracket(
    pdf_key: str,
    tclass: TournamentClass,
    rounds: Sequence[List[Match]],
    players: Sequence[Player],
    tree_size: int,
    score_entries_remaining: Sequence[ScoreEntry],
    fallback_tree_size_used: bool,
    qualification: Sequence[Match],
    *,
    logger: OperationLogger,
    logger_keys: Dict[str, str],
) -> None:
    if not rounds:
        logger.warning(logger_keys.copy(), "No rounds parsed for this class; skipping validation")
        return

    pdf_hash = LAST_PDF_HASHES.get(pdf_key)
    if pdf_hash:
        # logger.info(logger_keys.copy(), f"PDF hash: {pdf_hash}")
        pass
    if fallback_tree_size_used:
        logger.info(logger_keys.copy(), "ko_tree_size missing – inferred from winner labels")

    for idx, matches in enumerate(rounds):
        expected = max(tree_size // (2 ** (idx + 1)), 1)
        actual = len(matches)
        if actual != expected:
            msg = f"Expected {expected} matches in {_round_display_name(actual)} (round {idx}), parsed {actual}."
            logger_keys.update({
                "misc_validation_issues": msg
            })
            logger.warning(
                logger_keys.copy(),
                "Mismatch in expected vs parsed match count in round"
            )

    seen_keys: Set[Tuple[Optional[str], str]] = set()
    duplicates: List[str] = []
    for match in rounds[0]:
        if len(match.players) < 2:
            continue
        for player in match.players:
            key = _player_key(player)
            if key in seen_keys:
                duplicates.append(player.full_name)
            else:
                seen_keys.add(key)
    if duplicates:
        sample = ", ".join(sorted(set(duplicates))[:5])
        logger_keys.update({"duplicate_players_in_first_round": sample})
        logger.warning(logger_keys.copy(), "Duplicate player entries detected in first round")

    used_keys = {
        _player_key(p)
        for round_matches in rounds
        for match in round_matches
        for p in match.players
        if p
    }
    used_keys.update(
        {
            _player_key(p)
            for match in qualification
            for p in match.players
            if p
        }
    )
    missing_players = [p.full_name for p in players if _player_key(p) not in used_keys]
    if missing_players:
        sample = ", ".join(sorted(missing_players)[:5])
        logger_keys.update({"missing_players": sample})
        logger.warning(
            logger_keys.copy(),
            "One or more player(s) from the left column never appear in the bracket",
        )

    for idx, matches in enumerate(rounds):
        round_name = _round_display_name(len(matches))
        best_of_values: Set[int] = set()
        for match in matches:
            if len(match.players) == 2 and match.players[0] != match.players[1]:
                if match.scores is None and not getattr(match, "walkover", False):
                    logger_keys.update({"missing_score_for_match": f"{_format_name_with_club(match.players[0])} vs {_format_name_with_club(match.players[1])}"})
                    logger.warning(
                        logger_keys.copy(),
                        "Missing score for match",
                    )
                else:
                    inferred = _infer_best_of_from_scores(match.scores)
                    if inferred is not None:
                        best_of_values.add(inferred)
            if len(match.players) == 2 and match.winner is None:
                logger_keys.update({"round_name": round_name, "players": f"{match.players[0].full_name} vs {match.players[1].full_name}"})
                logger.warning(
                    logger_keys.copy(), "Missing winner for match",
                )
        if len(best_of_values) > 1:
            logger_keys.update({"inconsistent_best_of_in_round": f"{round_name}: {sorted(best_of_values)}"})
            logger.warning(logger_keys.copy(), "Inconsistent best-of detected in round")

    for idx in range(len(rounds) - 1):
        current = rounds[idx]
        nxt = rounds[idx + 1]
        if not current or not nxt:
            continue
        next_is_final = len(nxt) == 1
        winner_keys = {_player_key(match.winner) for match in current if match.winner}
        next_participants = {_player_key(p) for match in nxt for p in match.players if p}
        missing_advancers = winner_keys - next_participants
        if missing_advancers and not next_is_final:
            sample = ", ".join(sorted({name for _, name in missing_advancers})[:4])
            logger.warning(logger_keys.copy(), f"Winners not found in next round: {sample}")

        stray_participants = {key for key in next_participants if key not in winner_keys}
        if stray_participants and not next_is_final:
            sample = ", ".join(sorted({name for _, name in stray_participants})[:4])
            logger.warning(
                logger_keys.copy(),
                f"Participants in next round without recorded wins: {sample}",
            )

    remaining_scores = list(score_entries_remaining)
    for qual_match in qualification:
        if qual_match.scores is None:
            continue
        for idx, entry in enumerate(remaining_scores):
            if entry.scores == qual_match.scores:
                remaining_scores.pop(idx)
                break
    # If all non-WO matches already have scores, suppress leftover-token warning (common in split layouts).
    all_scored = True
    for round_matches in rounds:
        for match in round_matches:
            if getattr(match, "walkover", False):
                continue
            if len(match.players) == 2 and match.scores is None:
                all_scored = False
                break
        if not all_scored:
            break
    if remaining_scores and not all_scored:
        examples = ", ".join(
            f"x={round(entry.x, 1)} y={round(entry.center, 1)} {entry.scores}"
            for entry in remaining_scores[:3]
        )
        logger_keys.update({"tokens_not_attached": examples})
        logger.warning(
            logger_keys.copy(),
            "One or more score token(s) were not attached to any match.",
        )

QUAL_HEADER_RE = re.compile(
    r"\b("
    r"kval(?:matcher|spel|ifikation|ificering|ifisering)?"
    r"|återkval"
    r"|qualification(?:\s*round)?"
    r"|qualifying"
    r"|vinderen er understreget"
    r")\b",
    re.IGNORECASE,
)


def _find_qualification_header(words: Sequence[dict]) -> Optional[dict]:
    candidates = []
    page_height = max((float(w.get("bottom", 0)) for w in words), default=0.0)
    page_width = max((float(w.get("x1", 0)) for w in words), default=0.0)
    upper_exclusion = page_height * 0.35 if page_height else 180.0
    right_exclusion_x = page_width * 0.6 if page_width else 350.0

    # Estimate mid of player labels to prefer headers below the bracket
    player_centers = []
    for w in words:
        txt = w.get("text", "")
        if "," in txt and re.search(r"[A-Za-zÅÄÖåäö]", txt):
            player_centers.append((float(w.get("top", 0)) + float(w.get("bottom", 0))) / 2)
    player_mid_y = (sum(player_centers) / len(player_centers)) if player_centers else upper_exclusion
    for w in words:
        txt = w.get("text", "").replace(" ", " ").strip()
        if not txt:
            continue
        if QUAL_HEADER_RE.search(txt):
            center_y = (float(w.get("top", 0)) + float(w.get("bottom", 0))) / 2
            x0 = float(w.get("x0", 0))
            # Avoid the class title area (top-right)
            if center_y < upper_exclusion and x0 > right_exclusion_x:
                continue
            # Prefer headers below the main bracket body
            if center_y < player_mid_y * 0.9:
                if len(player_centers) >= 15 and "qual" in txt.lower():
                    pass
                elif len(player_centers) >= 15:
                    continue
            size = float(w.get("size", 0)) if "size" in w else 0.0
            candidates.append((center_y, -size, w))
    if not candidates:
        return None
    # Pick the lowest header (highest center_y), with tie-break on font size
    candidates.sort(key=lambda t: (-t[0], t[1]))
    return candidates[0][2]


def _extract_player_like_in_band(words: Sequence[dict], y_min: float, y_max: float) -> List[Player]:
    out: List[Player] = []
    for word in words:
        y = (float(word["top"]) + float(word["bottom"])) / 2
        if not (y_min <= y <= y_max):
            continue
        txt = word.get("text", "")
        if "," not in txt:
            continue
        left = txt.split(",", 1)[0]
        if not re.search(r"[A-Za-zÅÄÖåäö]", left):
            continue
        if ":" in left:
            continue
        cleaned = _strip_draw_prefix(txt)
        m = re.match(
            r"\s*(?:(\d{1,3})\s+)?([^,(]+?(?:\s+[^,(]+?)*)(?:\s*\(([^)]+)\))?,\s*(.+)",
            cleaned,
        )
        if not m:
            continue
        player_id_ext, raw_name, player_suffix_id, raw_club = m.groups()
        full_name = unicodedata.normalize('NFC', raw_name.strip())
        out.append(
            Player(
                full_name=full_name,
                club=unicodedata.normalize('NFC', raw_club.strip()),
                short=_make_short(full_name),
                center=y,
                x=float(word.get("x0", 0.0)),
                x1=float(word.get("x1", word.get("x0", 0.0))),
                player_id_ext=player_id_ext.strip() if player_id_ext else None,
                player_suffix_id=player_suffix_id.strip() if player_suffix_id else None,
            )
        )
    out.sort(key=lambda p: p.center)
    return out


def _assign_qualification_winners_by_presence(qualification: List[Match], ko_rounds: Sequence[Match]) -> None:
    ko_by_id = {p.player_id_ext for m in ko_rounds for p in m.players if p and p.player_id_ext}
    ko_by_name = {p.full_name for m in ko_rounds for p in m.players if p}
    ko_by_short = {p.short for m in ko_rounds for p in m.players if p}
    for m in qualification:
        if len(m.players) != 2:
            continue
        a, b = m.players
        a_in = (
            (a.player_id_ext and a.player_id_ext in ko_by_id)
            or (a.full_name in ko_by_name)
            or (a.short in ko_by_short)
        )
        b_in = (
            (b.player_id_ext and b.player_id_ext in ko_by_id)
            or (b.full_name in ko_by_name)
            or (b.short in ko_by_short)
        )
        if a_in and not b_in:
            m.winner = a
        elif b_in and not a_in:
            m.winner = b


def _assign_winner_from_scores(match: Match) -> None:
    """Fill winner based on signed score tokens if no winner is set."""
    if match.winner or match.scores is None or len(match.players) != 2:
        return
    p1_wins = sum(1 for value in match.scores if value >= 0)
    p2_wins = sum(1 for value in match.scores if value < 0)
    if p1_wins > p2_wins:
        match.winner = match.players[0]
    elif p2_wins > p1_wins:
        match.winner = match.players[1]


def _build_double_wo_bracket(
    players: Sequence[Player],
    winners: List[WinnerEntry],
    scores: List[ScoreEntry],
    wo_markers: Optional[List[Tuple[float, float, float]]] = None,
) -> Tuple[List[List[Match]], List[ScoreEntry]]:
    """
    Handle tiny brackets that include an explicit Dubbel-WO marker.

    Format notes:
    - Typical case: 3 players, Dubbel-WO in the empty quarter. We create a semi (p0 vs p1, WO if marked) and a final vs p2.
    - Degenerate case: 2 players + Dubbel-WO + WO tokens in the final column. We collapse to a single final (no synthetic semi).
    """
    if len(players) < 2:
        return [], scores

    sorted_players = sorted(players, key=lambda p: p.center)
    score_pool = list(scores)
    winners_pool = list(winners)
    markers_pool = list(wo_markers or [])

    def pop_score(center: float, tol: float = 80.0) -> Optional[Tuple[int, ...]]:
        return _assign_nearest_score(center, score_pool, tolerance=tol)

    def pop_winner(center: float, participants: Sequence[Player], tol: float = 80.0) -> Optional[Player]:
        entry = _assign_nearest_winner(center, winners_pool, tolerance=tol)
        if entry is None:
            return None
        try:
            return _match_short_to_full(entry.short, entry.center, participants, entry.player_id_ext)
        except ValueError:
            return None

    def pop_wo(center: float, tol_y: float = 30.0) -> bool:
        return _is_walkover(center, markers_pool, match_bounds=None, y_tolerance=tol_y, x_tolerance=1e9)

    # Degenerate: only two players – treat as a straight final (ignore Dubbel-WO columns)
    if len(sorted_players) == 2:
        p1, p2 = sorted_players
        center = (p1.center + p2.center) / 2.0
        winner = pop_winner(center, (p1, p2))
        scores_final = pop_score(center)
        match = Match(players=[p1, p2], winner=winner or p1, scores=scores_final, center=center)
        return [[match]], score_pool

    semi_players = sorted_players[:2]
    semi_center = sum(p.center for p in semi_players) / len(semi_players)
    semi_winner = pop_winner(semi_center, semi_players)
    semi_walkover = pop_wo(semi_center)
    semi_scores: Optional[Tuple[int, ...]] = None
    if not semi_walkover:
        semi_scores = pop_score(semi_center)
    if semi_winner is None and semi_players:
        semi_winner = semi_players[0]
    semi_match = Match(
        players=list(semi_players),
        winner=semi_winner,
        scores=semi_scores,
        center=semi_center,
        walkover=semi_walkover,
    )
    if semi_walkover and len(semi_match.players) == 2:
        forfeiter = semi_match.players[1] if _player_key(semi_winner) == _player_key(semi_match.players[0]) else semi_match.players[0]
        semi_match.walkover_forfeiter = forfeiter

    bye_player = sorted_players[-1]
    bye_match = Match(players=[bye_player], winner=bye_player, scores=None, center=bye_player.center, walkover=False)

    final_participants = [semi_match.winner or semi_players[0], bye_player]
    final_center = (final_participants[0].center + final_participants[1].center) / 2.0
    final_walkover = False
    final_winner = pop_winner(final_center, final_participants)
    final_scores = pop_score(final_center)
    if final_winner is None:
        final_winner = final_participants[0]
    final_match = Match(
        players=final_participants,
        winner=final_winner,
        scores=final_scores,
        center=final_center,
        walkover=final_walkover,
    )

    return [[semi_match, bye_match], [final_match]], score_pool


def _extract_wo_markers(words: Sequence[dict], y_min: float, y_max: float) -> List[Tuple[float, float, float]]:
    markers: List[Tuple[float, float, float]] = []
    wo_re = re.compile(r"^w[o0]$", re.IGNORECASE)
    for w in words:
        y = (float(w.get("top", 0)) + float(w.get("bottom", 0))) / 2
        if not (y_min <= y <= y_max):
            continue
        txt = w.get("text", "").strip().lower()
        if wo_re.match(txt):
            x0 = float(w.get("x0", 0.0))
            x1 = float(w.get("x1", x0))
            markers.append((y, x0, x1))
    return markers


def _qualification_match_bounds(a: Player, b: Player) -> Tuple[Optional[float], Optional[float]]:
    xs = [val for val in (a.x, b.x) if val is not None]
    x_ends: List[float] = []
    for player in (a, b):
        if getattr(player, "x1", None) is not None:
            x_ends.append(float(player.x1))  # type: ignore[arg-type]
        elif player.x is not None:
            x_ends.append(float(player.x))
    match_min = min(xs) if xs else None
    match_max = max(x_ends) if x_ends else (max(xs) if xs else None)
    return match_min, match_max


def _is_walkover(
    center: float,
    markers: List[Tuple[float, float, float]],
    *,
    match_bounds: Optional[Tuple[Optional[float], Optional[float]]] = None,
    y_tolerance: float = 26.0,
    x_tolerance: float = 120.0,
) -> bool:
    if not markers:
        return False

    match_min, match_max = match_bounds if match_bounds else (None, None)
    best_idx: Optional[int] = None
    best_distance: Optional[float] = None

    for idx, (marker_y, marker_x0, marker_x1) in enumerate(markers):
        if abs(center - marker_y) > y_tolerance:
            continue

        marker_center_x = (marker_x0 + marker_x1) / 2.0
        if match_min is None and match_max is None:
            distance = 0.0
        else:
            left = match_min if match_min is not None else match_max
            right = match_max if match_max is not None else match_min
            if left is None or right is None:
                continue
            if left > right:
                left, right = right, left
            if marker_center_x < left:
                distance = left - marker_center_x
            elif marker_center_x > right:
                distance = marker_center_x - right
            else:
                distance = 0.0

        if best_distance is None or distance < best_distance:
            best_distance = distance
            best_idx = idx

    if best_idx is not None and (best_distance is None or best_distance <= x_tolerance):
        markers.pop(best_idx)
        return True
    return False


def _assign_walkover_markers(
    matches: Sequence[Match],
    bounds: Sequence[Tuple[Optional[float], Optional[float]]],
    markers: List[Tuple[float, float, float]],
    *,
    y_tolerance: float = 26.0,
    x_tolerance: float = 120.0,
) -> Set[int]:
    """Map WO labels to the closest qualification matches using XY proximity."""
    walkovers: Set[int] = set()
    if not matches or not markers:
        return walkovers

    for marker_y, marker_x0, marker_x1 in markers:
        marker_center_x = (marker_x0 + marker_x1) / 2.0
        candidates: List[Tuple[float, float, int]] = []
        for idx, match in enumerate(matches):
            y_delta = abs(match.center - marker_y)
            if y_delta > y_tolerance:
                continue
            left, right = bounds[idx] if idx < len(bounds) else (None, None)
            if left is None and right is None:
                continue
            left = left if left is not None else right
            right = right if right is not None else left
            if left is None or right is None:
                continue
            if left > right:
                left, right = right, left
            if marker_center_x < left:
                distance = left - marker_center_x
            elif marker_center_x > right:
                distance = marker_center_x - right
            else:
                distance = 0.0
            candidates.append((distance, y_delta, idx))

        if not candidates:
            continue
        distance, _, idx = min(candidates, key=lambda tpl: (tpl[0], tpl[1]))
        if distance <= x_tolerance:
            walkovers.add(idx)

    return walkovers


def _fill_walkover_forfeiter(matches: Sequence[Match]) -> None:
    """
    Once winners are known, mark which side forfeited for WO matches.

    We store the forfeiting Player instance so later code can derive S1/S2
    even if player ordering changes.
    """
    for match in matches:
        if not getattr(match, "walkover", False):
            continue
        if match.walkover_forfeiter is not None:
            continue
        if match.winner is None or len(match.players) != 2:
            continue
        p1, p2 = match.players[0], match.players[1]
        if _player_key(match.winner) == _player_key(p1):
            match.walkover_forfeiter = p2
        elif _player_key(match.winner) == _player_key(p2):
            match.walkover_forfeiter = p1


def _extract_qualification_matches(words: Sequence[dict]) -> List[Match]:
    header = _find_qualification_header(words)
    if not header:
        return []
    header_center = (float(header["top"]) + float(header["bottom"])) / 2
    y_min = header_center + 5
    y_max = max((float(w.get("bottom", 0)) for w in words), default=header_center + 250)
    qual_players = _extract_player_like_in_band(words, y_min, y_max)
    if len(qual_players) < 2:
        return []
    all_scores = _extract_score_entries(words, (0, 10000))
    wo_markers = _extract_wo_markers(words, y_min, y_max)
    matches: List[Match] = []
    bounds: List[Tuple[Optional[float], Optional[float]]] = []

    def _add_match(p1: Player, p2: Player) -> None:
        center = (p1.center + p2.center) / 2.0
        bounds.append(_qualification_match_bounds(p1, p2))
        matches.append(Match(players=[p1, p2], winner=None, scores=None, center=center))

    bands = _cluster_columns([p.x or 0.0 for p in qual_players], max_gap=40.0)
    if len(bands) > 1:
        for band in bands:
            column_players = [p for p in qual_players if p.x is not None and band[0] <= p.x <= band[1]]
            column_players.sort(key=lambda p: p.center)
            idx = 0
            while idx + 1 < len(column_players):
                _add_match(column_players[idx], column_players[idx + 1])
                idx += 2
    else:
        idx = 0
        while idx + 1 < len(qual_players):
            _add_match(qual_players[idx], qual_players[idx + 1])
            idx += 2

    walkover_idxs = _assign_walkover_markers(matches, bounds, wo_markers)
    for idx, match in enumerate(matches):
        if idx in walkover_idxs:
            match.walkover = True
            continue
        sc = _assign_nearest_score(match.center, all_scores, tolerance=50.0)
        match.scores = sc
        _assign_winner_from_scores(match)
    return matches
