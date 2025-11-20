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

The implementation is intentionally stand-alone and verbose so that the parsing
logic can be audited and refined before we migrate it into
`scrapers/scrape_tournament_class_knockout_matches_ondata.py`.  It collects all
players from the left-most column, then walks the winner/score columns from
left to right, constructing `Match` objects round by round.  Additional
validation checks at the end highlight any suspicious situations (missing
scores, duplicate players, unused score tokens, etc.).
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

# Manual toggles used during ad-hoc testing (last assignment wins)
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['29622']           # RO8 test
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['30021']           # RO16 test
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['29866']           # Qualification + RO16 test -- 16 matches 
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['29625']           # RO32 test -- 19 matches
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['1006']            # RO64 test without player ID:s -- 47 matches
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['6955']          # RO128 test
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['25395']           # RO16 but missing ko_tree_size
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ["125"] # RO16 without player ID:s
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ["492"]

# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['30284']

# Map url -> md5 hash so repeated runs can detect PDF changes.
LAST_PDF_HASHES: Dict[str, str] = {}

DEBUG_OUTPUT: bool = False


def _debug_print(message: str) -> None:
    if DEBUG_OUTPUT:
        print(message)

# -------------------------------------------------------------------
# Your original parser code (unchanged logic)
# -------------------------------------------------------------------
WINNER_LABEL_PATTERN = re.compile(
    r"^(?:(\d{1,3})\s+)?([\wÅÄÖåäö\-]+(?:\s+[\wÅÄÖåäö\-]+)*)$",
    re.UNICODE,
)
# Matches a sequence of score tokens followed by a winner label inside the
# same text blob, e.g. ``"5, 8, 6, 11 169 Augustsson A"``.
COMBINED_SCORE_LABEL_RE = re.compile(
    r"^((?:-?\d+\s*,\s*)+-?\d+)\s+(.+)$"
)
# Heuristic x-bands for the earliest (RO64) column
# R64_WINNERS_X = (170, 210) # winner labels for RO64 (far-left)
# R64_SCORES_X = (210, 260) # score blobs near RO64 winners
R64_WINNERS_X   = (195, 250) # was (170, 210)
R64_SCORES_X    = (250, 305) # was (210, 260)

@dataclass
class Player:
    full_name: str
    club: str
    short: str
    center: float
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

@dataclass
class Match:
    players: List[Player]
    winner: Optional[Player]
    scores: Optional[Tuple[int, ...]]
    center: float


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

            players = _extract_players(words)
            qual_header = _find_qualification_header(words)
            if qual_header:
                qual_center = (float(qual_header["top"]) + float(qual_header["bottom"])) / 2
                players = [p for p in players if p.center < qual_center + 5]

            all_scores_page = sorted(_extract_score_entries(words, (0, 10000)), key=lambda s: (s.x, s.center))
            all_winners_page = _deduplicate_winner_entries(_extract_winner_entries(words, (0, 10000)))
            all_winners_page.sort(key=lambda w: (w.x, w.center))
            score_bands_page = _cluster_columns([s.x for s in all_scores_page])
            winner_bands_page = _cluster_columns([w.x for w in all_winners_page])

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

            # Group winners by x-bands (merge/split when large brackets collapse columns)
            if tree_size >= 64:
                winners_by_round = _allocate_winners_by_round(
                    all_winners_page,
                    winner_bands_page,
                    tree_size,
                )
            else:
                winners_by_round: List[List[WinnerEntry]] = []
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
                if score_band in available_score_bands:
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

            if score_entries_pool and all_rounds:
                final_round = all_rounds[-1]
                if final_round:
                    match = final_round[-1]
                    if match.scores is None and score_entries_pool:
                        best_entry = min(
                            score_entries_pool,
                            key=lambda s: abs(s.center - match.center),
                        )
                        if abs(best_entry.center - match.center) <= 80.0:
                            match.scores = best_entry.scores
                            score_entries_pool.remove(best_entry)
            if score_entries_pool and all_rounds:
                for matches in reversed(all_rounds):
                    for match in matches:
                        if match.scores is not None or len(match.players) < 2:
                            continue
                        best_entry = min(
                            score_entries_pool,
                            key=lambda s: abs(s.center - match.center),
                        )
                        if abs(best_entry.center - match.center) <= 80.0:
                            match.scores = best_entry.scores
                            score_entries_pool.remove(best_entry)
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

            qualification = _extract_qualification_matches(words)
            if qualification and all_rounds:
                _assign_qualification_winners_by_presence(qualification, all_rounds[0])
            if qualification:
                for match in qualification:
                    if match.scores is None:
                        continue
                    for idx, entry in enumerate(score_entries_pool):
                        if entry.scores == match.scores:
                            score_entries_pool.pop(idx)
                            break

            for matches in all_rounds:
                _ensure_winner_first(matches)
            if qualification:
                _ensure_winner_first(qualification)

            _validate_bracket(
                pdf_hash_key,
                tclass,
                all_rounds,
                players,
                tree_size,
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

            parsed_size = len(all_rounds[0]) * 2 if all_rounds else 0
            stored = tclass.ko_tree_size
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


def _player_key(player: Player) -> Tuple[Optional[str], str]:
    return (player.player_id_ext, player.full_name)


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
                player_id_ext=player_id_ext,
                player_suffix_id=None,
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
        if any(
            token in text
            for token in (
                "Slutspel",
                "Höstpool",
                "program",
                "Kvalifikation",
                "Kvalificering",
                "Vinderen",
                "Mesterskaberne",
                "Senior Elite",
            )
        ):
            continue
        m = WINNER_LABEL_PATTERN.match(text)
        if not m:
            continue
        player_id_ext, label = m.groups()
        if any(ch.isdigit() for ch in label):
            continue
        if " " not in label:
            continue
        label = unicodedata.normalize('NFC', label.strip())
        winners.append(
            WinnerEntry(
                short=label,
                center=_to_center(word),
                x=x0,
                player_id_ext=player_id_ext,
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
    for idx, entry in enumerate(pool):
        delta = abs(entry.center - center)
        if best_delta is None or delta < best_delta:
            best_delta = delta
            best_index = idx
    if best_delta is None or best_delta > tolerance or best_index is None:
        return None
    entry = pool.pop(best_index)
    return entry.scores


def _pop_score_aligned(
    pool: List[ScoreEntry],
    center: float,
    tolerance: float,
    *,
    min_x: Optional[float] = None,
    max_x: Optional[float] = None,
) -> Optional[Tuple[int, ...]]:
    if not pool:
        return None
    best_idx: Optional[int] = None
    best_delta: Optional[float] = None
    for idx, entry in enumerate(pool):
        if min_x is not None and entry.x < min_x:
            continue
        if max_x is not None and entry.x > max_x:
            continue
        delta = abs(entry.center - center)
        if best_delta is None or delta < best_delta:
            best_delta = delta
            best_idx = idx
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
    best_index = None
    best_delta = None
    for idx, entry in enumerate(pool):
        delta = abs(entry.center - center)
        if best_delta is None or delta < best_delta:
            best_delta = delta
            best_index = idx
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
    if best_idx is not None:
        return pool.pop(best_idx)
    return _assign_nearest_winner(center, pool, tolerance)


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

        if len(participants) == 0:
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
        elif len(participants) == 1:
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
            else:
                if winner_entry.short.strip():
                    _debug_print(f"Failed to match winner label {winner_entry.short!r} at y={winner_entry.center}")
                winner = None

        if winner is None and len(participants) == 1:
            winner = participants[0]

        match_scores: Optional[Tuple[int, ...]] = None
        if len(participants) >= 2 and winner_entry.short != '':
            match_scores = _pop_score_aligned(
                remaining_scores,
                winner_entry.center,
                tolerance=30.0,
                min_x=score_min,
                max_x=score_max,
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
) -> Tuple[List[Match], List[WinnerEntry], List[ScoreEntry]]:
    score_min, score_max = score_window
    remaining_scores = list(scores)
    remaining_winners = sorted(winners, key=lambda w: w.center)
    matches: List[Match] = []

    ordered_prev = sorted(previous_round, key=lambda m: m.center)
    pair_count = (len(ordered_prev) + 1) // 2

    for idx in range(pair_count):
        first_match = ordered_prev[2 * idx]
        second_match = ordered_prev[2 * idx + 1] if 2 * idx + 1 < len(ordered_prev) else None

        participants: List[Player] = []
        centers: List[float] = []

        primary = first_match.winner or (first_match.players[0] if first_match.players else None)
        if primary:
            participants.append(primary)
        if first_match.players:
            centers.append(first_match.center)

        if second_match:
            secondary = second_match.winner or (second_match.players[0] if second_match.players else None)
            if secondary:
                participants.append(secondary)
            if second_match.players:
                centers.append(second_match.center)

        if not participants:
            continue

        center = sum(centers) / len(centers) if centers else participants[0].center

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
    for match in previous_round:
        if match.winner is None:
            for player in match.players:
                if player in advancing_players:
                    match.winner = player
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


def _label_round(name: str, matches: Sequence[Match]) -> List[str]:
    lines: List[str] = [f"{name}:"]
    for match in matches:
        if len(match.players) < 2 and match.players:
            solo = match.players[0]
            lines.append(f"{_format_player(solo)} 		-> BYE")
            continue
        if len(match.players) < 2:
            lines.append("Unknown participants -> Winner: Unknown")
            continue
        left = match.players[0]
        right = match.players[1] if len(match.players) > 1 else None
        if right is None:
            winner_text = _format_player(match.winner) if match.winner else 'Unknown'
            token_text = f" -> Game tokens: {match.scores}" if match.scores else ''
            lines.append(f"{_format_player(left)} 		-> Winner: {winner_text}{token_text}")
            continue
        winner_label = (
            f"Winner: {_format_player(match.winner)}"
            if match.winner
            else "Winner: Unknown"
        )
        score_label = (
            f" -> Game tokens: {match.scores}"
            if match.scores is not None
            else ""
        )
        lines.append(
            f"{_format_player(left)} vs {_format_player(right)} 	-> {winner_label}{score_label}"
        )
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
            current.extend(entries)
            band_idx += 1
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
                if match.scores is None:
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
    if remaining_scores:
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
    r"\b(kval(?:ifikation|ificering|ifisering)?|karsinta)\b", re.IGNORECASE
)


def _find_qualification_header(words: Sequence[dict]) -> Optional[dict]:
    candidates = []
    for w in words:
        txt = w.get("text", "").replace(" ", " ").strip()
        if not txt:
            continue
        if QUAL_HEADER_RE.search(txt):
            size = float(w.get("size", 0)) if "size" in w else 0.0
            candidates.append((size, w))
    if not candidates:
        return None
    candidates.sort(key=lambda t: (-t[0], float(t[1].get("top", 0))))
    return candidates[0][1]


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
                player_id_ext=player_id_ext.strip() if player_id_ext else None,
                player_suffix_id=None,
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


def _extract_qualification_matches(words: Sequence[dict]) -> List[Match]:
    header = _find_qualification_header(words)
    if not header:
        return []
    header_center = (float(header["top"]) + float(header["bottom"])) / 2
    y_min = header_center + 5
    y_max = header_center + 250
    qual_players = _extract_player_like_in_band(words, y_min, y_max)
    if len(qual_players) < 2:
        return []
    all_scores = _extract_score_entries(words, (0, 10000))
    matches: List[Match] = []
    idx = 0
    while idx + 1 < len(qual_players):
        a = qual_players[idx]
        b = qual_players[idx + 1]
        center = (a.center + b.center) / 2.0
        sc = _assign_nearest_score(center, all_scores, tolerance=50.0)
        matches.append(Match(players=[a, b], winner=None, scores=sc, center=center))
        idx += 2
    return matches
