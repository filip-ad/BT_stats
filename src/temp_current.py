# src/scrapers/scrape_tournament_class_knockout_matches_ondata.py

from __future__ import annotations

import hashlib
import io
import re
from dataclasses import dataclass
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
)
from models.tournament import Tournament
from models.tournament_class import TournamentClass
from models.tournament_class_match_raw import TournamentClassMatchRaw

# Manual toggles used during ad-hoc testing (last assignment wins)
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['29622']           # RO8 test
SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['30021']           # RO16 test
SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['29866']           # Qualification + RO16 test
SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['29625']           # RO32 test
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['1006']            # RO64 test
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['6955']          # RO128 test
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['25395']           # RO16 but missing ko_tree_size

# Map url -> md5 hash so repeated runs can detect PDF changes.
LAST_PDF_HASHES: Dict[str, str] = {}

DEBUG_OUTPUT: bool = True


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
            "class_idx":                f"{idx}/{len(classes)}",
            "tournament":               tclass.shortname or tclass.longname or "N/A",
            "tournament_id":            str(tclass.tournament_id or "None"),
            "tournament_id_ext":        str(tid_ext or "None"),
            "tournament_class_id":      str(tclass.tournament_class_id or "None"),
            "tournament_class_id_ext":  str(cid_ext or "None"),
            "date":                     str(getattr(tclass, "startdate", None) or "None"),
            "stage":                    5,
        }

        
        if DEBUG_OUTPUT:
            header_line = f"===== {tclass.shortname or tclass.longname or 'N/A'} [cid_ext = '{cid_ext or 'None'}'] [tid_ext = '{tid_ext or 'None'}'] ====="
            _debug_print("\n" + header_line)
            # _debug_print(f"Tournament ID ext: {tid_ext or 'None'}")

        if not cid_ext:
            logger.failed(logger_keys.copy(), "No tournament_class_id_ext available for class")
            continue

        pdf_path, downloaded, msg = _download_pdf_ondata_by_tournament_class_and_stage(
            tournament_id_ext=tid_ext or "",
            class_id_ext=cid_ext or "",
            stage=5,
            force_download=False,
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

            round_sizes: List[int] = []
            matches_in_round = tree_size // 2
            while matches_in_round >= 1:
                round_sizes.append(matches_in_round)
                matches_in_round //= 2

            winners_by_round: List[List[WinnerEntry]] = []
            cursor_pos = 0
            remaining = total_winners
            for ridx, expected in enumerate(round_sizes):
                rounds_left = len(round_sizes) - ridx
                min_needed_for_rest = max(0, rounds_left - 1)
                take = min(expected, remaining - min_needed_for_rest)
                if take <= 0:
                    break
                chunk = all_winners_page[cursor_pos:cursor_pos + take]
                winners_by_round.append(chunk)
                cursor_pos += take
                remaining -= take
            if cursor_pos < total_winners:
                if winners_by_round:
                    winners_by_round[-1].extend(all_winners_page[cursor_pos:])
                else:
                    winners_by_round.append(all_winners_page[cursor_pos:])

            all_rounds: List[List[Match]] = []
            previous_round: Optional[List[Match]] = None
            available_score_bands = list(score_bands_page)
            score_entries_pool = list(all_scores_page)

            round_winner_entries: List[List[WinnerEntry]] = []
            for ridx, winner_chunk in enumerate(winners_by_round):
                if not winner_chunk:
                    continue
                round_winner_entries.append(list(winner_chunk))
                win_min = min(w.x for w in winner_chunk)
                win_max = max(w.x for w in winner_chunk)
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
                        winner_chunk,
                        scores_for_round,
                        score_window,
                    )
                    remaining_ids = {id(entry) for entry in leftover_scores}
                    consumed = [entry for entry in original_scores if id(entry) not in remaining_ids]
                else:
                    current_round, _, leftover_scores = _build_next_round(
                        previous_round,
                        winner_chunk,
                        scores_for_round,
                        players,
                        score_window,
                        winner_tolerance=24.0 + 4.0 * tolerance_step,
                        score_tolerance=28.0 + 4.0 * tolerance_step,
                    )
                    _fill_missing_winners(previous_round, current_round)
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
                        f"Unable to map match count {len(matches)} to stage id; defaulting to QF",
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
            if stored is None:
                logger.info(logger_keys.copy(), f"SELF-CHECK: DB ko_tree_size=NULL | parsed={parsed_size}")
            elif int(stored) == int(parsed_size):
                # logger.info(logger_keys.copy(), f"SELF-CHECK: parsed={parsed_size} == stored={stored}")
                if hasattr(logger, "inc_processed"):
                    logger.inc_processed()
            else:
                logger.warning(
                    logger_keys.copy(),
                    f"SELF-CHECK: parsed={parsed_size} != stored={stored}",
                )

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
            full_name = raw_name.strip()
            club = raw_club.strip()
            players.append(
                Player(
                    full_name=full_name,
                    club=club,
                    short=_make_short(full_name),
                    center=_to_center(word),
                    player_id_ext=player_id_ext,
                    player_suffix_id=None,
                )
            )
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
        winners.append(
            WinnerEntry(
                short=label.strip(),
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
    normalized = short.strip()
    if player_id_ext:
        id_candidates = [p for p in players if p.player_id_ext == player_id_ext]
        if id_candidates:
            return min(id_candidates, key=lambda p: abs(p.center - center))
    candidates = [p for p in players if p.short == normalized]
    if candidates:
        return min(candidates, key=lambda p: abs(p.center - center))
    alt_short = _make_short(normalized)
    if alt_short != normalized:
        candidates = [p for p in players if p.short == alt_short]
        if candidates:
            return min(candidates, key=lambda p: abs(p.center - center))
    prefix_matches = [p for p in players if p.full_name.startswith(normalized)]
    if prefix_matches:
        return min(prefix_matches, key=lambda p: abs(p.center - center))
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
) -> Tuple[List[Match], List[ScoreEntry]]:
    score_min, score_max = score_window
    remaining_scores = list(scores)
    matches: List[Match] = []

    sorted_winners = sorted(winners, key=lambda w: w.center)
    sorted_players = sorted(players, key=lambda p: p.center)
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
                winner = _match_short_to_full(
                    winner_entry.short,
                    winner_entry.center,
                    players,
                    winner_entry.player_id_ext,
                )
            except ValueError:
                winner = participants[0] if participants else None

        if winner is None and len(participants) == 1:
            winner = participants[0]

        match_scores: Optional[Tuple[int, ...]] = None
        if len(participants) >= 2:
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
                except ValueError:
                    candidate = None
                if candidate and candidate in participants:
                    winner = candidate
                elif candidate and len(participants) == 1:
                    winner = candidate
                else:
                    winner = None
        if winner is None and participants:
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
            logger.warning(
                logger_keys.copy(),
                (
                    f"Expected {expected} matches in {_round_display_name(actual)} (round {idx}), "
                    f"parsed {actual}."
                ),
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
        logger.warning(logger_keys.copy(), f"Duplicate player entries detected in first round: {sample}")

    used_keys = {
        _player_key(p)
        for round_matches in rounds
        for match in round_matches
        for p in match.players
        if p
    }
    missing_players = [p.full_name for p in players if _player_key(p) not in used_keys]
    if missing_players:
        sample = ", ".join(sorted(missing_players)[:5])
        logger.warning(
            logger_keys.copy(),
            f"{len(missing_players)} player(s) from the left column never appear in the bracket: {sample}",
        )

    for idx, matches in enumerate(rounds):
        round_name = _round_display_name(len(matches))
        best_of_values: Set[int] = set()
        for match in matches:
            if len(match.players) == 2 and match.players[0] != match.players[1]:
                if match.scores is None:
                    logger.warning(
                        logger_keys.copy(),
                        f"{round_name}: missing score for {match.players[0].full_name} vs {match.players[1].full_name}",
                    )
                else:
                    inferred = _infer_best_of_from_scores(match.scores)
                    if inferred is not None:
                        best_of_values.add(inferred)
            if len(match.players) == 2 and match.winner is None:
                logger.warning(
                    logger_keys.copy(),
                    f"{round_name}: missing winner for {match.players[0].full_name} vs {match.players[1].full_name}",
                )
        if len(best_of_values) > 1:
            logger.warning(logger_keys.copy(), f"{round_name}: inconsistent best-of detected {sorted(best_of_values)}")

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
        logger.warning(
            logger_keys.copy(),
            f"{len(remaining_scores)} score token(s) were not attached to any match. Example: {examples}",
        )


QUAL_HEADER_RE = re.compile(
    r"(kval(?:ifikation|ificering|ifisering)?|karsinta)", re.IGNORECASE
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
        full_name = raw_name.strip()
        out.append(
            Player(
                full_name=full_name,
                club=raw_club.strip(),
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


# # scrapers/scrape_tournament_class_knockout_matches_ondata.py

# from __future__ import annotations
# import io, re, logging
# from typing import List, Dict, Any, Tuple
# import pdfplumber

# from utils import (
#     parse_date,
#     OperationLogger,
#     _download_pdf_ondata_by_tournament_class_and_stage,
# )
# from config import (
#     SCRAPE_PARTICIPANTS_MAX_CLASSES,
#     SCRAPE_PARTICIPANTS_CLASS_ID_EXTS,
#     SCRAPE_PARTICIPANTS_TNMT_ID_EXTS,
#     SCRAPE_PARTICIPANTS_ORDER,
#     SCRAPE_PARTICIPANTS_CUTOFF_DATE,
# )
# from models.tournament import Tournament
# from models.tournament_class import TournamentClass
# from models.tournament_class_match_raw import TournamentClassMatchRaw

# def scrape_tournament_class_knockout_matches_ondata(cursor, run_id=None):
#     """
#     Parse KO (stage=5) bracket PDFs from OnData and write raw rows.
#     One DB row per KO match; tournament_class_stage_id set per round (R16/QF/SF/F etc).
#     """
#     logger = OperationLogger(
#         verbosity           = 2,
#         print_output        = False,
#         log_to_db           = True,
#         cursor              = cursor,
#         object_type         = "tournament_class_match_raw",
#         run_type            = "scrape",
#         run_id              = run_id
#     )

#     cutoff_date = parse_date(SCRAPE_PARTICIPANTS_CUTOFF_DATE) if SCRAPE_PARTICIPANTS_CUTOFF_DATE else None

#     classes = TournamentClass.get_filtered_classes(
#         cursor                  = cursor,
#         class_id_exts           = SCRAPE_PARTICIPANTS_CLASS_ID_EXTS,
#         tournament_id_exts      = SCRAPE_PARTICIPANTS_TNMT_ID_EXTS,
#         data_source_id          = 1 if (SCRAPE_PARTICIPANTS_CLASS_ID_EXTS or SCRAPE_PARTICIPANTS_TNMT_ID_EXTS) else None,
#         cutoff_date             = cutoff_date,
#         require_ended           = False,
#         allowed_structure_ids   = [1, 3],   # 1 = Groups+KO, 3 = KO only
#         allowed_type_ids        = [1],      # singles for now
#         max_classes             = SCRAPE_PARTICIPANTS_MAX_CLASSES,
#         order                   = SCRAPE_PARTICIPANTS_ORDER,
#     )

#     tournament_ids = [tc.tournament_id for tc in classes if tc.tournament_id is not None]
#     tid_to_ext = Tournament.get_id_ext_map_by_id(cursor, tournament_ids)

#     logger.info(f"Scraping tournament class KO matches for {len(classes)} classes from Ondata")

#     total_seen = total_inserted = total_skipped = 0

#     for idx, tc in enumerate(classes, 1):
#         tid_ext = tid_to_ext.get(tc.tournament_id)
#         cid_ext = tc.tournament_class_id_ext

#         logger_keys = {
#             "class_idx":                f"{idx}/{len(classes)}",
#             "tournament":               tc.shortname or tc.longname or "N/A",
#             "tournament_id":            str(tc.tournament_id or "None"),
#             "tournament_id_ext":        str(tid_ext or "None"),
#             "tournament_class_id":      str(tc.tournament_class_id or "None"),
#             "tournament_class_id_ext":  str(cid_ext or "None"),
#             "date":                     str(getattr(tc, "startdate", None) or "None"),
#             "stage":                    5,
#         }

#         # Download (or reuse cache) stage=5
#         pdf_path, downloaded, msg = _download_pdf_ondata_by_tournament_class_and_stage(
#             tournament_id_ext   = tid_ext or "",
#             class_id_ext        = cid_ext or "",
#             stage               = 5,
#             force_download      = False,
#         )
#         if msg:
#             # logger.info(logger_keys.copy(), msg)
#             pass

#         if not pdf_path:
#             logger.failed(logger_keys.copy(), "No valid KO PDF (stage=5) for class")
#             continue

#         # Parse KO PDF into rounds
#         try:
#             with open(pdf_path, "rb") as f:
#                 pdf_bytes = f.read()
#             rounds = _parse_knockout_pdf(pdf_bytes)
#             if not rounds:
#                 logger.warning(logger_keys.copy(), "KO parser returned 0 rounds (no bracket entries detected).")

#         except Exception as e:
#             logger.failed(logger_keys.copy(), f"KO PDF parsing failed: {e}")
#             continue

#         # Remove existing raw rows for KO stages for this class (whichever rounds we detected)
#         stage_ids_to_clear = {r["stage_id"] for r in rounds if r.get("stage_id")}
#         # If nothing detected, clear common KO stages to avoid dups on re-run
#         if not stage_ids_to_clear:
#             stage_ids_to_clear = {2, 3, 4, 5, 6, 7, 8}
#         for sid in sorted(stage_ids_to_clear):
#             TournamentClassMatchRaw.remove_for_class(
#                 cursor,
#                 tournament_class_id_ext=cid_ext,
#                 tournament_class_stage_id=sid,
#                 data_source_id=1,
#             )

#         kept = skipped = 0

#         for r in rounds:
#             stage_id = r.get("stage_id")
#             for mm in r.get("matches", []):
#                 if not mm.get("p2"):
#                     continue  # skip byes
#                 total_seen += 1

#                 p1 = mm.get("p1", {}) or {}
#                 p2 = mm.get("p2", {}) or {}

#                 p1_code = p1.get("code") or None
#                 p2_code = p2.get("code") or None

#                 tokens_raw = mm.get("tokens", [])
#                 tokens_csv = _normalize_sign_tokens(tokens_raw)
#                 best_of = None if tokens_csv == "WO" else _infer_best_of_from_sign(tokens_raw)

#                 def name_with_club(d):
#                     n = (d.get("name") or "").strip()
#                     c = (d.get("club") or None)
#                     return f"{n}" if not c else f"{n}, {c}"

#                 raw_line_text = (
#                     f"{(mm.get('match_id_ext') or '').strip()} "
#                     f"{(p1_code or '').strip()} {name_with_club(p1)} - "
#                     f"{(p2_code or '').strip()} {name_with_club(p2)} "
#                     f"{tokens_csv}"
#                 ).strip()

#                 row = TournamentClassMatchRaw(
#                     tournament_id_ext=tid_ext or "",
#                     tournament_class_id_ext=cid_ext or "",
#                     group_id_ext=None,                         # KO has no pool
#                     match_id_ext=(mm.get("match_id_ext") or None),

#                     s1_player_id_ext=p1_code,
#                     s2_player_id_ext=p2_code,
#                     s1_fullname_raw=p1.get("name"),
#                     s2_fullname_raw=p2.get("name"),
#                     s1_clubname_raw=p1.get("club"),
#                     s2_clubname_raw=p2.get("club"),

#                     game_point_tokens=tokens_csv or None,
#                     best_of=best_of,
#                     raw_line_text=raw_line_text,

#                     tournament_class_stage_id=stage_id or 6,   # default QF if unknown
#                     data_source_id=1,
#                 )

#                 match_keys = logger_keys.copy()
#                 match_keys.update({
#                     "group_id_ext": "KO",
#                     "match_id_ext": row.match_id_ext or "None",
#                     "round_stage_id": str(row.tournament_class_stage_id),
#                 })

#                 # We keep validation super-light for RAW
#                 is_valid, err = row.validate()
#                 if not is_valid:
#                     skipped += 1
#                     total_skipped += 1
#                     logger.failed(match_keys, f"Validation failed: {err}")
#                     continue

#                 try:
#                     row.compute_hash()
#                     row.insert(cursor)
#                     kept += 1
#                     total_inserted += 1
#                     logger.success(match_keys, "Raw KO match saved")
#                     if hasattr(logger, "inc_processed"):
#                         logger.inc_processed()
#                 except Exception as e:
#                     skipped += 1
#                     total_skipped += 1
#                     logger.failed(match_keys, f"Insert failed: {e}")

#         logger.info(logger_keys.copy(), f"Inserted: {kept}   Skipped: {skipped}")

#     logger.info(f"Scraping completed. Inserted: {total_inserted}, Skipped: {total_skipped}, Matches seen: {total_seen}")
#     logger.summarize()



# # ───────────────────────── Helpers: tokens ─────────────────────────

# def _tokenize_right(s: str) -> list[str]:
#     """Return signed tokens as strings or ['WO'] for walkovers."""
#     if not s:
#         return []
#     s = s.strip()
#     if re.fullmatch(r"WO", s, flags=re.IGNORECASE):
#         return ["WO"]
#     s = re.sub(r"\s*,\s*", " ", s)
#     return re.findall(r"[+-]?\d+", s)

# def _normalize_sign_tokens(tokens: List[str]) -> str:
#     """
#     Convert ['+9','-8','11'] -> '9, -8, 11'
#     If WO → returns 'WO'
#     """
#     if tokens and all(t.upper() == "WO" for t in tokens):
#         return "WO"
#     norm: List[str] = []
#     for raw in tokens or []:
#         t = str(raw).strip()
#         if t.startswith("+"):
#             norm.append(t[1:])
#         else:
#             norm.append(t)
#     return ", ".join(norm)

# def _infer_best_of_from_sign(tokens: list[str]) -> int | None:
#     """best_of = 2*max(wins) - 1 from signed tokens."""
#     p1 = p2 = 0
#     for raw in tokens or []:
#         if not re.fullmatch(r"[+-]?\d+", raw.strip()):
#             continue
#         v = int(raw)
#         if v >= 0: p1 += 1
#         else:      p2 += 1
#     if p1 == 0 and p2 == 0:
#         return None
#     return 2 * max(p1, p2) - 1


# # ───────────────────────── Helpers: PDF parsing (KO) ─────────────────────────
# # We detect "entry lines" (with a code + name, optionally club), cluster them by x-position
# # (columns = bracket rounds), pair adjacent entries vertically as matches, and grab
# # result tokens to the right of that pair (within the horizontal gap until the next column).

# # ───────────────────────── Helpers: extract KO bracket entries ─────────────────────────
# # code + name + club  e.g. "046 Wang Tom, IFK Täby BTK"
# _RE_ENTRY_WITH_CLUB = re.compile(
#     r"^\s*(?P<code>\d{1,3}(?:/\d{1,3})?)\s+(?P<name>.+?)\s*,\s*(?P<club>.+?)\s*$"
# )
# # code + name  e.g. "150 Ott D"
# _RE_ENTRY_SIMPLE = re.compile(
#     r"^\s*(?P<code>\d{1,3}(?:/\d{1,3})?)\s+(?P<name>.+?)\s*$"
# )
# # name + club (no code)  e.g. "Ohlsén Vigg, Laholms BTK Serve"
# _RE_ENTRY_NAME_CLUB = re.compile(
#     r"^\s*(?P<name>[^,]+?)\s*,\s*(?P<club>.+?)\s*$"
# )
# # short name only (no code, no comma)  e.g. "Wang L", "Zhu A", "Ott D"
# # (allow diacritics, hyphens, apostrophes; last token is 1–3 letters + optional dot)
# _RE_ENTRY_SHORT = re.compile(
#     r"^\s*(?P<name>[A-Za-zÅÄÖåäöÉéÍíÓóÚúÑñÜüÆæØøÇç'’\-.]+(?:\s+[A-Za-zÅÄÖåäöÉéÍíÓóÚúÑñÜüÆæØøÇç'’\-.]+)*)\s+[A-Za-zÅÄÖÉÍÓÚÑÜ]{1,3}\.?\s*$"
# )

# _RE_TOKEN = re.compile(r"^(?P<tokens>(?:[+-]?\d+(?:\s*,\s*[+-]?\d+)*|WO))$", re.IGNORECASE)

# _SEG_GAP            = 36.0  # px gap between words that indicates a new segment/column piece
# _PAIR_GAP_MAX       = 12.0  # Strict gap for pairing opponents
# _LOOSE_PAIR_GAP_MAX = 200.0  # Loose gap for later rounds, increased

# def _segment_to_entry(seg_words: list[dict]) -> dict | None:
#     """
#     Convert a contiguous set of words on the same row into a bracket entry if possible.
#     Returns a dict with geometry and parsed fields, or None if not an entry.
#     """
#     if not seg_words:
#         return None
#     # Raw segment text
#     text = " ".join(w["text"] for w in seg_words).strip()
#     if not text or len(text) < 2:
#         return None
#     lower = text.lower()
#     if any(bad in lower for bad in ("slutspel", "pool", "sets", "poäng", "poäng", "diff", "bröt", "brot")):
#         return None
#     # If the entire segment is just tokens, it's not an entry
#     if re.fullmatch(r"(?:WO|[+-]?\d+(?:\s*[,\s]\s*[+-]?\d+)*)", text, flags=re.IGNORECASE):
#         return None
#     # Extract trailing tokens
#     trailing_pattern = r"(WO|[+-]?\d+(?:\s*[,\s]\s*[+-]?\d+)*)\s*$"
#     m_trailing = re.search(trailing_pattern, text, flags=re.IGNORECASE)
#     if m_trailing:
#         raw_trailing = m_trailing.group(1)
#         cleaned = text[:m_trailing.start()].strip()
#     else:
#         raw_trailing = None
#         cleaned = text.strip()
#     if not cleaned:
#         return None
#     # Helper: must contain at least one letter
#     def _has_alpha(s: str) -> bool:
#         return re.search(r"[A-Za-zÅÄÖåäöÉéÍíÓóÚúÑñÜüÆæØøÇç]", s) is not None
#     m = (
#         _RE_ENTRY_WITH_CLUB.match(cleaned)
#         or _RE_ENTRY_SIMPLE.match(cleaned)
#         or _RE_ENTRY_NAME_CLUB.match(cleaned)
#         or _RE_ENTRY_SHORT.match(cleaned)
#     )
#     if not m:
#         return None
#     code = (m.groupdict().get("code") or None)
#     name = m.group("name").strip()
#     club = m.groupdict().get("club")
#     club = club.strip() if club else None
#     # Reject if the 'name' part has no letters (prevents "8, 5, 8, ..." etc)
#     if not _has_alpha(name):
#         return None
#     x0 = min(w["x0"] for w in seg_words)
#     x1 = max(w["x1"] for w in seg_words)
#     top = min(w["top"] for w in seg_words)
#     bottom = max(w["bottom"] for w in seg_words)
#     page = seg_words[0]["_page"]
#     page_w = page.width
#     return {
#         "type": "entry",
#         "text": cleaned,
#         "code": (code.strip() if code else None),
#         "name": name,
#         "club": club,
#         "x0": x0, "x1": x1, "top": top, "bottom": bottom,
#         "page": page, "page_w": page_w,
#         "tokens": _tokenize_right(raw_trailing) if raw_trailing else []
#     }

# def _segment_to_token(seg_words: list[dict]) -> dict | None:
#     text = " ".join(w["text"] for w in seg_words).strip()
#     if re.fullmatch(r"(?:WO|[+-]?\d+(?:\s*[,\s]\s*[+-]?\d+)*)", text, flags=re.IGNORECASE):
#         x0 = min(w["x0"] for w in seg_words)
#         x1 = max(w["x1"] for w in seg_words)
#         top = min(w["top"] for w in seg_words)
#         bottom = max(w["bottom"] for w in seg_words)
#         page = seg_words[0]["_page"]
#         page_w = page.width
#         return {
#             "type": "token",
#             "tokens": _tokenize_right(text),
#             "x0": x0, "x1": x1, "top": top, "bottom": bottom,
#             "page": page, "page_w": page_w
#         }
#     return None


# def _extract_entry_rows(pdf_bytes: bytes) -> list[dict]:
#     """
#     Build entry rows with geometry by:
#       1) grouping page words into y-rows,
#       2) splitting each row into left→right segments by large x-gaps,
#       3) parsing each segment as a KO bracket entry.
#     """
#     entries: list[dict] = []
#     with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
#         for page in pdf.pages:
#             words = page.extract_words(x_tolerance=2, y_tolerance=3, keep_blank_chars=False) or []
#             for w in words:
#                 w["_page"] = page

#             # group into y-rows
#             row_map: dict[int, list[dict]] = {}
#             rid, last_top = 0, None
#             for w in sorted(words, key=lambda w: (round(w["top"], 1), w["x0"])):  # keep it tight by y
#                 top = round(w["top"], 1)
#                 if last_top is None or abs(top - last_top) > 3.0:
#                     rid += 1
#                     last_top = top
#                     row_map[rid] = []
#                 row_map[rid].append(w)

#             # split row into segments and parse each segment
#             for row_words in row_map.values():
#                 row_words.sort(key=lambda w: w["x0"])
#                 seg: list[dict] = []
#                 prev_x1 = None

#                 def _flush():
#                     nonlocal seg
#                     if seg:
#                         ent = _segment_to_entry(seg)
#                         if ent:
#                             entries.append(ent)
#                         else:
#                             token_ent = _segment_to_token(seg)
#                             if token_ent:
#                                 entries.append(token_ent)
#                         seg = []

#                 for w in row_words:
#                     if prev_x1 is None or (w["x0"] - prev_x1) <= _SEG_GAP:
#                         seg.append(w)
#                     else:
#                         _flush()
#                         seg = [w]
#                     prev_x1 = w["x1"]
#                 _flush()

#     return entries

# def _cluster_columns(entries: list[dict], tolerance: float = 60.0) -> list[dict]:  # Increased tolerance to 60.0
#     """
#     Cluster entry rows by their x0 into columns. Returns a list of columns:
#       [{"x0": float, "x1": float, "rows": [entry_row, ...]}] sorted left→right.
#     """
#     cols = []
#     for e in sorted(entries, key=lambda r: r["x0"]):
#         placed = False
#         for c in cols:
#             if abs(e["x0"] - c["x0"]) <= tolerance:
#                 c["rows"].append(e)
#                 c["x0"] = min(c["x0"], e["x0"])
#                 c["x1"] = max(c["x1"], e["x1"])
#                 placed = True
#                 break
#         if not placed:
#             cols.append({"x0": e["x0"], "x1": e["x1"], "rows": [e]})
#     # sort & normalize row order
#     for c in cols:
#         c["rows"].sort(key=lambda r: r["top"])
#     cols.sort(key=lambda c: c["x0"])
#     return cols

# # Map #pairs in a column → tournament_class_stage_id
# _STAGE_BY_PAIRCOUNT = {
#     1: 8,    # Final
#     2: 7,    # SF
#     4: 6,    # QF
#     8: 5,    # R16
#     16: 4,   # R32
#     32: 3,   # R64
#     64: 2,   # R128
# }

# def _parse_knockout_pdf(pdf_bytes: bytes) -> list[dict]:
#     """
#     Returns a list of 'round' dicts:
#       [{"stage_id": int, "matches": [ {p1, p1_code, p2, p2_code, tokens}, ... ]}, ...]
#     """
#     items = _extract_entry_rows(pdf_bytes)
#     if not items:
#         logging.info("[KO parse] No items found.")
#         return []
#     logging.info(f"[KO parse] detected {len(items)} items (entries + tokens)")
#     cols = _cluster_columns(items, tolerance=60.0)  # Increased tolerance
#     logging.info(f"[KO parse] columns={len(cols)}; sizes={[len(c['rows']) for c in cols]}")

#     # Build matches per column, including byes as p2=None
#     pairs_per_col: list[list[dict]] = []
#     for col in cols:
#         entry_rows = [r for r in col["rows"] if r.get("type") == "entry"]
#         col_matches: list[dict] = []
#         i = 0
#         while i < len(entry_rows):
#             if i + 1 < len(entry_rows):
#                 a, b = entry_rows[i], entry_rows[i + 1]
#                 vgap = b["top"] - a["bottom"]
#                 atop_gap = b["top"] - a["top"]
#                 logging.info(f"[KO parse] Column x0={col['x0']:.1f}, Row {i}: top={a['top']:.1f}, bottom={a['bottom']:.1f}, text={a['text']}")
#                 logging.info(f"[KO parse] Column x0={col['x0']:.1f}, Row {i+1}: top={b['top']:.1f}, bottom={b['bottom']:.1f}, text={b['text']}")
#                 logging.info(f"[KO parse] Potential pair vgap={vgap:.1f}, atop_gap={atop_gap:.1f}")
#                 if vgap <= _PAIR_GAP_MAX and atop_gap <= _PAIR_GAP_MAX * 2:
#                     col_matches.append({"p1": a, "p2": b, "tokens": []})
#                     logging.info("[KO parse] Strict pair added")
#                     i += 2
#                     continue
#             a = entry_rows[i]
#             col_matches.append({"p1": a, "p2": None, "tokens": []})
#             logging.info("[KO parse] Bye/single added")
#             i += 1
#         # Fix: if no pairs made (all singles), but even number of rows, fallback to loose pairing
#         num_pairs = len([m for m in col_matches if m["p2"] is not None])
#         if num_pairs == 0 and len(entry_rows) % 2 == 0 and len(entry_rows) >= 2:
#             logging.info(f"[KO parse] No strict pairs, falling back to loose pairing for column x0={col['x0']:.1f}")
#             col_matches = []
#             i = 0
#             while i < len(entry_rows):
#                 if i + 1 < len(entry_rows):
#                     a, b = entry_rows[i], entry_rows[i + 1]
#                     vgap = b["top"] - a["bottom"]
#                     atop_gap = b["top"] - a["top"]
#                     if vgap <= _LOOSE_PAIR_GAP_MAX and atop_gap <= _LOOSE_PAIR_GAP_MAX * 2:
#                         col_matches.append({"p1": a, "p2": b, "tokens": []})
#                         logging.info("[KO parse] Loose pair added")
#                         i += 2
#                         continue
#                 a = entry_rows[i]
#                 col_matches.append({"p1": a, "p2": None, "tokens": []})
#                 logging.info("[KO parse] Loose bye added")
#                 i += 1
#         pairs_per_col.append(col_matches)

#     # Identify columns with at least one real match (p2 not None)
#     match_col_indices = [ci for ci, ps in enumerate(pairs_per_col) if ps and any(m["p2"] is not None for m in ps)]
#     if not match_col_indices:
#         logging.warning("[KO parse] No match columns detected after pairing.")
#         return []

#     # Stage mapping from RIGHT
#     stage_by_ci: dict[int, int] = {}
#     for rank_from_right, ci in enumerate(reversed(match_col_indices)):
#         stage_by_ci[ci] = max(2, 8 - rank_from_right)

#     # Build round objects
#     rounds: list[dict] = []
#     for ci in match_col_indices:
#         matches: list[dict] = []
#         for m in pairs_per_col[ci]:
#             if m["p2"] is None:
#                 continue  # skip byes
#             match = {
#                 "p1": m["p1"],
#                 "p2": m["p2"],
#                 "tokens": m["p1"]["tokens"] or m["p2"]["tokens"] or [],  # if any has tokens
#                 "match_id_ext": None,
#             }
#             matches.append(match)
#         stage_id = stage_by_ci.get(ci)
#         rounds.append({"stage_id": stage_id, "matches": matches})

#     # Assign tokens from next column's aligned entry or global tokens
#     token_rows = [item for item in items if item.get("type") == "token"]
#     for r_idx in range(len(rounds)):
#         current_round = rounds[r_idx]
#         ci = match_col_indices[r_idx]
#         next_ci = ci + 1
#         while next_ci < len(cols) and not cols[next_ci]["rows"]:
#             next_ci += 1
#         has_next_entry_tokens = False
#         if next_ci < len(cols):
#             next_col = cols[next_ci]
#             next_rows = [r for r in next_col["rows"] if r.get("type") == "entry"]
#             for match in current_round["matches"]:
#                 if match["tokens"]:
#                     continue
#                 p1 = match["p1"]
#                 p2 = match["p2"]
#                 min_top = min(p1["top"], p2["top"])
#                 max_bottom = max(p1["bottom"], p2["bottom"])
#                 center = (min_top + max_bottom) / 2
#                 closest = min(next_rows, key=lambda r: abs(r["top"] - center), default=None) if next_rows else None
#                 if closest and abs(closest["top"] - center) <= 30.0 and closest["tokens"]:
#                     match["tokens"] = closest["tokens"]
#                     logging.info(f"[KO parse] Assigned tokens {match['tokens']} from next entry {closest['text']} at center {center:.1f}, entry top {closest['top']:.1f}")
#                     has_next_entry_tokens = True
#         # If no assignment from next entry, use global tokens
#         if not has_next_entry_tokens:
#             for match in current_round["matches"]:
#                 if match["tokens"]:
#                     continue
#                 p1 = match["p1"]
#                 p2 = match["p2"]
#                 min_top = min(p1["top"], p2["top"])
#                 max_bottom = max(p1["bottom"], p2["bottom"])
#                 center = (min_top + max_bottom) / 2
#                 col_x1 = max(p1["x1"], p2["x1"])
#                 candidates = [t for t in token_rows if t["x0"] > col_x1 - 50]
#                 if not candidates:
#                     continue
#                 closest = min(candidates, key=lambda t: abs(t["top"] - center) + 0.01 * abs(t["x0"] - col_x1))
#                 delta_y = abs(closest["top"] - center)
#                 if delta_y <= 30.0:
#                     match["tokens"] = closest["tokens"]
#                     logging.info(f"[KO parse] Assigned tokens {match['tokens']} from global token at center {center:.1f}, token top {closest['top']:.1f}, delta_y={delta_y:.1f}")

#     # Debug counts
#     debug_counts = {}
#     for r in rounds:
#         debug_counts[r["stage_id"]] = debug_counts.get(r["stage_id"], 0) + len(r["matches"])
#     logging.info(f"[KO parse] stage_counts={debug_counts}")

#     return rounds