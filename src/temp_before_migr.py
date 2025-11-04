from __future__ import annotations

import hashlib
import io
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Set, Tuple

import pdfplumber
import requests

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
from utils import parse_date
from models.tournament_class import TournamentClass
from db import get_conn

# Manual toggles used during ad-hoc testing (last assignment wins)
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['29622']           # RO8 test
SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['30021']           # RO16 test
SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['29866']           # Qualification + RO16 test
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['29625']           # RO32 test
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['1006']            # RO64 test
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['6955']          # RO128 test
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['25395']           # RO16 but missing ko_tree_size

# Map url -> md5 hash so repeated runs can detect PDF changes.
LAST_PDF_HASHES: Dict[str, str] = {}

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
R64_WINNERS_X = (195, 250) # was (170, 210)
R64_SCORES_X = (250, 305) # was (210, 260)

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


def main() -> None:
    """Orchestrate the ad-hoc knockout parser pipeline for the configured classes."""

    # Step 1: Establish the database cursor used to filter relevant classes.
    conn, cursor = get_conn()
    try:
        cursor = conn.cursor()
        cutoff_date = parse_date(SCRAPE_PARTICIPANTS_CUTOFF_DATE) if SCRAPE_PARTICIPANTS_CUTOFF_DATE else None
        classes = TournamentClass.get_filtered_classes(
            cursor = cursor,
            class_id_exts = SCRAPE_PARTICIPANTS_CLASS_ID_EXTS,
            tournament_id_exts = SCRAPE_PARTICIPANTS_TNMT_ID_EXTS,
            data_source_id = 1 if (SCRAPE_PARTICIPANTS_CLASS_ID_EXTS or SCRAPE_PARTICIPANTS_TNMT_ID_EXTS) else None,
            cutoff_date = cutoff_date,
            require_ended = False,
            allowed_structure_ids = [1, 3], # Groups+KO or KO-only
            allowed_type_ids = [1], # singles
            max_classes = SCRAPE_PARTICIPANTS_MAX_CLASSES,
            order = SCRAPE_PARTICIPANTS_ORDER,
        )
        print(f"Found {len(classes)} classes\n")

        # Step 2: Iterate over each configured class and parse its bracket.
        for tclass in classes:
            ext = tclass.tournament_class_id_ext
            if not ext:
                print(f"⏭️ Skipping class_id={tclass.tournament_class_id}: no tournament_class_id_ext")
                continue
            url = f"https://resultat.ondata.se/ViewClassPDF.php?classID={ext}&stage=5"
            print(f"===== {tclass.shortname or tclass.longname} [ext={ext}] =====")
            print(f"URL: {url}\n")
            try:
                # Step 2a: Download the PDF and extract the raw word layer.
                words = fetch_words(url)
                # Step 2b: Parse participants from the left-most column.
                players = extract_players(words)
                qual_header = _find_qualification_header(words)
                if qual_header:
                    qual_center = (float(qual_header["top"]) + float(qual_header["bottom"])) / 2
                    players = [p for p in players if p.center < qual_center + 5]
                # Step 2c: Pre-compute grouped winner and score entries for later mapping.
                all_scores_page = sorted(extract_score_entries(words, (0, 10000)), key=lambda s: (s.x, s.center))
                all_winners_page = _deduplicate_winner_entries(extract_winner_entries(words, (0, 10000)))
                all_winners_page.sort(key=lambda w: (w.x, w.center))
                score_bands_page = _cluster_columns([s.x for s in all_scores_page])
                winner_bands_page = _cluster_columns([w.x for w in all_winners_page])

                total_winners = len(all_winners_page)
                if total_winners == 0:
                    print("No winner labels detected on page\n")
                    continue

                # Step 3: Determine the bracket size based on configured metadata.
                tree_size = int(tclass.ko_tree_size or 0)
                fallback_tree_size_used = False
                if tree_size < 2:
                    # ko_tree_size missing (or suspicious) – infer from number of winner labels
                    fallback_tree_size_used = True
                    tree_size = 2
                    while tree_size - 1 < total_winners and tree_size <= 512:
                        tree_size *= 2

                # Step 4: Build the expected number of matches per round.
                round_sizes: List[int] = []
                matches_in_round = tree_size // 2
                while matches_in_round >= 1:
                    round_sizes.append(matches_in_round)
                    matches_in_round //= 2

                # Step 5: Slice detected winners into round-specific buckets.
                winners_by_round: List[List[WinnerEntry]] = []
                cursor = 0
                remaining = total_winners
                for idx, expected in enumerate(round_sizes):
                    rounds_left = len(round_sizes) - idx
                    min_needed_for_rest = max(0, (rounds_left - 1))
                    take = min(expected, remaining - min_needed_for_rest)
                    if take <= 0:
                        break
                    chunk = all_winners_page[cursor:cursor + take]
                    winners_by_round.append(chunk)
                    cursor += take
                    remaining -= take
                if cursor < total_winners:
                    if winners_by_round:
                        winners_by_round[-1].extend(all_winners_page[cursor:])
                    else:
                        winners_by_round.append(all_winners_page[cursor:])

                all_rounds: List[List[Match]] = []
                previous_round: Optional[List[Match]] = None
                available_score_bands = list(score_bands_page)
                score_entries_pool = list(all_scores_page)

                # Step 6: Walk each round and stitch players, winners, and scores together.
                round_winner_entries: List[List[WinnerEntry]] = []
                for idx, winner_chunk in enumerate(winners_by_round):
                    if not winner_chunk:
                        continue
                    round_winner_entries.append(list(winner_chunk))
                    win_min = min(w.x for w in winner_chunk)
                    win_max = max(w.x for w in winner_chunk)
                    winner_band = (win_min, win_max)
                    score_band = find_closest_score_band(available_score_bands, winner_band)
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
                    tolerance_step = idx
                    if previous_round is None:
                        current_round, leftover_scores = build_first_round(
                            players,
                            winner_chunk,
                            scores_for_round,
                            score_window,
                        )
                        remaining_ids = {id(entry) for entry in leftover_scores}
                        consumed = [entry for entry in original_scores if id(entry) not in remaining_ids]
                    else:
                        current_round, _, leftover_scores = build_next_round(
                            previous_round,
                            winner_chunk,
                            scores_for_round,
                            players,
                            score_window,
                            winner_tolerance=24.0 + 4.0 * tolerance_step,
                            score_tolerance=28.0 + 4.0 * tolerance_step,
                        )
                        fill_missing_winners(previous_round, current_round)
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

                # Step 7: Create a synthetic final if the last round still contains multiple matches.
                if all_rounds and len(all_rounds[-1]) > 1:
                    semifinals = all_rounds[-1]
                    final_center_y = sum(m.center for m in semifinals) / len(semifinals)
                    final_winner_candidates = [
                        w
                        for w in all_winners_page
                        if winner_bands_page and w.x >= winner_bands_page[-1][1] - 1.0
                    ]
                    final_winner_entry = assign_nearest_winner(final_center_y, final_winner_candidates, tolerance=45.0)
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
                        final_scores = assign_nearest_score(final_center_y, final_scores_candidates, tolerance=40.0)
                    final_participants = [m.winner for m in semifinals if m.winner]
                    final_winner: Optional[Player] = None
                    if final_winner_entry is not None:
                        try:
                            final_winner = match_short_to_full(
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
                    fill_missing_winners(semifinals, [final_match])
                    all_rounds.append([final_match])
                    round_winner_entries.append(final_winner_candidates)
                # Step 8: Reconcile semifinal winners with the final column if present.
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
                            player = match_short_to_full(
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
                # Step 9: Attach any leftover score blobs to the closest incomplete matches.
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
                # Step 10: Backfill winners from later rounds into earlier matches.
                for ridx in range(len(all_rounds) - 2, -1, -1):
                    fill_missing_winners(all_rounds[ridx], all_rounds[ridx + 1])
                # Step 11: Refresh final participants from semifinal results when possible.
                if len(all_rounds) >= 2 and len(all_rounds[-1]) == 1:
                    semifinal_round = all_rounds[-2]
                    finalists = [match.winner for match in semifinal_round if match.winner]
                    if len(finalists) == 2:
                        all_rounds[-1][0].players = finalists
                # Step 12: Parse and annotate any qualification matches beneath the bracket.
                qualification = extract_qualification_matches(words)
                # Qualification assign using first KO round
                if qualification and all_rounds:
                    assign_qualification_winners_by_presence(qualification, all_rounds[0])
                    label_round("Qualification", qualification)
                    print()
                validate_bracket(
                    url,
                    tclass,
                    all_rounds,
                    players,
                    tree_size,
                    list(score_entries_pool),
                    fallback_tree_size_used,
                    qualification,
                )
                # Step 13: Emit a human-readable summary of each parsed round.
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
                    label_round(name, matches)
                    print()
                # Step 14: Compare parsed bracket size with the stored metadata for a quick health check.
                parsed_size = len(all_rounds[0]) * 2 if all_rounds else 0
                stored = tclass.ko_tree_size
                if stored is None:
                    print(f"\nℹ️ SELF-CHECK: DB ko_tree_size=NULL | parsed={parsed_size}\n")
                elif int(stored) == int(parsed_size):
                    print(f"\n✅ SELF-CHECK: parsed={parsed_size} == stored={stored}\n")
                else:
                    print(f"\n⚠️ SELF-CHECK: parsed={parsed_size} != stored={stored}\n")
            except Exception as e:
                print(f"❌ Parse error for {tclass.shortname or tclass.longname} [ext={ext}]: {e}\n")
                continue
    finally:
        conn.close()

def _player_key(player: Player) -> Tuple[Optional[str], str]:
    return (player.player_id_ext, player.full_name)


def fetch_words(url: str) -> List[dict]:
    """Download the PDF page and extract its word boxes, recording a hash."""

    response = requests.get(url, timeout=30)
    response.raise_for_status()
    content = response.content
    LAST_PDF_HASHES[url] = hashlib.md5(content).hexdigest()
    with pdfplumber.open(io.BytesIO(content)) as pdf:
        page = pdf.pages[0]
        return page.extract_words(keep_blank_chars=True)

def to_center(word: dict) -> float:
    return (float(word["top"]) + float(word["bottom"])) / 2

def make_short(name: str) -> str:
    parts = name.split()
    if len(parts) < 2:
        return name
    return f"{parts[0]} {parts[1][0]}"

def extract_players(words: Sequence[dict]) -> List[Player]:
    """Parse left-column “Name, Club” lines into Player objects."""
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
                    short=make_short(full_name),
                    center=to_center(word),
                    player_id_ext=player_id_ext,
                    player_suffix_id=None,
                )
            )
    players.sort(key=lambda p: p.center)
    return players

def _split_score_and_label(text: str) -> Tuple[Optional[str], str]:
    """Split a combined "scores + winner label" blob into its parts.
    Returns a tuple of (score_text or None, label_text). Both parts are
    stripped and have any draw prefixes removed.
    """
    cleaned = text.replace("\xa0", " ")
    cleaned = _strip_draw_prefix(cleaned)
    match = COMBINED_SCORE_LABEL_RE.match(cleaned)
    if match:
        score_text, label_text = match.groups()
        return score_text.strip(), label_text.strip()
    return None, cleaned.strip()

def extract_score_entries(words: Sequence[dict], x_range: Tuple[float, float]) -> List[ScoreEntry]:
    """Collect score blobs that fall inside the provided horizontal band."""
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
        entries.append(ScoreEntry(scores=tuple(nums), center=to_center(word), x=x0))
    entries.sort(key=lambda e: e.center)
    return entries

def extract_winner_entries(words: Sequence[dict], x_range: Tuple[float, float]) -> List[WinnerEntry]:
    """Collect winner labels from a column while skipping headings/noise."""
    start, stop = x_range
    winners: List[WinnerEntry] = []
    for word in words:
        x0 = float(word["x0"])
        if not (start <= x0 <= stop):
            continue
        text = word["text"].replace("\xa0", " ").strip()
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
        # skip obvious non-names
        if any(ch.isdigit() for ch in label):
            continue
        if " " not in label: # <-- NEW: drops 'Damsingel', 'Final', etc.
            continue
        winners.append(WinnerEntry(short=label.strip(),
                                   center=to_center(word),
                                   x=x0,
                                   player_id_ext=player_id_ext))
    winners.sort(key=lambda w: w.center)
    return winners

def match_short_to_full(
    short: str,
    center: float,
    players: Sequence[Player],
    player_id_ext: Optional[str] = None,
) -> Player:
    """Match a short winner label back to the canonical Player entry."""
    normalized = short.strip()
    if player_id_ext:
        id_candidates = [p for p in players if p.player_id_ext == player_id_ext]
        if id_candidates:
            return min(id_candidates, key=lambda p: abs(p.center - center))
    candidates = [p for p in players if p.short == normalized]
    if candidates:
        return min(candidates, key=lambda p: abs(p.center - center))
    alt_short = make_short(normalized)
    if alt_short != normalized:
        candidates = [p for p in players if p.short == alt_short]
        if candidates:
            return min(candidates, key=lambda p: abs(p.center - center))
    prefix_matches = [p for p in players if p.full_name.startswith(normalized)]
    if prefix_matches:
        return min(prefix_matches, key=lambda p: abs(p.center - center))
    raise ValueError(f"No player matches label {short!r}")

def assign_nearest_score(
    center: float,
    pool: List[ScoreEntry],
    tolerance: float = 20.0
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
    if best_delta is None or best_delta > tolerance:
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

def format_player(player: Player) -> str:
    prefix = f"[{player.player_id_ext}] " if player.player_id_ext else ""
    suffix = f"[{player.player_suffix_id}]" if player.player_suffix_id else ""
    return f"{prefix}{player.full_name}{suffix}, {player.club}"

def assign_nearest_winner(
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
    """Prefer winner labels that map cleanly to one of the participants."""
    best_idx: Optional[int] = None
    best_delta: Optional[float] = None
    for idx, entry in enumerate(pool):
        try:
            matched = match_short_to_full(
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
    return assign_nearest_winner(center, pool, tolerance)

def cluster_players_(players, max_gap=20.0) -> List[List[Player]]:
    if not players:
        return []
    players = sorted(players, key=lambda p: p.center)
    groups = []
    current = [players[0]]
    for p in players[1:]:
        if p.center - current[-1].center > max_gap:
            groups.append(current)
            current = [p]
        else:
            current.append(p)
    groups.append(current)
    return groups

def build_first_round(
    players: Sequence[Player],
    winners: Sequence[WinnerEntry],
    scores: Sequence[ScoreEntry],
    score_window: Tuple[Optional[float], Optional[float]],
) -> Tuple[List[Match], List[ScoreEntry]]:
    """Attach players to the earliest bracket round.

    We align players to winner labels using vertical proximity, allowing
    single-player BYE matches when only one entrant sits below a winner.
    """
    score_min, score_max = score_window
    remaining_scores = list(scores)
    matches: List[Match] = []

    # Step 1: Sort winners and players by their vertical position for stable matching.
    sorted_winners = sorted(winners, key=lambda w: w.center)
    sorted_players = sorted(players, key=lambda p: p.center)
    used_indices: set[int] = set()

    # Step 2: For each winner entry, collect neighbouring players within the band.
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
            # Step 2a: Fall back to the nearest unused players when no one is in-band.
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
            # Step 2b: Try to promote a nearby second player so the match is complete.
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

        # Step 3: Resolve the winner label back to a canonical player record.
        winner: Optional[Player] = None
        try:
            winner = match_short_to_full(
                winner_entry.short,
                winner_entry.center,
                participants or players,
                winner_entry.player_id_ext,
            )
        except ValueError:
            try:
                winner = match_short_to_full(
                    winner_entry.short,
                    winner_entry.center,
                    players,
                    winner_entry.player_id_ext,
                )
            except ValueError:
                winner = participants[0] if participants else None

        if winner is None and len(participants) == 1:
            winner = participants[0]

        # Step 4: Attach the score blob that lines up with this match row.
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
                match_scores = assign_nearest_score(
                    winner_entry.center,
                    remaining_scores,
                    tolerance=30.0,
                )

        # Step 5: Store the assembled match so later rounds can reference it.
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

def build_next_round(
    previous_round: Sequence[Match],
    winners: Sequence[WinnerEntry],
    scores: List[ScoreEntry],
    players: Sequence[Player],
    score_window: Tuple[Optional[float], Optional[float]],
    winner_tolerance: float = 18.0,
    score_tolerance: float = 20.0,
) -> Tuple[List[Match], List[WinnerEntry], List[ScoreEntry]]:
    """Pair winners from the previous round and attach the next winner/score column."""
    score_min, score_max = score_window
    remaining_scores = list(scores)
    remaining_winners = sorted(winners, key=lambda w: w.center)
    matches: List[Match] = []

    # Step 1: Sort previous winners so we can stitch them into the next column.
    ordered_prev = sorted(previous_round, key=lambda m: m.center)
    pair_count = (len(ordered_prev) + 1) // 2

    # Step 2: Pair up consecutive previous-round matches to form the new bracket row.
    for idx in range(pair_count):
        first_match = ordered_prev[2 * idx]
        second_match = ordered_prev[2 * idx + 1] if 2 * idx + 1 < len(ordered_prev) else None

        participants: List[Player] = []
        centers: List[float] = []

        # Step 2a: Seed the participant list with the winners from each source match.
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

        # Step 3: Identify the winner label closest to the combined centerline.
        winner_entry = _pop_matching_winner(center, participants, remaining_winners, winner_tolerance)
        if winner_entry is None and remaining_winners:
            winner_entry = min(remaining_winners, key=lambda w: abs(w.center - center))
            remaining_winners.remove(winner_entry)

        # Step 4: Map the winner label back to a participant (falling back when needed).
        winner: Optional[Player] = None
        if winner_entry is not None:
            try:
                winner = match_short_to_full(
                    winner_entry.short,
                    winner_entry.center,
                    participants,
                    winner_entry.player_id_ext,
                )
            except ValueError:
                try:
                    candidate = match_short_to_full(
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

        # Step 5: Pull the score blob that best aligns with the merged match row.
        match_scores = _pop_score_aligned(
            remaining_scores,
            center,
            score_tolerance,
            min_x=score_min,
            max_x=score_max,
        )
        if match_scores is None:
            match_scores = assign_nearest_score(center, remaining_scores, tolerance=score_tolerance)

        # Step 6: Capture the match so downstream rounds can consume it.
        matches.append(Match(players=participants, winner=winner, scores=match_scores, center=center))

    matches.sort(key=lambda m: m.center)
    return matches, remaining_winners, remaining_scores

def fill_missing_winners(previous_round: Sequence[Match], next_round: Sequence[Match]) -> None:
    """Ensure every previous-round match has its winner pointer set once the next round is known."""
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

def label_round(name: str, matches: Sequence[Match]) -> None:
    print(f"{name}:")
    for match in matches:
        if len(match.players) < 2 and match.players:
            solo = match.players[0]
            print(f"{format_player(solo)} \t\t-> BYE")
            continue
        if len(match.players) < 2:
            print("Unknown participants -> Winner: Unknown")
            continue
        left = match.players[0]
        right = match.players[1] if len(match.players) > 1 else None
        if right is None:
            print(f"{format_player(left)} \t\t-> Winner: {format_player(match.winner) if match.winner else 'Unknown'} {'-> Game tokens: ' + str(match.scores) if match.scores else ''}")
            continue
        winner_label = (
            f"Winner: {format_player(match.winner)}"
            if match.winner
            else "Winner: Unknown"
        )
        score_label = (
            f" -> Game tokens: {match.scores}"
            if match.scores is not None
            else ""
        )
        print(
            f"{format_player(left)} vs {format_player(right)} \t-> "
            f"{winner_label}{score_label}"
        )
DRAW_PREFIX_RE = re.compile(r"^\s*\d+\s*[>\.\)\-]\s*") # e.g. "1>", "2)", "3.", "4-"

def _strip_draw_prefix(text: str) -> str:
    """Remove a leading draw index like '1>' before parsing the player line."""
    return DRAW_PREFIX_RE.sub("", text.strip())

def _cluster_columns(xs: List[float], max_gap: float = 25.0) -> List[Tuple[float, float]]:
    """
    Group sorted x-positions into vertical bands; a new band starts when the gap exceeds max_gap.
    Returns a list of (x_start, x_end) bands from left to right.
    """
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

def find_closest_score_band(score_bands: List[Tuple[float, float]], winner_band: Tuple[float, float]) -> Tuple[float, float]:
    candidates = [sb for sb in score_bands if sb[0] > winner_band[0]]
    if not candidates:
        return (winner_band[1], winner_band[1] + 50)  # fallback
    return min(candidates, key=lambda sb: sb[0] - winner_band[1])


def _round_display_name(match_count: int) -> str:
    """Return a human readable round name given the number of matches."""
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


def _infer_best_of_from_scores(scores: Sequence[int]) -> Optional[int]:
    """Infer `best-of` from signed score tokens (positive => player 1 win)."""

    if not scores:
        return None
    p1_wins = sum(1 for value in scores if value >= 0)
    p2_wins = sum(1 for value in scores if value < 0)
    wins = max(p1_wins, p2_wins)
    if wins == 0:
        return None
    return 2 * wins - 1


def validate_bracket(
    url: str,
    tclass: TournamentClass,
    rounds: Sequence[List[Match]],
    players: Sequence[Player],
    tree_size: int,
    score_entries_remaining: Sequence[ScoreEntry],
    fallback_tree_size_used: bool,
    qualification: Sequence[Match],
) -> None:
    """Lightweight validation so suspicious brackets are easy to spot."""

    if not rounds:
        print("[WARN] No rounds parsed for this class; skipping validation.")
        return

    pdf_hash = LAST_PDF_HASHES.get(url)
    if pdf_hash:
        print(f"[INFO] PDF hash for {url}: {pdf_hash}")
    if fallback_tree_size_used:
        print("[INFO] ko_tree_size missing – fallback tree size inferred from winner labels.")

    # Step 1: Validate round coverage (expected match count per round).
    for idx, matches in enumerate(rounds):
        expected = max(tree_size // (2 ** (idx + 1)), 1)
        actual = len(matches)
        if actual != expected:
            print(
                f"[WARN] {tclass.shortname or tclass.longname}: expected {expected} matches "
                f"in {_round_display_name(actual)} (round {idx}), parsed {actual}."
            )

    # Step 2: Check player participation for duplicates or missing entrants.
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
        print(f"[WARN] Duplicate player entries detected in first round: {sample}")

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
        print(f"[WARN] {len(missing_players)} player(s) from the left column never appear in the bracket: {sample}")

    # Step 3: Flag matches that are missing scores or winners.
    for idx, matches in enumerate(rounds):
        round_name = _round_display_name(len(matches))
        best_of_values: Set[int] = set()
        for match in matches:
            if len(match.players) == 2 and match.players[0] != match.players[1]:
                if match.scores is None:
                    print(
                        f"[WARN] {round_name}: missing score for {match.players[0].full_name} vs "
                        f"{match.players[1].full_name}"
                    )
                else:
                    inferred = _infer_best_of_from_scores(match.scores)
                    if inferred is not None:
                        best_of_values.add(inferred)
            if len(match.players) == 2 and match.winner is None:
                print(
                    f"[WARN] {round_name}: missing winner for {match.players[0].full_name} vs "
                    f"{match.players[1].full_name}"
                )
        if len(best_of_values) > 1:
            print(f"[WARN] {round_name}: inconsistent best-of detected {sorted(best_of_values)}")

    # Step 4: Confirm that winners propagate forward between rounds (skip final).
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
            print(f"[WARN] Winners not found in next round: {sample}")

        stray_participants = {key for key in next_participants if key not in winner_keys}
        if stray_participants and not next_is_final:
            sample = ", ".join(sorted({name for _, name in stray_participants})[:4])
            print(f"[WARN] Participants in next round without recorded wins: {sample}")

    # Step 5: Report any score entries that were never consumed by the bracket.
    remaining_scores = list(score_entries_remaining)
    for qual_match in qualification:
        if qual_match.scores is None:
            continue
        for idx, entry in enumerate(remaining_scores):
            if entry.scores == qual_match.scores:
                remaining_scores.pop(idx)
                break
    if remaining_scores:
        print(
            f"[WARN] {len(remaining_scores)} score token(s) were not attached to any match. "
            "Example: "
            + ", ".join(
                f"x={round(entry.x,1)} y={round(entry.center,1)} {entry.scores}"
                for entry in remaining_scores[:3]
            )
        )

# --- Qualification detection ---
# Nordic-ish header variants (case-insensitive)
QUAL_HEADER_RE = re.compile(
    r"\b(kval(?:ifikation|ificering|ifisering)?|karsinta)\b", re.IGNORECASE
)

def _find_qualification_header(words: Sequence[dict]) -> Optional[dict]:
    """Return the word dict that looks like a 'Qualification' header."""
    # Prefer bold-ish / larger fonts if present; otherwise first match.
    candidates = []
    for w in words:
        txt = w.get("text", "").replace("\xa0", " ").strip()
        if not txt:
            continue
        if QUAL_HEADER_RE.search(txt):
            # pdfplumber sometimes exposes fontname/size; not always.
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
        # must have letters before the comma (avoid "7, 7, 4, 9")
        left = txt.split(",", 1)[0]
        if not re.search(r"[A-Za-zÅÄÖåäö]", left):
            continue
        # NEW: strip draw prefix if present
        cleaned = _strip_draw_prefix(txt)
        m = re.match(
            r"\s*(?:(\d{1,3})\s+)?([^,(]+?(?:\s+[^,(]+?)*)(?:\s*\(([^)]+)\))?,\s*(.+)",
            cleaned, # <-- use the cleaned string as the SUBJECT
        )
        if not m:
            continue
        player_id_ext, raw_name, player_suffix_id, raw_club = m.groups()
        full_name = raw_name.strip()
        out.append(
            Player(
                full_name=full_name,
                club=raw_club.strip(),
                short=make_short(full_name),
                center=y,
                player_id_ext=player_id_ext.strip() if player_id_ext else None,
                # Drop any parenthetical suffix entirely
                player_suffix_id=None,
            )
        )
    out.sort(key=lambda p: p.center)
    return out

def assign_qualification_winners_by_presence(qualification: List[Match], ko_rounds: Sequence[Match]) -> None:
    """If exactly one of the two players appears in the KO tree participants, mark them as winner."""
    # Step 1: Build identity sets from knockout rounds for quick lookup.
    ko_by_id = {p.player_id_ext for m in ko_rounds for p in m.players if p and p.player_id_ext}
    ko_by_name = {p.full_name for m in ko_rounds for p in m.players if p}
    for m in qualification:
        if len(m.players) != 2:
            continue
        a, b = m.players
        a_in = (a.player_id_ext and a.player_id_ext in ko_by_id) or (a.full_name in ko_by_name) or (a.short in {p.short for mm in ko_rounds for p in mm.players})
        b_in = (b.player_id_ext and b.player_id_ext in ko_by_id) or (b.full_name in ko_by_name) or (b.short in {p.short for mm in ko_rounds for p in mm.players})
        if a_in and not b_in:
            m.winner = a
        elif b_in and not a_in:
            m.winner = b
        # Step 2: Leave the winner as unknown when both or neither players appear in KO rounds.

def extract_qualification_matches(words: Sequence[dict]) -> List[Match]:
    """Find a 'Qualification' section; pair adjacent player lines as matches.
    Winner is unknown (underline not in text layer). Scores assigned by proximity."""
    header = _find_qualification_header(words)
    if not header:
        return []
    # Set a vertical window below the header; generous height to catch variants.
    header_center = (float(header["top"]) + float(header["bottom"])) / 2
    y_min = header_center + 5
    y_max = header_center + 250 # big enough for a handful of qual matches
    # Pull any player-like lines inside this band (independent of x)
    qual_players = _extract_player_like_in_band(words, y_min, y_max)
    if len(qual_players) < 2:
        return []
    # Get all score entries from the whole page (more robust than x-bands)
    all_scores = extract_score_entries(words, (0, 10000)) # any x; we’ll use proximity
    matches: List[Match] = []
    idx = 0
    while idx + 1 < len(qual_players):
        a = qual_players[idx]
        b = qual_players[idx + 1]
        center = (a.center + b.center) / 2.0
        sc = assign_nearest_score(center, all_scores, tolerance=50.0)
        matches.append(Match(players=[a, b], winner=None, scores=sc, center=center))
        idx += 2
    return matches
# -------------------------------------------------------------------
# Batch runner:
# - uses TournamentClass.get_filtered_classes(...)
# - builds PDF URL per class
# - prints SELF-CHECK only (parsed vs stored)
# -------------------------------------------------------------------

if __name__ == "__main__":
    main()
