"""Extract the knockout bracket for tournament PDFs published on resultat.ondata.

Fetches the first page of a tournament class PDF from OnData, reconstructs
the knockout bracket by spatial layout, and prints every round with players,
clubs, and game scores.
"""

from __future__ import annotations

import argparse
import io
import re
from dataclasses import dataclass
from typing import List, Optional, Sequence, Tuple

import pdfplumber
import requests


# Example fallback if no URL is given
DEFAULT_PDF_URL = "https://resultat.ondata.se/ViewClassPDF.php?classID=29866&stage=5"

# Heuristic x-bands for optional Round-of-64 columns
R64_WINNERS_X = (195, 250)
R64_SCORES_X = (250, 305)

WINNER_LABEL_PATTERN = re.compile(
    r"^(?:(\d{1,3})\s+)?([\wÅÄÖåäö\-]+(?:\s+[\wÅÄÖåäö\-]+)*)$",
    re.UNICODE,
)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

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


@dataclass
class WinnerEntry:
    short: str
    center: float
    player_id_ext: Optional[str]


@dataclass
class Match:
    players: List[Player]
    winner: Optional[Player]
    scores: Optional[Tuple[int, ...]]
    center: float


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def fetch_words(url: str) -> List[dict]:
    """Fetch the first page of the PDF and extract word boxes."""
    headers = {"User-Agent": "Mozilla/5.0 (compatible; BT-Stats/1.0)"}
    response = requests.get(url, timeout=30, headers=headers)
    response.raise_for_status()
    payload = response.content
    if not payload.lstrip().startswith(b"%PDF"):
        snippet = payload[:200].decode("latin-1", errors="replace")
        raise ValueError(
            f"Response from {url} was not a PDF (content-type={response.headers.get('content-type')}); "
            f"payload preview: {snippet!r}"
        )
    with pdfplumber.open(io.BytesIO(payload)) as pdf:
        page = pdf.pages[0]
        return page.extract_words(keep_blank_chars=True)


def to_center(word: dict) -> float:
    return (float(word["top"]) + float(word["bottom"])) / 2


def make_short(name: str) -> str:
    parts = name.split()
    if len(parts) < 2:
        return name
    return f"{parts[0]} {parts[1][0]}"


# ---------------------------------------------------------------------------
# Extraction routines
# ---------------------------------------------------------------------------

def extract_players(words: Sequence[dict]) -> List[Player]:
    players: List[Player] = []
    for word in words:
        if float(word["x0"]) < 200 and "," in word["text"]:
            match = re.match(
                r"\s*(?:(\d{1,3})\s+)?([^,(]+?(?:\s+[^,(]+?)*)(?:\s*\(([^)]+)\))?,\s*(.+)",
                word["text"],
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


def extract_score_entries(words: Sequence[dict], x_range: Tuple[float, float]) -> List[ScoreEntry]:
    start, stop = x_range
    entries: List[ScoreEntry] = []
    for word in words:
        x0 = float(word["x0"])
        if not (start <= x0 <= stop):
            continue
        raw = word["text"]
        match = re.match(r"(-?\d+(?:\s*,\s*-?\d+)*)", raw)
        if not match:
            continue
        scores = tuple(int(token) for token in re.findall(r"-?\d+", match.group(1)))
        entries.append(ScoreEntry(scores=scores, center=to_center(word)))
    entries.sort(key=lambda e: e.center)
    return entries


def extract_winner_entries(words: Sequence[dict], x_range: Tuple[float, float]) -> List[WinnerEntry]:
    start, stop = x_range
    winners: List[WinnerEntry] = []
    for word in words:
        x0 = float(word["x0"])
        if not (start <= x0 <= stop):
            continue
        text = word["text"].replace("\xa0", " ").strip()
        if not text:
            continue
        if any(token in text for token in ("Slutspel", "Höstpool", "program")):
            continue
        match = WINNER_LABEL_PATTERN.match(text)
        if not match:
            continue
        player_id_ext, label = match.groups()
        if len(label.split()) < 2:
            continue
        if any(char.isdigit() for char in label):
            continue
        winners.append(
            WinnerEntry(short=label.strip(), center=to_center(word), player_id_ext=player_id_ext)
        )
    winners.sort(key=lambda w: w.center)
    return winners


# ---------------------------------------------------------------------------
# Core mapping and matching
# ---------------------------------------------------------------------------

def match_short_to_full(
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
    raise ValueError(f"No player match for {short!r}")


def format_player(player: Player) -> str:
    if not player:
        return "Unknown"
    prefix = f"[{player.player_id_ext}] " if player.player_id_ext else ""
    suffix = f"[{player.player_suffix_id}]" if player.player_suffix_id else ""
    return f"{prefix}{player.full_name}{suffix}, {player.club}"


def assign_nearest_score(
    center: float, pool: List[ScoreEntry], tolerance: float = 15.0
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
    return pool.pop(best_index).scores


def assign_nearest_winner(
    center: float, pool: List[WinnerEntry], tolerance: float = 15.0
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


# ---------------------------------------------------------------------------
# Round builders
# ---------------------------------------------------------------------------

def build_round_of_16(
    players: Sequence[Player],
    winners: Sequence[WinnerEntry],
    scores: List[ScoreEntry],
    winner_tolerance: float = 12.0,
) -> List[Match]:
    remaining_scores = scores.copy()
    matches: List[Match] = []
    for winner_entry in winners:
        participants = [
            player
            for player in players
            if abs(player.center - winner_entry.center) <= winner_tolerance
        ]
        winner: Optional[Player] = None
        try:
            winner = match_short_to_full(
                winner_entry.short,
                winner_entry.center,
                players,
                winner_entry.player_id_ext,
            )
        except ValueError:
            pass
        match_scores = assign_nearest_score(winner_entry.center, remaining_scores)
        matches.append(Match(players=participants, winner=winner, scores=match_scores, center=winner_entry.center))
    return matches


def build_round_from_previous(
    previous_round: Sequence[Match],
    winners: Sequence[WinnerEntry],
    scores: List[ScoreEntry],
    players: Sequence[Player],
    winner_tolerance: float = 18.0,
    score_tolerance: float = 20.0,
) -> List[Match]:
    remaining_scores = scores.copy()
    remaining_winners = list(winners)
    matches: List[Match] = []
    idx = 0
    while idx < len(previous_round):
        first = previous_round[idx]
        second = previous_round[idx + 1] if idx + 1 < len(previous_round) else None
        participants: List[Player] = []
        if first.winner:
            participants.append(first.winner)
        if second and second.winner:
            participants.append(second.winner)
        center = (first.center + (second.center if second else first.center)) / 2
        nearest_winner = assign_nearest_winner(center, remaining_winners, winner_tolerance)
        nearest_score = assign_nearest_score(center, remaining_scores, score_tolerance)
        winner = None
        if nearest_winner:
            try:
                winner = match_short_to_full(
                    nearest_winner.short,
                    nearest_winner.center,
                    players,
                    nearest_winner.player_id_ext,
                )
            except ValueError:
                pass
        matches.append(Match(players=participants, winner=winner, scores=nearest_score, center=center))
        idx += 2
    return matches


def build_quarterfinals(r16_matches, qf_scores, players, sf_winners):
    return build_round_from_previous(r16_matches, sf_winners, qf_scores, players)


def build_semifinals(qf_matches, sf_scores, sf_winners, players):
    return build_round_from_previous(qf_matches, sf_winners, sf_scores, players)


def build_final(sf_matches, final_scores, final_winner_entry, players):
    winners = [final_winner_entry]
    return build_round_from_previous(sf_matches, winners, final_scores, players)[0]


def fill_missing_winners(source_round, target_round):
    """Propagate winners if target round lacks explicit ones."""
    for src, tgt in zip(source_round, target_round):
        if not tgt.players and src.winner:
            tgt.players.append(src.winner)


# ---------------------------------------------------------------------------
# Printing / labeling
# ---------------------------------------------------------------------------

def label_round(round_name: str, matches: Sequence[Match]) -> None:
    print(f"===== {round_name} =====")
    for match in matches:
        if len(match.players) < 2:
            continue
        left, right = match.players
        winner_label = (
            f"Winner: {format_player(match.winner)}"
            if match.winner
            else "Winner: Unknown"
        )
        score_label = (
            f" | Scores: {match.scores}"
            if match.scores
            else ""
        )
        print(f"{format_player(left)} vs {format_player(right)} -> {winner_label}{score_label}")


# ---------------------------------------------------------------------------
# CLI / main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract and print knockout matches from a tournament PDF."
    )
    parser.add_argument(
        "url",
        nargs="?",
        default=DEFAULT_PDF_URL,
        help="Tournament PDF URL (defaults to an example class).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    words = fetch_words(args.url)
    players = extract_players(words)

    r16_winners = extract_winner_entries(words, R64_WINNERS_X)
    r16_scores = extract_score_entries(words, (270, 320))

    r64_matches: Optional[List[Match]] = None
    ro32_matches: Optional[List[Match]] = None
    r16_matches: List[Match]

    # Detect whether the leftmost column corresponds to RO32 or RO64.
    if len(r16_winners) > 16:
        r64_scores = extract_score_entries(words, R64_SCORES_X)
        r64_matches = build_round_of_16(players, r16_winners, r64_scores or r16_scores, winner_tolerance=18.0)
        r32_winners = extract_winner_entries(words, (300, 350))
        r32_scores = extract_score_entries(words, (320, 380))
        ro32_matches = build_round_from_previous(
            r64_matches, r32_winners, r32_scores, players, winner_tolerance=18.0
        )
        r16_progress_winners = extract_winner_entries(words, (360, 410)) or extract_winner_entries(words, (350, 420))
        r16_progress_scores = extract_score_entries(words, (360, 420))
        r16_matches = build_round_from_previous(
            ro32_matches, r16_progress_winners, r16_progress_scores, players
        )
    else:
        r16_matches = build_round_of_16(players, r16_winners, r16_scores)

    if len(r16_matches) > 8 and ro32_matches is None:
        ro32_matches = r16_matches
        r16_advancing_winners = extract_winner_entries(words, (300, 350))
        r16_advancing_scores = extract_score_entries(words, (320, 380))
        r16_matches = build_round_from_previous(ro32_matches, r16_advancing_winners, r16_advancing_scores, players)

    qf_scores = extract_score_entries(words, (330, 410))
    sf_winners = extract_winner_entries(words, (380, 430 if r64_matches else 420))
    qf_matches = build_quarterfinals(r16_matches, qf_scores, players, sf_winners)
    fill_missing_winners(r16_matches, qf_matches)
    if ro32_matches:
        fill_missing_winners(ro32_matches, r16_matches)
    if r64_matches and ro32_matches:
        fill_missing_winners(r64_matches, ro32_matches)

    sf_scores = extract_score_entries(words, (420, 500))
    semifinals = build_semifinals(qf_matches, sf_scores, sf_winners, players)
    fill_missing_winners(qf_matches, semifinals)

    final_scores = extract_score_entries(words, (500, 560))
    final_winner_list = extract_winner_entries(words, (460, 520)) or extract_winner_entries(words, (520, 600))
    final_match = build_final(semifinals, final_scores, final_winner_list[0], players)

    if r64_matches:
        label_round("RO64", r64_matches)
        print()
    if ro32_matches:
        label_round("RO32", ro32_matches)
        print()
    label_round("RO16", r16_matches)
    print()
    label_round("RO8/QF", qf_matches)
    print()
    label_round("RO4/SF", semifinals)
    print()
    label_round("RO2/Final", [final_match])


if __name__ == "__main__":
    main()
