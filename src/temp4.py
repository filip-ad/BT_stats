"""Extract the knockout bracket for tournament PDFs published on resultat.ondata.

The utility fetches the first page of the PDF, rebuilds the bracket based on
the positional layout, and prints every round with the corresponding players,
their clubs, optional external three-digit identifiers (``player_id_ext``),
and game tokens.  A PDF URL can be supplied on the command line; otherwise the
Lekstorps Höstpool example is used.
"""

from __future__ import annotations

import argparse
import io
import re
from dataclasses import dataclass
from typing import List, Optional, Sequence, Tuple

import pdfplumber
import requests


DEFAULT_PDF_URL = "https://resultat.ondata.se/ViewClassPDF.php?classID=30021&stage=5"
DEFAULT_PDF_URL = "https://resultat.ondata.se/ViewClassPDF.php?classID=30018&stage=5"
# DEFAULT_PDF_URL = "https://resultat.ondata.se/ViewClassPDF.php?classID=29603&stage=5"

WINNER_LABEL_PATTERN = re.compile(
    r"^(?:(\d{3})\s+)?([\wÅÄÖåäö\-]+(?:\s+[\wÅÄÖåäö\-]+)*)$",
    re.UNICODE,
)


@dataclass
class Player:
    full_name: str
    club: str
    short: str
    center: float
    player_id_ext: Optional[str]


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


def fetch_words(url: str) -> List[dict]:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    with pdfplumber.open(io.BytesIO(response.content)) as pdf:
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
    players: List[Player] = []
    for word in words:
        if float(word["x0"]) < 200 and "," in word["text"]:
            match = re.match(r"\s*(?:(\d{3})\s+)?([^,]+),\s*(.+)", word["text"])
            if not match:
                continue
            player_id_ext, raw_name, raw_club = match.groups()
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
        raw = word["text"].strip().replace("−", "-")
        if "," not in raw:
            continue
        if not re.search(r"\d", raw):
            continue
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
        winners.append(
            WinnerEntry(short=label.strip(), center=to_center(word), player_id_ext=player_id_ext)
        )
    winners.sort(key=lambda w: w.center)
    return winners


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
    tolerance: float = 15.0,
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


def format_player(player: Player) -> str:
    prefix = f"{player.player_id_ext} " if player.player_id_ext else ""
    return f"{prefix}{player.full_name}, {player.club}"


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


def build_round_of_16(
    players: Sequence[Player],
    winners: Sequence[WinnerEntry],
    scores: List[ScoreEntry],
) -> List[Match]:
    remaining_scores = scores.copy()
    matches: List[Match] = []
    for winner_entry in winners:
        participants = [
            player
            for player in players
            if abs(player.center - winner_entry.center) <= 12
        ]
        winner = match_short_to_full(
            winner_entry.short,
            winner_entry.center,
            players,
            winner_entry.player_id_ext,
        )
        match_scores = assign_nearest_score(winner_entry.center, remaining_scores)
        matches.append(
            Match(players=participants, winner=winner, scores=match_scores, center=winner_entry.center)
        )
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
        if first.winner is not None:
            participants.append(first.winner)
        if second and second.winner is not None:
            participants.append(second.winner)
        center_source = [first.center]
        if second is not None:
            center_source.append(second.center)
        center = sum(center_source) / len(center_source)

        winner_entry = assign_nearest_winner(center, remaining_winners, tolerance=winner_tolerance)
        winner: Optional[Player] = None
        if winner_entry is not None:
            winner = match_short_to_full(
                winner_entry.short,
                winner_entry.center,
                players,
                winner_entry.player_id_ext,
            )
        elif len(participants) == 1:
            winner = participants[0]

        match_scores = assign_nearest_score(center, remaining_scores, tolerance=score_tolerance)
        matches.append(
            Match(players=participants, winner=winner, scores=match_scores, center=center)
        )
        idx += 2
    return matches


def build_quarterfinals(
    r16_matches: Sequence[Match],
    scores: List[ScoreEntry],
    players: Sequence[Player],
    next_round_winners: Optional[Sequence[WinnerEntry]] = None,
) -> List[Match]:
    remaining_scores = scores.copy()

    matches: List[Match] = []
    semifinal_players: List[Player] = []
    if next_round_winners:
        semifinal_players = [
            match_short_to_full(w.short, w.center, players, w.player_id_ext)
            for w in next_round_winners
        ]
    pair_count = (len(r16_matches) + 1) // 2
    for idx in range(pair_count):
        first_match = r16_matches[2 * idx]
        second_match = r16_matches[2 * idx + 1] if 2 * idx + 1 < len(r16_matches) else None
        first = first_match.winner or (first_match.players[0] if first_match.players else None)
        second = None
        if second_match:
            second = second_match.winner or (
                second_match.players[0] if second_match.players else None
            )
        if first is None or second is None:
            continue
        participants = [first, second]
        centers = [first_match.center]
        if second_match:
            centers.append(second_match.center)
        center = sum(centers) / len(centers)
        match_scores = assign_nearest_score(center, remaining_scores)
        winner: Optional[Player] = None
        for candidate in semifinal_players:
            if candidate in participants:
                winner = candidate
                break
        if winner is None:
            for candidate in participants:
                if candidate == first_match.winner or (
                    second_match and candidate == second_match.winner
                ):
                    winner = candidate
                    break
        if winner is None:
            winner = participants[0]
        matches.append(Match(players=participants, winner=winner, scores=match_scores, center=center))
    return matches


def build_semifinals(
    quarterfinals: Sequence[Match],
    scores: List[ScoreEntry],
    winners: Sequence[WinnerEntry],
    players: Sequence[Player],
) -> List[Match]:
    remaining_scores = scores.copy()
    mapped_winners = [
        match_short_to_full(w.short, w.center, players, w.player_id_ext) for w in winners
    ]
    matches: List[Match] = []
    for idx in range(2):
        first_match = quarterfinals[2 * idx]
        second_match = quarterfinals[2 * idx + 1]
        first = first_match.winner or (first_match.players[0] if first_match.players else None)
        second = second_match.winner or (second_match.players[0] if second_match.players else None)
        if first is None or second is None:
            continue
        participants = [first, second]
        center = (quarterfinals[2 * idx].center + quarterfinals[2 * idx + 1].center) / 2
        match_scores = assign_nearest_score(center, remaining_scores, tolerance=25.0)
        winner = mapped_winners[idx] if idx < len(mapped_winners) else None
        if winner is None or winner not in participants:
            for candidate in participants:
                if candidate == first_match.winner or candidate == second_match.winner:
                    winner = candidate
                    break
        if winner is None:
            winner = participants[0]
        matches.append(Match(players=participants, winner=winner, scores=match_scores, center=center))
    return matches


def fill_missing_winners(previous_round: Sequence[Match], next_round: Sequence[Match]) -> None:
    advancing_players: List[Player] = [player for match in next_round for player in match.players]
    for match in previous_round:
        if match.winner is None:
            for player in match.players:
                if player in advancing_players:
                    match.winner = player
                    break


def build_final(
    semifinals: Sequence[Match],
    scores: List[ScoreEntry],
    winner_entry: WinnerEntry,
    players: Sequence[Player],
) -> Match:
    participants = [match.winner for match in semifinals]
    participants = [p for p in participants if p is not None]
    center = sum(match.center for match in semifinals) / len(semifinals)
    match_scores = assign_nearest_score(center, scores.copy(), tolerance=40.0)
    winner = match_short_to_full(
        winner_entry.short,
        winner_entry.center,
        players,
        winner_entry.player_id_ext,
    )
    return Match(players=participants, winner=winner, scores=match_scores, center=center)


def label_round(round_name: str, matches: Sequence[Match]) -> None:
    print(f"{round_name}:")
    for match in matches:
        if len(match.players) < 2 and match.players:
            solo = match.players[0]
            print(f"{format_player(solo)} \t\t-> BYE")
            continue
        if len(match.players) < 2:
            print("Unknown participants -> Winner: Unknown")
            continue
        left, right = match.players
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract and print knockout matches from a tournament PDF."
    )
    parser.add_argument(
        "url",
        nargs="?",
        default=DEFAULT_PDF_URL,
        help="Tournament PDF URL (defaults to the Lekstorps Höstpool example).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    words = fetch_words(args.url)
    players = extract_players(words)

    r16_winners = extract_winner_entries(words, (220, 240))
    r16_scores = extract_score_entries(words, (270, 320))
    r16_matches = build_round_of_16(players, r16_winners, r16_scores)

    ro32_matches: Optional[List[Match]] = None
    if len(r16_matches) > 8:
        ro32_matches = r16_matches
        r16_advancing_winners = extract_winner_entries(words, (300, 350))
        r16_advancing_scores = extract_score_entries(words, (320, 380))
        r16_matches = build_round_from_previous(
            ro32_matches,
            r16_advancing_winners,
            r16_advancing_scores,
            players,
        )

    qf_scores = extract_score_entries(words, (330, 410))
    sf_winners = extract_winner_entries(words, (380, 420))
    qf_matches = build_quarterfinals(r16_matches, qf_scores, players, sf_winners)
    fill_missing_winners(r16_matches, qf_matches)
    if ro32_matches:
        fill_missing_winners(ro32_matches, r16_matches)

    sf_scores = extract_score_entries(words, (420, 500))
    semifinals = build_semifinals(qf_matches, sf_scores, sf_winners, players)
    fill_missing_winners(qf_matches, semifinals)

    final_scores = extract_score_entries(words, (500, 560))
    final_winner_entry = extract_winner_entries(words, (460, 520))[0]
    final_match = build_final(semifinals, final_scores, final_winner_entry, players)

    if ro32_matches:
        label_round("RO32", ro32_matches)
        print()
        label_round("RO16", r16_matches)
    else:
        label_round("RO16", r16_matches)
    print()
    label_round("RO8/QF", qf_matches)
    print()
    label_round("RO4/SF", semifinals)
    print()
    label_round("RO2/Final", [final_match])


if __name__ == "__main__":
    main()