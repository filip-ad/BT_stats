from __future__ import annotations
import io
import re
from dataclasses import dataclass
from typing import List, Optional, Sequence, Tuple
import pdfplumber
import requests
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
SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['29622']           # RO8 test
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['30021'] # RO16 test
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['29625'] # RO32 test
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['1006'] # RO64 test
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['6955'] # RO128 test
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['29866']         # Qualification + RO16 test

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
            raw_text = _strip_draw_prefix(word["text"])
            match = re.match(
                r"\s*(?:(\d{1,3})\s+)?([^,(]+?(?:\s+[^,(]+?)*)(?:\s*\(([^)]+)\))?,\s*(.+)",
                # word["text"],
                raw_text,
            )
            if not match:
                continue
            player_id_ext, raw_name, player_suffix_id, raw_club = match.groups()
            if player_id_ext:
                player_id_ext = player_id_ext.strip()
            full_name = raw_name.strip()
            club = raw_club.strip()
            # players.append(
            # Player(
            # full_name=full_name,
            # club=club,
            # short=make_short(full_name),
            # center=to_center(word),
            # player_id_ext=player_id_ext,
            # player_suffix_id=player_suffix_id.strip() if player_suffix_id else None,
            # )
            # )
            players.append(
                Player(
                    full_name=full_name,
                    club=club,
                    short=make_short(full_name),
                    center=to_center(word),
                    player_id_ext=player_id_ext,
                    # Drop any parenthetical suffix entirely
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
        nums = [n for n in nums if n != 0] # keep existing 0-filter
        if not nums:
            continue
        entries.append(ScoreEntry(scores=tuple(nums), center=to_center(word), x=x0))
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
        _score_text, text = _split_score_and_label(text)
        if not text:
            continue
        if any(token in text for token in ("Slutspel", "Höstpool", "program", "Kvalifikation", "Kvalificering")):
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
def cluster_players(players, max_gap=20.0) -> List[List[Player]]:
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
def build_first_round(players_groups, winners, scores) -> List[Match]:
    remaining_scores = list(scores)
    remaining_winners = list(winners)
    matches: List[Match] = []
    for group in players_groups:
        center = sum(p.center for p in group) / len(group)
        winner_entry = assign_nearest_winner(center, remaining_winners, tolerance=30.0)
        winner = None
        if winner_entry:
            try:
                winner = match_short_to_full(
                    winner_entry.short,
                    winner_entry.center,
                    group,
                    winner_entry.player_id_ext,
                )
            except ValueError:
                pass
        match_scores = assign_nearest_score(center, remaining_scores, tolerance=30.0)
        if len(group) == 1:
            match_scores = None
            if winner is None:
                winner = group[0]
        matches.append(Match(players=group, winner=winner, scores=match_scores, center=center))
    return matches
def build_next_round(
    previous_round: Sequence[Match],
    winners: Sequence[WinnerEntry],
    scores: List[ScoreEntry],
    players: Sequence[Player],
    winner_tolerance: float = 18.0,
    score_tolerance: float = 20.0,
) -> Tuple[List[Match], List[WinnerEntry], List[ScoreEntry]]:
    remaining_scores = list(scores)
    remaining_winners = list(winners)
    matches: List[Match] = []
    pair_count = (len(previous_round) + 1) // 2
    for idx in range(pair_count):
        first_match = previous_round[2 * idx]
        second_match = previous_round[2 * idx + 1] if 2 * idx + 1 < len(previous_round) else None
        first = first_match.winner or (first_match.players[0] if first_match.players else None)
        second = None
        if second_match:
            second = second_match.winner or (
                second_match.players[0] if second_match.players else None
            )
        if first is None or (second_match and second is None):
            continue
        participants = [first]
        if second:
            participants.append(second)
        centers = [first_match.center]
        if second_match:
            centers.append(second_match.center)
        center = sum(centers) / len(centers)
        winner: Optional[Player] = None
        winner_entry = assign_nearest_winner(center, remaining_winners, tolerance=winner_tolerance)
        if winner_entry is not None:
            try:
                winner = match_short_to_full(
                    winner_entry.short,
                    winner_entry.center,
                    participants,
                    winner_entry.player_id_ext,
                )
            except ValueError:
                winner = None
        if winner is None:
            for candidate in participants:
                if candidate == first_match.winner or (
                    second_match and candidate == second_match.winner
                ):
                    winner = candidate
                    break
        if winner is None and participants:
            winner = participants[0]
        match_scores = assign_nearest_score(center, remaining_scores, tolerance=score_tolerance)
        matches.append(Match(players=participants, winner=winner, scores=match_scores, center=center))
    return matches, remaining_winners, remaining_scores
def fill_missing_winners(previous_round: Sequence[Match], next_round: Sequence[Match]) -> None:
    advancing_players: List[Player] = [player for match in next_round for player in match.players]
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
def _cluster_columns(xs: List[float], max_gap: float = 35.0) -> List[Tuple[float, float]]:
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
def find_closest_score_band(score_bands: List[Tuple[float, float]], winner_band: Tuple[float, float]) -> Tuple[float, float]:
    candidates = [sb for sb in score_bands if sb[0] > winner_band[0]]
    if not candidates:
        return (winner_band[1], winner_band[1] + 50)  # fallback
    return min(candidates, key=lambda sb: sb[0] - winner_band[1])
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
    # Build identity sets from KO rounds
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
        # if both or neither match, leave as Unknown
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
def main() -> None:
    # 1) DB cursor for get_filtered_classes
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
        for tclass in classes:
            ext = tclass.tournament_class_id_ext
            if not ext:
                print(f"⏭️ Skipping class_id={tclass.tournament_class_id}: no tournament_class_id_ext")
                continue
            url = f"https://resultat.ondata.se/ViewClassPDF.php?classID={ext}&stage=5"
            print(f"===== {tclass.shortname or tclass.longname} [ext={ext}] =====")
            print(f"URL: {url}\n")
            try:
                # --- fetch and parse words/players as you already do ---
                words = fetch_words(url)
                players = extract_players(words)
                # Calculate participant tolerance
                if players:
                    deltas = [players[i+1].center - players[i].center for i in range(len(players)-1)]
                    if deltas:
                        median_delta = sorted(deltas)[len(deltas)//2]
                        participant_max_gap = median_delta
                    else:
                        participant_max_gap = 20.0
                else:
                    participant_max_gap = 20.0
                player_groups = cluster_players(players, participant_max_gap)
                # Precompute full-page winners/scores to detect late-stage columns.
                all_scores_page = extract_score_entries(words, (0, 10000))
                all_winners_page = extract_winner_entries(words, (0, 10000))
                score_bands_page = _cluster_columns([s.x for s in all_scores_page])
                winner_bands_page = _cluster_columns([w.x for w in all_winners_page])
                # Dynamically select late bands: last 3 for QF, SF, Final
                late_winner_bands: List[Tuple[float, float]] = winner_bands_page[-3:] if len(winner_bands_page) >= 3 else winner_bands_page
                late_winner_entries = [w for w in all_winners_page if any(start <= w.x <= end for start, end in late_winner_bands)]
                late_winner_entries.sort(key=lambda e: e.center)
                late_score_bands: List[Tuple[float, float]] = score_bands_page[-3:] if len(score_bands_page) >= 3 else score_bands_page
                late_score_entries = [s for s in all_scores_page if any(start <= s.x <= end for start, end in late_score_bands)]
                late_score_entries.sort(key=lambda e: e.center)
                # --- Build KO rounds, now with optional RO64 probe ---
                r64_matches: Optional[List[Match]] = None
                r32_matches: Optional[List[Match]] = None
                # Probe RO64 (far-left column)
                r64_winners = extract_winner_entries(words, R64_WINNERS_X)
                if len(r64_winners) >= 32:
                    r64_scores = extract_score_entries(words, R64_SCORES_X)
                    r64_matches = build_first_round(player_groups, r64_winners, r64_scores)
                    # Build RO32 from RO64
                    r32_winners = extract_winner_entries(words, (260, 360))
                    r32_scores = extract_score_entries(words, (260, 420))
                    r32_matches, _, _ = build_next_round(
                        r64_matches, r32_winners, r32_scores, players,
                        winner_tolerance=18.0, score_tolerance=20.0
                    )
                    # Build R16 from RO32
                    r16_winners = extract_winner_entries(words, (360, 420))
                    r16_scores = extract_score_entries(words, (400, 460))
                    r16_matches, _, _ = build_next_round(
                        r32_matches, r16_winners, r16_scores, players,
                        winner_tolerance=18.0, score_tolerance=20.0
                    )
                else:
                    # No RO64: determine first round range
                    if tclass.ko_tree_size and tclass.ko_tree_size <32:
                        first_winner_band = winner_bands_page[0] if winner_bands_page else (170, 210)
                        first_score_band = find_closest_score_band(score_bands_page, first_winner_band)
                    else:
                        first_winner_band = (220, 240)
                        first_score_band = (270, 320)
                    first_winners = extract_winner_entries(words, first_winner_band)
                    first_scores = extract_score_entries(words, first_score_band)
                    r16_matches = build_first_round(player_groups, first_winners, first_scores)
                    if len(r16_matches) >8:
                        # In this branch, r16_matches initially *are* the RO32 pairs
                        r32_matches = r16_matches
                        r16_adv_winners = extract_winner_entries(words, (260, 360))
                        r16_adv_scores = extract_score_entries(words, (260, 420))
                        r16_matches, _, _ = build_next_round(
                            r32_matches, r16_adv_winners, r16_adv_scores, players
                        )
                # Fill upstream winners for consistency
                if r32_matches:
                    fill_missing_winners(r32_matches, r16_matches)
                if r64_matches:
                    fill_missing_winners(r64_matches, r32_matches)
                # Qualification now that we know R16 participants
                qualification = extract_qualification_matches(words)
                # Collect all rounds
                all_rounds: List[List[Match]] = []
                if r64_matches:
                    all_rounds.append(r64_matches)
                if r32_matches:
                    all_rounds.append(r32_matches)
                all_rounds.append(r16_matches)
                # Late rounds with general loop
                remaining_winners = list(late_winner_entries)
                remaining_scores = list(late_score_entries)
                late_round_index = 0
                current = r16_matches
                while len(current) > 1:
                    next_round, remaining_winners, remaining_scores = build_next_round(
                        current,
                        remaining_winners,
                        remaining_scores,
                        players,
                        winner_tolerance=25 + 5 * late_round_index,
                        score_tolerance=25 + 5 * late_round_index,
                    )
                    fill_missing_winners(current, next_round)
                    all_rounds.append(next_round)
                    current = next_round
                    late_round_index +=1
                # If ended with len==1, do not append again
                # Qualification assign using first KO round
                if qualification and all_rounds:
                    assign_qualification_winners_by_presence(qualification, all_rounds[0])
                    label_round("Qualification", qualification)
                    print()
                # --- Print rounds ---
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
                # --- Self-check vs stored ko_tree_size ---
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
if __name__ == "__main__":
    main()