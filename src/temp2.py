# GPT think

import io
import re
import requests
import pdfplumber
from typing import List, Dict, Optional, Tuple

PDF_URL = "https://resultat.ondata.se/ViewClassPDF.php?classID=30021&stage=5"

# Calibrated for OnData PDFs: update if needed for other layouts
ROUND_THRESHOLDS = [320, 410, 520]  # x0 boundaries -> round 1..N = [(..320), [320..410), [410..520), [>=520]]

ROUND_LABELS = {
    1: "RO16",
    2: "RO8/QF",
    3: "RO4/SF",
    4: "RO2/Final",
}

# -------- geometry helpers --------
def y_center(item: dict) -> float:
    return (item["top"] + item["bottom"]) / 2.0

def x_round(x_value: float) -> int:
    for idx, boundary in enumerate(ROUND_THRESHOLDS, start=1):
        if x_value < boundary:
            return idx
    return len(ROUND_THRESHOLDS) + 1

def dist(a: float, b: float) -> float:
    return abs(a - b)

# -------- parsing helpers --------
def short_name(full_name: str) -> str:
    """
    "Zhu Arvid" -> "Zhu A"
    "Tallborn Åsberg Eric" -> "Tallborn Å"
    Robust enough for Swedish characters.
    """
    parts = full_name.split()
    if not parts:
        return full_name.strip()
    if len(parts) == 1:
        return parts[0]
    return f"{parts[0]} {parts[1][0]}"

def load_pdf_words(url: str):
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
        page = pdf.pages[0]
        words = page.extract_words(keep_blank_chars=True, use_text_flow=True)  # use_text_flow helps order a bit
    # Normalize structure
    norm = []
    for w in words:
        norm.append({
            "text": w["text"].strip(),
            "x0": float(w["x0"]),
            "x1": float(w["x1"]),
            "top": float(w["top"]),
            "bottom": float(w["bottom"]),
            "y": y_center(w),
        })
    return norm

def extract_entrants(words: List[dict], left_bound: float = 260.0) -> List[dict]:
    """
    Entrants lines look like: 'Surname Given, Club Name'
    Left column is typically x0 < ~250-260.
    """
    entrants = []
    for w in words:
        if "," in w["text"] and w["x0"] < left_bound:
            # Split only at first comma to preserve commas inside club names (very rare, but safe)
            raw_name, raw_club = w["text"].split(",", 1)
            name = raw_name.strip()
            club = raw_club.strip()
            entrants.append({
                "full": name,
                "club": club,
                "short": short_name(name),
                "y": w["y"],
            })
    entrants.sort(key=lambda p: p["y"])
    return entrants

SCORE_RE = re.compile(r"^\s*([0-9,\-\s]+)\s*$")

def extract_scores(words: List[dict]) -> List[dict]:
    """
    Score tokens appear as comma-separated integers, sometimes with negatives.
    """
    out = []
    for w in words:
        m = SCORE_RE.match(w["text"])
        if not m:
            continue
        tokens_str = m.group(1)
        # Be conservative: must contain at least one digit
        if not any(ch.isdigit() for ch in tokens_str):
            continue
        try:
            tokens = [int(t.strip()) for t in tokens_str.split(",") if t.strip()]
        except ValueError:
            continue
        out.append({
            "tokens": tokens,
            "y": w["y"],
            "x": w["x0"],
            "round": x_round(w["x0"]),
        })
    out.sort(key=lambda s: (s["round"], s["y"]))
    return out

def looks_like_winner_hint(text: str) -> bool:
    """
    Winner hint is typically a short form: 'Ohlsén V', 'Zhu A', 'Jörgensen T'
    Heuristics: no comma, no digits, <= 15 chars, at least one space.
    """
    if len(text) > 15 or "," in text:
        return False
    if any(ch.isdigit() for ch in text):
        return False
    if " " not in text:
        return False
    return True

def extract_winner_hints(words: List[dict], min_x: float = 300.0) -> List[dict]:
    hints = []
    for w in words:
        t = w["text"]
        if w["x0"] >= min_x and looks_like_winner_hint(t):
            hints.append({
                "hint": t.strip(),
                "short": t.strip(),  # already short format
                "y": w["y"],
                "x": w["x0"],
                "round": x_round(w["x0"]),
            })
    # sort by round,y so nearest lookup is stable
    hints.sort(key=lambda h: (h["round"], h["y"]))
    return hints

# -------- bracket assembly --------
def nearest(items: List[dict], target_y: float, k: int = 1) -> List[dict]:
    return sorted(items, key=lambda it: dist(it["y"], target_y))[:k]

def associate_hint_to_score(score: dict, hints: List[dict], max_dy: float = 12.0) -> Optional[str]:
    """
    Attach the nearest winner hint to a score row if close enough vertically.
    Returns the short-tag string (e.g., 'Zhu A') or None.
    """
    cand = nearest(hints, score["y"], k=1)
    if cand and dist(cand[0]["y"], score["y"]) <= max_dy:
        return cand[0]["short"]
    return None

def build_round1_matches(entrants: List[dict], scores_r1: List[dict], hints: List[dict]) -> Tuple[List[dict], List[dict]]:
    """
    Returns (matches_r1, winners_r1)
    match: {left, right, tokens, winner_full, winner_club, y}
    left/right: entrant dict
    """
    used_ids = set()
    matches = []
    winners = []

    # Index entrants by short for winner matching
    short_to_idx = {}
    for i, p in enumerate(entrants):
        short_to_idx.setdefault(p["short"], []).append(i)

    for s in scores_r1:
        # Pick two closest entrants around this score y
        pair = nearest(entrants, s["y"], k=2)
        if len(pair) < 2:
            continue
        left, right = sorted(pair, key=lambda p: p["y"])
        # record their indices to avoid counting BYE later
        left_idx = entrants.index(left)
        right_idx = entrants.index(right)
        used_ids.add(left_idx)
        used_ids.add(right_idx)

        # winner by short-hint near the score
        hint = associate_hint_to_score(s, hints)
        winner = None
        if hint:
            # disambiguate to these 2 players
            if hint == left["short"]:
                winner = left
            elif hint == right["short"]:
                winner = right
            else:
                # Try fuzzy: if hint startswith surname and first letter matches
                def matches(player):
                    sn = player["short"]
                    return sn.split()[0] == hint.split()[0] and sn.split()[1][0] == hint.split()[1][0]
                if matches(left):
                    winner = left
                elif matches(right):
                    winner = right

        match = {
            "round": 1,
            "left": left,
            "right": right,
            "tokens": s["tokens"],
            "winner": winner,
            "y": s["y"],
        }
        matches.append(match)

        if winner:
            winners.append({
                "full": winner["full"],
                "club": winner["club"],
                "short": winner["short"],
                "y": s["y"],  # carry forward at the score's y for next-round proximity
            })

    # BYEs = entrants not used in any R1 score
    for i, p in enumerate(entrants):
        if i not in used_ids:
            matches.append({
                "round": 1,
                "left": p,
                "right": None,
                "tokens": None,
                "winner": p,   # advances automatically
                "y": p["y"],
                "bye": True,
            })
            winners.append({
                "full": p["full"],
                "club": p["club"],
                "short": p["short"],
                "y": p["y"],
            })

    # Sort matches by y for clean printing
    matches.sort(key=lambda m: m["y"])
    return matches, winners

def build_later_round(scores: List[dict], prev_winners: List[dict], hints: List[dict], round_no: int) -> Tuple[List[dict], List[dict]]:
    """
    Generic for R2..R4
    Returns (matches_round, winners_round)
    """
    # We'll greedily consume the two nearest available winners for each score row
    # but to avoid reusing, we'll mark consumed indices.
    matches = []
    winners = []

    available = prev_winners[:]  # list of dicts with 'y' positions
    taken = set()

    for s in [sc for sc in scores if sc["round"] == round_no]:
        # pick two nearest unused winners
        candidates = sorted(
            [(i, pw, dist(pw["y"], s["y"])) for i, pw in enumerate(available) if i not in taken],
            key=lambda t: t[2]
        )
        if len(candidates) < 2:
            continue
        (i1, p1, _), (i2, p2, _) = candidates[0], candidates[1]
        taken.add(i1)
        taken.add(i2)
        left, right = sorted([p1, p2], key=lambda p: p["y"])

        # winner by hint
        hint = associate_hint_to_score(s, hints)
        winner = None
        if hint:
            if hint == left["short"]:
                winner = left
            elif hint == right["short"]:
                winner = right
            else:
                # fuzzy fallback
                def matches(player):
                    sn = player["short"]
                    return sn.split()[0] == hint.split()[0] and sn.split()[1][0] == hint.split()[1][0]
                if matches(left):
                    winner = left
                elif matches(right):
                    winner = right

        matches.append({
            "round": round_no,
            "left": left,
            "right": right,
            "tokens": s["tokens"],
            "winner": winner,
            "y": s["y"],
        })

        if winner:
            winners.append({
                "full": winner["full"],
                "club": winner["club"],
                "short": winner["short"],
                "y": s["y"],
            })

    matches.sort(key=lambda m: m["y"])
    return matches, winners

# -------- pretty printing --------
def label_player(p: dict) -> str:
    return f"{p['full']}, {p['club']}"

def print_round(label: str, matches: List[dict]):
    print(f"{label}:")
    for m in matches:
        if m.get("bye"):
            print(f"{label_player(m['left'])}\t\t-> BYE (meaning moving on automatically, identified by no opponent on corresponding match row/line)")
            continue
        left = label_player(m["left"])
        right = label_player(m["right"])
        tok = f" -> Game tokens: ({', '.join(str(t) for t in m['tokens'])})" if m["tokens"] is not None else ""
        if m["winner"]:
            w = f"{m['winner']['full']}, {m['winner']['club']}"
            print(f"{left} vs {right}\t-> Winner: {w}\t{tok}")
        else:
            print(f"{left} vs {right}\t-> Winner: Unknown\t{tok}")
    print()

# -------- main driver --------
def main():
    words = load_pdf_words(PDF_URL)
    entrants = extract_entrants(words)
    scores = extract_scores(words)
    hints = extract_winner_hints(words)

    # Round 1
    r1_scores = [s for s in scores if s["round"] == 1]
    r1_matches, r1_winners = build_round1_matches(entrants, r1_scores, hints)

    # Round 2..N
    all_round_matches = {1: r1_matches}
    prev_winners = r1_winners
    max_round = max([s["round"] for s in scores] + [1])

    for r in range(2, max_round + 1):
        r_matches, r_winners = build_later_round(scores, prev_winners, hints, round_no=r)
        all_round_matches[r] = r_matches
        prev_winners = r_winners

    # Print in desired order/labels
    for r in sorted(all_round_matches.keys()):
        label = ROUND_LABELS.get(r, f"Round {r}")
        print_round(label, all_round_matches[r])

if __name__ == "__main__":
    main()
